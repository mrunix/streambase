/*
 * =====================================================================================
 *
 *       Filename:  OceanbaseDb.cc
 *
 *        Version:  1.0
 *        Created:  04/13/2011 09:55:53 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#include "sb_db.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_crc64.h"
#include "common/serialization.h"
#include "common/ob_tsi_factory.h"
#include <string>
#include "db_record_set.h"
#include "db_table_info.h"
#include "db_record.h"
#include "ob_api_util.h"

const int64_t kMaxLogSize = 256 * 1024 * 1024;  /* log file size 256M */

namespace sb {
using namespace common;
namespace api {
OceanbaseDb::OceanbaseDb(const char* ip, unsigned short port, int64_t timeout) {
  inited_ = false;
  name_server_.set_ipv4_addr(ip, port);
  timeout_ = timeout;
  TBSYS_LOG(INFO, "timeout is %d", timeout_);
}

OceanbaseDb::OceanbaseDb(int32_t ip, unsigned short port) {
  inited_ = false;
  name_server_.set_ipv4_addr(ip, port);
}

OceanbaseDb::~OceanbaseDb() {
  transport_.stop();
  transport_.wait();
  free_tablet_cache();
}

int OceanbaseDb::get(std::string& table, std::vector<std::string>& columns, const DbRowKey& rowkey, DbRecordSet& rs) {
  int ret = OB_SUCCESS;
  common::ObServer server;

  if (!rs.inited()) {
    TBSYS_LOG(INFO, "DbRecordSet Not init ,please init it first");
    ret = common::OB_ERROR;
  }

  int retries = kTabletDupNr;

  while (retries--) {
    ret = get_tablet_location(table, rowkey, server);
    if (ret != common::OB_SUCCESS) {
      TBSYS_LOG(ERROR, "No Mergeserver available");
      break;
    }

    ret = do_server_get(server, rowkey, rs.get_scanner(), rs.get_buffer(), table, columns);
    if (ret == OB_SUCCESS) {
      break;
    } else {
      char err_msg[128];

      server.to_string(err_msg, 128);
      TBSYS_LOG(WARN, "failed when get data from %s", err_msg);
      if (ret == OB_ERROR_OUT_OF_RANGE) {
        TBSYS_LOG(ERROR, "rowkey out of range");
        break;
      }

      mark_ms_failure(server, table, rowkey);
    }
  }

  return ret;
}

int OceanbaseDb::init() {
  streamer_.setPacketFactory(&packet_factory_);
  transport_.start();
  client_.initialize(&transport_, &streamer_);

  inited_ = true;
  return OB_SUCCESS;
}

int OceanbaseDb::global_init(const char* log_dir, const char* level) {
  ob_init_memory_pool();
  //      TBSYS_LOGGER.setFileName(log_dir);
  TBSYS_LOGGER.setMaxFileSize(kMaxLogSize);
  TBSYS_LOGGER.setLogLevel(level);
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  return OB_SUCCESS;
}

int OceanbaseDb::search_tablet_cache(std::string& table, const DbRowKey& rowkey, TabletInfo& loc) {
  tbsys::CThreadGuard guard(&cache_lock_);
  CacheSet::iterator itr = cache_set_.find(table);
  if (itr == cache_set_.end())
    return OB_ERROR;

  CacheRow::iterator row_itr = itr->second.lower_bound(rowkey);
  if (row_itr == itr->second.end())
    return OB_ERROR;

  loc = (row_itr->second);
  return OB_SUCCESS;
}

void OceanbaseDb::try_mark_server_fail(TabletInfo& tablet_info, ObServer& server, bool& do_erase_tablet) {
  int i;
  for (i = 0; i < kTabletDupNr; i++) {
    if (tablet_info.slice_[i].ip_v4 == server.get_ipv4() &&
        tablet_info.slice_[i].ms_port == server.get_port()) {
      tablet_info.slice_[i].server_avail = false;
      break;
    }
  }

  //check wether tablet is available, if not delete it from set
  for (i = 0; i < kTabletDupNr ; i++) {
    if (tablet_info.slice_[i].server_avail == true) {
      break;
    }
  }

  if (i == kTabletDupNr) {
    do_erase_tablet = true;
  } else {
    do_erase_tablet = false;
  }
}

void OceanbaseDb::mark_ms_failure(ObServer& server, std::string& table, const ObString& rowkey) {
  int ret = OB_SUCCESS;
  CacheRow::iterator row_itr;

  tbsys::CThreadGuard guard(&cache_lock_);

  CacheSet::iterator itr = cache_set_.find(table);
  if (itr == cache_set_.end())
    ret = OB_ERROR;

  if (ret == OB_SUCCESS) {
    row_itr = itr->second.lower_bound(rowkey);
    if (row_itr == itr->second.end())
      ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    TabletInfo& tablet_info = row_itr->second;
    bool do_erase_tablet = false;

    try_mark_server_fail(tablet_info, server, do_erase_tablet);
    if (do_erase_tablet)
      itr->second.erase(row_itr);
  } else {
    TBSYS_LOG(WARN, "tablet updated, no such rowkey");
  }
}

int OceanbaseDb::get_tablet_location(std::string& table, const DbRowKey& rowkey, common::ObServer& server) {
  int ret = OB_SUCCESS;
  TabletInfo tablet_info;
  TabletSliceLocation loc;

  ret = search_tablet_cache(table, rowkey, tablet_info);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(INFO, "Table %s not find in tablet cache, do root server get", table.c_str());

    ret = get_ms_location(rowkey, table);
    if (ret == OB_SUCCESS)
      ret = search_tablet_cache(table, rowkey, tablet_info);
    else {
      TBSYS_LOG(ERROR, "get_ms_location faild , retcode:[%d]", ret);
    }
  }

  if (ret == common::OB_SUCCESS) {
    ret = tablet_info.get_one_avail_slice(loc, rowkey);
    if (ret == common::OB_SUCCESS) {
      server.set_ipv4_addr(loc.ip_v4, loc.ms_port);
    } else {
      TBSYS_LOG(ERROR, "No available Merger Server");
    }
  }

  return ret;
}

int OceanbaseDb::get_ms_location(const DbRowKey& row_key, std::string& table_name) {
  int ret = OB_SUCCESS;
  ObScanner scanner;
  std::vector<std::string> columns;
  columns.push_back("*");
  DbRecordSet ds(k2M);

  if (!inited_) {
    TBSYS_LOG(ERROR, "OceanbaseDb is not Inited, plesase Initialize it first");
    ret = OB_ERROR;
  } else if ((ret = ds.init()) != common::OB_SUCCESS) {
    TBSYS_LOG(INFO, "DbRecordSet Init error");
  }

  if (ret == OB_SUCCESS) {
    int retires = kTabletDupNr;

    while (retires--) {
      ret = do_server_get(name_server_, row_key, ds.get_scanner(),
                          ds.get_buffer(), table_name, columns);
      if (ret == OB_SUCCESS)
        break;
    }

    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "do server get failed, ret=[%d]", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    DbRecordSet::Iterator itr = ds.begin();
    while (itr != ds.end()) {
      DbRecord* recp;
      TabletInfo tablet_info;

      itr.get_record(&recp);
      if (recp == NULL) {
        TBSYS_LOG(WARN, "NULL record skip line");
        itr++;
        continue;
      }

      ret = tablet_info.parse_from_record(recp);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "pase from record failed");
        break;
      }

      insert_tablet_cache(table_name, tablet_info.get_end_key(), tablet_info);
      itr++;
    }
  }

  return ret;
}


void OceanbaseDb::insert_tablet_cache(std::string& table, const DbRowKey& rowkey, TabletInfo& tablet) {
  tbsys::CThreadGuard guard(&cache_lock_);

  CacheSet::iterator set_itr = cache_set_.find(table);
  if (set_itr == cache_set_.end()) {
    CacheRow row;

    row.insert(CacheRow::value_type(rowkey, tablet));
    cache_set_.insert(CacheSet::value_type(table, row));
  } else {
    CacheRow::iterator row_itr = set_itr->second.find(rowkey);
    if (row_itr != set_itr->second.end()) {
      set_itr->second.erase(row_itr);
      TBSYS_LOG(DEBUG, "deleting cache table is %s", table.c_str());
    }

    TBSYS_LOG(DEBUG, "insert cache table is %s", table.c_str());
    set_itr->second.insert(std::make_pair(rowkey, tablet));
  }
}		/* -----  end of method OceanbaseDb::insert_tablet_cache  ----- */


void OceanbaseDb::free_tablet_cache() {
  CacheSet::iterator set_itr = cache_set_.begin();
  while (set_itr != cache_set_.end()) {
    CacheRow::iterator row_itr = set_itr->second.begin();
    while (row_itr != set_itr->second.end()) {
      row_itr++;
    }
    set_itr++;
  }
}		/* -----  end of method OceanbaseDb::free  ----- */

int OceanbaseDb::do_server_get(common::ObServer& server, const DbRowKey& row_key, ObScanner& scanner,
                               ObDataBuffer& data_buff, std::string& table_name,
                               std::vector<std::string>& columns) {
  int ret;
  ObCellInfo cell;
  ObGetParam* get_param = GET_TSI(ObGetParam);
  if (get_param == NULL) {
    TBSYS_LOG(ERROR, "can't allocate memory from TSI");
    return OB_ERROR;
  }
  get_param->reset();

  ObBorderFlag border;
  border.set_inclusive_start();
  border.set_inclusive_end();
  border.set_max_value();
  border.set_min_value();

  ObVersionRange version_range;

  version_range.start_version_ = 0;
  version_range.end_version_ = 0;

  version_range.border_flag_ = border;
  get_param->set_version_range(version_range);

  cell.row_key_ = row_key;
  cell.table_name_.assign_ptr(const_cast<char*>(table_name.c_str()), table_name.length());

  std::vector<std::string>::iterator itr = columns.begin();
  while (itr != columns.end()) {
    cell.column_name_.assign_ptr(const_cast<char*>(itr->c_str()), itr->length());
    int ret = get_param->add_cell(cell);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "add cell to get param failed:ret[%d]", ret);
      return ret;
    }
    itr++;
  }

  data_buff.get_position() = 0;
  ret = get_param->serialize(data_buff.get_data(), data_buff.get_capacity(),
                             data_buff.get_position());
  if (OB_SUCCESS == ret) {
    ret = client_.send_request(server, OB_GET_REQUEST, 1,
                               timeout_, data_buff);
  }

  int64_t pos = 0;

  char ip_buf[25];
  server.ip_to_string(ip_buf, 25);
  TBSYS_LOG(DEBUG, "Merger server ip is %s, port is %d", ip_buf, server.get_port());

  ObResultCode result_code;
  if (OB_SUCCESS == ret)
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);

  if (OB_SUCCESS != ret)
    TBSYS_LOG(ERROR, "do_server_get deserialize result failed:pos[%ld], ret[%d]", pos, ret);
  else
    ret = result_code.result_code_;

  if (OB_SUCCESS == ret) {
    scanner.clear();
    ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
      TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%lld], ret[%d]", pos, ret);
  }

  if (ret != OB_SUCCESS)
    TBSYS_LOG(ERROR, "do server get failed:%d", ret);
  return ret;
}


int OceanbaseDb::fetch_schema(common::ObSchemaManager& schema_manager) {
  int ret;
  char* buff = new char[k2M];
  if (buff == NULL) {
    TBSYS_LOG(ERROR, "Fetch schema faild, due to memory lack");
    return common::OB_MEM_OVERFLOW;
  }

  ObDataBuffer data_buff;
  data_buff.set_data(buff, k2M);
  data_buff.get_position() = 0;

  serialization::encode_vi64(data_buff.get_data(),
                             data_buff.get_capacity(), data_buff.get_position(), 1);

  ret = client_.send_request(name_server_, OB_FETCH_SCHEMA, 1,
                             100000, data_buff);

  int64_t pos = 0;
  ObResultCode result_code;
  if (OB_SUCCESS == ret)
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);

  if (OB_SUCCESS != ret)
    TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
  else
    ret = result_code.result_code_;

  if (OB_SUCCESS == ret) {
    ret = schema_manager.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "deserialize schema from buff failed:"
                "version[%ld], pos[%ld], ret[%d]", 1, pos, ret);
    } else {
      TBSYS_LOG(DEBUG, "fetch schema succ:version[%l]", schema_manager.get_version());
    }
  }

  delete [] buff;

  return ret;
}
}
}
