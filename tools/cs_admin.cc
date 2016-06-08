/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         cs_admin.cc is for what ...
 *
 *  Version: $Id: cs_admin.cc 2010年12月03日 13时48分14秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "cs_admin.h"
#include <vector>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_string.h"
#include "common/utility.h"
#include "chunkserver/ob_tablet_image.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_trailer.h"
#include "ob_server_stats.h"
#include "ob_cluster_stats.h"
#include "common_func.h"


using namespace std;
using namespace sb;
using namespace sb::common;
using namespace sb::chunkserver;
using namespace sb::sstable;
using namespace sb::tools;

//--------------------------------------------------------------------
// class GFactory
//--------------------------------------------------------------------
GFactory GFactory::instance_;

GFactory::GFactory() {
}

GFactory::~GFactory() {
}

void GFactory::init_cmd_map() {
  cmd_map_["dump_tablet_image"] = CMD_DUMP_TABLET;
  cmd_map_["dump_server_stats"] = CMD_DUMP_SERVER_STATS;
  cmd_map_["dump_cluster_stats"] = CMD_DUMP_CLUSTER_STATS;
  cmd_map_["manual_merge"] = CMD_MANUAL_MERGE;
  cmd_map_["manual_drop_tablet"] = CMD_MANUAL_DROP_TABLET;
  cmd_map_["manual_gc"] = CMD_MANUAL_GC;
  cmd_map_["get_tablet_info"] = CMD_RS_GET_TABLET_INFO;
  cmd_map_["scan_root_table"] = CMD_SCAN_ROOT_TABLE;
}

int GFactory::initialize(const ObServer& remote_server) {
  common::ob_init_memory_pool();
  common::ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  init_cmd_map();

  int ret = client_.initialize();
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "initialize client object failed, ret:%d", ret);
    return ret;
  }

  ret = rpc_stub_.initialize(remote_server, &client_.get_client_manager());
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "initialize rpc stub failed, ret:%d", ret);
  }
  return ret;
}

int GFactory::start() {
  client_.start();
  return OB_SUCCESS;
}

int GFactory::stop() {
  client_.stop();
  return OB_SUCCESS;
}

int GFactory::wait() {
  client_.wait();
  return OB_SUCCESS;
}


int parse_command(const char* cmdlist, vector<string>& param) {
  int cmd = CMD_MIN;
  const int CMD_MAX_LEN = 1024;
  char key_buf[CMD_MAX_LEN];
  snprintf(key_buf, CMD_MAX_LEN, "%s", cmdlist);
  char* key = key_buf;
  char* token = NULL;
  while (*key == ' ') key++; // 去掉前空格
  token = key + strlen(key);
  while (*(token - 1) == ' ' || *(token - 1) == '\n' || *(token - 1) == '\r') token --;
  *token = '\0';
  if (key[0] == '\0') {
    return cmd;
  }
  token = strchr(key, ' ');
  if (token != NULL) {
    *token = '\0';
  }
  const map<string, int>& cmd_map = GFactory::get_instance().get_cmd_map();
  map<string, int>::const_iterator it = cmd_map.find(key);
  if (it == cmd_map.end()) {
    return CMD_MIN;
  } else {
    cmd = it->second;
  }

  if (token != NULL) {
    token ++;
    key = token;
  } else {
    key = NULL;
  }
  //分解
  param.clear();
  while ((token = strsep(&key, " ")) != NULL) {
    if (token[0] == '\0') continue;
    param.push_back(token);
  }
  return cmd;
}

int get_tablet_info(const vector<string>& param) {
  int ret = 0;
  if (param.size() <= 2) {
    fprintf(stderr, "get_tablet_info: input param error\n");
    fprintf(stderr, "get_tablet_info table_id table_name range_str\n");
    return OB_ERROR;
  }

  ObRange range;

  int64_t table_id = strtoll(param[0].c_str(), NULL, 10);
  const char* table_name = param[1].c_str();
  ret  = parse_range_str(param[2].c_str(), 1, range);
  if (OB_SUCCESS != ret) return ret;

  int safe_copy_num = 3;
  ObTabletLocation location[safe_copy_num];

  ret = GFactory::get_instance().get_rpc_stub().get_tablet_info(
          table_id, table_name, range, location, safe_copy_num);
  fprintf(stderr, "return size = %d, ret= %d\n", safe_copy_num, ret);
  if (OB_SUCCESS != ret) return ret;

  char host_string[256];
  for (int i = 0; i < safe_copy_num; ++i) {
    location[i].chunkserver_.to_string(host_string, 256);
    fprintf(stderr, "location (%d), version = %ld , host = %s \n",
            i, location[i].tablet_version_, host_string);
  }

  return ret;
}

void dump_multi_version_tablet_image(ObMultiVersionTabletImage& image, bool load_sstable) {
  ObTablet* tablet  = NULL;
  uint64_t crcsum = 0;
  int tablet_index = 0;
  char range_bufstr[OB_MAX_FILE_NAME_LENGTH];

  int ret = image.begin_scan_tablets();
  while (OB_SUCCESS == ret) {
    ret = image.get_next_tablet(tablet);
    if (OB_SUCCESS == ret) {
      tablet->get_range().to_string(range_bufstr, OB_MAX_FILE_NAME_LENGTH);
      tablet->get_checksum(crcsum);
      fprintf(stderr, "tablet(%d) : %s \n", tablet_index, range_bufstr);
      fprintf(stderr, "\t info: data version = %ld , disk no = %d, "
              "row count = %ld, occupy size = %ld , crc sum = %ld , merged= %d\n",
              tablet->get_data_version(), tablet->get_disk_no(), tablet->get_row_count(),
              tablet->get_occupy_size(), crcsum, tablet->is_merged());
      // dump sstable content
      const ObArrayHelper<ObSSTableId>& sstable_id_array = tablet->get_sstable_id_list();
      const ObArrayHelper<ObSSTableReader*>& sstable_reader_array = tablet->get_sstable_reader_list();

      if (!load_sstable) {
        for (int64_t idx = 0; idx < sstable_id_array.get_array_index(); ++idx) {
          ObSSTableId id = *sstable_id_array.at(idx);
          fprintf(stderr, "\t sstable[%ld]:%ld\n", idx, id.sstable_file_id_);
        }
      } else {
        for (int64_t idx = 0; idx < sstable_reader_array.get_array_index(); ++idx) {
          ObSSTableReader* reader = *sstable_reader_array.at(idx);
          fprintf(stderr, "\t sstable[%ld]:%ld, size = %d, block count = %ld , row count = %ld\n",
                  idx, sstable_id_array.at(idx)->sstable_file_id_,
                  reader->get_trailer().get_size(),
                  reader->get_trailer().get_block_count(),
                  reader->get_trailer().get_row_count());
        }
      }

      ++tablet_index;
    }
    if (NULL != tablet) image.release_tablet(tablet);
  }
  image.end_scan_tablets();
}

int dump_tablet_image(const vector<string>& param) {
  int ret = 0;
  if (param.size() < 1) {
    fprintf(stderr, "dump_tablet_image: input param error\n");
    fprintf(stderr, "dump_tablet_image disk_no \n");
    fprintf(stderr, "dump_tablet_image disk_no_1, disk_no_2, ... \n");
    fprintf(stderr, "dump_tablet_image disk_no_min~disk_no_max \n");
    return OB_ERROR;
  }

  const string& disk_list = param[0];
  int32_t disk_no_size = 64;
  int32_t disk_no_array[disk_no_size];
  ret = parse_number_range(disk_list.c_str(), disk_no_array, disk_no_size);
  if (ret) {
    fprintf(stderr, "parse disk_list(%s) parameter failure\n", disk_list.c_str());
    return ret;
  }

  fprintf(stderr, "dump disk count=%d\n", disk_no_size);

  int64_t pos = 0;
  FileInfoCache cache;
  cache.init(10);
  ObMultiVersionTabletImage image_obj(cache);
  for (int i = 0; i < disk_no_size; ++i) {
    for (int idx = 0; idx < 2; ++idx) {
      ObString image_buf;
      ret = GFactory::get_instance().get_rpc_stub().cs_dump_tablet_image(idx, disk_no_array[i], image_buf);
      if (OB_SUCCESS != ret) {
        //fprintf(stderr, "disk = %d has no tablets \n ", disk_no_array[i]);
      } else {
        pos = 0;
        ret = image_obj.deserialize(disk_no_array[i], image_buf.ptr(), image_buf.length(), pos);
      }
    }
  }

  dump_multi_version_tablet_image(image_obj, false);
  return ret;
}

typedef std::map<ObString, map<string, int64_t> > SimpleRootTable;

int store_root_table(ObScanner& scanner, SimpleRootTable& root_table, bool& end_of_table, ObString& end_row_key) {
  int ret = OB_SUCCESS;

  ObScannerIterator iter;
  int row_count = 0;
  int column_index = 0;
  bool is_row_changed = false;

  char column_strbuf[OB_MAX_ROW_KEY_LENGTH];
  map<string, int64_t> root_item;
  int64_t column_value = 0;
  ObString last_row_key;

  end_of_table = false;
  root_table.clear();

  for (iter = scanner.begin(); iter != scanner.end(); iter++) {
    ObCellInfo* cell_info = NULL;
    iter.get_cell(&cell_info, &is_row_changed);
    if (NULL == cell_info) {
      TBSYS_LOG(ERROR, "get null cell_info");
      ret = OB_ERROR;
      break;
    }

    if (is_row_changed) {
      column_index = 0;

      //hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
      if (row_count >= 1) {
        ///strncpy(column_strbuf, cell_info->row_key_.ptr(), cell_info->row_key_.length());
        //string row_key_str(column_strbuf);
        //fprintf(stderr, "column_strbuf=%s\n", column_strbuf);
        //hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
        root_table.insert(make_pair(last_row_key, root_item));
        root_item.clear();
      }

      ++row_count;
    } else {
      last_row_key = cell_info->row_key_;
      ++column_index;
    }



    if (cell_info->value_.get_type() == ObIntType) {
      cell_info->value_.get_int(column_value);
    }

    memset(column_strbuf, 0, OB_MAX_ROW_KEY_LENGTH);
    strncpy(column_strbuf, cell_info->column_name_.ptr(), cell_info->column_name_.length());
    string column_name(column_strbuf);
    root_item.insert(make_pair(column_name, column_value));
    //fprintf(stderr, "column_name=%s, column_value=%ld\n", column_name.c_str(), column_value);


    /*
    TBSYS_LOG(DEBUG, "---------------------%d----------------------\n", row_count);
    hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
    TBSYS_LOG(DEBUG,"table_id:%lu, table_name:%.*s, column_id:%ld, column_name:%.*s, is_row_changed:%d\n",
        cell_info->table_id_, cell_info->table_name_.length(), cell_info->table_name_.ptr(),
        cell_info->column_id_, cell_info->column_name_.length(), cell_info->column_name_.ptr(), is_row_changed);
    cell_info->value_.dump();
    TBSYS_LOG(DEBUG,"---------------------%d----------------------, check ret=%d\n", row_count, ret);
    */

    if (OB_SUCCESS != ret) break;
  }

  if (OB_SUCCESS == ret && last_row_key.ptr()) {
    root_table.insert(make_pair(last_row_key, root_item));
    if (last_row_key.length() >= 1 && *(unsigned char*)(last_row_key.ptr()) == 0xff) {
      end_of_table = true;
    }
    end_row_key = last_row_key;
  }


  //fprintf(stderr, "row_count=%d, size=%d\n", row_count,root_table.size());
  return ret;
}

int64_t sum_total_line(const SimpleRootTable& root_table) {
  SimpleRootTable::const_iterator it = root_table.begin();
  map<string, int64_t>::const_iterator item_it ;
  int64_t total_line = 0;

  while (it != root_table.end()) {
    //const ObString & row_key = it->first;
    //hex_dump(row_key.ptr(), row_key.length());
    const map<string, int64_t>& item = it->second;
    //fprintf(stderr, "item.size=%d\n", item.size());
    if (item.end() != (item_it = item.find("occupy_size"))) {
      //fprintf(stderr, "line=%ld\n", item_it->second);
      total_line += item_it->second;
    } else {
      fprintf(stderr, "error cannot find occupy_size\n");
      //hex_dump(row_key.ptr(), row_key.length());
    }

    ++it;
  }
  return total_line;
}

int scan_root_table(const vector<string>& param) {
  int ret = 0;
  if (param.size() < 1) {
    fprintf(stderr, "scan_root_table: input param error\n");
    fprintf(stderr, "scan_root_table table_name\n");
    return OB_ERROR;
  }

  const char* input_table_name = param[0].c_str();

  ObScanParam scan_param;
  ObScanner scanner;
  ObString table_name(0, strlen(input_table_name), (char*)input_table_name);

  ObRange query_range;
  query_range.border_flag_.set_min_value();
  query_range.border_flag_.set_max_value();

  SimpleRootTable root_table;

  bool end_of_table = false;
  ObString end_row_key;

  int count = 0;
  int64_t sum_line = 0;
  int64_t total_line = 0;

  scan_param.set(OB_INVALID_ID, table_name, query_range);
  while (OB_SUCCESS == ret) {
    ret = GFactory::get_instance().get_rpc_stub().cs_scan(scan_param, scanner);
    if (OB_SUCCESS != ret) {
      fprintf(stderr, "scan root server error, ret = %d", ret);
    } else {
      store_root_table(scanner, root_table, end_of_table, end_row_key);
      total_line = sum_total_line(root_table);
      sum_line  += total_line;
      //fprintf(stderr, "size=%d,total_line=%ld\n", (int)root_table.size(), sum_total_line(root_table));
    }

    //fprintf(stderr, "end_of_table=%d\n", end_of_table);
    //hex_dump(end_row_key.ptr(), end_row_key.length());

    if (end_of_table) {
      break;
    } else {
      scan_param.reset();
      scanner.reset();

      query_range.border_flag_.unset_min_value();
      query_range.border_flag_.set_max_value();
      query_range.start_key_ = end_row_key;
      scan_param.set(OB_INVALID_ID, table_name, query_range);
    }
    if (++count >= 10) break;
  }

  fprintf(stderr, "sum_total_line=%ld\n", sum_line);
  return ret;
}

int manual_merge(const vector<string>& param) {
  int ret = 0;
  if (param.size() < 1) {
    fprintf(stderr, "manual_merge: input param error\n");
    fprintf(stderr, "manual_merge memtable_frozen_version [init_flag=0]\n");
    return OB_ERROR;
  }
  int64_t memtable_frozen_version = strtoll(param[0].c_str(), NULL, 10);
  int32_t init_flag = 0;
  if (param.size() > 1) init_flag = atoi(param[1].c_str());
  ret = GFactory::get_instance().get_rpc_stub().start_merge(memtable_frozen_version, init_flag);
  return ret;
}

int manual_drop_tablet(const vector<string>& param) {
  int ret = 0;
  if (param.size() < 1) {
    fprintf(stderr, "manual_drop_tablet: input param error\n");
    fprintf(stderr, "manual_drop_tablet memtable_frozen_version \n");
    return OB_ERROR;
  }
  int64_t memtable_frozen_version = strtoll(param[0].c_str(), NULL, 10);
  ret = GFactory::get_instance().get_rpc_stub().drop_tablets(memtable_frozen_version);
  return ret;
}

int manual_gc(const vector<string>& param) {
  int32_t copy = 3;
  if (param.size() < 1) {
    fprintf(stderr, "reserve 3 copies");
  } else {
    copy = atoi(param[0].c_str());
    fprintf(stderr, "reserve %d copy(s)", copy);
  }
  int ret = GFactory::get_instance().get_rpc_stub().start_gc(copy);
  return ret;
}

enum StatsObjectType {
  ServerStats = 1,
  ClusterStats = 2,
};

ObServerStats* create_stats_object(
  const StatsObjectType obj_type,
  ObClientRpcStub& rpc_stub,
  const int32_t server_type,
  const int32_t show_header,
  const char* table_filter_string,
  const char* index_filter_string
) {
  ObServerStats* server_stats = NULL;
  int ret = 0;
  if (obj_type == ServerStats) {
    server_stats = new ObServerStats(rpc_stub, server_type);
  } else if (obj_type == ClusterStats) {
    server_stats = new ObClusterStats(rpc_stub, server_type);
  } else {
    return NULL;
  }

  server_stats->set_show_header(show_header);

  int32_t table_id_size = 128;
  int32_t table_id_array[table_id_size];
  int32_t show_index_size = 128;
  int32_t show_index_array[show_index_size];
  if (NULL != table_filter_string) {
    ret = parse_number_range(table_filter_string, table_id_array, table_id_size);
    if (OB_SUCCESS == ret && table_id_size > 0) {
      if (table_id_size > 1 || table_id_array[0] != 0) {
        for (int32_t i = 0 ; i < table_id_size; ++i) {
          server_stats->add_table_filter(table_id_array[i]);
        }
      }
    } else if (OB_SUCCESS != ret) {
      fprintf(stderr, "parse show table id(%s) failed\n", table_filter_string);
      return NULL;
    }
  }

  if (NULL != index_filter_string) {
    ret = parse_number_range(index_filter_string, show_index_array, show_index_size);
    if (OB_SUCCESS == ret && show_index_size > 0) {
      if (show_index_size > 1 || show_index_array[0] != 0) {
        for (int32_t i = 0 ; i < show_index_size; ++i) {
          server_stats->add_index_filter(show_index_array[i]);
        }
      }
    } else if (OB_SUCCESS != ret) {
      fprintf(stderr, "parse show index id(%s) failed\n", index_filter_string);
      return NULL;
    }
  }

  // special case for daily merge
  if (server_type == 5) {
    //printf("special case for daily merge, set table filter 0 \n");
    server_stats->clear_table_filter();
    // only get table 0
    server_stats->add_table_filter(0);
  }

  return server_stats;

}


int loop_dump(
  ObServerStats& server_stats,
  const int32_t interval,
  const int32_t run_count
) {
  int32_t count = 0;
  int ret = 0;
  while (true) {
    ret = server_stats.summary(count, interval);
    if (OB_SUCCESS != ret) {
      fprintf(stderr, "summary error, ret=%d\n", ret);
      break;
    }
    ++count;
    if (run_count > 0 && count >= run_count) break;
    sleep(interval);
  }
  return ret;
}

int dump_stats_info(StatsObjectType objtype, const vector<string>& param) {
  static const int32_t server_count = 6;
  static const char* server_name[server_count] = {"unknown", "rs", "cs", "ms", "ups", "dm"};
  int ret = 0;
  if (param.size() < 1) {
    fprintf(stderr, "dump_stats_info: input param error\n");
    fprintf(stderr, "%s <server_type=rs/cs/ms/ups/dm> [interval=1] "
            "[count = 0] [show_header_count = 50] "
            "[table = 0/1001~1005/1001,1003] [show_index = 0/0~11/0,2,5]"
            "\n", objtype == ServerStats ? "dump_server_stats" : "dump_cluster_stats");
    return OB_ERROR;
  }

  const char* server_type_str = param[0].c_str();
  int32_t server_type = atoi(param[0].c_str());
  for (int32_t i = 0; i < server_count; ++i) {
    if (strcmp(server_type_str, server_name[i]) == 0) {
      server_type = i;
    }
  }

  //printf("str=%s,server_type=%d\n", server_type_str, server_type);

  int32_t interval = 1;
  int32_t run_count = 0 ;
  int32_t show_header = 50;

  const char* table_filter_string = NULL;
  const char* index_filter_string = NULL;
  if (param.size() > 1) interval = atoi(param[1].c_str());
  if (param.size() > 2) run_count = atoi(param[2].c_str());
  if (param.size() > 3) show_header = atoi(param[3].c_str());
  if (param.size() > 4) table_filter_string = param[4].c_str();
  if (param.size() > 5) index_filter_string = param[5].c_str();

  ObClientRpcStub rpc_stub;
  ret = rpc_stub.initialize(
          GFactory::get_instance().get_rpc_stub().get_remote_server(),
          &GFactory::get_instance().get_base_client().get_client_manager());
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "initialize rpc stub failed, ret = %d\n", ret);
    return ret;
  }


  ObServerStats* server_stats = create_stats_object(
                                  objtype,
                                  rpc_stub,
                                  server_type,
                                  show_header,
                                  table_filter_string,
                                  index_filter_string
                                );

  if (NULL == server_stats) return ret;

  ret = loop_dump(
          *server_stats,
          interval,
          run_count
        );

  return ret;
}

int do_work(const int cmd, const vector<string>& param) {
  int ret = OB_SUCCESS;
  switch (cmd) {
  case CMD_DUMP_TABLET:
    ret = dump_tablet_image(param);
    break;
  case CMD_DUMP_SERVER_STATS:
    ret = dump_stats_info(ServerStats, param);
    break;
  case CMD_DUMP_CLUSTER_STATS:
    ret = dump_stats_info(ClusterStats, param);
    break;
  case CMD_MANUAL_MERGE:
    ret = manual_merge(param);
    break;
  case CMD_MANUAL_DROP_TABLET:
    ret = manual_drop_tablet(param);
    break;
  case CMD_MANUAL_GC:
    ret = manual_gc(param);
    break;
  case CMD_RS_GET_TABLET_INFO:
    ret = get_tablet_info(param);
    break;
  case CMD_SCAN_ROOT_TABLE:
    ret = scan_root_table(param);
    break;
  default:
    fprintf(stderr, "unsupported command : %d\n", cmd);
    ret = OB_ERROR;
    break;
  }
  return ret;
}

void usage() {
  fprintf(stderr, "usage: ./cs_admin -s chunkserver_ip -p chunkserver_port -i \"command args\"\n");
}

int main(const int argc, char* argv[]) {

  const char* addr = NULL;
  const char* cmdstring = NULL;
  int32_t port = 0;

  int ret = 0;
  int silent = 0;

  ObServer chunk_server;


  while ((ret = getopt(argc, argv, "s:p:i:q")) != -1) {
    switch (ret) {
    case 's':
      addr = optarg;
      break;
    case 'i':
      cmdstring = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'q':
      silent = 1;
      break;
    default:
      fprintf(stderr, "%s is not identified\n", optarg);
      usage();
      break;
    };
  }

  if (silent) TBSYS_LOGGER.setLogLevel("ERROR");

  ret = chunk_server.set_ipv4_addr(addr, port);
  if (!ret) {
    fprintf(stderr, "chunkserver addr invalid, addr=%s, port=%d\n", addr, port);
    usage();
    return ret;
  }

  ret = GFactory::get_instance().initialize(chunk_server);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "initialize GFactory error, ret=%d\n", ret);
    return ret;
  }
  ret = GFactory::get_instance().start();
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "start GFactory error, ret=%d\n", ret);
    return ret;
  }

  vector<string> param;
  int cmd = parse_command(cmdstring, param);
  ret = do_work(cmd, param);

  GFactory::get_instance().stop();
  GFactory::get_instance().wait();

  return ret;
}
