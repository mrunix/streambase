/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_syschecker.cc for define syschecker worker.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_syschecker.h"
#include "common/ob_malloc.h"
#include "common/utility.h"

namespace sb {
namespace syschecker {
using namespace common;

ObSyschecker::ObSyschecker()
  : rule_(syschecker_schema_), client_(servers_mgr_),
    write_worker_(client_, rule_, stat_),
    read_worker_(client_, rule_, stat_) {

}

ObSyschecker::~ObSyschecker() {

}

int ObSyschecker::init_servers_manager() {
  int ret                       = OB_SUCCESS;
  int64_t merge_server_count    = 0;
  const ObServer* merge_server  = NULL;

  ret = servers_mgr_.init(param_.get_merge_server_count() + 2);
  if (OB_SUCCESS == ret) {
    ret = servers_mgr_.set_root_server(param_.get_root_server());
  }

  if (OB_SUCCESS == ret) {
    ret = servers_mgr_.set_update_server(param_.get_update_server());
  }

  if (OB_SUCCESS == ret) {
    merge_server_count = param_.get_merge_server_count();
    merge_server = param_.get_merge_server();
    for (int64_t i = 0;
         i < merge_server_count && NULL != merge_server && OB_SUCCESS == ret;
         ++i) {
      ret = servers_mgr_.add_merge_server(merge_server[i]);
    }
  }

  return ret;
}

int ObSyschecker::get_cell_value(const ObCellInfo& cell_info,
                                 int64_t& value) {
  int ret             = OB_SUCCESS;
  float floatv        = 0.0;
  double doublev      = 0.0;
  ObObjType cell_type = cell_info.value_.get_type();
  ObString varcharv;

  if (OB_SUCCESS == ret) {
    switch (cell_type) {
    case ObIntType:
      cell_info.value_.get_int(value);
      break;

    case ObFloatType:
      cell_info.value_.get_float(floatv);
      value = static_cast<int64_t>(floatv);
      break;

    case ObDoubleType:
      cell_info.value_.get_double(doublev);
      value = static_cast<int64_t>(doublev);
      break;

    case ObDateTimeType:
      cell_info.value_.get_datetime(value);
      break;

    case ObPreciseDateTimeType:
      cell_info.value_.get_precise_datetime(value);
      break;

    case ObVarcharType:
      cell_info.value_.get_varchar(varcharv);
      value = strtoll(varcharv.ptr(), NULL, 10);
      break;

    case ObExtendType:
      if (cell_info.value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST) {
        TBSYS_LOG(WARN, "row doesn't exist for get rowkey range");
        ret = OB_ERROR;
      }
      break;

    case ObCreateTimeType:
    case ObModifyTimeType:
    case ObNullType:
    case ObSeqType:
    default:
      TBSYS_LOG(WARN, "invalid cell info type %d", cell_type);
      ret = OB_ERROR;
      break;
    }
  }

  return ret;
}

int ObSyschecker::read_rowkey_range(ObScanner& scanner) {
  int ret         = OB_SUCCESS;
  int64_t index   = 0;
  int64_t prefix  = 0;
  int64_t suffix  = 0;
  ObScannerIterator iter;
  ObCellInfo cell_info;

  for (iter = scanner.begin();
       iter != scanner.end() && OB_SUCCESS == ret;
       iter++, index++) {
    cell_info.reset();
    ret = iter.get_cell(cell_info);
    if (OB_SUCCESS == ret) {
      if (0 == index) {
        ret = get_cell_value(cell_info, prefix);
        if (OB_SUCCESS == ret) {
          TBSYS_LOG(DEBUG, "get cur_max_prefix=%ld", prefix);
          rule_.add_cur_max_prefix(prefix + param_.get_syschecker_no());
        }
      } else if (1 == index) {
        ret = get_cell_value(cell_info, suffix);
        if (OB_SUCCESS == ret) {
          TBSYS_LOG(DEBUG, "get cur_max_suffix=%ld", suffix);
          rule_.add_cur_max_suffix(suffix + param_.get_syschecker_no());
        }
      } else {
        TBSYS_LOG(WARN, "more than 2 cell returned for get rowkey range");
        ret = OB_ERROR;
      }
    }
  }

  return ret;
}

int ObSyschecker::init_rowkey_range() {
  int ret                               = OB_SUCCESS;
  const ObTableSchema* wt_schema        = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  const char* column_name               = NULL;
  int32_t column_size                   = 0 ;
  ObGetParam get_param;
  ObScanner scanner;
  ObCellInfo cell_info;
  ObString table_name;
  char key_buf[MAX_SYSCHECKER_ROWKEY_LEN];
  ObString row_key;
  ObVersionRange ver_range;

  //the row with key 0 in wide table stores row key range
  memset(key_buf, 0, MAX_SYSCHECKER_ROWKEY_LEN);
  row_key.assign(key_buf, MAX_SYSCHECKER_ROWKEY_LEN);
  table_name = syschecker_schema_.get_wt_name();

  //build get_param
  get_param.set_is_result_cached(true);
  wt_schema = syschecker_schema_.get_wt_schema();
  if (NULL != wt_schema) {
    column_schema = syschecker_schema_.get_schema_manager().
                    get_table_schema(wt_schema->get_table_id(), column_size);
    for (int64_t i = 0; i < 2 && column_size >= 2 && OB_SUCCESS == ret; ++i) {
      cell_info.reset();
      cell_info.table_name_ = table_name;
      cell_info.row_key_ = row_key;
      if (NULL != &column_schema[i]) {
        column_name = column_schema[i].get_name();
        cell_info.column_name_.assign(const_cast<char*>(column_name),
                                      strlen(column_name));
        ret = get_param.add_cell(cell_info);
      } else {
        ret = OB_ERROR;
      }
    }
  } else {
    TBSYS_LOG(WARN, "wide table schema is invalid, wt_schema=%p",
              wt_schema);
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret) {
    ver_range.start_version_ = 0;
    ver_range.end_version_ = INT64_MAX - 1;
    ver_range.border_flag_.set_inclusive_start();
    ver_range.border_flag_.set_inclusive_end();
    get_param.set_version_range(ver_range);
    ret = client_.ms_get(get_param, scanner);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to get cell from merge server, rowkey:");
      hex_dump(row_key.ptr(), row_key.length(), true, TBSYS_LOG_LEVEL_WARN);
    }
  }

  if (OB_SUCCESS == ret) {
    ret = read_rowkey_range(scanner);
  }

  return ret;
}

int ObSyschecker::init() {
  int ret = OB_SUCCESS;

  ret = param_.load_from_config();
  if (OB_SUCCESS == ret) {
    ret = init_servers_manager();
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to init servers manager");
    }
  }

  //for debuging
  if (0) {
    param_.dump_param();
  }

  if (OB_SUCCESS == ret) {
    ret = client_.init(param_.get_network_time_out());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to init client");
    }
  }

  if (OB_SUCCESS == ret) {
    ret = client_.fetch_schema(0, syschecker_schema_.get_schema_manager());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to featch schema from root server");
    }
  }

  if (OB_SUCCESS == ret) {
    ret = syschecker_schema_.init();
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to init syschecker schema manager");
    }
  }

  if (OB_SUCCESS == ret) {
    if (param_.is_specified_read_param()) {
      //for debuging
      rule_.add_cur_max_prefix(1000000);
      rule_.add_cur_max_suffix(1000000);
    } else {
      ret = init_rowkey_range();
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "failed to get current key range from sb");
      }
    }
  }

  if (OB_SUCCESS == ret) {
    ret = rule_.init(param_.get_syschecker_count(),
                     param_.is_specified_read_param(),
                     param_.is_operate_full_row());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to init syschecker rule");
    }
  }

  return ret;
}

int ObSyschecker::start() {
  int ret                     = OB_SUCCESS;
  int64_t write_thread_count  = 0;
  int64_t read_thread_count   = 0;
  int64_t stat_dump_interval  = 0;

  srandom(time(0));
  ret = ob_init_memory_pool();
  if (OB_SUCCESS == ret) {
    ret = init();
  }

  if (OB_SUCCESS == ret) {
    write_thread_count = param_.get_write_thread_count();
    read_thread_count = param_.get_read_thread_count();
    stat_dump_interval = param_.get_stat_dump_interval();

    if (write_thread_count > 0) {
      write_worker_.setThreadCount(write_thread_count);
      write_worker_.start();
    }

    if (read_thread_count > 0) {
      read_worker_.setThreadCount(read_thread_count);
      read_worker_.start();
    }

    if (stat_dump_interval > 0) {
      ret = stat_.init(stat_dump_interval);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "failed to init stat");
      }
    }

    if (OB_SUCCESS == ret) {
      ret = wait();
    }
  }

  return ret;
}

int ObSyschecker::stop() {
  write_worker_.stop();
  read_worker_.stop();

  return OB_SUCCESS;
}

int ObSyschecker::wait() {
  write_worker_.wait();
  read_worker_.wait();
  stat_.dump_stat();
  if (!param_.is_specified_read_param()) {
    write_rowkey_range();
  }
  client_.stop();
  client_.wait();

  return OB_SUCCESS;
}

int ObSyschecker::set_cell_value(ObObj& obj, const ObObjType type,
                                 char* value_buf, const int64_t value) {
  int ret           = OB_SUCCESS;
  int64_t value_len = 0;
  ObString varchar;

  switch (type) {
  case ObIntType:
    obj.set_int(value);
    break;
  case ObFloatType:
    obj.set_float(value);
    break;
  case ObDoubleType:
    obj.set_double(value);
    break;
  case ObDateTimeType:
    obj.set_datetime(value);
    break;
  case ObPreciseDateTimeType:
    obj.set_precise_datetime(value);
    break;
  case ObVarcharType:
    if (NULL != value_buf) {
      value_len = sprintf(value_buf, "%ld", value);
      varchar.assign(value_buf, value_len);
      obj.set_varchar(varchar);
    } else {
      ret = OB_ERROR;
    }
    break;
  case ObNullType:
  case ObSeqType:
  case ObCreateTimeType:
  case ObModifyTimeType:
  case ObExtendType:
  case ObMaxType:
  default:
    TBSYS_LOG(WARN, "wrong object type %d for store rowkey range", type);
    ret = OB_ERROR;
    break;
  }

  return ret;
}

int ObSyschecker::write_rowkey_range() {
  int ret                               = OB_SUCCESS;
  const ObTableSchema* wt_schema        = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  const char* column_name_str           = NULL;
  int32_t column_size                   = 0;
  int64_t prefix                        = 0;
  int64_t suffix                        = 0;
  ObMutator mutator;
  ObScanner scanner;
  ObString table_name;
  char key_buf[MAX_SYSCHECKER_ROWKEY_LEN];
  ObString row_key;
  ObString column_name;
  ObObj obj;
  char value_buf[32];

  //the row with key 0 in wide table stores row key range
  memset(key_buf, 0, MAX_SYSCHECKER_ROWKEY_LEN);
  row_key.assign(key_buf, MAX_SYSCHECKER_ROWKEY_LEN);
  table_name = syschecker_schema_.get_wt_name();

  //build mutator
  wt_schema = syschecker_schema_.get_wt_schema();
  if (NULL != wt_schema) {
    column_schema = syschecker_schema_.get_schema_manager().
                    get_table_schema(wt_schema->get_table_id(), column_size);
    for (int64_t i = 0; i < 2 && column_size >= 2 && OB_SUCCESS == ret; ++i) {
      if (NULL != &column_schema[i]) {
        if (0 == i) {
          prefix = rule_.get_cur_max_prefix();
          ret = set_cell_value(obj, column_schema[i].get_type(),
                               value_buf, prefix);
        } else if (1 == i) {
          suffix = rule_.get_cur_max_suffix();
          ret = set_cell_value(obj, column_schema[i].get_type(),
                               value_buf, suffix);
        }
        if (OB_SUCCESS == ret) {
          column_name_str = column_schema[i].get_name();
          column_name.assign(const_cast<char*>(column_name_str),
                             strlen(column_name_str));
          ret = mutator.update(table_name, row_key, column_name, obj);
        }
      } else {
        ret = OB_ERROR;
      }
    }
  } else {
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret) {
    ret = client_.ups_apply(mutator);
  }

  return ret;
}
} // end namespace syschecker
} // end namespace sb
