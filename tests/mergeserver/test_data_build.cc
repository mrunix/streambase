/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_data_build.cc for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include <getopt.h>
#include <string>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>
#include "../updateserver/mock_client.h"
#include "../updateserver/test_utils.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"

using namespace std;
using namespace sb;
using namespace sb::common;
const int64_t TIMEOUT =  50000000L;
static const int64_t MAX_ROW_KEY = 1024ll * 1024ll * 16ll - 1ll;
static const uint64_t MAX_COLUMN_ID = 255;
static const int64_t delete_row_interval = 100;

int64_t get_cur_time_us() {
  struct timeval now;
  gettimeofday(&now, NULL);
  return (now.tv_sec * 1000000 + now.tv_usec);
}

struct CParam {
  char* server_addr_;
  int32_t server_port_;
  int32_t interval_;
  char* schema_fname_;
  int64_t start_key_;
  int64_t end_key_;
  ObString table_;
  const ObSchema* schema_;
  char* operation_;
  sb::common::ObSchemaManager* schema_mgr_;
  int64_t cur_version_;
};

void print_usage() {
  fprintf(stderr, "\n");
  fprintf(stderr, "batch_client [OPTION]\n");
  fprintf(stderr, "   -a|--addr server address\n");
  fprintf(stderr, "   -p|--port server port\n");
  fprintf(stderr, "   -m|--schema schema file\n");
  fprintf(stderr, "   -t|--table table\n");
  fprintf(stderr, "   -s|--start start rowkey must int type [0~%ld]\n", MAX_ROW_KEY);
  fprintf(stderr, "   -e|--end end rowkey must int type\n");
  fprintf(stderr, "   -o|--operation operation [scan,update]\n");
  fprintf(stderr, "   -v|--version specify current version of active memory table\n");
  fprintf(stderr, "   -h|--help print this help info\n");
  fprintf(stderr, "\n");
}

int check_schema(CParam& param) {
  int err = OB_SUCCESS;
  const ObSchema* schema = NULL;
  uint64_t table_id = OB_INVALID_ID;
  table_id = param.schema_mgr_->get_table_id(param.table_);
  if (OB_INVALID_ID == table_id) {
    TBSYS_LOG(WARN, "table not exist [table_name:%.*s]", param.table_.length(), param.table_.ptr());
    err = OB_INVALID_ARGUMENT;
  } else {
    schema = param.schema_mgr_->get_table_schema(table_id);
    if (NULL == schema) {
      TBSYS_LOG(WARN, "table not exist [table_name:%.*s]", param.table_.length(), param.table_.ptr());
      err = OB_INVALID_ARGUMENT;
    } else {
      param.schema_ = schema;
    }
  }
  if (OB_SUCCESS == err) {
    if (schema->get_rowkey_max_length() != static_cast<int64_t>(sizeof(uint32_t))) {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "table's rowkey len must be %u", sizeof(uint32_t));
    }
  }
  if (OB_SUCCESS == err) {
    if (!schema->is_row_key_fixed_len()) {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "table's rowkey len must be fixed");
    }
  }
  if (OB_SUCCESS == err) {
    const ObColumnSchema* column_begin = schema->column_begin();
    const ObColumnSchema* column_end  = schema->column_end();
    const ObJoinInfo*           join_info = NULL;
    for (; column_begin < column_end && OB_SUCCESS == err; column_begin++) {
      join_info = schema->find_join_info(column_begin->get_id());
      if (NULL != join_info) {
        TBSYS_LOG(WARN, "there should not be join info");
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err && column_begin->get_id() > MAX_COLUMN_ID) {
        TBSYS_LOG(WARN, "column id too max, MAX_COLUMN_ID:%ld", MAX_COLUMN_ID);
        err = OB_INVALID_ARGUMENT;
      }
    }
  }
  return err;
}

int parse_cmd_args(int argc, char** argv, CParam& param) {
  int err = OB_SUCCESS;
  int opt = 0;
  memset(&param, 0x00, sizeof(param));
  const char* opt_string = "a:p:m:t:s:e:o:i:v:h";
  struct option longopts[] = {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'p'},
    {"schema", 1, NULL, 'm'},
    {"table", 1, NULL, 't'},
    {"start", 1, NULL, 's'},
    {"end", 1, NULL, 'e'},
    {"operation", 1, NULL, 'o'},
    {"interval", 1, NULL, 'i'},
    {"version", 1, NULL, 'v'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  param.interval_ = 1;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
    case 'a':
      param.server_addr_ = optarg;
      break;
    case 'p':
      param.server_port_ = atoi(optarg);
      break;
    case 'i':
      param.interval_ = atoi(optarg);
      break;
    case 'm':
      param.schema_fname_ = optarg;
      break;
    case 't':
      param.table_.assign(optarg, strlen(optarg));;
      break;
    case 's':
      param.start_key_ = atol(optarg);
      break;
    case 'e':
      param.end_key_ = atol(optarg);
      break;
    case 'o':
      param.operation_ = optarg;
      break;
    case 'v':
      param.cur_version_ = atol(optarg);
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }

  if (NULL == param.server_addr_
      || 0 == param.server_port_
      || NULL == param.schema_fname_
      || NULL == param.table_.ptr()
      || NULL == param.operation_
      || 0 > param.start_key_
      || param.start_key_ > param.end_key_
      || 0 > param.start_key_
      || 2 > param.cur_version_
     ) {
    print_usage();
    exit(1);
  }
  if (strcmp(param.operation_, "scan") != 0
      && strcmp(param.operation_, "update") != 0) {
    print_usage();
    exit(1);
  }

  if (OB_SUCCESS == err) {
    param.schema_mgr_ = new ObSchemaManager;
    if (NULL == param.schema_mgr_) {
      TBSYS_LOG(WARN, "%s", "fail to allocate memory for schema manager");
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  tbsys::CConfig config;
  if (OB_SUCCESS == err && !param.schema_mgr_->parse_from_file(param.schema_fname_, config)) {
    TBSYS_LOG(WARN, "fail to load schema from file [fname:%s]", param.schema_fname_);
    err = OB_SCHEMA_ERROR;
  }
  if (OB_SUCCESS == err) {
    err = check_schema(param);
    if (OB_SUCCESS != err) {
      err = OB_SCHEMA_ERROR;
    }
  }
  return err;
}

void init_mock_client(const char* addr, int32_t port, MockClient& client) {
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  client.init(dst_host);
}

int delete_rows(CParam& param, MockClient& client) {
  int err = OB_SUCCESS;
  ObMutator mutator;
  ObString table_name;
  ObString column_name;
  ObString row_key_str;
  table_name.assign(const_cast<char*>(param.schema_->get_table_name()),
                    strlen(param.schema_->get_table_name()));
  int64_t each_update_row_num = 3000;
  bool req_sended = true;
  uint32_t key_val = 0;
  row_key_str.assign((char*)&key_val, sizeof(key_val));
  for (uint32_t i = param.start_key_; OB_SUCCESS == err && i < param.end_key_; i++) {
    if (i % delete_row_interval != 0) {
      continue;
    }
    key_val = htonl(i);
    err = mutator.del_row(table_name, row_key_str);
    if (OB_SUCCESS == err) {
      req_sended = false;
    }
    if (OB_SUCCESS == err && i > 0 && (i % each_update_row_num == 0 || i == param.end_key_ - 1)) {
      err = client.ups_apply(mutator, TIMEOUT);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "fail to update ups [err:%d]", err);
      } else {
        TBSYS_LOG(WARN, "update [i:%ld,param.start_key_:%ld,param.end_key_:%ld]", i,
                  param.start_key_, param.end_key_);
        req_sended = true;
        mutator.reset();
      }
    }
  }
  if (!req_sended) {
    err = client.ups_apply(mutator, TIMEOUT);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to update ups [err:%d]", err);
    } else {
      TBSYS_LOG(WARN, "update [param.start_key_:%ld,param.end_key_:%ld]",
                param.start_key_, param.end_key_);
      req_sended = true;
      mutator.reset();
    }
  }
  return err;
}

int update(CParam& param, MockClient& client) {
  int err = OB_SUCCESS;
  const ObColumnSchema* column_beg = param.schema_->column_begin();
  const ObColumnSchema* column_end = param.schema_->column_end();
  ObMutator mutator;
  ObString table_name;
  ObString column_name;
  ObString row_key_str;
  ObObj val_obj;
  table_name.assign(const_cast<char*>(param.schema_->get_table_name()),
                    strlen(param.schema_->get_table_name()));
  bool req_sended = false;
  int64_t each_update_row_num = 3000;
  uint32_t key_val = 0;
  row_key_str.assign((char*)&key_val, sizeof(key_val));
  for (uint32_t i = param.start_key_; OB_SUCCESS == err && i < param.end_key_; i += param.interval_) {
    key_val = htonl(i);
    column_beg = param.schema_->column_begin();
    while (OB_SUCCESS == err && column_beg < column_end) {
      if (column_beg->get_type() == ObIntType) {
        column_name.assign(const_cast<char*>(column_beg->get_name()), strlen(column_beg->get_name()));
        val_obj.set_int(column_beg->get_id()*param.cur_version_);
        err = mutator.update(table_name, row_key_str, column_name, val_obj);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to add muatator [err:%d]", err);
        } else {
          req_sended = false;
          column_beg ++;
        }
      }
    }
    if (OB_SUCCESS == err && i > 0 && (i % each_update_row_num == 0 || i == param.end_key_ - 1)) {
      err = client.ups_apply(mutator, TIMEOUT);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "fail to update ups [err:%d]", err);
      } else {
        TBSYS_LOG(WARN, "update [i:%u,param.start_key_:%ld,param.end_key_:%ld]", i,
                  param.start_key_, param.end_key_);
        mutator.reset();
      }
    }
  }
  if (OB_SUCCESS == err) {
    err = delete_rows(param, client);
  }
  return err;
}

int check_result(CParam& param, ObScanner& result, uint32_t start_key, ObScanParam::Direction direction) {
  int err = OB_SUCCESS;
  const ObColumnSchema* column_beg = param.schema_->column_begin();
  const ObColumnSchema* column_end = param.schema_->column_end();
  ObString row_key_str;
  ObString column_name;
  ObCellInfo* cur_cell = NULL;
  bool is_row_changed = false;
  ObObj val_obj;
  int64_t row_num = 0;
  uint32_t key_val = 0;
  row_key_str.assign((char*)&key_val, sizeof(key_val));

  err = result.next_cell();
  if (OB_SUCCESS != err && OB_ITER_END != err) {
    TBSYS_LOG(WARN, "fail to next cell from result [err:%d]", err);
  }
  if (OB_SUCCESS == err && ObScanParam::BACKWARD == direction) {
    ObCellInfo* first_cell = NULL;
    err = result.get_cell(&first_cell);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to get first cell from ObScanner [err:%d]", err);
    } else {
      start_key = ntohl(*(uint32_t*)first_cell->row_key_.ptr());
    }
  }
  while (OB_SUCCESS == err) {
    if (start_key % delete_row_interval == 0) {
      start_key ++;
      continue;
    }
    key_val = htonl(start_key);
    for (column_beg = param.schema_->column_begin(); column_beg < column_end && OB_SUCCESS == err; column_beg ++) {
      if (column_beg->get_type() == ObIntType) {
        column_name.assign(const_cast<char*>(column_beg->get_name()), strlen(column_beg->get_name()));
        val_obj.set_int(column_beg->get_id()*param.cur_version_);
        err = result.get_cell(&cur_cell, &is_row_changed);
        if (OB_SUCCESS == err) {
          if (cur_cell->column_name_ != column_name
              || cur_cell->table_name_ != param.table_
              || cur_cell->row_key_ != row_key_str
              || cur_cell->value_ != val_obj) {
            TBSYS_LOG(WARN, "unexpected error");
            err = OB_ERR_UNEXPECTED;
          }
        } else {
          TBSYS_LOG(WARN, "fail to get cell from scanner [err:%d]", err);
        }
        if (OB_SUCCESS == err) {
          err = result.next_cell();
          if (OB_SUCCESS != err && OB_ITER_END != err) {
            TBSYS_LOG(WARN, "fail to next cell from result [err:%d]", err);
          }
          if (OB_ITER_END == err && column_beg + 1 != column_end) {
            TBSYS_LOG(WARN, "result err, not row width");
            err = OB_ERR_UNEXPECTED;
          }
        }
      }
    }
    if (OB_SUCCESS == err) {
      start_key ++;
      row_num ++;
    }
  }
  if (OB_ITER_END == err) {
    err = OB_SUCCESS;
  } else {
    TBSYS_LOG(WARN, "unexpected error");
  }
  return err;
}

int scan(CParam& param, MockClient& client) {
  int err = OB_SUCCESS;
  const ObColumnSchema* column_beg = param.schema_->column_begin();
  const ObColumnSchema* column_end = param.schema_->column_end();
  ObScanParam scan_param;
  ObString table_name;
  ObString column_name;
  ObString start_key_str;
  ObString end_key_str;
  ObObj    val_obj;
  ObRange       range;
  ObVersionRange version_range;
  version_range.border_flag_.set_min_value();
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  version_range.border_flag_.set_inclusive_end();
  table_name.assign(const_cast<char*>(param.schema_->get_table_name()),
                    strlen(param.schema_->get_table_name()));
  uint32_t start_key_val = htonl(param.start_key_);
  uint32_t end_key_val = htonl(param.end_key_);
  start_key_str.assign((char*)&start_key_val, sizeof(start_key_val));
  end_key_str.assign((char*)&end_key_val, sizeof(end_key_val));
  range.start_key_ = start_key_str;
  range.end_key_ = end_key_str;
  range.table_id_ = OB_INVALID_ID;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  range.border_flag_.unset_min_value();
  range.border_flag_.unset_max_value();
  uint32_t cur_key  = param.start_key_;
  scan_param.set(OB_INVALID_ID, table_name, range);
  scan_param.set_version_range(version_range);
  for (; column_beg < column_end && OB_SUCCESS == err; column_beg++) {
    if (column_beg->get_type() == ObIntType) {
      column_name.assign(const_cast<char*>(column_beg->get_name()), strlen(column_beg->get_name()));
      err = scan_param.add_column(column_name);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "fail to add column to scan param, [err:%d]", err);
      }
    }
  }
  ObString last_row_key;
  ObScanner result;
  /// scan forward
  TBSYS_LOG(WARN, "scan forward");
  while (cur_key < param.end_key_ && OB_SUCCESS == err) {
    start_key_val  = htonl(cur_key);
    scan_param.set(OB_INVALID_ID, table_name, range);

    err = client.ups_scan(scan_param, result, TIMEOUT);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to scan from server [err:%d]", err);
    } else {
      err = check_result(param, result, cur_key, ObScanParam::FORWARD);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "check result fail");
      } else {
        err = result.get_last_row_key(last_row_key);
        if (OB_SUCCESS != err || last_row_key.length() != sizeof(uint32_t)) {
          TBSYS_LOG(WARN, "fail to get last key from scanner [err:%d,last_row_key.length():%ld]", err,
                    last_row_key.length());
        } else {
          uint32_t* last_key_val = (uint32_t*)last_row_key.ptr();
          cur_key = ntohl(*last_key_val) + 1;
        }
      }
    }
    TBSYS_LOG(WARN, "scan one round [cur_key:%ld,param.start_key_:%ld,param.end_key_:%ld]", cur_key,
              param.start_key_, param.end_key_);
  }
  /// scan backward
  TBSYS_LOG(WARN, "scan backward");
  ObString first_row_key;
  scan_param.set_scan_direction(ObScanParam::BACKWARD);
  start_key_val = htonl(param.start_key_);
  cur_key = param.end_key_;
  while (cur_key > param.start_key_ && OB_SUCCESS == err) {
    end_key_val = htonl(cur_key);
    scan_param.set(OB_INVALID_ID, table_name, range);
    static const int32_t backward_scan_count = 2048;
    scan_param.set_limit_info(0, backward_scan_count);

    err = client.ups_scan(scan_param, result, TIMEOUT);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to scan from server [err:%d]", err);
    } else {
      err = check_result(param, result, cur_key - 1, ObScanParam::BACKWARD);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "check result fail");
      }
      if (OB_SUCCESS == err) {
        err = result.get_last_row_key(last_row_key);
        if (OB_SUCCESS != err || last_row_key.length() != sizeof(uint32_t)) {
          TBSYS_LOG(WARN, "fail to get last key from scanner [err:%d,last_row_key.length():%ld]", err,
                    last_row_key.length());
        } else {
          uint32_t* plast_key_val = (uint32_t*)last_row_key.ptr();
          uint32_t last_key_val =  ntohl(*plast_key_val);
          if (cur_key > 0) {
            cur_key --;
          }
          if (cur_key % delete_row_interval == 0 && cur_key > 0) {
            cur_key --;
          }
          if (cur_key % delete_row_interval != 0 && cur_key != last_key_val) {
            TBSYS_LOG(WARN, "ms error [expect_last_rowkey:%u,real:%u]", cur_key, last_key_val);
            err = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (OB_SUCCESS == err) {
        err = result.reset_iter();
        if (OB_SUCCESS == err) {
          ObCellInfo* tmp_cell = NULL;
          err = result.get_cell(&tmp_cell);
          if (OB_SUCCESS == err) {
            uint32_t* next_key_val = (uint32_t*)tmp_cell->row_key_.ptr();
            cur_key = ntohl(*next_key_val);
            if (0 == cur_key) {
              break;
            }
          } else {
            TBSYS_LOG(WARN, "fail to get first cell from result [err:%d]", err);
          }
        } else {
          TBSYS_LOG(WARN, "fail to reset iterator of result [err:%d]", err);
        }
      }
    }
    TBSYS_LOG(WARN, "scan one round [cur_key:%ld,param.start_key_:%ld,param.end_key_:%ld]", cur_key,
              param.start_key_, param.end_key_);
    if (cur_key == 1) {
      break;
    }
  }
  /// check count
  int64_t expect_count = 0;
  for (cur_key = param.start_key_; cur_key < param.end_key_; cur_key ++) {
    if (cur_key % delete_row_interval != 0) {
      expect_count ++;
    }
  }
  if (OB_SUCCESS == err) {
    start_key_val = htonl(param.start_key_);
    end_key_val = htonl(param.end_key_);
    scan_param.reset();
    scan_param.set_scan_direction(ObScanParam::FORWARD);
    scan_param.set(OB_INVALID_ID, table_name, range);
    scan_param.add_column(ObGroupByParam::COUNT_ROWS_COLUMN_NAME);
    ObString as_column_name;
    as_column_name.assign(const_cast<char*>("count"), strlen("count"));
    err = scan_param.add_aggregate_column(ObGroupByParam::COUNT_ROWS_COLUMN_NAME, as_column_name, COUNT);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to add aggregate column [err:%d]", err);
    }
    int64_t time_us = get_cur_time_us();
    if (OB_SUCCESS == err) {
      err = client.ups_scan(scan_param, result, TIMEOUT);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "fail to scan server [err:%d]", err);
      } else {
        time_us = get_cur_time_us() - time_us;
      }
    }
    if (OB_SUCCESS == err) {
      int64_t cell_num = 0;
      ObCellInfo* cur_cell = NULL;
      bool is_row_changed = false;
      int64_t count = 0;
      while (OB_SUCCESS == err) {
        err = result.next_cell();
        if (OB_SUCCESS != err && OB_ITER_END != err) {
          TBSYS_LOG(WARN, "fail to get next cell [err:%d]", err);
        }
        if (OB_SUCCESS == err) {
          err = result.get_cell(&cur_cell, &is_row_changed);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to get cell [err:%d]", err);
          } else {
            cell_num ++;
            if (cur_cell->value_.get_int(count) != OB_SUCCESS) {
              TBSYS_LOG(WARN, "fail to get int val from result");
              err = OB_ERR_UNEXPECTED;
            } else {
              if (count != expect_count) {
                TBSYS_LOG(WARN, "count result error, [count:%ld,expect_count:%ld,param.end_key_:%ld,param.start_key_:%ld]", count,
                          expect_count, param.end_key_, param.start_key_);
                err = OB_ERR_UNEXPECTED;
              } else {
                TBSYS_LOG(WARN, "count:%ld,time_us:%ld", count, time_us);
              }
            }
          }
        }
      }
      if (cell_num != 1) {
        TBSYS_LOG(WARN, "result cell number wrong [cell_num:%ld]", cell_num);
      }
    }
    if (OB_ITER_END == err) {
      err = OB_SUCCESS;
    }
  }
  return err;
}

int main(int argc, char** argv) {
  int err = OB_SUCCESS;
  ob_init_memory_pool();
  CParam param;
  err = parse_cmd_args(argc, argv, param);
  MockClient client;
  if (OB_SUCCESS == err) {
    init_mock_client(param.server_addr_, param.server_port_, client);
    if (strcmp(param.operation_, "scan") == 0) {
      err = scan(param, client);
    } else {
      err = update(param, client);
    }
    TBSYS_LOG(WARN, "result:%d", err);
  }
  client.destroy();
  delete param.schema_mgr_;
  return err;
}



