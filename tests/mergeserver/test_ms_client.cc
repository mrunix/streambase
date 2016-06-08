/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_ms_client.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include <getopt.h>
#include <string>
#include <unistd.h>
#include "../updateserver/mock_client.h"
#include "../updateserver/test_utils.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"

using namespace std;
using namespace sb;
using namespace sb::common;
const int64_t TIMEOUT =  10000000L;

struct CParam {
  char* server_addr_;
  int32_t server_port_;
  char* schema_fname_;
  char* table_name_;
  int64_t add_count_;
  int64_t start_key_;
  int64_t end_key_;
  bool read_master_;
  sb::common::ObSchemaManager* schema_mgr_;
};


void print_usage() {
  fprintf(stderr, "\n");
  fprintf(stderr, "ms get client[OPTION]\n");
  fprintf(stderr, "   -a|--addr server address\n");
  fprintf(stderr, "   -p|--port server port\n");
  fprintf(stderr, "   -r|--read master instance\n");
  fprintf(stderr, "   -m|--schema schema file\n");
  fprintf(stderr, "   -s|--start start rowkey must int type\n");
  fprintf(stderr, "   -e|--end end rowkey must int type\n");
  fprintf(stderr, "   -t|--table check join table name\n");
  fprintf(stderr, "   -h|--help print this help info\n");
  fprintf(stderr, "\n");
}

int parse_cmd_args(int argc, char** argv, CParam& param) {
  int err = OB_SUCCESS;
  int opt = 0;
  const char* opt_string = "a:p:c:m:t:s:e:rh";
  struct option longopts[] = {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'p'},
    {"count", 1, NULL, 'c'},
    {"master", 0, NULL, 'r'},
    {"schema", 1, NULL, 'm'},
    {"table", 1, NULL, 't'},
    {"start", 1, NULL, 's'},
    {"end", 1, NULL, 'e'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  memset(&param, 0, sizeof(param));
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
    case 'a':
      param.server_addr_ = optarg;
      break;
    case 'p':
      param.server_port_ = atoi(optarg);
      break;
    case 'm':
      param.schema_fname_ = optarg;
      break;
    case 't':
      param.table_name_ = optarg;
      break;
    case 'r':
      param.read_master_ = true;
      break;
    case 'c':
      param.add_count_ = atol(optarg);
      break;
    case 's':
      param.start_key_ = atol(optarg);
      break;
    case 'e':
      param.end_key_ = atol(optarg);
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
      || NULL == param.table_name_
      || 0 > param.add_count_
      || 0 > param.start_key_
      || param.start_key_ >= param.end_key_
     ) {
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
  return err;
}


int check_result(const int64_t count, const ObSchema* schema,
                 const ObString& table_name, const ObString& rowkey, ObScanner& scanner) {
  int err = OB_SUCCESS;
  if (NULL == schema) {
    TBSYS_LOG(ERROR, "check schema failed");
    err = OB_ERROR;
  } else {
    int64_t row_key = 0;
    uint64_t column_id = 0;
    int64_t value = 0;
    const ObColumnSchema* column = NULL;
    ObCellInfo* cur_cell = NULL;
    while (scanner.next_cell() == OB_SUCCESS) {
      err = scanner.get_cell(&cur_cell);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(ERROR, "get cell failed:ret[%d]", err);
        break;
      } else {
        if ((cur_cell->value_.get_type() != ObIntType)
            || (cur_cell->table_name_ != table_name)
            || (cur_cell->row_key_ != rowkey)) {
          TBSYS_LOG(ERROR, "check table name rowkey or type failed");
          hex_dump(cur_cell->row_key_.ptr(), cur_cell->row_key_.length());
          cur_cell->value_.dump();
          err = OB_ERROR;
          break;
        } else {
          column = schema->find_column_info(cur_cell->column_name_);
          if (NULL == column) {
            TBSYS_LOG(ERROR, "find column info failed");
            err = OB_ERROR;
            break;
          }
          column_id = column->get_id();
          int64_t pos = 0;
          serialization::decode_i64(cur_cell->row_key_.ptr(), cur_cell->row_key_.length(), pos, &row_key);
          cur_cell->value_.get_int(value);
          // TBSYS_LOG(INFO, "check value:value[%ld], rowkey[%ld], column[%lu]", value, row_key, column_id);
          if ((uint64_t)value != (((row_key << 24) | (column_id << 16)) | (count + 1))) {
            TBSYS_LOG(ERROR, "check value failed:cell[%ld], check[%ld], rowkey[%ld], column[%lu], count[%ld]",
                      value, (((row_key << 24) | (column_id << 16)) | (count + 1)), row_key, column_id, count);
            err = OB_ERROR;
            break;
          }
        }
      }
    }
  }
  return err;
}

int get(CParam& param, MockClient& client) {
  int err = OB_SUCCESS;
  ObString table_name;
  table_name.assign(param.table_name_, strlen(param.table_name_));
  const ObSchema* schema = NULL;
  uint64_t table_id = param.schema_mgr_->get_table_id(table_name);
  if (table_id == 0) {
    TBSYS_LOG(ERROR, "fail to find table id:name[%s]", param.table_name_);
    err = OB_ERROR;
  } else {
    schema = param.schema_mgr_->get_table_schema(table_id);
    if (NULL == schema) {
      TBSYS_LOG(ERROR, "check schema failed:id[%lu]", table_id);
      err = OB_ERROR;
    }
  }
  if (OB_SUCCESS == err) {
    ObString rowkey_str;
    ObString column_str;
    ObObj value_obj;
    ObCellInfo cell[128];
    ObScanner scanner;
    char buffer[128];
    const ObColumnSchema* column_info = NULL;
    for (int64_t i = param.start_key_; i <= param.end_key_; ++i) {
      ObGetParam get_param;
      ObVersionRange version_range;
      version_range.border_flag_.set_min_value();
      version_range.border_flag_.set_max_value();
      get_param.set_version_range(version_range);
      get_param.set_is_result_cached(true);
      get_param.set_is_read_consistency(param.read_master_);
      int64_t pos = 0;
      serialization::encode_i64(buffer, sizeof(buffer), pos, i);
      rowkey_str.assign(buffer, pos);
      int count = 0;
      for (column_info = schema->column_begin(); column_info != schema->column_end(); ++column_info) {
        if (NULL == column_info) {
          TBSYS_LOG(ERROR, "check column info failed:table[%lu], rowkey[%ld]", table_id, i);
          err = OB_ERROR;
          break;
        }
        column_str.assign(const_cast<char*>(column_info->get_name()), strlen(column_info->get_name()));
        cell[count].table_name_ = table_name;
        cell[count].row_key_ = rowkey_str;
        cell[count].column_name_ = column_str;
        if (column_info->get_type() == ObIntType) {
          err = get_param.add_cell(cell[count]);
          if (err != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "add cell failed:table[%lu], rowkey[%ld], ret[%d]", table_id, i, err);
            break;
          }
        }
        ++count;
      }
      if (err == OB_SUCCESS) {
        err = client.ups_get(get_param, scanner, TIMEOUT);
        if (err != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "check get failed:table[%lu], rowkey[%ld], ret[%d]", table_id, i, err);
          break;
        } else {
          err = check_result(param.add_count_, schema, table_name, rowkey_str, scanner);
          if (err != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "check result failed:table[%lu], rowkey[%ld], ret[%d]", table_id, i, err);
            break;
          } else {
            TBSYS_LOG(INFO, "check result succ:table[%lu], rowkey[%ld]", table_id, i);
          }
        }
      } else {
        break;
      }
    }
  }
  return err;
}


void init_mock_client(const char* addr, int32_t port, MockClient& client) {
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  client.init(dst_host);
}

int main(int argc, char** argv) {
  int err = OB_SUCCESS;
  ob_init_memory_pool();
  CParam param;
  err = parse_cmd_args(argc, argv, param);
  MockClient client;
  if (OB_SUCCESS == err) {
    init_mock_client(param.server_addr_, param.server_port_, client);
    err = get(param, client);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "check get failed:ret[%d]", err);
    }
  }
  client.destroy();
  delete param.schema_mgr_;
  return err;
}



