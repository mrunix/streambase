/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_ups_client.cc for ...
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
  int64_t start_key_;
  int64_t end_key_;
  int64_t count_;
  sb::common::ObSchemaManager* schema_mgr_;
};


void print_usage() {
  fprintf(stderr, "\n");
  fprintf(stderr, "batch_client [OPTION]\n");
  fprintf(stderr, "   -a|--addr server address\n");
  fprintf(stderr, "   -p|--port server port\n");
  fprintf(stderr, "   -m|--schema schema file\n");
  fprintf(stderr, "   -c|--count add count\n");
  fprintf(stderr, "   -s|--start start rowkey must int type\n");
  fprintf(stderr, "   -e|--end end rowkey must int type\n");
  fprintf(stderr, "   -t|--table apply update table name\n");
  fprintf(stderr, "   -h|--help print this help info\n");
  fprintf(stderr, "\n");
}

int parse_cmd_args(int argc, char** argv, CParam& param) {
  int err = OB_SUCCESS;
  int opt = 0;
  memset(&param, 0x00, sizeof(param));
  const char* opt_string = "a:p:c:m:t:s:e:h";
  struct option longopts[] = {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'p'},
    {"schema", 1, NULL, 'm'},
    {"table", 1, NULL, 't'},
    {"start", 1, NULL, 's'},
    {"count", 1, NULL, 'c'},
    {"end", 1, NULL, 'e'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
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
    case 's':
      param.start_key_ = atol(optarg);
      break;
    case 'e':
      param.end_key_ = atol(optarg);
      break;
    case 'c':
      param.count_ = atol(optarg);
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
      || 0 > param.count_
      || 0 > param.start_key_
      || param.start_key_ > param.end_key_
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

int apply(CParam& param, MockClient& client) {
  int err = OB_SUCCESS;
  ObString table_name;
  table_name.assign(param.table_name_, strlen(param.table_name_));
  uint64_t table_id = param.schema_mgr_->get_table_id(table_name);
  if (table_id == 0) {
    TBSYS_LOG(ERROR, "fail to find table id:name[%s]", param.table_name_);
    err = OB_ERROR;
  } else {
    const ObSchema* schema = param.schema_mgr_->get_table_schema(table_id);
    if (NULL == schema) {
      TBSYS_LOG(ERROR, "check schema failed:id[%lu]", table_id);
      err = OB_ERROR;
    } else {
      ObString rowkey_str;
      ObString column_str;
      ObMutator mutator;
      ObObj value_obj;
      int64_t column_id = 0;
      char buffer[128];
      const ObColumnSchema* column_info = NULL;
      for (int64_t i = param.start_key_; i <= param.end_key_; ++i) {
        int64_t pos = 0;
        serialization::encode_i64(buffer, sizeof(buffer), pos, i);
        rowkey_str.assign(buffer, pos);
        for (column_info = schema->column_begin(); column_info != schema->column_end(); ++column_info) {
          mutator.reset();
          if (NULL == column_info) {
            TBSYS_LOG(ERROR, "check column info failed:table[%lu], rowkey[%lu]", table_id, i);
            err = OB_ERROR;
            break;
          }
          column_id = column_info->get_id();
          column_str.assign(const_cast<char*>(column_info->get_name()), strlen(column_info->get_name()));
          if (column_info->get_type() == ObIntType) {
            //value_obj.set_int(1, true);
            value_obj.set_int(((i << 24) | (column_id << 16)) | (param.count_ + 1));
            err = mutator.update(table_name, rowkey_str, column_str, value_obj);
            if (OB_SUCCESS == err) {
              err = client.ups_apply(mutator, TIMEOUT);
              TBSYS_LOG(DEBUG, "update table:%.*s rowkey:%ld column:%.*s err:%d",
                        table_name.length(), table_name.ptr(), i,
                        column_str.length(), column_str.ptr(), err);
            } else {
              TBSYS_LOG(ERROR, "update table:%.*s rowkey:%ld column:%.*s err:%d",
                        table_name.length(), table_name.ptr(), i,
                        column_str.length(), column_str.ptr(), err);
              break;
            }
          }
          /*
          else
          {
            TBSYS_LOG(WARN, "check column type failed:table:%.*s rowkey:%.*s column:%.*s type:%lu",
                table_name.length(), table_name.ptr(),
                rowkey_str.length(), rowkey_str.ptr(), column_str.length(), column_str.ptr(), column_info->get_type());
          }
          */
        }
        //
        if (err != OB_SUCCESS) {
          break;
        }
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
    err = apply(param, client);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "check apply mutation failed:ret[%d]", err);
    }
  }
  client.destroy();
  delete param.schema_mgr_;
  return err;
}



