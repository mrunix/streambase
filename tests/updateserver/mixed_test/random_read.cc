/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * random_read.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include <getopt.h>
#include "common/ob_malloc.h"
#include "mutator_builder.h"
#include "../test_utils.h"
#include "../mock_client.h"
#include "utils.h"
#include "row_checker.h"

using namespace sb;
using namespace common;

struct CmdLine {
  static const int64_t DEFAULT_TABLE_START_VERSION = 2;
  static const int32_t DEFAULT_PREFIX_START = 1;
  static const int32_t DEFAULT_SUFFIX_LENGTH = 20;
  static const int32_t DEFAULT_TEST_TIMES = 10240;
  static const int32_t DEFAULT_ROW_NUM = 1024;
  static const int32_t DEFAULT_CELL_NUM = 10;
  static const int32_t DEFAULT_MAX_CELL = 32;

  char* serv_addr;
  int32_t serv_port;
  char* root_addr;
  int32_t root_port;
  bool using_id;
  bool get2read;
  int64_t table_start_version;
  char* schema_file;
  int32_t prefix_start;
  int32_t suffix_length;
  int32_t test_times;
  int32_t row_num;
  int32_t cell_num;
  int32_t max_cell;
  bool check;

  CmdLine() {
    serv_addr = NULL;
    serv_port = -1;
    root_addr = NULL;
    root_port = -1;
    using_id = false;
    get2read = false;
    table_start_version = DEFAULT_TABLE_START_VERSION;
    schema_file = NULL;
    prefix_start = DEFAULT_PREFIX_START;
    suffix_length = DEFAULT_SUFFIX_LENGTH;
    test_times = DEFAULT_TEST_TIMES;
    row_num = DEFAULT_ROW_NUM;
    cell_num = DEFAULT_CELL_NUM;
    max_cell = DEFAULT_MAX_CELL;
    check = false;
  };

  void log_all() {
    TBSYS_LOG(INFO, "serv_addr=%s serv_port=%d root_addr=%s root_port=%d using_id=%d get2read=%d table_start_version=%ld "
              "schema_file=%s prefix_start=%d suffix_length=%d max_cell=%d "
              "test_times=%d row_num=%d cell_num=%d check=%d",
              serv_addr, serv_port, root_addr, root_port, using_id, get2read, table_start_version,
              schema_file, prefix_start, suffix_length, max_cell,
              test_times, row_num, cell_num, check);
  };

  bool is_valid() {
    bool bret = false;
    if (NULL != serv_addr
        && 0 < serv_port
        && ((NULL != root_addr
             && 0 < root_port)
            || NULL != schema_file)) {
      bret = true;
    }
    return bret;
  };
};

void print_usage() {
  fprintf(stderr, "\n");
  fprintf(stderr, "random_read [OPTION]\n");
  fprintf(stderr, "   -a|--serv_addr updateserver/mergeserver address\n");
  fprintf(stderr, "   -p|--serv_port updateserver/mergeserver port\n");
  fprintf(stderr, "   -r|--root_addr nameserver address\n");
  fprintf(stderr, "   -o|--root_port nameserver port\n");
  fprintf(stderr, "   -i|--using_id using table_id and column_id to scan mergeserver[if not set, default using name]\n");
  fprintf(stderr, "   -d|--get2read using get protocol to read[if not set, default using scan protocol to read]\n\n");

  fprintf(stderr, "   -t|--table_start_version to read from updateserver[default %ld]\n", CmdLine::DEFAULT_TABLE_START_VERSION);
  fprintf(stderr, "   -f|--schema_file if schema_file set, will not fetch from nameserver\n");
  fprintf(stderr, "   -s|--prefix_start rowkey prefix start number, must be same as mutil_write[default %d]\n", CmdLine::DEFAULT_PREFIX_START);
  fprintf(stderr, "   -l|--suffix_length rowkey suffix length, must be same as multi_write[default %d]\n", CmdLine::DEFAULT_SUFFIX_LENGTH);
  fprintf(stderr, "   -c|--max_cell max cell number per row-mutate, must be same as multi_write[default %d]\n", CmdLine::DEFAULT_MAX_CELL);
  fprintf(stderr, "   -k|--check check the read result or not\n\n");

  fprintf(stderr, "   -m|--test_times random get test times[default %d], support MAX\n", CmdLine::DEFAULT_TEST_TIMES);
  fprintf(stderr, "   -u|--row_num row number of each random get test[default %d]\n", CmdLine::DEFAULT_ROW_NUM);
  fprintf(stderr, "   -n|--cell_num max cell number to get of each row[default %d]\n", CmdLine::DEFAULT_CELL_NUM);

  fprintf(stderr, "\n");
}

void parse_cmd_line(int argc, char** argv, CmdLine& clp) {
  int opt = 0;
  const char* opt_string = "a:p:r:o:idt:f:s:l:m:u:n:c:kh";
  struct option longopts[] = {
    {"serv_addr", 1, NULL, 'a'},
    {"serv_port", 1, NULL, 'p'},
    {"root_addr", 1, NULL, 'r'},
    {"root_port", 1, NULL, 'o'},
    {"using_id", 0, NULL, 'i'},
    {"get2read", 0, NULL, 'd'},
    {"table_start_version", 1, NULL, 't'},
    {"schema_file", 1, NULL, 'f'},
    {"prefix_start", 1, NULL, 's'},
    {"suffix_length", 1, NULL, 'l'},
    {"test_times", 1, NULL, 'm'},
    {"row_num", 1, NULL, 'u'},
    {"cell_num", 1, NULL, 'n'},
    {"max_cell", 1, NULL, 'c'},
    {"check", 1, NULL, 'k'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
    case 'a':
      clp.serv_addr = optarg;
      break;
    case 'p':
      clp.serv_port = atoi(optarg);
      break;
    case 'r':
      clp.root_addr = optarg;
      break;
    case 'o':
      clp.root_port = atoi(optarg);
      break;
    case 'i':
      clp.using_id = true;
      break;
    case 'd':
      clp.get2read = true;
      break;
    case 't':
      clp.table_start_version = atol(optarg);
      break;
    case 'f':
      clp.schema_file = optarg;
      break;
    case 's':
      clp.prefix_start = atoi(optarg);
      break;
    case 'l':
      clp.suffix_length = atoi(optarg);
      break;
    case 'm':
      if (0 == strcmp("MAX", optarg)) {
        clp.test_times = INT32_MAX;
      } else {
        clp.test_times = atoi(optarg);
      }
      break;
    case 'u':
      clp.row_num = atoi(optarg);
      break;
    case 'n':
      clp.cell_num = atoi(optarg);
      break;
    case 'c':
      clp.max_cell = atoi(optarg);
      break;
    case 'k':
      clp.check = true;
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }
  if (!clp.is_valid()) {
    print_usage();
    exit(-1);
  }
}

int fetch_schema(const char* root_addr, const int32_t root_port, ObSchemaManager& schema_mgr) {
  int ret = OB_SUCCESS;
  ObServer dst_host;
  dst_host.set_ipv4_addr(root_addr, root_port);
  MockClient client;
  client.init(dst_host);
  ret = client.fetch_schema(0, schema_mgr, TIMEOUT_MS);
  client.destroy();
  return ret;
}

void trans_name2id(ObCellInfo& ci, const ObSchema& schema) {
  ci.table_id_ = schema.get_table_id();
  if (NULL == ci.column_name_.ptr()) {
    ci.column_id_ = OB_INVALID_ID;
  } else {
    ci.column_id_ = schema.find_column_info(ci.column_name_)->get_id();
  }
}

bool get_check_row(const ObSchema& schema, const ObString& row_key, CellinfoBuilder& cb,
                   MockClient& client, const int64_t table_start_version, const bool using_id) {
  bool bret = false;

  ObGetParam get_param;
  const ObColumnSchema* iter = NULL;
  for (iter = schema.column_begin(); iter != schema.column_end(); iter++) {
    ObCellInfo ci;
    ci.row_key_ = row_key;
    if (using_id) {
      ci.table_id_ = schema.get_table_id();
      ci.column_id_ = iter->get_id();
    } else {
      ci.table_name_.assign_ptr(const_cast<char*>(schema.get_table_name()), strlen(schema.get_table_name()));
      ci.column_name_.assign_ptr(const_cast<char*>(iter->get_name()), strlen(iter->get_name()));
    }
    get_param.add_cell(ci);
  }
  ObVersionRange version_range;
  version_range.start_version_ = table_start_version;
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  get_param.set_version_range(version_range);

  ObScanner scanner;
  int ret = client.ups_get(get_param, scanner, TIMEOUT_MS);
  if (OB_SUCCESS == ret) {
    RowChecker rc;
    while (OB_SUCCESS == scanner.next_cell()) {
      ObCellInfo* ci = NULL;
      if (OB_SUCCESS == scanner.get_cell(&ci)) {
        if (!using_id) {
          trans_name2id(*ci, schema);
        }
        rc.add_cell(ci);
      }
    }
    bret = rc.check_row(cb, schema);
  } else {
    TBSYS_LOG(WARN, "get ret=%d", ret);
  }
  return bret;
}

int random_scan_check(MutatorBuilder& mb, MockClient& client, const int64_t table_start_version, const bool using_id, const bool check) {
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t prefix = 0;
  ObScanParam scan_param;
  PageArena<char> allocer;
  ret = mb.build_scan_param(scan_param, allocer, table_start_version, using_id, i, prefix);
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  RowChecker rc;
  int64_t row_counter = 0;
  ObScanner scanner;
  while (!is_fullfilled
         && OB_SUCCESS == ret) {
    int64_t timeu = tbsys::CTimeUtil::getTime();
    ret = client.ups_scan(scan_param, scanner, TIMEOUT_MS);
    TBSYS_LOG(INFO, "server_scan ret=%d timeu=%ld", ret, tbsys::CTimeUtil::getTime() - timeu);
    if (check
        && OB_SUCCESS == ret) {
      while (OB_SUCCESS == scanner.next_cell()) {
        ObCellInfo* ci = NULL;
        bool is_row_changed = false;
        if (OB_SUCCESS == scanner.get_cell(&ci, &is_row_changed)) {
          if (!using_id) {
            trans_name2id(*ci, mb.get_schema(i));
          }
          if (is_row_changed && 0 != rc.cell_num()) {
            std::string row_key_str(rc.get_cur_rowkey().ptr(), 0, rc.get_cur_rowkey().length());
            //fprintf(stderr, "[%.*s] ", rc.get_cur_rowkey().length(), rc.get_cur_rowkey().ptr());

            bool get_row_bret = get_check_row(mb.get_schema(i), rc.get_cur_rowkey(), mb.get_cellinfo_builder(i), client, table_start_version, using_id);
            //fprintf(stderr, "[get_row check_ret=%d] ", bret);

            bool cell_info_bret = rc.check_row(mb.get_cellinfo_builder(i), mb.get_schema(i));
            //fprintf(stderr, "[cell_info check_ret=%d]\n", bret);
            TBSYS_LOG(INFO, "[%s] [get_row check_ret=%d] [cell_info check_ret=%d]", row_key_str.c_str(), get_row_bret, cell_info_bret);
          }
          if (is_row_changed
              && 0 != rc.rowkey_num()
              && rc.is_prefix_changed(ci->row_key_)) {
            std::string row_key_str(rc.get_last_rowkey().ptr(), 0, rc.get_last_rowkey().length());
            //fprintf(stderr, "[%.*s] ", rc.get_last_rowkey().length(), rc.get_last_rowkey().ptr());

            bool bret = rc.check_rowkey(mb.get_rowkey_builder(i), &prefix);
            //fprintf(stderr, "row_key check_ret=%d\n", bret);
            TBSYS_LOG(INFO, "[%s] [row_key check_ret=%d]", row_key_str.c_str(), bret);
          }
          rc.add_cell(ci);
          if (is_row_changed) {
            rc.add_rowkey(ci->row_key_);
            row_counter++;
          }
        }
      }
      if (0 != rc.cell_num()) {
        std::string row_key_str(rc.get_cur_rowkey().ptr(), 0, rc.get_cur_rowkey().length());
        //fprintf(stderr, "[%.*s] ", rc.get_cur_rowkey().length(), rc.get_cur_rowkey().ptr());

        bool get_row_bret = get_check_row(mb.get_schema(i), rc.get_cur_rowkey(), mb.get_cellinfo_builder(i), client, table_start_version, using_id);
        //fprintf(stderr, "[get_row check_ret=%d] ", bret);

        bool cell_info_bret = rc.check_row(mb.get_cellinfo_builder(i), mb.get_schema(i));
        //fprintf(stderr, "[cell_info check_ret=%d]\n", bret);
        TBSYS_LOG(INFO, "[%s] [get_row check_ret=%d] [cell_info check_ret=%d]", row_key_str.c_str(), get_row_bret, cell_info_bret);
      }
    }
    scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
    ObRange* range = const_cast<ObRange*>(scan_param.get_range());
    scanner.get_last_row_key(range->start_key_);
    range->border_flag_.unset_min_value();
    range->border_flag_.unset_inclusive_start();
  }
  if (check
      && 0 != rc.rowkey_num()) {
    std::string row_key_str(rc.get_last_rowkey().ptr(), 0, rc.get_last_rowkey().length());
    //fprintf(stderr, "[%.*s] ", rc.get_last_rowkey().length(), rc.get_last_rowkey().ptr());

    bool bret = rc.check_rowkey(mb.get_rowkey_builder(i), &prefix);
    //fprintf(stderr, "row_key check_ret=%d\n", bret);
    TBSYS_LOG(INFO, "[%s] [row_key check_ret=%d]", row_key_str.c_str(), bret);
  }
  TBSYS_LOG(INFO, "table_id=%lu row_counter=%ld", mb.get_schema(i).get_table_id(), row_counter);
  return ret;
}

int random_get_check(MutatorBuilder& mb, MockClient& client, const int64_t table_start_version, const bool using_id,
                     const int64_t row_num, const int64_t cell_num_per_row, const bool check) {
  int ret = OB_SUCCESS;
  ObScanner scanner;
  ObGetParam get_param;
  PageArena<char> allocer;
  mb.build_get_param(get_param, allocer, table_start_version, using_id, row_num, cell_num_per_row);
  int64_t timeu = tbsys::CTimeUtil::getTime();
  ret = client.ups_get(get_param, scanner, TIMEOUT_MS);
  TBSYS_LOG(INFO, "server_get ret=%d timeu=%ld", ret, tbsys::CTimeUtil::getTime() - timeu);
  if (check
      && OB_SUCCESS == ret) {
    RowChecker rc;
    int64_t schema_pos = -1;
    int64_t row_counter = 0;
    //ObCellInfo cc;
    while (OB_SUCCESS == scanner.next_cell()) {
      ObCellInfo* ci = NULL;
      bool is_row_changed = false;
      if (OB_SUCCESS == scanner.get_cell(&ci, &is_row_changed)) {
        if (is_row_changed) {
          row_counter++;
        }
        if (is_row_changed
            && ObActionFlag::OP_ROW_DOES_NOT_EXIST == ci->value_.get_ext()) {
          TBSYS_LOG(WARN, "[%.*s] not exist", ci->row_key_.length(), ci->row_key_.ptr());
          continue;
        }
        int64_t tmp_schema_pos = -1;
        if (!using_id) {
          tmp_schema_pos = mb.get_schema_pos(ci->table_name_);
          trans_name2id(*ci, mb.get_schema(tmp_schema_pos));
        } else {
          tmp_schema_pos = mb.get_schema_pos(ci->table_id_);
        }
        if (is_row_changed && 0 != rc.cell_num()) {
          std::string row_key_str(rc.get_cur_rowkey().ptr(), 0, rc.get_cur_rowkey().length());
          //fprintf(stderr, "[%.*s] ", rc.get_cur_rowkey().length(), rc.get_cur_rowkey().ptr());

          bool get_row_bret = get_check_row(mb.get_schema(schema_pos), rc.get_cur_rowkey(), mb.get_cellinfo_builder(schema_pos), client, table_start_version, using_id);
          //fprintf(stderr, "[get_row check_ret=%d] ", bret);

          bool cell_info_bret = rc.check_row(mb.get_cellinfo_builder(schema_pos), mb.get_schema(schema_pos));
          //fprintf(stderr, "[cell_info check_ret=%d]\n", bret);
          TBSYS_LOG(INFO, "[%s] [get_row check_ret=%d] [cell_info check_ret=%d]", row_key_str.c_str(), get_row_bret, cell_info_bret);
        }
        if (ObActionFlag::OP_NOP != ci->value_.get_ext()) {
          schema_pos = tmp_schema_pos;
          rc.add_cell(ci);
          //cc = *ci;
        }
      }
    }
    bool is_fullfilled = false;
    int64_t fullfilled_item_num = 0;
    scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
    TBSYS_LOG(INFO, "row_counter=%ld fullfilled_item_num=%ld get_param_cell_size=%ld get_param_row_size=%ld is_fullfilled=%d "
              "row_is_equal=%d cell_is_equal=%d",
              row_counter, fullfilled_item_num, get_param.get_cell_size(), get_param.get_row_size(), is_fullfilled,
              !is_fullfilled || row_counter == get_param.get_row_size(),
              !is_fullfilled || fullfilled_item_num == get_param.get_cell_size());
    if (!is_fullfilled
        && 0 != rc.cell_num()) {
      std::string row_key_str(rc.get_cur_rowkey().ptr(), 0, rc.get_cur_rowkey().length());
      //fprintf(stderr, "[%.*s] ", rc.get_cur_rowkey().length(), rc.get_cur_rowkey().ptr());

      bool get_row_bret = get_check_row(mb.get_schema(schema_pos), rc.get_cur_rowkey(), mb.get_cellinfo_builder(schema_pos), client, table_start_version, using_id);
      //fprintf(stderr, "[get_row check_ret=%d] ", bret);

      bool cell_info_bret = rc.check_row(mb.get_cellinfo_builder(schema_pos), mb.get_schema(schema_pos));
      //fprintf(stderr, "[cell_info check_ret=%d]\n", bret);
      TBSYS_LOG(INFO, "[%s] [get_row check_ret=%d] [cell_info check_ret=%d]", row_key_str.c_str(), get_row_bret, cell_info_bret);
    }
  }
  return ret;
}

int main(int argc, char** argv) {
  CmdLine clp;
  parse_cmd_line(argc, argv, clp);

  TBSYS_LOGGER.rotateLog("random_read.log");
  TBSYS_LOGGER.setFileName("random_read.log");
  TBSYS_LOGGER.setLogLevel("info");
  clp.log_all();

  ob_init_memory_pool();

  ObSchemaManager schema_mgr;
  if (NULL != clp.schema_file) {
    tbsys::CConfig config;
    if (!schema_mgr.parse_from_file(clp.schema_file, config)) {
      TBSYS_LOG(WARN, "parse schema fail");
      exit(-1);
    }
  } else if (OB_SUCCESS != fetch_schema(clp.root_addr, clp.root_port, schema_mgr)) {
    TBSYS_LOG(WARN, "fetch schema fail");
    exit(-1);
  }
  schema_mgr.print_info();

  MutatorBuilder mb;
  mb.init(schema_mgr, clp.prefix_start, clp.suffix_length, clp.serv_addr, clp.serv_port, clp.table_start_version, clp.max_cell);

  ObServer dst_host;
  dst_host.set_ipv4_addr(clp.serv_addr, clp.serv_port);
  MockClient client;
  client.init(dst_host);
  for (int32_t i = 0; i < clp.test_times; i++) {
    if (clp.get2read) {
      int ret = random_get_check(mb, client, clp.table_start_version, clp.using_id, clp.row_num, clp.cell_num, clp.check);
      TBSYS_LOG(INFO, "random_get_check ret=%d", ret);
    } else {
      int ret = random_scan_check(mb, client, clp.table_start_version, clp.using_id, clp.check);
      TBSYS_LOG(INFO, "random_scan_check ret=%d", ret);
    }
  }
  client.destroy();
}


