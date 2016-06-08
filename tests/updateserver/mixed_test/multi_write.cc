/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * multi_write.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include <getopt.h>
#include "common/ob_malloc.h"
#include "mutator_builder.h"
#include "../test_utils.h"
#include "utils.h"

using namespace sb;
using namespace common;

struct CmdLine {
  static const int64_t DEFAULT_TABLE_START_VERSION = 2;
  static const int32_t DEFAULT_PREFIX_START = 1;
  static const int32_t DEFAULT_PREFIX_END = 1024;
  static const int32_t DEFAULT_MUTATOR_NUM = 1024;
  static const int32_t DEFAULT_SUFFIX_LENGTH = 20;
  static const int32_t DEFAULT_MAX_ROW = 128;
  static const int32_t DEFAULT_MAX_SUFFIX = 32;
  static const int32_t DEFAULT_MAX_CELL = 32;

  char* serv_addr;
  int32_t serv_port;
  char* root_addr;
  int32_t root_port;
  char* merge_addr;
  int32_t merge_port;
  bool using_id;
  int64_t table_start_version;
  char* schema_file;
  int32_t prefix_start;
  int32_t prefix_end;
  int32_t mutator_num;
  int32_t suffix_length;

  int32_t max_row;
  int32_t max_suffix;
  int32_t max_cell;

  void log_all() {
    TBSYS_LOG(INFO, "serv_addr=%s serv_port=%d root_addr=%s root_port=%d merge_addr=%s merge_addr=%d using_id=%d "
              "table_start_version=%ld schema_file=%s prefix_start=%d prefix_end=%d "
              "mutator_num=%d suffix_length=%d "
              "max_row=%d max_suffix=%d max_cell=%d",
              serv_addr, serv_port, root_addr, root_port, merge_addr, merge_port, using_id,
              table_start_version, schema_file, prefix_start, prefix_end,
              mutator_num, suffix_length,
              max_row, max_suffix, max_cell);
  };

  CmdLine() {
    serv_addr = NULL;
    serv_port = -1;
    root_addr = NULL;
    root_port = -1;
    merge_addr = NULL;
    merge_port = -1;
    using_id = false;
    schema_file = NULL;
    table_start_version = DEFAULT_TABLE_START_VERSION;
    prefix_start = DEFAULT_PREFIX_START;
    prefix_end = DEFAULT_PREFIX_END;
    mutator_num = DEFAULT_MUTATOR_NUM;
    suffix_length = DEFAULT_SUFFIX_LENGTH;
    max_row = DEFAULT_MAX_ROW;
    max_suffix = DEFAULT_MAX_SUFFIX;
    max_cell = DEFAULT_MAX_CELL;
  };

  bool is_valid() {
    bool bret = false;
    if (NULL != serv_addr
        && 0 < serv_port
        && ((NULL != root_addr
             && 0 < root_port)
            || NULL != schema_file)
        && NULL != merge_addr
        && 0 < merge_port) {
      bret = true;
    }
    return bret;
  };
};

void print_usage() {
  fprintf(stderr, "\n");
  fprintf(stderr, "multi_write [OPTION]\n");
  fprintf(stderr, "   -a|--serv_addr updateserver address\n");
  fprintf(stderr, "   -p|--serv_port updateserver port\n");
  fprintf(stderr, "   -r|--root_addr nameserver address\n");
  fprintf(stderr, "   -o|--root_port nameserver port\n");
  fprintf(stderr, "   -d|--merge_addr mergeserver address\n");
  fprintf(stderr, "   -g|--merge_port mergeserver port\n");
  fprintf(stderr, "   -i|--using_id using table_id and column_id to scan mergeserver[if not set, default using name]\n\n");

  fprintf(stderr, "   -t|--table_start_version to read from updateserver[default %ld]\n", CmdLine::DEFAULT_TABLE_START_VERSION);
  fprintf(stderr, "   -f|--schema_file if schema_file set, will not fetch from nameserver\n");
  fprintf(stderr, "   -s|--prefix_start rowkey prefix start number[default %d]\n", CmdLine::DEFAULT_PREFIX_START);
  fprintf(stderr, "   -e|--prefix_end rowkey prefix start number[default %d], support MAX and 0\n", CmdLine::DEFAULT_PREFIX_END);
  fprintf(stderr, "   -n|--mutator_num total mutator number to apply[default %d], support MAX\n", CmdLine::DEFAULT_MUTATOR_NUM);
  fprintf(stderr, "   -l|--suffix_length rowkey suffix length[default %d]\n", CmdLine::DEFAULT_SUFFIX_LENGTH);
  fprintf(stderr, "   -m|--max_row max row number per mutator[default %d]\n", CmdLine::DEFAULT_MAX_ROW);
  fprintf(stderr, "   -u|--max_suffix max suffix number per prefix[default %d]\n", CmdLine::DEFAULT_MAX_SUFFIX);
  fprintf(stderr, "   -c|--max_cell max cell number per row-mutate[default %d]\n", CmdLine::DEFAULT_MAX_CELL);

  fprintf(stderr, "\n");
}

void parse_cmd_line(int argc, char** argv, CmdLine& clp) {
  int opt = 0;
  const char* opt_string = "a:p:r:o:d:g:it:f:s:e:n:l:m:u:c:h";
  struct option longopts[] = {
    {"serv_addr", 1, NULL, 'a'},
    {"serv_port", 1, NULL, 'p'},
    {"root_addr", 1, NULL, 'r'},
    {"root_port", 1, NULL, 'o'},
    {"merge_addr", 1, NULL, 'd'},
    {"merge_port", 1, NULL, 'g'},
    {"using_id", 0, NULL, 'i'},
    {"table_start_version", 1, NULL, 't'},
    {"schema_file", 1, NULL, 'f'},
    {"prefix_start", 1, NULL, 's'},
    {"prefix_end", 1, NULL, 'e'},
    {"mutator_num", 1, NULL, 'n'},
    {"suffix_length", 1, NULL, 'l'},
    {"max_row", 1, NULL, 'm'},
    {"max_suffix", 1, NULL, 'u'},
    {"max_cell", 1, NULL, 'c'},
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
    case 'd':
      clp.merge_addr = optarg;
      break;
    case 'g':
      clp.merge_port = atoi(optarg);
      break;
    case 'i':
      clp.using_id = true;
      break;
    case 't':
      clp.table_start_version = atoi(optarg);
      break;
    case 'f':
      clp.schema_file = optarg;
      break;
    case 's':
      clp.prefix_start = atoi(optarg);
      break;
    case 'e':
      if (0 == strcmp("MAX", optarg)) {
        clp.prefix_end = INT32_MAX;
      } else {
        clp.prefix_end = atoi(optarg);
      }
      break;
    case 'n':
      if (0 == strcmp("MAX", optarg)) {
        clp.mutator_num = INT32_MAX;
      } else {
        clp.mutator_num = atoi(optarg);
      }
      break;
    case 'l':
      clp.suffix_length = atoi(optarg);
      break;
    case 'm':
      clp.max_row = atoi(optarg);
      break;
    case 'u':
      clp.max_suffix = atoi(optarg);
      break;
    case 'c':
      clp.max_cell = atoi(optarg);
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

int main(int argc, char** argv) {
  CmdLine clp;
  parse_cmd_line(argc, argv, clp);

  TBSYS_LOGGER.rotateLog("multi_write.log");
  TBSYS_LOGGER.setFileName("multi_write.log");
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
  mb.init(schema_mgr,
          clp.prefix_start, clp.suffix_length,
          clp.merge_addr, clp.merge_port, clp.table_start_version,
          clp.max_cell, clp.max_row, clp.max_suffix);
  if (OB_SUCCESS != mb.init_prefix_end(clp.prefix_end)) {
    TBSYS_LOG(WARN, "init prerfix end fail");
    exit(-1);
  }

  ObServer dst_host;
  dst_host.set_ipv4_addr(clp.serv_addr, clp.serv_port);
  MockClient client;
  client.init(dst_host);
  for (int64_t i = 0; i < clp.mutator_num; i++) {
    ObMutator mutator;
    PageArena<char> allocer;
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = mb.build_mutator(mutator, allocer, clp.using_id))) {
      TBSYS_LOG(WARN, "build_mutator fail ret=%d\n", ret);
      break;
    }
    int64_t timeu = tbsys::CTimeUtil::getTime();
    ret = client.ups_apply(mutator, TIMEOUT_MS);
    TBSYS_LOG(INFO, "apply ret=%d timeu=%ld\n", ret, tbsys::CTimeUtil::getTime() - timeu);
  }
  client.destroy();
}


