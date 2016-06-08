/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mock_client.cc,v 0.1 2010/10/08 16:55:10 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <getopt.h>
#include <unistd.h>
#include "mock_client.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "test_utils.h"
#include "ob_final_data.h"

const int64_t timeout = 100 * 1000 * 1000L;

void delay_drop_memtable(MockClient& client) {
  int err = client.delay_drop_memtable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void immediately_drop_memtable(MockClient& client) {
  int err = client.immediately_drop_memtable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void enable_memtable_checksum(MockClient& client) {
  int err = client.enable_memtable_checksum(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void disable_memtable_checksum(MockClient& client) {
  int err = client.disable_memtable_checksum(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void get_table_time_stamp(MockClient& client, const uint64_t major_version) {
  int64_t time_stamp = 0;
  int err = client.get_table_time_stamp(major_version, time_stamp, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err) {
    fprintf(stdout, "major_version=%lu time_stamp=%ld %s\n", major_version, time_stamp, time2str(time_stamp));
  }
}

void switch_commit_log(MockClient& client) {
  uint64_t new_log_file_id = 0;
  int err = client.switch_commit_log(new_log_file_id, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err) {
    fprintf(stdout, "new_log_file_id=%lu\n", new_log_file_id);
  }
}

void load_new_store(MockClient& client) {
  int err = client.load_new_store(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void reload_all_store(MockClient& client) {
  int err = client.reload_all_store(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void reload_store(MockClient& client, int64_t store_handle) {
  int err = client.reload_store(store_handle, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void umount_store(MockClient& client, const char* umount_dir) {
  int err = client.umount_store(umount_dir, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void force_report_frozen_version(MockClient& client) {
  int err = client.force_report_frozen_version(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void erase_sstable(MockClient& client) {
  int err = client.erase_sstable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void store_memtable(MockClient& client, int64_t store_all) {
  int err = client.store_memtable(store_all, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void get_bloomfilter(MockClient& client, int64_t version) {
  sb::common::TableBloomFilter table_bf;
  int err = client.get_bloomfilter(version, table_bf, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void fetch_ups_stat_info(MockClient& client) {
  sb::updateserver::UpsStatMgr stat_mgr;
  int err = client.fetch_ups_stat_info(stat_mgr, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err) {
    stat_mgr.print_info();
  }
}

void get_last_frozen_version(MockClient& client) {
  int64_t version = 0;
  int err = client.get_last_frozen_version(version, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err) {
    fprintf(stdout, "last_frozen_version=%ld\n", version);
  }
}

void clear_active_memtable(MockClient& client) {
  int err = client.clear_active_memtable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void memory_watch(MockClient& client) {
  UpsMemoryInfo memory_info;
  int err = client.memory_watch(memory_info, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err) {
    fprintf(stdout, "memory_watch err=%d\n", err);
    fprintf(stdout, "memory_info_version=%ld\n", memory_info.version);
    fprintf(stdout, "total_size=%ld\n", memory_info.total_size);
    fprintf(stdout, "cur_limit_size=%ld\n", memory_info.cur_limit_size);
    fprintf(stdout, "memtable_used=%ld\n", memory_info.table_mem_info.memtable_used);
    fprintf(stdout, "memtable_total=%ld\n", memory_info.table_mem_info.memtable_total);
    fprintf(stdout, "memtable_limit=%ld\n", memory_info.table_mem_info.memtable_limit);
  }
}

void memory_limit(const char* memory_limit, const char* memtable_limit, MockClient& client) {
  UpsMemoryInfo param;
  UpsMemoryInfo memory_info;
  param.cur_limit_size = (NULL == memory_limit) ?  0 : atoll(memory_limit);
  param.table_mem_info.memtable_limit = (NULL == memtable_limit) ? 0 : atoll(memtable_limit);
  int err = client.memory_limit(param, memory_info, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err) {
    fprintf(stdout, "memory_limit err=%d\n", err);
    fprintf(stdout, "memory_info_version=%ld\n", memory_info.version);
    fprintf(stdout, "total_size=%ld\n", memory_info.total_size);
    fprintf(stdout, "cur_limit_size=%ld\n", memory_info.cur_limit_size);
    fprintf(stdout, "memtable_used=%ld\n", memory_info.table_mem_info.memtable_used);
    fprintf(stdout, "memtable_total=%ld\n", memory_info.table_mem_info.memtable_total);
    fprintf(stdout, "memtable_limit=%ld\n", memory_info.table_mem_info.memtable_limit);
  }
}

void priv_queue_conf(const char* conf_str, MockClient& client) {
  UpsPrivQueueConf param;
  UpsPrivQueueConf priv_queue_conf;
  sscanf(conf_str, "%ld%ld%ld%ld", &param.low_priv_network_lower_limit,
         &param.low_priv_network_upper_limit, &param.low_priv_adjust_flag,
         &param.low_priv_cur_percent);

  int err = client.priv_queue_conf(param, priv_queue_conf, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err) {
    fprintf(stdout, "priv_queue_conf err=%d\n", err);
    fprintf(stdout, "low_priv_network_lower_limit=%ld\n", priv_queue_conf.low_priv_network_lower_limit);
    fprintf(stdout, "low_priv_network_upper_limit=%ld\n", priv_queue_conf.low_priv_network_upper_limit);
    fprintf(stdout, "low_priv_adjust_flag=%ld\n", priv_queue_conf.low_priv_adjust_flag);
    fprintf(stdout, "low_priv_cur_percent=%ld\n", priv_queue_conf.low_priv_cur_percent);
  }
}

void dump_memtable(const char* dump_dir, MockClient& client) {
  if (NULL == dump_dir) {
    dump_dir = "/tmp";
  }
  int err = client.dump_memtable(dump_dir, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  fprintf(stdout, "dump dest=[%s]\n", dump_dir);
}

void dump_schemas(MockClient& client) {
  int err = client.dump_schemas(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void force_fetch_schema(MockClient& client) {
  int err = client.force_fetch_schema(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void get_clog_cursor(MockClient& client) {
  int err = OB_SUCCESS;
  ObLogCursor log_cursor;
  if (OB_SUCCESS != (err = client.get_max_clog_id(log_cursor, timeout))) {
    TBSYS_LOG(ERROR, "client.get_max_clog_id()=>%d", err);
  }
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  fprintf(stdout, "log_file_id=%lu, log_seq_id=%lu, log_offset=%lu\n", log_cursor.file_id_, log_cursor.log_id_, log_cursor.offset_);
}

void get_clog_master(MockClient& client) {
  ObServer server;
  int err = client.get_clog_master(server, timeout);
  char addr[256];
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  server.to_string(addr, sizeof(addr));
  fprintf(stdout, "%s\n", addr);
}

void reload_conf(const char* fname, MockClient& client) {
  int err = client.reload_conf(fname, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  fprintf(stdout, "relaod fname=[%s]\n", fname);
}

void apply(const char* fname, PageArena<char>& allocer, MockClient& client) {
  ObMutator mutator;
  ObMutator result;
  read_cell_infos(fname, CELL_INFOS_SECTION, allocer, mutator, result);
  int err = client.ups_apply(mutator, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void total_scan(const char* fname, PageArena<char>& allocer, MockClient& client, const char* version_range) {
  ObScanner scanner;
  ObScanParam scan_param;
  read_scan_param(fname, SCAN_PARAM_SECTION, allocer, scan_param);
  scan_param.set_version_range(str2range(version_range));
  scan_param.set_is_read_consistency(false);

  int64_t total_row = 0;
  int64_t total_timeu = 0;
  while (true) {
    int64_t timeu = tbsys::CTimeUtil::getTime();
    int err = client.ups_scan(scan_param, scanner, timeout);
    timeu = tbsys::CTimeUtil::getTime() - timeu;
    if (OB_SUCCESS != err) {
      fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
      break;
    } else {
      //int64_t row_counter = 0;
      //while (OB_SUCCESS == scanner.next_cell())
      //{
      //  ObCellInfo *ci = NULL;
      //  bool is_row_changed = false;
      //  scanner.get_cell(&ci, &is_row_changed);
      //  fprintf(stdout, "%s\n", updateserver::print_cellinfo(ci, "CLI_SCAN"));
      //  if (is_row_changed)
      //  {
      //    row_counter++;
      //  }
      //}
      bool is_fullfilled = false;
      int64_t fullfilled_num = 0;
      ObString last_rk;
      scanner.get_last_row_key(last_rk);
      scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
      fprintf(stdout, "[SINGLE_SCAN] is_fullfilled=%s fullfilled_num=%ld timeu=%ld last_row_key=[%s]\n",
              STR_BOOL(is_fullfilled), fullfilled_num, timeu, print_string(last_rk));
      if (0 == fullfilled_num) {
        break;
      } else {
        total_row += fullfilled_num;
        total_timeu += timeu;
        const_cast<ObRange*>(scan_param.get_range())->start_key_ = last_rk;
        const_cast<ObRange*>(scan_param.get_range())->border_flag_.unset_min_value();
        const_cast<ObRange*>(scan_param.get_range())->border_flag_.unset_inclusive_start();
      }
    }
  }
  fprintf(stdout, "[TOTAL_SCAN] total_row=%ld total_timeu=%ld\n", total_row, total_timeu);
}

int scan(const char* fname, PageArena<char>& allocer, MockClient& client, const char* version_range,
         const char* expect_result_fname, const char* schema_fname) {
  ObScanner scanner;
  ObScanParam scan_param;
  read_scan_param(fname, SCAN_PARAM_SECTION, allocer, scan_param);
  scan_param.set_version_range(str2range(version_range));
  scan_param.set_is_read_consistency(false);
  int err = client.ups_scan(scan_param, scanner, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  bool res = true;
  // check result
  if (OB_SUCCESS == err) {
    int64_t row_counter = 0;
    while (OB_SUCCESS == scanner.next_cell()) {
      ObCellInfo* ci = NULL;
      bool is_row_changed = false;
      scanner.get_cell(&ci, &is_row_changed);
      fprintf(stdout, "%s\n", updateserver::print_cellinfo(ci, "CLI_SCAN"));
      if (is_row_changed) {
        row_counter++;
      }
    }
    bool is_fullfilled = false;
    int64_t fullfilled_num = 0;
    ObString last_rk;
    scanner.get_last_row_key(last_rk);
    scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
    fprintf(stdout, "is_fullfilled=%s fullfilled_num=%ld row_counter=%ld last_row_key=[%s]\n",
            STR_BOOL(is_fullfilled), fullfilled_num, row_counter, print_string(last_rk));
    if (OB_SUCCESS == err && NULL != expect_result_fname) {
      err = scanner.reset_iter();
      if (OB_SUCCESS == err) {
        ObFinalResult expect_result;
        err = expect_result.parse_from_file(expect_result_fname, schema_fname);
        if (OB_SUCCESS == err) {
          res = check_result(scan_param, scanner, expect_result);
        }
      } else {
        TBSYS_LOG(WARN, "fail to reset scanner.iterator");
      }
    }
  }
  return err;
}

int param_get(const char* fname, MockClient& client) {
  int err = OB_SUCCESS;
  int fd = open(fname, O_RDONLY);
  if (-1 == fd) {
    fprintf(stdout, "open [%s] fail errno=%u\n", fname, errno);
    err = OB_ERROR;
  } else {
    struct stat st;
    fstat(fd, &st);
    char* buf = (char*)malloc(st.st_size);
    if (NULL == buf) {
      fprintf(stdout, "malloc buf fail size=%ld\n", st.st_size);
      err = OB_ERROR;
    } else {
      read(fd, buf, st.st_size);
      int64_t pos = 0;
      ObGetParam get_param;
      if (OB_SUCCESS != get_param.deserialize(buf, st.st_size, pos)) {
        fprintf(stdout, "deserialize get_param fail\n");
        err = OB_ERROR;
      } else {
        get_param.set_is_read_consistency(false);
        ObScanner scanner;
        int err = client.ups_get(get_param, scanner, timeout);
        fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
        if (OB_SUCCESS == err) {
          int64_t row_counter = 0;
          while (OB_SUCCESS == scanner.next_cell()) {
            ObCellInfo* ci = NULL;
            bool is_row_changed = false;
            scanner.get_cell(&ci, &is_row_changed);
            fprintf(stdout, "%s\n", updateserver::print_cellinfo(ci, "CLI_GET"));
            if (is_row_changed) {
              row_counter++;
            }
          }
          bool is_fullfilled = false;
          int64_t fullfilled_num = 0;
          ObString last_rk;
          scanner.get_last_row_key(last_rk);
          scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
          fprintf(stdout, "is_fullfilled=%s fullfilled_num=%ld row_counter=%ld last_row_key=[%s]\n",
                  STR_BOOL(is_fullfilled), fullfilled_num, row_counter, print_string(last_rk));
        }
      }
      free(buf);
    }
    close(fd);
  }
  return err;
}

int get(const char* fname, PageArena<char>& allocer, MockClient& client, const char* version_range,
        const char* expect_result_fname, const char* schema_fname) {
  ObScanner scanner;
  ObGetParam get_param;
  read_get_param(fname, GET_PARAM_SECTION, allocer, get_param);
  get_param.set_is_read_consistency(false);
  get_param.set_version_range(str2range(version_range));
  int err = client.ups_get(get_param, scanner, timeout);
  bool res = true;
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err) {
    // check result
    int64_t row_counter = 0;
    while (OB_SUCCESS == scanner.next_cell()) {
      ObCellInfo* ci = NULL;
      bool is_row_changed = false;
      scanner.get_cell(&ci, &is_row_changed);
      fprintf(stdout, "%s\n", updateserver::print_cellinfo(ci, "CLI_GET"));
      if (is_row_changed) {
        row_counter++;
      }
    }
    bool is_fullfilled = false;
    int64_t fullfilled_num = 0;
    ObString last_rk;
    scanner.get_last_row_key(last_rk);
    scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
    fprintf(stdout, "is_fullfilled=%s fullfilled_num=%ld row_counter=%ld last_row_key=[%s]\n",
            STR_BOOL(is_fullfilled), fullfilled_num, row_counter, print_string(last_rk));
    if (OB_SUCCESS == err && NULL != expect_result_fname) {
      err = scanner.reset_iter();
      if (OB_SUCCESS == err) {
        ObFinalResult expect_result;
        err = expect_result.parse_from_file(expect_result_fname, schema_fname);
        if (OB_SUCCESS == err) {
          res = check_result(get_param, scanner, expect_result);
        }
      } else {
        TBSYS_LOG(WARN, "fail to reset scanner.iterator");
      }
    }
  }
  return err;
}

void minor_freeze(MockClient& client) {
  int err = client.freeze(timeout, false);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void major_freeze(MockClient& client) {
  int err = client.freeze(timeout, true);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void fetch_schema(MockClient& client, int64_t timestamp) {
  ObSchemaManagerV2* schema_mgr = new(std::nothrow) ObSchemaManagerV2();
  if (NULL == schema_mgr) {
    fprintf(stdout, "[%s] new ObSchemaManagerV2 fail\n", __FUNCTION__);
  } else {
    int err = client.fetch_schema(timestamp, *schema_mgr, timeout);
    fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
    if (OB_SUCCESS == err) {
      uint64_t cur_table_id = OB_INVALID_ID;
      const ObColumnSchemaV2* iter = NULL;
      for (iter = schema_mgr->column_begin(); iter != schema_mgr->column_end(); iter++) {
        if (NULL != iter) {
          if (iter->get_table_id() != cur_table_id) {
            const ObTableSchema* table_schema = schema_mgr->get_table_schema(iter->get_table_id());
            if (NULL != table_schema) {
              fprintf(stdout, "[TABLE_SCHEMA] table_id=%lu table_type=%d table_name=%s split_pos=%d rowkey_max_length=%d\n",
                      iter->get_table_id(), table_schema->get_table_type(), table_schema->get_table_name(),
                      table_schema->get_split_pos(), table_schema->get_rowkey_max_length());
            } else {
              fprintf(stdout, "[TABLE_SCHEMA] table_id=%lu\n", iter->get_table_id());
            }
            cur_table_id = iter->get_table_id();
          }
          fprintf(stdout, "              [COLUMN_SCHEMA] column_id=%lu column_name=%s column_type=%d size=%ld\n",
                  iter->get_id(), iter->get_name(), iter->get_type(), iter->get_size());
        }
      }

    }
    delete schema_mgr;
  }
}

void drop(MockClient& client) {
  int err = client.drop_memtable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void init_mock_client(const char* addr, int32_t port, MockClient& client) {
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  client.init(dst_host);
}

struct CmdLineParam {
  char* serv_addr;
  int32_t serv_port;
  char* cmd_type;
  char* ini_fname;
  int64_t timestamp;
  char* version_range;
  char* memory_limit;
  char* memtable_limit;
  char* priv_queue_conf;
  char* expected_result_fname;
  char* schema_fname;
};

void print_usage() {
  fprintf(stdout, "\n");
  fprintf(stdout, "ups_ctrl [OPTION]\n");
  fprintf(stdout, "   -a|--serv_addr server address\n");
  fprintf(stdout, "   -p|--serv_port server port\n");
  fprintf(stdout, "   -t|--cmd_type command type[apply|get|get_clog_cursor|get_clog_master|param_get|scan|total_scan|minor_freeze|major_freeze|fectch_schema|drop|dump_memtable|dump_schemas|force_fetch_schema|"
          "reload_conf|memory_watch|memory_limit|priv_queue_conf|clear_active_memtable|get_last_frozen_version|fetch_ups_stat_info|get_bloomfilter|"
          "store_memtable|erase_sstable|load_new_store|reload_all_store|reload_store|umount_store|force_report_frozen_version|switch_commit_log|get_table_time_stamp|"
          "enable_memtable_checksum|disable_memtable_checksum|"
          "delay_drop_memtable|immediately_drop_memtable]\n");
  fprintf(stdout, "   -f|--ini_fname ini file name\n");
  fprintf(stdout, "   -s|--timestamp must be set while cmd_type is [fectch_schema|get_bloomfilter|reload_store|get_table_time_stamp], optional for [store_memtable]\n");
  fprintf(stdout, "   -r|--version_range must be set while cmd_type is [get|scan]\n");
  fprintf(stdout, "   -l|--memory_limit could be set while cmd_type is memory_limit\n");
  fprintf(stdout, "   -c|--memtable_limit could be set while cmd_type is memory_limit\n");
  fprintf(stdout, "   -q|--priv_queue_conf network_lower_limit network_upper_limit adjust_flag low_priv_cur_percent\n");
  fprintf(stdout, "   -e|--expect_result expected result of the read operation [optional]\n");
  fprintf(stdout, "   -m|--schema schema of expect_result [must specify when -e was given]\n");
  fprintf(stdout, "   -h|--help print this help info\n");
  fprintf(stdout, "   -z|--time2str micro time to string\n");
  fprintf(stdout, "\n");
}

void parse_cmd_line(int argc, char** argv, CmdLineParam& clp) {
  int opt = 0;
  const char* opt_string = "a:p:t:f:s:z:r:l:q:e:m:c:o:h";
  struct option longopts[] = {
    {"serv_addr", 1, NULL, 'a'},
    {"serv_port", 1, NULL, 'p'},
    {"cmd_type", 1, NULL, 't'},
    {"ini_fname", 1, NULL, 'f'},
    {"timestamp", 1, NULL, 's'},
    {"time2str", 1, NULL, 'z'},
    {"version_range", 1, NULL, 'r'},
    {"memory_limit", 1, NULL, 'l'},
    {"priv_queue_conf", 1, NULL, 'q'},
    {"active_limit", 1, NULL, 'c'},
    {"expect_result", 1, NULL, 'e'},
    {"schema", 1, NULL, 'm'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  memset(&clp, 0, sizeof(clp));
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
    case 'a':
      clp.serv_addr = optarg;
      break;
    case 'p':
      clp.serv_port = atoi(optarg);
      break;
    case 't':
      clp.cmd_type = optarg;
      break;
    case 'f':
      clp.ini_fname = optarg;
      break;
    case 's':
      clp.timestamp = atol(optarg);
      break;
    case 'r':
      clp.version_range = optarg;
      break;
    case 'l':
      clp.memory_limit = optarg;
      break;
    case 'q':
      clp.priv_queue_conf = optarg;
      break;
    case 'c':
      clp.memtable_limit = optarg;
      break;
    case 'z': {
      int64_t timestamp = atol(optarg);
      fprintf(stdout, "timestamp=%ld str=%s\n",
              timestamp, time2str(timestamp));
      exit(1);
    }
    case 'e':
      clp.expected_result_fname = optarg;
      break;
    case 'm':
      clp.schema_fname = optarg;
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }
  if (NULL == clp.serv_addr
      || 0 == clp.serv_port
      || NULL == clp.cmd_type
      || (NULL == clp.ini_fname && 0 == strcmp("apply", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("scan", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("total_scan", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("get", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("param_get", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("reload_conf", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("umount_store", clp.cmd_type))
      || (NULL == clp.version_range && 0 == strcmp("scan", clp.cmd_type))
      || (NULL == clp.version_range && 0 == strcmp("total_scan", clp.cmd_type))
      || (NULL == clp.version_range && 0 == strcmp("get", clp.cmd_type))
      || (0 == clp.timestamp && 0 == strcmp("get_bloomfilter", clp.cmd_type))
      || (0 == clp.timestamp && 0 == strcmp("fectch_schema", clp.cmd_type))
      || (0 == clp.timestamp && 0 == strcmp("get_table_time_stamp", clp.cmd_type))
      || (0 == clp.timestamp && 0 == strcmp("reload_store", clp.cmd_type))) {
    print_usage();
    fprintf(stdout, "serv_addr=%s serv_port=%d cmd_type=%s ini_fname=%s timestamp=%ld version_range=%s\n",
            clp.serv_addr, clp.serv_port, clp.cmd_type, clp.ini_fname, clp.timestamp, clp.version_range);
    exit(-1);
  }

  if (NULL != clp.expected_result_fname && NULL == clp.schema_fname) {
    print_usage();
    fprintf(stdout, "-e and -m must appear together");
    exit(-1);
  }
}

int main(int argc, char** argv) {
  TBSYS_LOGGER.setFileName("ups_admin.log", true);
  TBSYS_LOG(INFO, "ups_admin start==================================================");
  ob_init_memory_pool();

  CmdLineParam clp;
  parse_cmd_line(argc, argv, clp);

  MockClient client;
  init_mock_client(clp.serv_addr, clp.serv_port, client);

  PageArena<char> allocer;

  if (0 == strcmp("apply", clp.cmd_type)) {
    apply(clp.ini_fname, allocer, client);
  } else if (0 == strcmp("get_clog_cursor", clp.cmd_type)) {
    get_clog_cursor(client);
  } else if (0 == strcmp("get_clog_master", clp.cmd_type)) {
    get_clog_master(client);
  } else if (0 == strcmp("get", clp.cmd_type)) {
    get(clp.ini_fname, allocer, client, clp.version_range, clp.expected_result_fname,
        clp.schema_fname);
  } else if (0 == strcmp("param_get", clp.cmd_type)) {
    param_get(clp.ini_fname, client);
  } else if (0 == strcmp("scan", clp.cmd_type)) {
    scan(clp.ini_fname, allocer, client, clp.version_range, clp.expected_result_fname,
         clp.schema_fname);
  } else if (0 == strcmp("total_scan", clp.cmd_type)) {
    total_scan(clp.ini_fname, allocer, client, clp.version_range);
  } else if (0 == strcmp("minor_freeze", clp.cmd_type)) {
    minor_freeze(client);
  } else if (0 == strcmp("major_freeze", clp.cmd_type)) {
    major_freeze(client);
  } else if (0 == strcmp("fetch_schema", clp.cmd_type)) {
    fetch_schema(client, clp.timestamp);
  } else if (0 == strcmp("drop", clp.cmd_type)) {
    drop(client);
  } else if (0 == strcmp("dump_memtable", clp.cmd_type)) {
    dump_memtable(clp.ini_fname, client);
  } else if (0 == strcmp("dump_schemas", clp.cmd_type)) {
    dump_schemas(client);
  } else if (0 == strcmp("force_fetch_schema", clp.cmd_type)) {
    force_fetch_schema(client);
  } else if (0 == strcmp("reload_conf", clp.cmd_type)) {
    reload_conf(clp.ini_fname, client);
  } else if (0 == strcmp("memory_watch", clp.cmd_type)) {
    memory_watch(client);
  } else if (0 == strcmp("memory_limit", clp.cmd_type)) {
    memory_limit(clp.memory_limit, clp.memtable_limit, client);
  } else if (0 == strcmp("priv_queue_conf", clp.cmd_type)) {
    priv_queue_conf(clp.priv_queue_conf, client);
  } else if (0 == strcmp("clear_active_memtable", clp.cmd_type)) {
    clear_active_memtable(client);
  } else if (0 == strcmp("get_last_frozen_version", clp.cmd_type)) {
    get_last_frozen_version(client);
  } else if (0 == strcmp("fetch_ups_stat_info", clp.cmd_type)) {
    fetch_ups_stat_info(client);
  } else if (0 == strcmp("get_bloomfilter", clp.cmd_type)) {
    get_bloomfilter(client, clp.timestamp);
  } else if (0 == strcmp("store_memtable", clp.cmd_type)) {
    store_memtable(client, clp.timestamp);
  } else if (0 == strcmp("erase_sstable", clp.cmd_type)) {
    erase_sstable(client);
  } else if (0 == strcmp("load_new_store", clp.cmd_type)) {
    load_new_store(client);
  } else if (0 == strcmp("reload_all_store", clp.cmd_type)) {
    reload_all_store(client);
  } else if (0 == strcmp("reload_store", clp.cmd_type)) {
    reload_store(client, clp.timestamp);
  } else if (0 == strcmp("umount_store", clp.cmd_type)) {
    umount_store(client, clp.ini_fname);
  } else if (0 == strcmp("force_report_frozen_version", clp.cmd_type)) {
    force_report_frozen_version(client);
  } else if (0 == strcmp("switch_commit_log", clp.cmd_type)) {
    switch_commit_log(client);
  } else if (0 == strcmp("get_table_time_stamp", clp.cmd_type)) {
    get_table_time_stamp(client, clp.timestamp);
  } else if (0 == strcmp("disable_memtable_checksum", clp.cmd_type)) {
    disable_memtable_checksum(client);
  } else if (0 == strcmp("enable_memtable_checksum", clp.cmd_type)) {
    enable_memtable_checksum(client);
  } else if (0 == strcmp("immediately_drop_memtable", clp.cmd_type)) {
    immediately_drop_memtable(client);
  } else if (0 == strcmp("delay_drop_memtable", clp.cmd_type)) {
    delay_drop_memtable(client);
  } else {
    print_usage();
  }

  client.destroy();
  TBSYS_LOG(INFO, "ups_admin end==================================================");
  return 0;
}
