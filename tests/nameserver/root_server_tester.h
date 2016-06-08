/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * root_server_tester.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_TESTS_ROOTSERVER_ROOT_SERVER_TESTER_H_
#define OCEANBASE_TESTS_ROOTSERVER_ROOT_SERVER_TESTER_H_
#include "nameserver/ob_root_server2.h"
#include "nameserver/ob_root_worker.h"
namespace sb {
using namespace common;
namespace nameserver {
class ObRootServerTester {
 public:
  explicit ObRootServerTester(NameServer* root_server):
    root_server_(root_server) {
  }
  void init_root_table_by_report() { root_server_->init_root_table_by_report();}
  ObChunkServerManager& get_server_manager() { return root_server_->server_manager_;}
  ObServerStatus& get_update_server_status() { return root_server_->update_server_status_;}
  ObServerStatus* get_update_server() { return &(root_server_->update_server_status_);}
  int64_t& get_lease_duration() {return root_server_->lease_duration_;}
  common::ObSchemaManagerV2*& get_schema_manager() {return root_server_->schema_manager_;}

  NameTable*& get_root_table_for_query() {return root_server_->root_table_for_query_;}
  NameTable*& root_table_for_build() {return root_server_->root_table_for_build_;}
  char* get_schema_file_name() {return root_server_->schema_file_name_;}
  bool& get_have_inited() {return root_server_->have_inited_;}
  int64_t get_time_stamp_changing() { return root_server_->time_stamp_changing_;}
  ObRootLogWorker* get_log_worker() { return root_server_->log_worker_; }
  int get_server_status() { return root_server_->server_status_; }
  void set_server_status(int status) { root_server_->server_status_ = status; }
  //void set_master(bool is_master) { root_server_->is_master_ = is_master; }
  bool get_create_table_done() { return root_server_->new_table_created_; }
  int64_t& get_one_safe_duration() { return root_server_->safe_lost_one_duration_;}
  int64_t& get_wait_init_time() { return root_server_->wait_init_time_;}
  void stop_thread() {
    root_server_->heart_beat_checker_.stop();
    root_server_->heart_beat_checker_.wait();
    TBSYS_LOG(DEBUG, "heart beat checker stoped");
    root_server_->balance_worker_.stop();
    root_server_->balance_worker_.wait();
    TBSYS_LOG(DEBUG, "balancer stoped");
    root_server_->root_table_modifier_.stop();
    root_server_->root_table_modifier_.wait();
    TBSYS_LOG(DEBUG, "table modifier stoped");
  }
  NameServer* root_server_;

};
class ObRootWorkerForTest : public NameWorker {
 public:
  ObRootWorkerForTest() {
    start_new_send_times = unload_old_table_times = 0;
  }
  int cs_start_merge(const common::ObServer& server, const int64_t time_stamp, const int32_t init_flag) {
    start_new_send_times++;
    return OB_SUCCESS;
  }
  int up_freeze_mem(const common::ObServer& server, const int64_t time_stamp) {
    start_new_send_times++;
    return OB_SUCCESS;
  }
  int unload_old_table(const common::ObServer& server, const int64_t time_stamp, const int64_t remain_time) {
    unload_old_table_times++;
    return OB_SUCCESS;
  }
  int cs_create_tablet(const common::ObServer& server, const common::ObRange& range) {
    return OB_SUCCESS;
  }
  int hb_to_cs(const common::ObServer& server, const int64_t lease_time) {
    return OB_SUCCESS;
  }
  virtual int cs_migrate(const common::ObRange& range,
                         const common::ObServer& src_server, const common::ObServer& dest_server, bool keep_src) {
    TBSYS_LOG(INFO, "will do cs_migrate");
    char t1[100];
    char t2[100];
    src_server.to_string(t1, 100);
    dest_server.to_string(t2, 100);
    range.hex_dump(TBSYS_LOG_LEVEL_INFO);
    TBSYS_LOG(INFO, "src server = %s dest server = %s keep src = %d", t1, t2, keep_src);
    root_server_.migrate_over(range, src_server, dest_server, keep_src, 1);
    root_server_.print_alive_server();
    return OB_SUCCESS;
  }
  void change_schema_test(NameServer* root_server, const int64_t time_stamp, const int32_t init_flag) {
    //schemaChanger* tt = new schemaChanger(root_server, time_stamp, init_flag);
    //tt->start();
  }
  int start_new_send_times;
  int unload_old_table_times;
  NameServer* get_root_server() {return &root_server_;}

};

class ObRootServer2ForTest : public NameServer {
 public:
  ObRootServer2ForTest() {
    has_been_called_ = false;
  }

  int migrate_over(const ObRange& range, const common::ObServer& src_server, const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version) {
    has_been_called_ = true;
    return OB_SUCCESS;
  }

  int report_tablets(const ObServer& server, const ObTabletReportInfoList& tablets, const int64_t time_stamp) {
    has_been_called_ = true;
    return OB_SUCCESS;
  }

  bool has_been_called_;

};

}
}
#endif

