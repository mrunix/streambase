/*
 * src/nameserver/.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for .
 *
 * Library: nameserver
 * Package: nameserver
 * Module :
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef OCEANBASE_ROOT_SERVER_LOG_WORKER_H
#define OCEANBASE_ROOT_SERVER_LOG_WORKER_H
#include "common/ob_server.h"
#include "common/ob_range.h"
#include "common/ob_tablet_info.h"
#include "common/ob_log_entry.h"
#include "nameserver/chunk_server_manager.h"
#include "common/ob_ups_info.h"
#include "common/ob_client_config.h"
namespace sb {
namespace nameserver {
class ObRootLogManager;
class NameServer;
class ObRootLogWorker {
 public:
  ObRootLogWorker();

 public:
  void set_root_server(NameServer* name_server);
  void set_log_manager(ObRootLogManager* log_manager);
  int apply(common::LogCommand cmd, const char* log_data, const int64_t& data_len);

  uint64_t get_cur_log_file_id();
  uint64_t get_cur_log_seq();

 public:
  int sync_schema(const int64_t timestamp);
  int regist_cs(const common::ObServer& server, const int64_t timestamp);
  int regist_ms(const common::ObServer& server, const int64_t timestamp);
  int server_is_down(const common::ObServer& server, const int64_t timestamp);
  int report_cs_load(const common::ObServer& server, const int64_t capacity, const int64_t used);
  int cs_migrate_done(const common::ObRange& range, const common::ObServer& src_server, const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version);

  int start_switch_root_table(const int64_t timestamp);
  int start_report(const int64_t timestamp, const bool init_flag);
  int drop_current_build();
  int drop_last_cs_during_merge();

  int report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets, const int64_t timestamp);

  int add_new_tablet(const int count, const common::ObTabletInfo tablet, const int* server_indexs, const int64_t mem_vwesion);
  int create_table_done();

  int begin_balance();
  int balance_done();

  int us_mem_freezing(const common::ObServer& server, const int64_t timestamp);
  int us_mem_frozen(const common::ObServer& server, const int64_t timestamp);
  int cs_start_merging(const common::ObServer& server);
  int cs_merge_over(const common::ObServer& server, const int64_t timestamp);
  int unload_cs_done(const common::ObServer& server);
  int unload_us_done(const common::ObServer& server);

  int sync_us_frozen_version(const int64_t frozen_version);
  int set_ups_list(const common::ObUpsList& ups_list);
  int set_client_config(const common::ObClientConfig& client_conf);
 private:
  int log_server(const common::LogCommand cmd, const common::ObServer& server);
  int log_server_with_ts(const common::LogCommand cmd, const common::ObServer& server, const int64_t timestamp);
  int flush_log(const common::LogCommand cmd, const char* log_buffer, const int64_t& serialize_size);

 public:
  int do_check_point(const char* log_data, const int64_t& log_length);
  int do_schema_sync(const char* log_data, const int64_t& log_length);
  int do_cs_regist(const char* log_data, const int64_t& log_length);
  int do_ms_regist(const char* log_data, const int64_t& log_length);
  int do_server_down(const char* log_data, const int64_t& log_length);
  int do_cs_load_report(const char* log_data, const int64_t& log_length);
  int do_cs_migrate_done(const char* log_data, const int64_t& log_length);

  int do_start_switch(const char* log_data, const int64_t& log_length);
  int do_start_report(const char* log_data, const int64_t& log_length);
  int do_report_tablets(const char* log_data, const int64_t& log_length);

  int do_add_new_tablet(const char* log_data, const int64_t& log_length);
  int do_create_table_done();

  int do_begin_balance();
  int do_balance_done();

  int do_drop_current_build();
  int do_drop_last_cs_during_merge();

  int do_us_mem_freezing(const char* log_data, const int64_t& log_length);
  int do_us_mem_frozen(const char* log_data, const int64_t& log_length);
  int do_cs_start_merging(const char* log_data, const int64_t& log_length);
  int do_cs_merge_over(const char* log_data, const int64_t& log_length);
  int do_cs_unload_done(const char* log_data, const int64_t& log_length);
  int do_us_unload_done(const char* log_data, const int64_t& log_length);
  int do_sync_frozen_version(const char* log_data, const int64_t& log_length);
  int do_set_ups_list(const char* log_data, const int64_t& log_length);
  int do_set_client_config(const char* log_data, const int64_t& log_length);

  void reset_cs_hb_time();

  void exit();

 private:

 private:
  NameServer* name_server_;
  ObRootLogManager* log_manager_;
};
} /* nameserver */
} /* sb */

#endif /* end of include guard: OCEANBASE_ROOT_SERVER_LOG_WORKER_H */

