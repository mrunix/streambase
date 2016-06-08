/*
 * src/nameserver/name_server.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for NameServer.
 *
 * Library: nameserver
 * Package: nameserver
 * Module : NameServer
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#include <tbsys.h>

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_obi_role.h"
#include "common/ob_obi_config.h"
#include "common/ob_ups_info.h"
#include "common/ob_client_config.h"
#include "nameserver/chunk_server_manager.h"
#include "nameserver/nameserver_table.h"
#include "nameserver/nameserver_log_worker.h"
class ObBalanceTest;
class ObBalanceTest_test_n_to_2_Test;
class ObBalanceTest_test_timeout_Test;
class ObBalanceTest_test_rereplication_Test;

namespace sb {
namespace common {
class ObSchemaManagerV2;
class ObRange;
class ObTabletInfo;
class ObTabletLocation;
class ObScanner;
class ObCellInfo;
class ObTabletReportInfoList;
}
namespace nameserver {
class NameTable;
class ObRootServerTester;
class NameWorker;
class NameServer {
 public:
  static const int STATUS_INIT         = 0;
  static const int STATUS_NEED_REPORT  = 1;
  static const int STATUS_NEED_BUILD   = 2;
  static const int STATUS_CHANGING     = 3;
  static const int STATUS_NEED_BALANCE = 4;
  static const int STATUS_BALANCING    = 5;
  static const int STATUS_SLEEP        = 6;
  static const int STATUS_INTERRUPT_BALANCING    = 7;
  static const char* ROOT_TABLE_EXT;
  static const char* CHUNKSERVER_LIST_EXT;
  enum {
    BUILD_FOR_SWITCH = 0,
    BUILD_FOR_REPORT,
  };
  enum {
    BUILD_SYNC_FLAG_NONE = 0,
    BUILD_SYNC_INIT_OK = 1,
    BUILD_SYNC_FLAG_FREEZE_MEM = 2,
    BUILD_SYNC_FLAG_CAN_ACCEPT_NEW_TABLE = 3,
    BUILD_SYNC_FLAG_CS_START_MERGE_OK = 4,
  };
  enum RereplicationAction {
    RA_NOP = 0,
    RA_COPY = 1,
    RA_DELETE = 2
  };
  NameServer();
  virtual ~NameServer();

  bool init(const char* config_file_name, const int64_t now, NameWorker* worker);
  void start_threads();
  void stop_threads();

  bool reload_config(const char* config_file_name);
  bool start_switch();
  void drop_current_build();
  /*
   * 从本地读取新schema, 判断兼容性
   */
  int switch_schema(const int64_t time_stamp);
  /*
   * 切换过程中, update server冻结内存表 或者chunk server 进行merge等耗时操作完成
   * 发送消息调用此函数
   */
  int waiting_jsb_done(const common::ObServer& server, const int64_t frozen_mem_version);

  /*
   * chunk serve和merege server注册
   * @param out status 0 do not start report 1 start report
   */
  int regist_server(const common::ObServer& server, bool is_merge_server, int32_t& status, int64_t time_stamp = -1);
  /*
   * chunk server更新自己的磁盘情况信息
   */
  int update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used);
  /*
   * 迁移完成操作
   */
  virtual int migrate_over(const common::ObRange& range, const common::ObServer& src_server,
                           const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version);
  bool get_schema(common::ObSchemaManagerV2& out_schema) const;
  int64_t get_schema_version() const;

  int find_root_table_key(const uint64_t table_id, const common::ObString& table_name, const int32_t max_key_len,
                          const common::ObString& key, common::ObScanner& scanner) const;

  int find_root_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner) const;
  int find_root_table_range(const common::ObScanParam& scan_param, common::ObScanner& scanner) const;

  //this will be called when a server echo rt_start_merge
  int echo_start_merge_received(const common::ObServer& server);
  int echo_update_server_freeze_mem();
  int echo_unload_received(const common::ObServer& server);
  virtual int report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets, const int64_t time_stamp);

  int receive_hb(const common::ObServer& server, common::ObRole role);
  common::ObServer get_update_server_info() const;
  int32_t get_update_server_inner_port() const;
  int64_t get_merge_delay_interval() const;

  uint64_t get_table_info(const common::ObString& table_name, int32_t& max_row_key_length) const;
  int get_table_info(const uint64_t table_id, common::ObString& table_name, int32_t& max_row_key_length) const;

  int get_server_status() const;
  int64_t get_time_stamp_changing() const;
  int64_t get_lease() const;
  int get_server_index(const common::ObServer& server) const;

  int get_cs_info(ObChunkServerManager* out_server_manager) const;

  void print_alive_server() const;
  bool is_master() const;
  void dump_root_table() const;
  void dump_unusual_tablets() const;
  void dump_migrate_info() const;
  bool reload_config(bool did_write_log);
  void use_new_schema();
  int do_check_point(const uint64_t ckpt_id); // dump current root table and chunkserver list into file
  int recover_from_check_point(const int server_status, const uint64_t ckpt_id); // recover root table and chunkserver list from file
  int report_frozen_memtable(const int64_t frozen_version, bool did_replay);
  // 用于slave启动过程中的同步
  void wait_init_finished();
  const common::ObiRole& get_obi_role() const;
  int set_obi_role(const common::ObiRole& role);
  int get_obi_config(common::ObiConfig& obi_config) const;
  int set_obi_config(const common::ObiConfig& conf);
  int set_obi_config(const common::ObServer& rs_addr, const common::ObiConfig& conf);
  const common::ObUpsList& get_ups_list() const;
  int set_ups_config(const common::ObServer& ups, int32_t ms_read_percentage, int32_t cs_read_percentage);
  int set_ups_list(const common::ObUpsList& ups_list);
  const common::ObClientConfig& get_client_config() const;
  int set_client_config(const common::ObClientConfig& client_conf);
  int do_stat(int stat_key, char* buf, const int64_t buf_len, int64_t& pos);
  int serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const;
  int serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const;

  friend class ObRootServerTester;
  friend class ObRootLogWorker;
  friend class ::ObBalanceTest;
  friend class ::ObBalanceTest_test_n_to_2_Test;
  friend class ::ObBalanceTest_test_timeout_Test;
  friend class ::ObBalanceTest_test_rereplication_Test;
 private:
  /*
   * 收到汇报消息后调用
   */
  int got_reported(const common::ObTabletReportInfoList& tablets, const int server_index, const int64_t frozen_mem_version);

  // new version of balance algorithm
  void do_new_balance();
  int nb_calculate_sstable_count(const uint64_t table_id, int64_t& avg_size, int64_t& avg_count, int32_t& cs_num);
  int nb_balance_by_table(const uint64_t table_id, bool& scan_next_table);
  int nb_find_dest_cs(NameTable::const_iterator meta, int64_t low_bound, int32_t cs_num,
                      int32_t& dest_cs_idx, ObChunkServerManager::iterator& dest_it);
  uint64_t nb_get_next_table_id(int32_t table_count, int32_t seq = -1);
  int32_t nb_get_table_count();
  int send_msg_migrate(const common::ObServer& src, const common::ObServer& dest, const common::ObRange& range, bool keep_src);
  int nb_start_batch_migrate();
  int nb_check_migrate_timeout();
  bool nb_is_in_batch_migrating();
  void nb_batch_migrate_done();
  int nb_trigger_next_migrate(const common::ObRange& range, int32_t src_cs_idx, int32_t dest_cs_idx, bool keep_src);
  bool nb_is_curr_table_balanced(int64_t avg_sstable_count, const common::ObServer& except_cs) const;
  bool nb_is_curr_table_balanced(int64_t avg_sstable_count) const;
  void nb_print_balance_info() const;
  void nb_print_migrate_infos() const;
  int nb_add_copy(NameTable::const_iterator it, const common::ObTabletInfo* tablet, int64_t low_bound, int32_t cs_num);
  int nb_select_copy_src(NameTable::const_iterator it,
                         int32_t& src_cs_idx, ObChunkServerManager::iterator& src_it);
  int nb_check_rereplication(NameTable::const_iterator it, RereplicationAction& act);
  int nb_check_add_migrate(NameTable::const_iterator it, const common::ObTabletInfo* tablet, int64_t avg_count, int32_t cs_num);

  bool nb_is_all_tables_balanced(const common::ObServer& except_cs); // only for testing
  bool nb_is_all_tables_balanced(); // only for testing
  bool nb_is_all_tablets_replicated(int32_t expected_replicas_num);    // only for testing

  void nb_print_balance_info(char* buf, const int64_t buf_len, int64_t& pos) const; // for monitor
  void nb_print_balance_infos(char* buf, const int64_t buf_len, int64_t& pos); // for monitor

  /*
   * 系统初始化的时候, 处理汇报消息,
   * 信息放到root table for build 结构中
   */
  int got_reported_for_build(const common::ObTabletInfo& tablet,
                             const int32_t server_index, const int64_t version);
  /*
   * 处理汇报消息, 直接写到当前的root table中
   * 如果发现汇报消息中有对当前root table的tablet的分裂或者合并
   * 要调用采用写拷贝机制的处理函数
   */
  int got_reported_for_query_table(const common::ObTabletReportInfoList& tablets,
                                   const int32_t server_index, const int64_t frozen_mem_version);
  /*
   * 写拷贝机制的,处理汇报消息
   */
  int got_reported_with_copy(const common::ObTabletReportInfoList& tablets,
                             const int32_t server_index, const int64_t have_done_index);

  void create_new_table(common::ObSchemaManagerV2* schema);
  void create_new_table_in_init(common::ObSchemaManagerV2* schema, NameTable* root_table_tmp);
  void create_new_table();
  int slave_create_new_table(const common::ObTabletInfo& tablet, const int32_t* t_server_index, const int32_t replicas_num, const int64_t mem_version);
  void get_available_servers_for_new_table(int* server_index, int32_t expected_num, int32_t& results_num);
  /*
   * 生成查询的输出cell
   */
  int make_out_cell(common::ObCellInfo& out_cell, NameTable::const_iterator start,
                    NameTable::const_iterator end, common::ObScanner& scanner, const int32_t max_row_count,
                    const int32_t max_key_len) const;

  // @return 0 do not copy, 1 copy immediately, -1 delayed copy
  int need_copy(int32_t available_num, int32_t lost_num);
  // stat related functions
  void do_stat_start_time(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_local_time(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_common(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_schema_version(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_mem(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_table_num(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_tablet_num(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_cs(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_ms(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_ups(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_frozen_time(char* buf, const int64_t buf_len, int64_t& pos);
  void do_stat_client_config(char* buf, const int64_t buf_len, int64_t& pos);

  int make_checkpointing();
  int init_root_table_by_report();
  bool start_report(bool init_flag = false);
  /*
   * 在一个tabelt的各份拷贝中, 寻找合适的备份替换掉
   */
  int write_new_info_to_root_table(
    const common::ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index,
    NameTable::const_iterator& first, NameTable::const_iterator& last, NameTable* p_root_table);

  bool all_tablet_is_the_last_frozen_version() const;
  int load_ups_list(tbsys::CConfig& config);
  int load_client_config(tbsys::CConfig& config);
  int create_root_table_for_build();
  DISALLOW_COPY_AND_ASSIGN(NameServer);

 private:
  ObChunkServerManager server_manager_;
  mutable tbsys::CRWLock server_manager_rwlock_;

  ObServerStatus update_server_status_;
  int32_t ups_inner_port_;
  common::ObUpsList ups_list_;

  int64_t lease_duration_;
  common::ObSchemaManagerV2* schema_manager_;
  //this one will protected schemaManager
  //actually schemaManager is accessed only in a schema change process
  mutable tbsys::CRWLock schema_manager_rwlock_;

  int64_t time_stamp_changing_;
  int64_t frozen_mem_version_;

  mutable tbsys::CThreadMutex root_table_build_mutex_; //any time only one thread can modify root_table
  //ObRootTable one for query
  //another for receive reporting and build new one
  NameTable* root_table_for_query_;
  ObTabletInfoManager* tablet_manager_for_query_;
  mutable tbsys::CRWLock root_table_rwlock_; //every query root table should rlock this

  NameTable* root_table_for_build_;
  ObTabletInfoManager* tablet_manager_for_build_;

  char config_file_name_[common::OB_MAX_FILE_NAME_LENGTH];
  char schema_file_name_[common::OB_MAX_FILE_NAME_LENGTH];
  bool have_inited_;
  mutable bool new_table_created_;

  int migrate_wait_seconds_;

  int server_status_;
  mutable int build_sync_flag_;
  mutable tbsys::CThreadMutex status_mutex_;
  int64_t safe_lost_one_duration_;
  int64_t wait_init_time_;
  int64_t max_merge_duration_;
  int64_t cs_merge_command_interval_mseconds_;
  bool first_cs_had_registed_;
  volatile bool receive_stop_;
  bool drop_this_build_;
  bool drop_last_cs;
  int32_t safe_copy_count_in_init_;
  int32_t safe_copy_count_in_merge_;
  int32_t create_table_in_init_;

  int64_t last_frozen_mem_version_;
  int64_t pre_frozen_mem_version_;
  int64_t last_frozen_time_;

  NameWorker* worker_;//who does the net job
  ObRootLogWorker* log_worker_;

  common::ObiRole obi_role_;        // my role as sb instance
  int32_t tablet_replicas_num_;
  time_t start_time_;
  common::ObClientConfig client_config_;
  common::ObServer my_addr_;

  tbsys::CThreadCond balance_worker_sleep_cond_;
  int64_t balance_worker_sleep_us_;
  int64_t balance_worker_idle_sleep_us_;
  int64_t balance_tolerance_count_;
  int64_t balance_start_time_us_;
  int64_t balance_timeout_us_;
  int64_t balance_timeout_us_delta_;
  int64_t balance_max_timeout_us_;
  int64_t balance_last_migrate_succ_time_;
  int32_t balance_next_table_seq_;
  int32_t balance_batch_migrate_count_;
  int32_t balance_batch_migrate_done_num_;
  int32_t balance_select_dest_start_pos_;
  int32_t balance_max_concurrent_migrate_num_;
  int32_t balance_max_migrate_out_per_cs_;
  int32_t balance_batch_copy_count_; // for monitor purpose
  int32_t enable_balance_;
  int32_t enable_rereplication_;
  bool balance_testing_;
  static const int32_t DEFAULT_TABLET_REPLICAS_NUM = 3;
  static const int64_t MIN_BALANCE_WORKER_SLEEP_US = 1000LL * 1000; // 1s
 protected:
  class rootTableModifier : public tbsys::CDefaultRunnable {
   public:
    explicit rootTableModifier(NameServer* name_server);
    void run(tbsys::CThread* thread, void* arg);
   private:
    NameServer* name_server_;
  };//report, switch schema and balance job
  rootTableModifier root_table_modifier_;

  class balanceWorker : public tbsys::CDefaultRunnable {
   public:
    explicit balanceWorker(NameServer* name_server);
    void run(tbsys::CThread* thread, void* arg);
   private:
    NameServer* name_server_;
  };
  balanceWorker balance_worker_;

  class heartbeatChecker : public tbsys::CDefaultRunnable {
   public:
    explicit heartbeatChecker(NameServer* name_server);
    void run(tbsys::CThread* thread, void* arg);
   private:
    NameServer* name_server_;
  };
  heartbeatChecker heart_beat_checker_;

};

}
}

#endif

