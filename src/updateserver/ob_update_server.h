/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_update_server.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_H__
#define __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_H__

#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/batch_packet_queue_thread.h"
#include "common/ob_role_mgr.h"
#include "common/ob_log_writer.h"
#include "common/ob_fetch_runnable.h"
#include "common/ob_check_runnable.h"
#include "common/ob_packet_queue_thread.h"
#include "common/priority_packet_queue_thread.h"

#include "ob_ups_rpc_stub.h"
#include "ob_update_server_param.h"
#include "ob_ups_table_mgr.h"
#include "ob_ups_log_mgr.h"
#include "ob_ups_replay_runnable.h"
#include "ob_ups_stat.h"
#include "ob_sstable_mgr.h"
#include "ob_transfer_sstable_query.h"
#include "ob_ups_fetch_runnable.h"
#include "ob_obi_slave_stat.h"
#include "ob_commit_log_receiver.h"
#include "ob_ups_fetch_lsync.h"

namespace sb {
namespace updateserver {
class UpsWarmUpDuty : public common::ObTimerTask {
  static const int64_t MAX_DUTY_IDLE_TIME = 2000L * 1000L * 1000L; // 2000s
 public:
  UpsWarmUpDuty();
  virtual ~UpsWarmUpDuty();
  virtual void runTimerTask();
  bool drop_start();
  void drop_end();
  void finish_immediately();
 private:
  int64_t duty_start_time_;
  int64_t cur_warm_up_percent_;
  volatile uint64_t duty_waiting_;
};

class ObUpdateServer
  : public common::ObBaseServer, public tbnet::IPacketQueueHandler, public common::IBatchPacketQueueHandler {
 public:
  ObUpdateServer(ObUpdateServerParam& param);
  ~ObUpdateServer();
 private:
  DISALLOW_COPY_AND_ASSIGN(ObUpdateServer);
 public:
  tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection* connection, tbnet::Packet* packet);
  bool handleBatchPacket(tbnet::Connection* connection, tbnet::PacketQueue& packetQueue);
  /** packet queue handler */
  bool handlePacketQueue(tbnet::Packet* packet, void* args);
  bool handleBatchPacketQueue(const int64_t batch_num, tbnet::Packet** packets, void* args);

  /** called before start server */
  virtual int initialize();
  virtual void wait_for_queue();
  virtual void destroy();
  void cleanup();

  virtual int start_service();
  virtual void stop();
 public:
  const common::ObClientManager& get_client_manager() const;
  ObUpsRpcStub& get_ups_rpc_stub();

  common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;
  const ObUpdateServerParam& get_param() const {
    return param_;
  }

  ObUpsTableMgr& get_table_mgr() {
    return table_mgr_;
  }

  ObUpsLogMgr& get_log_mgr() {
    return log_mgr_;
  }

  const common::ObServer& get_self() {
    return self_addr_;
  }

  inline common::ObRoleMgr& get_role_mgr() {
    return role_mgr_;
  }

  inline const common::ObRoleMgr& get_role_mgr() const {
    return role_mgr_;
  }

  inline UpsStatMgr& get_stat_mgr() {
    return stat_mgr_;
  }

  inline SSTableMgr& get_sstable_mgr() {
    return sstable_mgr_;
  }

  inline ObTransferSSTableQuery& get_sstable_query() {
    return sstable_query_;
  }

  inline common::ObServer get_lsync_server() const {
    common::ObServer ret;
    server_lock_.rdlock();
    ret = lsync_server_;
    server_lock_.unlock();
    return ret;
  }

  inline void set_lsync_server(const char* ip, const int32_t port) {
    server_lock_.wrlock();
    lsync_server_.set_ipv4_addr(ip, port);
    server_lock_.unlock();
    fetch_lsync_.set_lsync_server(get_lsync_server());
  }

  int update_schema(const bool always_try, const bool write_log);

  int submit_major_freeze();

  int submit_handle_frozen();

  int submit_report_freeze();

  int submit_force_drop();

  int submit_delay_drop();

  int submit_immediately_drop();

  void schedule_warm_up_duty();

 private:
  int start_threads();
  int start_master_master_();
  int start_master_slave_();
  int start_slave_master_();
  int start_slave_slave_();
  int enter_master_master();
  int enter_slave_master();
  int switch_to_master_master();
  int reregister_followed(const common::ObServer& master);
  int reregister_standalone();
  int set_schema();
  int set_timer_major_freeze();
  int set_timer_handle_fronzen();
  int init_slave_log_mgr();
  int register_and_start_fetch(const common::ObServer& master, uint64_t& replay_point);
  int slave_standalone_prepare(uint64_t& log_id_start, uint64_t& log_seq_start);
  int init_replay_thread(const uint64_t log_id_start, const uint64_t log_seq_start);
  int start_standalone_();

  int set_self_(const char* dev_name, const int32_t port);

  int slave_register_followed(const common::ObServer& master, ObUpsFetchParam& fetch_param);
  int slave_register_standalone(uint64_t& log_id_start, uint64_t& log_seq_start);
 private:
  int set_obi_role(const int32_t version, common::ObDataBuffer& in_buff,
                   tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_slave_write_log(const int32_t version, common::ObDataBuffer& in_buff,
                          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_set_sync_limit(const int32_t version, common::ObDataBuffer& in_buff,
                         tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_get_clog_cursor(const int32_t version,
                          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_get_clog_master(const int32_t version, tbnet::Connection* conn,
                          const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_ping(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_get(const int32_t version, common::ObDataBuffer& in_buff,
              tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
              const int64_t start_time, const int64_t packet_timewait, const int32_t priority);
  int ups_scan(const int32_t version, common::ObDataBuffer& in_buff,
               tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
               const int64_t start_time, const int64_t packet_timewait, const int32_t priority);
  int ups_slave_register(const int32_t version, common::ObDataBuffer& in_buff,
                         tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_slave_quit(const int32_t version, common::ObDataBuffer& in_buff,
                     tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int ups_dump_text_memtable(const int32_t version, common::ObDataBuffer& in_buff,
                             tbnet::Connection* conn, const uint32_t channel_id);
  int ups_dump_text_schemas(const int32_t version,
                            tbnet::Connection* conn, const uint32_t channel_id);
  int ups_force_fetch_schema(const int32_t version,
                             tbnet::Connection* conn, const uint32_t channel_id);
  int ups_reload_conf(const int32_t version, common::ObDataBuffer& in_buf,
                      tbnet::Connection* conn, const uint32_t channel_id);
  int ups_renew_lease(const int32_t version, common::ObDataBuffer& in_buf,
                      tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_grant_lease(const int32_t version, common::ObDataBuffer& in_buf,
                      tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_change_vip(const int32_t version, common::ObDataBuffer& in_buff,
                     tbnet::Connection* conn, const uint32_t channel_id);
  int ups_memory_watch(const int32_t version,
                       tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_memory_limit_set(const int32_t version, common::ObDataBuffer& in_buff,
                           tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_priv_queue_conf_set(const int32_t version, common::ObDataBuffer& in_buff,
                              tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_clear_active_memtable(const int32_t version,
                                tbnet::Connection* conn, const uint32_t channel_id);
  int ups_switch_commit_log(const int32_t version,
                            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_get_last_frozen_version(const int32_t version,
                                  tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_get_table_time_stamp(const int32_t version, common::ObDataBuffer& in_buff,
                               tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_enable_memtable_checksum(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_disable_memtable_checksum(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_fetch_stat_info(const int32_t version,
                          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int ups_start_transaction(const MemTableTransType type, UpsTableMgrTransHandle& handle);
  int ups_apply(UpsTableMgrTransHandle& handle, common::ObDataBuffer& in_buff);
  int ups_end_transaction(tbnet::Packet** packets, const int64_t start_idx,
                          const int64_t last_idx, UpsTableMgrTransHandle& handle, int32_t last_err_code);

  int ups_freeze_memtable(const int32_t version, common::ObPacket* packet_orig, common::ObDataBuffer& in_buff, const int pcode);
  int ups_switch_schema(const int32_t version, common::ObPacket* packet_orig, common::ObDataBuffer& in_buf);
  int ups_create_memtable_index();
  int ups_drop_memtable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_delay_drop_memtable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_immediately_drop_memtable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_drop_memtable();
  int ups_get_bloomfilter(const int32_t version, common::ObDataBuffer& in_buff,
                          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int ups_store_memtable(const int32_t version, common::ObDataBuffer& in_buf,
                         tbnet::Connection* conn, const uint32_t channel_id);
  int ups_erase_sstable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_handle_frozen();
  int ups_load_new_store(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_reload_all_store(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_froce_report_frozen_version(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id);
  int ups_reload_store(const int32_t version, common::ObDataBuffer& in_buff,
                       tbnet::Connection* conn, const uint32_t channel_id);
  int ups_umount_store(const int32_t version, common::ObDataBuffer& in_buff,
                       tbnet::Connection* conn, const uint32_t channel_id);

  int response_result_(int32_t ret_code, int32_t cmd_type, int32_t func_version,
                       tbnet::Connection* conn, const uint32_t channel_id);
  int response_scanner_(int32_t ret_code, const common::ObScanner& scanner,
                        int32_t cmd_type, int32_t func_version,
                        tbnet::Connection* conn, const uint32_t channel_id,
                        common::ObDataBuffer& out_buff);
  int response_fetch_param_(int32_t ret_code, const ObUpsFetchParam& fetch_param,
                            int32_t cmd_type, int32_t func_version,
                            tbnet::Connection* conn, const uint32_t channel_id,
                            common::ObDataBuffer& out_buff);
  int response_lease_(int32_t ret_code, const common::ObLease& lease,
                      int32_t cmd_type, int32_t func_version,
                      tbnet::Connection* conn, const uint32_t channel_id,
                      common::ObDataBuffer& out_buff);
  template <class T>
  int response_data_(int32_t ret_code, const T& data,
                     int32_t cmd_type, int32_t func_version,
                     tbnet::Connection* conn, const uint32_t channel_id,
                     common::ObDataBuffer& out_buff, const int32_t* priority = NULL);
  int low_priv_speed_control_(const int64_t scanner_size);

  template <class Queue>
  int submit_async_task_(const common::PacketCode pcode, Queue& qthread, int32_t& task_queue_size);

  int report_frozen_version_();

 private:
  static const int MY_VERSION = 1;
  static const int32_t RPC_BUFFER_SIZE = 1024 * 1024 * 2; //2MB
  static const int32_t DEFAULT_TASK_READ_QUEUE_SIZE = 1000;
  static const int32_t DEFAULT_TASK_WRITE_QUEUE_SIZE = 1000;
  static const int32_t DEFAULT_TASK_LEASE_QUEUE_SIZE = 100;
  static const int32_t DEFAULT_TASK_LOG_QUEUE_SIZE = 100;
  static const int32_t DEFAULT_STORE_THREAD_QUEUE_SIZE = 100;
  static const int64_t DEFAULT_REQUEST_TIMEOUT_RESERVE = 100 * 1000; // 100ms
  static const int64_t DEFAULT_NETWORK_TIMEOUT = 1000 * 1000;
  static const int64_t DEFAULT_USLEEP_INTERVAL = 10 * 1000;
  static const int64_t DEFAULT_GET_OBI_ROLE_INTERVAL = 2000 * 1000;
  static const int64_t LEASE_ON = 1;
  static const int64_t LEASE_OFF = 0;
  static const int64_t DEFAULT_SLAVE_QUIT_TIMEOUT = 1000 * 1000; // 1s

  static const int64_t SEG_STAT_TIMES = 1000;
  static const int64_t SEG_STAT_SIZE = 100; // 100MB

 private:
  ObUpdateServerParam& param_;
  common::ObPacketFactory packet_factory_;
  common::ObClientManager client_manager_;
  ObUpsRpcStub ups_rpc_stub_;
  common::ThreadSpecificBuffer rpc_buffer_;
  common::ThreadSpecificBuffer my_thread_buffer_;

  //tbnet::PacketQueueThread read_thread_queue_; // for read task
  common::PriorityPacketQueueThread read_thread_queue_; // for read task
  common::BatchPacketQueueThread write_thread_queue_; // for write task
  common::ObPacketQueueThread lease_thread_queue_; // for lease
  common::ObPacketQueueThread log_thread_queue_;
  common::ObPacketQueueThread store_thread_; // store sstable
  ObUpsFetchRunnable fetch_thread_;
  ObUpsReplayRunnable log_replay_thread_;
  common::ObCheckRunnable check_thread_;
  int32_t read_task_queue_size_;
  int32_t write_task_queue_size_;
  int32_t lease_task_queue_size_;
  int32_t log_task_queue_size_;
  int32_t store_thread_queue_size_;

  ObUpsTableMgr table_mgr_;
  common::ObRoleMgr role_mgr_;
  common::ObiRole obi_role_;
  ObiSlaveStat obi_slave_stat_;
  common::ObServer name_server_;
  common::ObServer ups_master_;
  common::ObServer self_addr_;
  common::ObServer ups_inst_master_;
  common::ObServer lsync_server_;
  mutable common::SpinRWLock server_lock_;
  common::ObSlaveMgr slave_mgr_;
  ObUpsLogMgr log_mgr_;
  UpsStatMgr stat_mgr_;
  SSTableMgr sstable_mgr_;
  ObTransferSSTableQuery sstable_query_;
  MajorFreezeDuty major_freeze_duty_;
  HandleFrozenDuty handle_frozen_duty_;
  UpsWarmUpDuty warm_up_duty_;
  common::ObTimer timer_;
  ObCommitLogReceiver clog_receiver_;
  ObUpsFetchLsync fetch_lsync_;

  int64_t start_trans_timestamp_;
  bool is_first_log_;
  bool is_registered_;
};
}
}

#endif //__OB_UPDATE_SERVER_H__



