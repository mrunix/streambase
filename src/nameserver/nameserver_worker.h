/*
 * src/nameserver/nameserver_worker.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for NameServerWorker.
 *
 * Library: nameserver
 * Package: nameserver
 * Module : NameServerWorker
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef OCEANBASE_ROOTSERVER_ROOT_WORKER_H_
#define OCEANBASE_ROOTSERVER_ROOT_WORKER_H_

#include "common/ob_define.h"
#include "common/ob_base_server.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_fetch_runnable.h"
#include "common/ob_role_mgr.h"
#include "common/ob_slave_mgr.h"
#include "common/ob_check_runnable.h"
#include "common/ob_packet_queue_thread.h"
#include "nameserver/nameserver.h"
#include "nameserver/nameserver_rpc_stub.h"
#include "nameserver/nameserver_log_replay.h"
#include "nameserver/nameserver_log_manager.h"
#include "nameserver/nameserver_stat.h"
#include "nameserver/nameserver_fetch_thread.h"

namespace sb {
namespace common {
class ObDataBuffer;
class ObServer;
class ObScanner;
class ObTabletReportInfoList;
class ObGetParam;
class ObScanParam;
class ObRange;
}

namespace nameserver {

class NameServerWorker : public sb::common::ObBaseServer, public tbnet::IPacketQueueHandler {
 public:
  NameServerWorker();
  virtual ~NameServerWorker();

  tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection* connection, tbnet::Packet* packet);
  bool handleBatchPacket(tbnet::Connection* connection, tbnet::PacketQueue& packetQueue);
  bool handlePacketQueue(tbnet::Packet* packet, void* args);

  int initialize();
  int start_service();
  void wait_for_queue();
  void destroy();

  bool start_merge();
  int set_config_file_name(const char* conf_file_name);

  NameServerLogManager* get_log_manager();
  common::ObRoleMgr* get_role_manager();
  common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;
  virtual NameServerRpcStub& get_rpc_stub();
  int64_t get_rpc_timeout() const;
  ObRootStatManager& get_stat_manager();
 private:
  // notice that in_buff can not be const.
  int rt_get_update_server_info(const int32_t version, common::ObDataBuffer& in_buff,
                                tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
                                bool use_inner_port = false);

  int rt_get_merge_delay_interval(const int32_t version, common::ObDataBuffer& in_buff,
                                  tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_get(const int32_t version, common::ObDataBuffer& in_buff,
             tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_scan(const int32_t version, common::ObDataBuffer& in_buff,
              tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_fetch_schema(const int32_t version, common::ObDataBuffer& in_buff,
                      tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_fetch_schema_version(const int32_t version, common::ObDataBuffer& in_buff,
                              tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_report_tablets(const int32_t version, common::ObDataBuffer& in_buff,
                        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_waiting_jsb_done(const int32_t version, common::ObDataBuffer& in_buff,
                          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_register(const int32_t version, common::ObDataBuffer& in_buff,
                  tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_migrate_over(const int32_t version, common::ObDataBuffer& in_buff,
                      tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_report_capacity_info(const int32_t version, common::ObDataBuffer& in_buff,
                              tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_heartbeat(const int32_t version, common::ObDataBuffer& in_buff,
                   tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_dump_cs_info(const int32_t version, common::ObDataBuffer& in_buff,
                      tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_fetch_stats(const int32_t version, common::ObDataBuffer& in_buff,
                     tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_ping(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_slave_quit(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_update_server_report_freeze(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_slave_register(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_renew_lease(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_grant_lease(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_get_obi_role(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_set_obi_role(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_admin(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_change_log_level(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_stat(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int rt_get_obi_config(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_set_obi_config(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_get_ups(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_set_ups_config(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_get_client_config(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_get_cs_list(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
  int rt_get_ms_list(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

  int do_stat(int stat_key, char* buf, const int64_t buf_len, int64_t& pos);
  int do_admin(int admin_cmd);

  int start_as_master();
  int start_as_slave();

  int slave_register_(common::ObFetchParam& fetch_param);
  int rt_slave_write_log(const int32_t version, common::ObDataBuffer& in_buffer, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buffer);

  int get_obi_role_from_master();
 protected:
  bool is_registered_;
  char config_file_name_[common::OB_MAX_FILE_NAME_LENGTH];
  NameServer name_server_;
  common::ObPacketQueueThread read_thread_queue_;
  common::ObPacketQueueThread write_thread_queue_;
  common::ObPacketQueueThread log_thread_queue_;
  int task_read_queue_size_;
  int task_write_queue_size_;
  int task_log_queue_size_;
  int64_t network_timeout_;
  int64_t expected_process_us_;
  common::ThreadSpecificBuffer my_thread_buffer;
  common::ObClientManager client_manager;

  common::ObServer rt_master_;
  common::ObServer self_addr_;
  common::ObRoleMgr role_mgr_;
  common::ObSlaveMgr slave_mgr_;
  common::ObCheckRunnable check_thread_;
  ObRootFetchThread fetch_thread_;

  NameServerRpcStub ns_rpc_stub_;
  ObRootLogReplay log_replay_thread_;
  NameServerLogManager log_manager_;
  ObRootStatManager stat_manager_;
};

inline NameServerRpcStub& NameServerWorker::get_rpc_stub() {
  return ns_rpc_stub_;
}

inline int64_t NameServerWorker::get_rpc_timeout() const {
  return network_timeout_;
}

inline ObRootStatManager& NameServerWorker::get_stat_manager() {
  return stat_manager_;
}
}
}

#endif


