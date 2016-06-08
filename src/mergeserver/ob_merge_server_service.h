/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_server_service.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_SERVICE_H_
#define OCEANBASE_MERGESERVER_SERVICE_H_

#include "tbnet.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_obi_role.h"
#include "common/thread_buffer.h"
#include "ob_ms_schema_task.h"
#include "ob_ms_monitor_task.h"
#include "ob_ms_lease_task.h"
#include "ob_ms_ups_task.h"
#include "ob_merge_join_agent.h"

namespace sb {
namespace mergeserver {
class ObMergeServer;
class ObMergerRpcProxy;
class ObMergerRpcStub;
class ObMergerSchemaManager;
class ObMergerServiceMonitor;
class ObMergerTabletLocationCache;
static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024 * 1024 * 2; //2MB
static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000; // usleep 1 s
class ObMergeServerService {
 public:
  ObMergeServerService();
  ~ObMergeServerService();

 public:
  int initialize(ObMergeServer* merge_server);
  int start();
  int destroy();
 public:
  /// extend lease valid time = sys.cur_timestamp + delay
  void extend_lease(const int64_t delay);

  /// check lease expired
  bool check_lease(void) const;

  /// register to root server
  int register_root_server(void);

  int do_request(
    const int64_t receive_time,
    const int32_t packet_code,
    const int32_t version,
    const int32_t channel_id,
    tbnet::Connection* connection,
    common::ObDataBuffer& in_buffer,
    common::ObDataBuffer& out_buffer);

  //int get_agent(ObMergeJoinAgent *&agent);
  void handle_failed_request(const int64_t timeout, const int32_t packet_code);
 private:
  // lease init 20s
  static const int64_t DEFAULT_LEASE_TIME = 20 * 1000 * 1000L;

  // warning: fetch schema interval can not be too long
  // because of the heartbeat handle will block tbnet thread
  static const int64_t FETCH_SCHEMA_INTERVAL = 30 * 1000;

  // heartbeat
  int ms_heartbeat(
    const int64_t receive_time,
    const int32_t version,
    const int32_t channel_id,
    tbnet::Connection* connection,
    common::ObDataBuffer& in_buffer,
    common::ObDataBuffer& out_buffer);
  // clear cache
  int ms_clear(
    const int64_t receive_time,
    const int32_t version,
    const int32_t channel_id,
    tbnet::Connection* connection,
    common::ObDataBuffer& in_buffer,
    common::ObDataBuffer& out_buffer);
  // monitor stat
  int ms_stat(
    const int64_t receive_time,
    const int32_t version,
    const int32_t channel_id,
    tbnet::Connection* connection,
    common::ObDataBuffer& in_buffer,
    common::ObDataBuffer& out_buffer);
  // get query
  int ms_get(
    const int64_t receive_time,
    const int32_t version,
    const int32_t channel_id,
    tbnet::Connection* connection,
    common::ObDataBuffer& in_buffer,
    common::ObDataBuffer& out_buffer);
  // scan query
  int ms_scan(
    const int64_t receive_time,
    const int32_t version,
    const int32_t channel_id,
    tbnet::Connection* connection,
    common::ObDataBuffer& in_buffer,
    common::ObDataBuffer& out_buffer);
 private:
  // init server properties
  int init_ms_properties();
  // check read master role
  bool check_instance_role(const bool read_master) const;

 private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeServerService);
  ObMergeServer* merge_server_;
  bool inited_;
  // is registered or not
  volatile bool registered_;
  // lease timeout time
  int64_t lease_expired_time_;
  // instance role type
  common::ObiRole instance_role_;

 private:
  ObMergerRpcProxy*  rpc_proxy_;
  ObMergerRpcStub*   rpc_stub_;
  ObMergerSchemaManager* schema_mgr_;
  ObMergerUpsTask fetch_ups_task_;
  ObMergerSchemaTask fetch_schema_task_;
  ObMergerLeaseTask check_lease_task_;
  ObMergerMonitorTask monitor_task_;
  ObMergerTabletLocationCache* location_cache_;
  ObMergerServiceMonitor* service_monitor_;
  common::ThreadSpecificBuffer merge_join_agent_buffer_;
};

inline void ObMergeServerService::extend_lease(const int64_t delay) {
  lease_expired_time_ = tbsys::CTimeUtil::getTime() + delay;
}

inline bool ObMergeServerService::check_lease(void) const {
  return tbsys::CTimeUtil::getTime() <= lease_expired_time_;
}
} /* mergeserver */
} /* sb */

#endif /* end of include guard: OCEANBASE_MERGESERVER_SERVICE_H_ */


