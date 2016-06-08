/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_server_service.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "ob_merge_server_service.h"
#include "ob_merge_server.h"
#include "common/ob_define.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/ob_result.h"
#include "common/ob_action_flag.h"
#include "common/ob_trace_log.h"
#include "common/ob_tsi_factory.h"
#include "ob_ms_rpc_proxy.h"
#include "ob_ms_rpc_stub.h"
#include "ob_ms_service_monitor.h"
#include "ob_ms_schema_manager.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_get_cell_stream.h"
#include "ob_ms_scan_cell_stream.h"
#include "ob_merge_join_agent.h"
#include "ob_read_param_decoder.h"
#include "ob_ms_tsi.h"

using namespace sb;
using namespace sb::common;
using namespace sb::mergeserver;

namespace {
int ms_get_merge_join_agent(ObMergeJoinAgent*& agent, ThreadSpecificBuffer& merge_join_agent_buffer,
                            ObMergerRpcProxy& rpc_proxy) {
  int err = OB_SUCCESS;
  static __thread void* thread_agent = NULL;
  agent = NULL;
  if (NULL == thread_agent) {
    ThreadSpecificBuffer::Buffer* agent_buffer = merge_join_agent_buffer.get_buffer();
    if (NULL == agent_buffer) {
      TBSYS_LOG(WARN, "%s", "fail to get thread specific buffer for agent");
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
    if (OB_SUCCESS == err) {
      agent_buffer->reset();
      if (NULL == agent_buffer->current()
          || agent_buffer->remain() < static_cast<int32_t>(sizeof(ObMergeJoinAgent))) {
        TBSYS_LOG(ERROR, "%s", "logic error, thread buffer is null");
      } else {
        agent = new(agent_buffer->current()) ObMergeJoinAgent(rpc_proxy);
        thread_agent = agent;
      }
    }
  } else {
    agent = reinterpret_cast<ObMergeJoinAgent*>(thread_agent);
  }
  return err;
}
}

namespace sb {
namespace mergeserver {
ObMergeServerService::ObMergeServerService()
  : merge_server_(NULL), inited_(false), registered_(false), rpc_proxy_(NULL), rpc_stub_(NULL),
    schema_mgr_(NULL), location_cache_(NULL), service_monitor_(NULL),
    merge_join_agent_buffer_(sizeof(ObMergeJoinAgent)) {
  lease_expired_time_ = tbsys::CTimeUtil::getTime() + DEFAULT_LEASE_TIME;
}

ObMergeServerService::~ObMergeServerService() {
  destroy();
}

int ObMergeServerService::start() {
  return init_ms_properties();
}

int ObMergeServerService::initialize(ObMergeServer* merge_server) {
  int rc = OB_SUCCESS;
  if (inited_) {
    rc = OB_INIT_TWICE;
  } else {
    merge_server_ = merge_server;
    //TODO do init task
    //rc = init_ms_properties_();
  }
  return rc;
}

int ObMergeServerService::register_root_server() {
  int err = OB_SUCCESS;
  registered_ = false;
  while (!merge_server_->is_stoped()) {
    err = rpc_stub_->register_server(merge_server_->get_params().get_network_timeout(),
                                     merge_server_->get_root_server(),
                                     merge_server_->get_self(), true);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to register merge server to root server [root_ip:%d,root_port:%d]",
                merge_server_->get_root_server().get_ipv4(),
                merge_server_->get_root_server().get_port());
      usleep(RETRY_INTERVAL_TIME);
    } else {
      registered_ = true;
      lease_expired_time_ = tbsys::CTimeUtil::getTime() + DEFAULT_LEASE_TIME;
      break;
    }
  }
  return err;
}

// check instance role is right for read master
bool ObMergeServerService::check_instance_role(const bool read_master) const {
  bool result = true;
  if ((true == read_master) && (instance_role_.get_role() != ObiRole::MASTER)) {
    result = false;
  }
  return result;
}

int ObMergeServerService::init_ms_properties() {
  int err  = OB_SUCCESS;
  ObSchemaManagerV2* newest_schema_mgr = NULL;
  if (OB_SUCCESS == err) {
    newest_schema_mgr = new(std::nothrow)ObSchemaManagerV2;
    if (NULL == newest_schema_mgr) {
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
  }

  if (OB_SUCCESS == err) {
    rpc_stub_ = new(std::nothrow)ObMergerRpcStub();
    if (NULL == rpc_stub_) {
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
  }

  if (OB_SUCCESS == err) {
    err = rpc_stub_->init(merge_server_->get_rpc_buffer(),
                          &(merge_server_->get_client_manager()));
    if (OB_SUCCESS == err) {
      err = register_root_server();
      if (err != OB_SUCCESS) {
        TBSYS_LOG(WARN, "server register to root failed:ret[%d]", err);
      }
    }
  }

  if (OB_SUCCESS == err) {
    ObServer ups;
    for (int32_t i = 0; i <= merge_server_->get_params().get_retry_times(); ++i) {
      err = rpc_stub_->find_server(merge_server_->get_params().get_network_timeout(),
                                   merge_server_->get_root_server(), ups);
      if (OB_SUCCESS == err) {
        merge_server_->set_update_server(ups);
        break;
      }
      usleep(RETRY_INTERVAL_TIME);
    }
  }
  /// ugly implimentation, tell ObScanMergeJoinAgentImp that can return uncomplete results
  if (OB_SUCCESS == err) {
    ObScanMergeJoinAgentImp::set_return_uncomplete_result(merge_server_->get_params().allow_return_uncomplete_result());
  }

  if (OB_SUCCESS == err) {
    schema_mgr_ = new(std::nothrow)ObMergerSchemaManager;
    if (NULL == schema_mgr_) {
      err = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int32_t i = 0; i <= merge_server_->get_params().get_retry_times(); ++i) {
        err = rpc_stub_->fetch_schema(merge_server_->get_params().get_network_timeout(),
                                      merge_server_->get_root_server(),
                                      0, *newest_schema_mgr);
        if (OB_SUCCESS == err) {
          schema_mgr_->init(*newest_schema_mgr);
          break;
        }
        usleep(RETRY_INTERVAL_TIME);
      }
    }
  }

  if (OB_SUCCESS == err) {
    rpc_proxy_ = new(std::nothrow)ObMergerRpcProxy(merge_server_->get_params().get_retry_times(),
                                                   merge_server_->get_params().get_network_timeout(),
                                                   merge_server_->get_root_server(),
                                                   merge_server_->get_update_server(),
                                                   merge_server_->get_self());
    if (NULL == rpc_proxy_) {
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
  }

  if (OB_SUCCESS == err) {
    location_cache_ = new(std::nothrow)ObMergerTabletLocationCache;
    if (NULL == location_cache_) {
      err = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      err = location_cache_->init(merge_server_->get_params().get_tablet_location_cache_size(),
                                  1024, merge_server_->get_params().get_tablet_location_cache_timeout());
    }
  }

  if (OB_SUCCESS == err) {
    service_monitor_ = new(std::nothrow)ObMergerServiceMonitor(0);
    if (NULL == service_monitor_) {
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
  }

  if (OB_SUCCESS == err) {
    err = rpc_proxy_->init(rpc_stub_, schema_mgr_, location_cache_, service_monitor_);
  }

  /// set update server black list param
  if (OB_SUCCESS == err) {
    err = rpc_proxy_->set_blacklist_param(merge_server_->get_params().get_ups_blacklist_timeout(),
                                          merge_server_->get_params().get_ups_fail_count());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "set update server black list param failed:ret[%d]", err);
    }
  }

  if (OB_SUCCESS == err) {
    int32_t count = 0;
    for (int32_t i = 0; i <= merge_server_->get_params().get_retry_times(); ++i) {
      err = rpc_proxy_->fetch_update_server_list(count);
      if (OB_SUCCESS == err) {
        TBSYS_LOG(INFO, "fetch update server list succ:count[%ld]", count);
        break;
      }
      usleep(RETRY_INTERVAL_TIME);
    }
  }

  if (OB_SUCCESS == err) {
    err = merge_server_->get_timer().init();
    if (err != OB_SUCCESS) {
      TBSYS_LOG(INFO, "timer init failed:ret[%d]", err);
    }
  }

  // lease check timer task
  if (OB_SUCCESS == err) {
    check_lease_task_.init(this);
    err = merge_server_->get_timer().schedule(check_lease_task_,
                                              merge_server_->get_params().get_lease_check_interval(), true);
    if (OB_SUCCESS == err) {
      TBSYS_LOG(INFO, "%s", "lease check timer schedule succ");
    } else {
      TBSYS_LOG(ERROR, "lease check timer schedule failed:ret[%d]", err);
    }
  }

  // monitor timer task
  if (OB_SUCCESS == err) {
    err = merge_server_->get_timer().schedule(monitor_task_,
                                              merge_server_->get_params().get_monitor_interval(), true);
    if (OB_SUCCESS == err) {
      TBSYS_LOG(INFO, "%s", "monitor timer schedule succ");
    } else {
      TBSYS_LOG(ERROR, "monitor timer schedule failed:ret[%d]", err);
    }
  }

  // fetch ups timer task
  if (OB_SUCCESS == err) {
    fetch_ups_task_.init(rpc_proxy_);
    err = merge_server_->get_timer().schedule(fetch_ups_task_,
                                              merge_server_->get_params().get_fetch_ups_interval(), true);
    if (OB_SUCCESS == err) {
      TBSYS_LOG(INFO, "%s", "fetch ups list timer schedule succ");
    } else {
      TBSYS_LOG(ERROR, "fetch ups list timer schedule failed:ret[%d]", err);
    }
  }

  if (OB_SUCCESS != err) {
    if (rpc_proxy_) {
      delete rpc_proxy_;
      rpc_proxy_ = NULL;
    }

    if (rpc_stub_) {
      delete rpc_stub_;
      rpc_stub_ = NULL;
    }

    if (schema_mgr_) {
      delete schema_mgr_;
      schema_mgr_ = NULL;
    }

    if (location_cache_) {
      delete location_cache_;
      location_cache_ = NULL;
    }
  }

  if (newest_schema_mgr) {
    delete newest_schema_mgr;
    newest_schema_mgr = NULL;
  }

  if (OB_SUCCESS == err) {
    inited_ = true;
  }
  return err;
}

int ObMergeServerService::destroy() {
  int rc = OB_SUCCESS;
  if (inited_) {
    inited_ = false;
    merge_server_->get_timer().destroy();
    merge_server_ = NULL;
    delete rpc_proxy_;
    rpc_proxy_ = NULL;
    delete rpc_stub_;
    rpc_stub_ = NULL;
    delete schema_mgr_;
    schema_mgr_ = NULL;
    delete location_cache_;
    location_cache_ = NULL;
    delete service_monitor_;
    service_monitor_ = NULL;
  } else {
    rc = OB_NOT_INIT;
  }
  return rc;
}

void ObMergeServerService::handle_failed_request(const int64_t timeout, const int32_t packet_code) {
  if (!inited_) { //|| !registered_)
    TBSYS_LOG(WARN, "%s", "merge server has not inited or registered");
  } else {
    // no need deserialize the packet to get the table id
    switch (packet_code) {
    case OB_SCAN_REQUEST:
      service_monitor_->inc(0, ObMergerServiceMonitor::FAIL_SCAN_OP_COUNT);
      service_monitor_->inc(0, ObMergerServiceMonitor::FAIL_SCAN_OP_TIME, timeout);
      break;
    case OB_GET_REQUEST:
      service_monitor_->inc(0, ObMergerServiceMonitor::FAIL_GET_OP_COUNT);
      service_monitor_->inc(0, ObMergerServiceMonitor::FAIL_GET_OP_TIME, timeout);
      break;
    default:
      TBSYS_LOG(WARN, "handle overflow or timeout packet not include statistic info:packet[%d]", packet_code);
    }
  }
}

int ObMergeServerService::do_request(
  const int64_t receive_time,
  const int32_t packet_code,
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  int rc = OB_SUCCESS;
  if (!inited_) { //|| !registered_)
    TBSYS_LOG(WARN, "%s", "merge server has not inited or registered");
    rc = OB_NOT_INIT;
  }

  if (rc == OB_SUCCESS) {
    switch (packet_code) {
    case OB_REQUIRE_HEARTBEAT:
      rc = ms_heartbeat(receive_time, version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_SCAN_REQUEST:
      rc = ms_scan(receive_time, version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_GET_REQUEST:
      rc = ms_get(receive_time, version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_FETCH_STATS:
      rc = ms_stat(receive_time, version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_CLEAR_REQUEST:
      rc = ms_clear(receive_time, version, channel_id, connection, in_buffer, out_buffer);
      break;
    default:
      TBSYS_LOG(WARN, "check packet type failed:type[%d]", packet_code);
      rc = OB_ERROR;
    }
  }
  return rc;
}

int ObMergeServerService::ms_heartbeat(
  const int64_t start_time,
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  FILL_TRACE_LOG("step 1. start process heartbeat");
  ObResultCode rc;
  int32_t& err = rc.result_code_;
  UNUSED(start_time);
  UNUSED(channel_id);
  UNUSED(connection);
  UNUSED(out_buffer);
  int64_t lease_duration = 0;
  err = serialization::decode_vi64(in_buffer.get_data(),
                                   in_buffer.get_capacity(), in_buffer.get_position(), &lease_duration);
  if (OB_SUCCESS != err) {
    TBSYS_LOG(ERROR, "parse heartbeat input lease_duration param failed:ret[%d]", err);
  } else {
    TBSYS_LOG(DEBUG, "get ms heart_beat lease_duration=%ld", lease_duration);
    if (lease_duration <= 0) {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "check lease duration failed:duration[%ld]", lease_duration);
    }
  }

  int64_t local_version = schema_mgr_->get_latest_version();
  int64_t schema_version = 0;
  if (OB_SUCCESS == err) {
    err = serialization::decode_vi64(in_buffer.get_data(), in_buffer.get_capacity(),
                                     in_buffer.get_position(), &schema_version);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "parse heartbeat schema version failed:ret[%d]", err);
    } else if (local_version > schema_version) {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "check schema local version gt than new version:"
                "local[%ld], new[%ld]", local_version, schema_version);
    }
  }

  const static int32_t HEARTBEAT_VERSION = 3;
  if ((version == HEARTBEAT_VERSION) && (OB_SUCCESS == err)) {
    ObiRole role;
    err = role.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "deserialize instance role failed:ret[%d]", err);
    } else if (instance_role_.get_role() != role.get_role()) {
      TBSYS_LOG(INFO, "change instance role:from[%d], to[%d]", instance_role_.get_role(), role.get_role());
      instance_role_ = role;
    }
  }

  FILL_TRACE_LOG("step 2. decode heartbeat:lease[%ld], role[%d], local[%ld], version[%ld]",
                 lease_duration, instance_role_.get_role(), local_version, schema_version);

  if (OB_SUCCESS == err) {
    err = rpc_proxy_->async_heartbeat(merge_server_->get_self());
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "heartbeat to root server failed:ret[%d]", err);
    } else {
      // extend lease
      extend_lease(lease_duration);
      TBSYS_LOG(DEBUG, "%s", "heartbeat to root server succ");
    }
  }

  // fetch new schema in a temp timer task
  if ((OB_SUCCESS == err) && (local_version < schema_version)) {
    fetch_schema_task_.init(rpc_proxy_, schema_mgr_);
    fetch_schema_task_.set_version(local_version, schema_version);
    srand(tbsys::CTimeUtil::getTime());
    merge_server_->get_timer().schedule(fetch_schema_task_, random() % FETCH_SCHEMA_INTERVAL, false);
  }

  FILL_TRACE_LOG("step 3. process heartbeat finish:ret[%d]", err);
  CLEAR_TRACE_LOG();
  return err;
}

int ObMergeServerService::ms_clear(
  const int64_t start_time,
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  FILL_TRACE_LOG("step 1. start clear tablet location cache");
  const int32_t MS_CLEAR_VERSION = 1;
  UNUSED(start_time);
  UNUSED(in_buffer);
  UNUSED(version);
  ObResultCode rc;
  int32_t& err = rc.result_code_;
  if (NULL != location_cache_) {
    err = location_cache_->clear();
    TBSYS_LOG(INFO, "clear tablet location cache:ret[%d]", err);
  } else {
    err = OB_ERROR;
  }

  int32_t send_err  = OB_SUCCESS;
  err = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  if (OB_SUCCESS == err) {
    send_err = merge_server_->send_response(OB_CLEAR_RESPONSE, MS_CLEAR_VERSION,
                                            out_buffer, connection, channel_id);
  }

  FILL_TRACE_LOG("step 2. process clear cache finish:ret[%d]", err);
  PRINT_TRACE_LOG();
  CLEAR_TRACE_LOG();
  return send_err;
}

int ObMergeServerService::ms_stat(
  const int64_t start_time,
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  FILL_TRACE_LOG("step 1. start monitor stat");
  const int32_t MS_MONITOR_VERSION = 1;
  ObResultCode rc;
  int32_t& err = rc.result_code_;
  int32_t send_err  = OB_SUCCESS;
  UNUSED(in_buffer);
  UNUSED(start_time);
  UNUSED(version);
  err = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  if (OB_SUCCESS == err) {
    if (NULL != service_monitor_) {
      err = service_monitor_->serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                                        out_buffer.get_position());
    } else {
      err = OB_NO_MONITOR_DATA;
    }
  }

  if (OB_SUCCESS == err) {
    send_err = merge_server_->send_response(OB_FETCH_STATS_RESPONSE, MS_MONITOR_VERSION,
                                            out_buffer, connection, channel_id);
  }

  FILL_TRACE_LOG("step 2. process monitor stat finish:ret[%d]", err);
  CLEAR_TRACE_LOG();
  return send_err;
}

int ObMergeServerService::ms_get(
  const int64_t start_time,
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  FILL_TRACE_LOG("step 1. start serve ms_get");
  const int32_t MS_GET_VERSION = 1;
  ObResultCode rc;
  int32_t& err = rc.result_code_;
  int32_t send_err  = OB_SUCCESS;
  err = OB_SUCCESS;
  ObGetParam* org_get_param = GET_TSI_MULT(ObGetParam, ORG_PARAM_ID);
  ObGetParam* decoded_get_param = GET_TSI_MULT(ObGetParam, DECODED_PARAM_ID);
  ObReadParam& decoded_read_param = *decoded_get_param;
  const ObSchemaManagerV2* schema_mgr = NULL;
  ObMSGetCellStream cs_stream(rpc_proxy_);
  ObMSGetCellStream ups_stream(rpc_proxy_);
  ObMSGetCellStream ups_join_stream(rpc_proxy_);
  ObScanner* result_scanner = GET_TSI_MULT(ObScanner, RESULT_SCANNER_ID);
  int64_t fullfilled_item_num = 0;
  ObMergeJoinAgent* agent = NULL;
  //err = get_agent(agent);
  err = ms_get_merge_join_agent(agent, merge_join_agent_buffer_, *rpc_proxy_);
  if (OB_SUCCESS == err && MS_GET_VERSION != version) {
    err = OB_ERROR_FUNC_VERSION;
  }
  if (OB_SUCCESS == err &&
      (NULL == org_get_param || NULL == decoded_get_param || NULL == result_scanner)) {
    TBSYS_LOG(WARN, "fail to allocate memory for request");
    err = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_SUCCESS == err) {
    org_get_param->reset();
    decoded_get_param->reset();
    result_scanner->reset();
  }

  /// decode request
  if (OB_SUCCESS == err) {
    err = org_get_param->deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
                                     in_buffer.get_position());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to parse ObGetParam [err:%d]", err);
    } else {
      decoded_read_param = *dynamic_cast<ObReadParam*>(org_get_param);
    }
  }

  if (0 >= org_get_param->get_cell_size()) {
    TBSYS_LOG(WARN, "get param cell size error, [org_get_param:%ld]", org_get_param->get_cell_size());
    err = OB_INVALID_ARGUMENT;
  }

  /// get local newest schema
  if (OB_SUCCESS == err) {
    err =  rpc_proxy_->get_schema(ObMergerRpcProxy::LOCAL_NEWEST, &schema_mgr);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "%s", "fail to get the latest schema, unexpected error");
      err = OB_SCHEMA_ERROR;
    }
  }
  if (OB_SUCCESS == err) {
    err = ob_decode_get_param(*org_get_param, *schema_mgr, *decoded_get_param);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to decode get param [err:%d]", err);
    }
  }

  FILL_TRACE_LOG("step 2. finish parse the schema for ms_get:err[%d]", err);
  /// do request
  if (OB_SUCCESS == err) {
    err = agent->set_request_param(*decoded_get_param, ups_stream,
                                   ups_join_stream, *schema_mgr,
                                   merge_server_->get_params().get_intermediate_buffer_size());
    if (OB_INVALID_START_VERSION == err || OB_CS_TABLET_NOT_EXIST == err) {
      err = agent->set_request_param(*decoded_get_param, ups_stream,
                                     ups_join_stream, *schema_mgr,
                                     merge_server_->get_params().get_intermediate_buffer_size());
    }
  }
  if (OB_SUCCESS == err) {
    err = agent->next_cell();
  }
  bool size_overflow = false;
  bool cur_row_not_exist = false;
  bool is_row_changed = false;
  ObCellInfo* cur_cell = NULL;
  /// prepare result
  while (OB_SUCCESS == err
         && result_scanner->get_serialize_size() < RESPONSE_PACKET_BUFFER_SIZE
         && !size_overflow) {
    if (fullfilled_item_num >= org_get_param->get_cell_size()) {
      TBSYS_LOG(ERROR, "unexpected error, get more cell than needed [got_cell_num:%ld,org_get_param:%ld]",
                fullfilled_item_num, org_get_param->get_cell_size());
      err = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS == err) {
      err = agent->get_cell(&cur_cell, &is_row_changed);
      if (OB_SUCCESS == err && is_row_changed) {
        if (cur_cell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST) {
          cur_row_not_exist = true;
        } else {
          cur_row_not_exist = false;
        }
      }
    }
    if (OB_SUCCESS == err && (!cur_row_not_exist || is_row_changed)) {
      cur_cell->table_name_ = (*org_get_param)[fullfilled_item_num]->table_name_;
      cur_cell->column_name_ = (*org_get_param)[fullfilled_item_num]->column_name_;
      /// @todo (wushi wushi.ly@taobao.com) make sure add empty row cells or not
      cur_cell->table_id_ = OB_INVALID_ID;
      cur_cell->column_id_ = OB_INVALID_ID;
      err = result_scanner->add_cell(*cur_cell);
      if (OB_SIZE_OVERFLOW == err) {
        int64_t roll_cell_idx = fullfilled_item_num - 1;
        size_overflow = true;
        while (((*decoded_get_param)[roll_cell_idx]->table_id_
                == (*decoded_get_param)[fullfilled_item_num - 1]->table_id_)
               && ((*decoded_get_param)[roll_cell_idx]->row_key_
                   == (*decoded_get_param)[fullfilled_item_num - 1]->row_key_)) {
          roll_cell_idx --;
        }
        fullfilled_item_num = roll_cell_idx + 1;
        err = result_scanner->rollback();
      }
      if (OB_SUCCESS == err && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG) {
        TBSYS_LOG(DEBUG, "table_name:%.*s,rowkey:%.*s,column_name:%.*s,ext:%ld,type:%d",
                  cur_cell->table_name_.length(), cur_cell->table_name_.ptr(),
                  cur_cell->row_key_.length(), cur_cell->row_key_.ptr(),
                  cur_cell->column_name_.length(), cur_cell->column_name_.ptr(),
                  cur_cell->value_.get_ext(), cur_cell->value_.get_type());
        hex_dump(cur_cell->row_key_.ptr(), cur_cell->row_key_.length());
      }
    }
    if (OB_SUCCESS == err && !size_overflow) {
      err = agent->next_cell();
      fullfilled_item_num ++;
    }
  }

  if (OB_SUCCESS == err || OB_ITER_END == err) {
    bool is_fullfilled = false;
    is_fullfilled = (fullfilled_item_num == org_get_param->get_cell_size());
    err = result_scanner->set_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
    FILL_TRACE_LOG("step 3. finish get all the data for ms_get:[err:%d,fullfilled_item_num:%ld,"
                   "is_fullfulled:%d]", err, fullfilled_item_num, is_fullfilled);
  }

  int err_code = rc.result_code_;
  /// always send error code
  err = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  if (OB_SUCCESS == err) {
    err = result_scanner->serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                                    out_buffer.get_position());
  }

  FILL_TRACE_LOG("step 4. finish serialize the scanner result for ms_get:err[%d]", err);

  if (OB_SUCCESS == err) {
    send_err = merge_server_->send_response(OB_GET_RESPONSE, MS_GET_VERSION, out_buffer,
                                            connection, channel_id);
  }

  if (NULL != service_monitor_) {
    int64_t table_id = 0;
    int64_t end_time = tbsys::CTimeUtil::getTime();
    if ((*decoded_get_param)[0] != NULL) {
      table_id = (*decoded_get_param)[0]->table_id_;
    }

    if (OB_SUCCESS == err_code) {
      service_monitor_->inc(table_id, ObMergerServiceMonitor::SUCC_GET_OP_COUNT);
      service_monitor_->inc(table_id, ObMergerServiceMonitor::SUCC_GET_OP_TIME, end_time - start_time);
    } else {
      service_monitor_->inc(table_id, ObMergerServiceMonitor::FAIL_GET_OP_COUNT);
      service_monitor_->inc(table_id, ObMergerServiceMonitor::FAIL_GET_OP_TIME, end_time - start_time);
    }
  }

  FILL_TRACE_LOG("step 5. at last send scanner reponse for ms_get:send_err[%d], code[%d]",
                 send_err, err_code);

  if (NULL != schema_mgr) {
    rpc_proxy_->release_schema(schema_mgr);
  }
  if (NULL != agent) {
    agent->clear();
  }
  PRINT_TRACE_LOG();
  CLEAR_TRACE_LOG();
  return send_err;
}



int ObMergeServerService::ms_scan(
  const int64_t start_time,
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  FILL_TRACE_LOG("step 1. start serve ms_scan");
  const int32_t MS_SCAN_VERSION = 1;
  ObResultCode rc;
  int32_t& err = rc.result_code_;
  int32_t send_err  = OB_SUCCESS;
  err = OB_SUCCESS;
  ObScanParam* org_scan_param = GET_TSI_MULT(ObScanParam, ORG_PARAM_ID);
  ObScanParam* decoded_scan_param = GET_TSI_MULT(ObScanParam, DECODED_PARAM_ID);
  ObReadParam& decoded_read_param = *decoded_scan_param;
  const ObSchemaManagerV2* schema_mgr = NULL;
  ObMSScanCellStream cs_stream(rpc_proxy_);
  ObMSScanCellStream ups_stream(rpc_proxy_);
  ObMSGetCellStream ups_join_stream(rpc_proxy_);
  ObScanner* result_scanner = GET_TSI_MULT(ObScanner, RESULT_SCANNER_ID);
  int64_t fullfilled_row_num = 0;
  ObMergeJoinAgent* agent = NULL;
  //err = get_agent(agent);
  err = ms_get_merge_join_agent(agent, merge_join_agent_buffer_, *rpc_proxy_);
  if (OB_SUCCESS == err && MS_SCAN_VERSION != version) {
    err = OB_ERROR_FUNC_VERSION;
  }
  if (OB_SUCCESS == err &&
      (NULL == org_scan_param || NULL == decoded_scan_param || NULL == result_scanner)) {
    TBSYS_LOG(WARN, "fail to allocate memory for request");
    err = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_SUCCESS == err) {
    org_scan_param->reset();
    decoded_scan_param->reset();
    result_scanner->reset();
  }
  /// decode request
  if (OB_SUCCESS == err) {
    err = org_scan_param->deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
                                      in_buffer.get_position());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to parse ObScanParam [err:%d]", err);
    } else {
      decoded_read_param = *dynamic_cast<ObReadParam*>(org_scan_param);
    }
  }

  /// get local newest schema
  if (OB_SUCCESS == err) {
    err =  rpc_proxy_->get_schema(ObMergerRpcProxy::LOCAL_NEWEST, &schema_mgr);
    if ((OB_SUCCESS != err) || (NULL == schema_mgr)) {
      TBSYS_LOG(WARN, "%s", "fail to get the latest schema, unexpected error");
      err = OB_ERR_UNEXPECTED;
    }
  }

  /// decode and check scan param
  if (OB_SUCCESS == err) {
    err = ob_decode_scan_param(*org_scan_param, *schema_mgr, *decoded_scan_param);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to decode scan param [err:%d]", err);
    }
  }
  FILL_TRACE_LOG("step 2. finish parse the schema for ms_scan:err[%d]", err);

  /// do request
  if (OB_SUCCESS == err) {
    err = agent->set_request_param(*decoded_scan_param, ups_stream,
                                   ups_join_stream, *schema_mgr,
                                   merge_server_->get_params().get_intermediate_buffer_size());
    if (OB_INVALID_START_VERSION == err || OB_CS_TABLET_NOT_EXIST == err) {
      err = agent->set_request_param(*decoded_scan_param, ups_stream,
                                     ups_join_stream, *schema_mgr,
                                     merge_server_->get_params().get_intermediate_buffer_size());
    }
  }

  if (OB_SUCCESS == err) {
    err = agent->next_cell();
  }

  /// prepare result
  ObCellInfo* cur_cell = NULL;
  bool size_over_flow = false;
  int64_t result_row_width = (org_scan_param->get_group_by_param().get_aggregate_row_width() > 0)
                             ? org_scan_param->get_group_by_param().get_aggregate_row_width()
                             : org_scan_param->get_column_name_size();
  while (OB_SUCCESS == err
         && result_scanner->get_serialize_size() < RESPONSE_PACKET_BUFFER_SIZE
         && !size_over_flow) {
    int32_t cell_idx = 0;
    for (cell_idx = 0;
         cell_idx < result_row_width
         && OB_SUCCESS == err  && !size_over_flow;
         cell_idx++) {
      err = agent->get_cell(&cur_cell);
      if (OB_SUCCESS == err) {
        cur_cell->table_name_ = org_scan_param->get_table_name();
        if (org_scan_param->get_group_by_param().get_aggregate_row_width() > 0) {
          err = org_scan_param->get_group_by_param().get_aggregate_column_name(cell_idx,
                cur_cell->column_name_);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to get aggregate column name [cell_idx:%ld,err:%d]", cell_idx, err);
          }
        } else {
          cur_cell->column_name_ = org_scan_param->get_column_name()[cell_idx];
        }
        cur_cell->table_id_ = OB_INVALID_ID;
        cur_cell->column_id_ = OB_INVALID_ID;
        if (OB_SUCCESS == err) {
          err = result_scanner->add_cell(*cur_cell);
          if (OB_SIZE_OVERFLOW == err) {
            size_over_flow = true;
            err = result_scanner->rollback();
          }
        }
        if (OB_SUCCESS == err && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG) {
          TBSYS_LOG(DEBUG, "table_name:%.*s,rowkey:%.*s,column_name:%.*s,ext:%ld,type:%d",
                    cur_cell->table_name_.length(), cur_cell->table_name_.ptr(),
                    cur_cell->row_key_.length(), cur_cell->row_key_.ptr(),
                    cur_cell->column_name_.length(), cur_cell->column_name_.ptr(),
                    cur_cell->value_.get_ext(), cur_cell->value_.get_type());
          hex_dump(cur_cell->row_key_.ptr(), cur_cell->row_key_.length());
        }
      }
      if (OB_SUCCESS == err && !size_over_flow) {
        err = agent->next_cell();
      }
    }
    if (((OB_SUCCESS == err) || (OB_ITER_END == err))
        && result_row_width != cell_idx
        && !size_over_flow) {
      TBSYS_LOG(ERROR, "unexpected error, row not fullfilled when "
                "result finished:column_size[%lu], cell[%d]",
                result_row_width, cell_idx);
      err = OB_SCHEMA_ERROR;
    }
    if (((OB_SUCCESS == err) || (OB_ITER_END == err))
        && !size_over_flow) {
      fullfilled_row_num ++;
    }
  }

  if (OB_SUCCESS == err || OB_ITER_END == err) {
    bool is_fullfilled = agent->is_request_fullfilled();
    if (size_over_flow) {
      is_fullfilled = false;
    }
    err = result_scanner->set_is_req_fullfilled(is_fullfilled, fullfilled_row_num);
    FILL_TRACE_LOG("step 3. finish get all the data for ms_scan:[err:%d, fullfilled_row_num:%ld,"
                   "is_fullfilled:%d]", err, fullfilled_row_num, is_fullfilled);
  }

  int err_code = rc.result_code_;
  /// always send error code
  err = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  if (OB_SUCCESS == err) {
    err = result_scanner->serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                                    out_buffer.get_position());
  }

  FILL_TRACE_LOG("step 4. finish serialize the scanner result for ms_scan:err[%d]", err);

  if (OB_SUCCESS == err) {
    send_err = merge_server_->send_response(OB_SCAN_RESPONSE, MS_SCAN_VERSION, out_buffer,
                                            connection, channel_id);
  }

  /// inc monitor counter
  if (NULL != service_monitor_) {
    int64_t end_time = tbsys::CTimeUtil::getTime();
    uint64_t table_id = decoded_scan_param->get_table_id();
    if (OB_SUCCESS == err_code) {
      service_monitor_->inc(table_id, ObMergerServiceMonitor::SUCC_SCAN_OP_COUNT);
      service_monitor_->inc(table_id, ObMergerServiceMonitor::SUCC_SCAN_OP_TIME, end_time - start_time);
    } else {
      service_monitor_->inc(table_id, ObMergerServiceMonitor::FAIL_SCAN_OP_COUNT);
      service_monitor_->inc(table_id, ObMergerServiceMonitor::FAIL_SCAN_OP_TIME, end_time - start_time);
    }
  }

  FILL_TRACE_LOG("step 5. at last send scanner reponse for ms_scan:send_err[%d], code[%d]",
                 send_err, err_code);
  if (NULL != schema_mgr) {
    rpc_proxy_->release_schema(schema_mgr);
  }

  if (NULL != agent) {
    agent->clear();
  }

  PRINT_TRACE_LOG();
  CLEAR_TRACE_LOG();
  return send_err;
}
} /* mergeserver */
} /* sb */





