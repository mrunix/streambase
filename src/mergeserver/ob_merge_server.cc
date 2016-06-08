/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_server.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "common/ob_trace_log.h"
#include "ob_merge_server.h"
#include <time.h>

using namespace tbnet;
using namespace sb::common;

namespace sb {
namespace mergeserver {
ObMergeServer::ObMergeServer()
  : response_buffer_(RESPONSE_PACKET_BUFFER_SIZE), rpc_buffer_(RESPONSE_PACKET_BUFFER_SIZE) {
}

int ObMergeServer::set_self(const char* dev_name, const int32_t port) {
  int ret = OB_SUCCESS;
  int32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
  if (0 == ip) {
    TBSYS_LOG(ERROR, "cannot get valid local addr on dev:%s.", dev_name);
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret) {
    bool res = self_.set_ipv4_addr(ip, port);
    if (!res) {
      TBSYS_LOG(ERROR, "chunk server dev:%s, port:%d is invalid.",
                dev_name, port);
      ret = OB_ERROR;
    }
  }
  return ret;
}

bool ObMergeServer::is_stoped() const {
  return stoped_;
}

int ObMergeServer::start_service() {
  return service_.start();
}

int ObMergeServer::initialize() {
  int ret = OB_SUCCESS;
  set_batch_process(false); // disable batch process mode
  ret = ms_params_.load_from_config();
  if (ret == OB_SUCCESS) {
    ms_params_.dump_config(); // dump config
  }

  if (ret == OB_SUCCESS) {
    ret = set_listen_port(ms_params_.get_listen_port());
  }

  if (ret == OB_SUCCESS) {
    ret = set_dev_name(ms_params_.get_device_name());
    if (OB_SUCCESS == ret) {
      ret = set_self(ms_params_.get_device_name(),
                     ms_params_.get_listen_port());
    }
  }

  if (ret == OB_SUCCESS) {
    ret = init_root_server();
  }

  if (ret == OB_SUCCESS) {
    ret = set_default_queue_size(ms_params_.get_task_queue_size());
  }

  if (ret == OB_SUCCESS) {
    ret = set_thread_count(ms_params_.get_task_thread_size());
  }

  if (ret == OB_SUCCESS) {
    ret = set_min_left_time(ms_params_.get_task_left_time());
  }

  if (ret == OB_SUCCESS) {
    ret = set_packet_factory(&packet_factory_);
  }

  if (ret == OB_SUCCESS) {
    ret = task_timer_.init();
  }

  if (OB_SUCCESS == ret) {
    ret = client_manager_.initialize(get_transport(), get_packet_streamer());
  }

  if (ret == OB_SUCCESS) {
    ret = ObSingleServer::initialize();
  }

  if (ret == OB_SUCCESS) {
    ret = service_.initialize(this);
  }

  return ret;
}

void ObMergeServer::destroy() {
  task_timer_.destroy();
  ///service_.destroy();
  ObSingleServer::destroy();
}

int ObMergeServer::init_root_server() {
  int ret = OB_SUCCESS;
  bool res = root_server_.set_ipv4_addr(
               ms_params_.get_root_server_ip(), ms_params_.get_root_server_port());
  if (!res) {
    TBSYS_LOG(ERROR, "root server address invalid: %s:%d",
              ms_params_.get_root_server_ip(), ms_params_.get_root_server_port());
    ret = OB_ERROR;
  }
  return ret;
}


void ObMergeServer::set_update_server(const sb::common::ObServer& ups) {
  update_server_ = ups;
}

common::ThreadSpecificBuffer* ObMergeServer::get_rpc_buffer() {
  return &rpc_buffer_;
}

common::ThreadSpecificBuffer::Buffer* ObMergeServer::get_response_buffer() const {
  return response_buffer_.get_buffer();
}

const common::ObServer& ObMergeServer::get_self() const {
  return self_;
}
const common::ObServer& ObMergeServer::get_root_server() const {
  return root_server_;
}

const common::ObServer& ObMergeServer::get_update_server() const {
  return update_server_;
}

const ObMergeServerParams& ObMergeServer::get_params() const {
  return ms_params_;
}

ObTimer& ObMergeServer::get_timer() {
  return task_timer_;
}

const common::ObClientManager& ObMergeServer::get_client_manager() const {
  return client_manager_;
}

// overflow packet
bool ObMergeServer::handle_overflow_packet(ObPacket* base_packet) {
  handle_no_response_request(base_packet);
  // must return false
  return false;
}

void ObMergeServer::handle_no_response_request(ObPacket* base_packet) {
  if (NULL == base_packet || !base_packet->isRegularPacket()) {
    TBSYS_LOG(WARN, "packet is illegal, discard.");
  } else {
    service_.handle_failed_request(base_packet->get_source_timeout(), base_packet->get_packet_code());
  }
}

void ObMergeServer::handle_timeout_packet(ObPacket* base_packet) {
  handle_no_response_request(base_packet);
}

IPacketHandler::HPRetCode ObMergeServer::handlePacket(Connection* connection, Packet* packet) {
  IPacketHandler::HPRetCode rc = IPacketHandler::FREE_CHANNEL;
  if (NULL == packet || !packet->isRegularPacket()) {
    TBSYS_LOG(WARN, "packet is illegal, discard.");
  } else {
    ObPacket* ob_packet = (ObPacket*) packet;
    ob_packet->set_connection(connection);
    // handle heartbeat packet directly (in tbnet event loop thread)
    // generally, heartbeat service nerver be blocked and must be
    // response immediately, donot put into work thread pool.
    if (ob_packet->get_packet_code() == OB_REQUIRE_HEARTBEAT) {
      ObSingleServer::handle_request(ob_packet);
    } else {
      rc = ObSingleServer::handlePacket(connection, packet);
    }
  }
  return rc;
}

int ObMergeServer::do_request(common::ObPacket* base_packet) {
  int ret = OB_SUCCESS;
  ObPacket* ob_packet = base_packet;
  int32_t packet_code = ob_packet->get_packet_code();
  int32_t version = ob_packet->get_api_version();
  int32_t channel_id = ob_packet->getChannelId();
  ret = ob_packet->deserialize();
  if (OB_SUCCESS == ret) {
    FILL_TRACE_LOG("start handle get, packet wait=%ld",
                   tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts());
    ObDataBuffer* in_buffer = ob_packet->get_buffer();
    if (NULL == in_buffer) {
      TBSYS_LOG(ERROR, "%s", "in_buffer is NUll should not reach this");
    } else {
      tbnet::Connection* connection = ob_packet->get_connection();
      ThreadSpecificBuffer::Buffer* thread_buffer = response_buffer_.get_buffer();
      if (NULL != thread_buffer) {
        thread_buffer->reset();
        ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
        //TODO read thread stuff multi thread
        TBSYS_LOG(DEBUG, "handle packet, packe code is %d, packet:%p",
                  packet_code, ob_packet);
        ret = service_.do_request(ob_packet->get_receive_ts(), packet_code, version,
                                  channel_id, connection, *in_buffer, out_buffer);
      } else {
        TBSYS_LOG(ERROR, "%s", "get thread buffer error, ignore this packet");
      }
    }
  }
  return ret;
}
} /* mergeserver */
} /* sb */


