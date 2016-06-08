/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_chunk_server.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     maoqi <maoqi@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *     ruohai <ruohai@taobao.com>
 *
 */

#include <stdint.h>
#include <tblog.h>
#include "sstable/ob_sstable_stat.h"
#include "ob_chunk_server.h"
#include "ob_chunk_server_main.h"

using namespace sb::common;

namespace sb {
#ifndef NO_STAT
namespace sstable {
using namespace sb::chunkserver;

void set_stat(const uint64_t table_id, const int32_t index, const int64_t value) {
  switch (index) {
  case INDEX_BLOCK_INDEX_CACHE_HIT:
    OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_BLOCK_INDEX_CACHE_HIT, value);
    break;
  case INDEX_BLOCK_INDEX_CACHE_MISS:
    OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_BLOCK_INDEX_CACHE_MISS, value);
    break;
  case INDEX_BLOCK_CACHE_HIT:
    OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_BLOCK_CACHE_HIT, value);
    break;
  case INDEX_BLOCK_CACHE_MISS:
    OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_BLOCK_CACHE_MISS, value);
    break;
  case INDEX_DISK_IO_NUM:
    OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_DISK_IO_NUM, value);
    break;
  case INDEX_DISK_IO_BYTES:
    OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_DISK_IO_BYTES, value);
    break;
  default:
    break;
  }
}

void inc_stat(const uint64_t table_id, const int32_t index, const int64_t inc_value) {
  switch (index) {
  case INDEX_BLOCK_INDEX_CACHE_HIT:
    OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_BLOCK_INDEX_CACHE_HIT, inc_value);
    break;
  case INDEX_BLOCK_INDEX_CACHE_MISS:
    OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_BLOCK_INDEX_CACHE_MISS, inc_value);
    break;
  case INDEX_BLOCK_CACHE_HIT:
    OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_BLOCK_CACHE_HIT, inc_value);
    break;
  case INDEX_BLOCK_CACHE_MISS:
    OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_BLOCK_CACHE_MISS, inc_value);
    break;
  case INDEX_DISK_IO_NUM:
    OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_DISK_IO_NUM, inc_value);
    break;
  case INDEX_DISK_IO_BYTES:
    OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_DISK_IO_BYTES, inc_value);
    break;
  default:
    break;
  }
}
}
#endif

namespace chunkserver {

ObChunkServer::ObChunkServer()
  : response_buffer_(RESPONSE_PACKET_BUFFER_SIZE),
    rpc_buffer_(RPC_BUFFER_SIZE) {
}

ObChunkServer::~ObChunkServer() {
}

common::ThreadSpecificBuffer::Buffer* ObChunkServer::get_rpc_buffer() const {
  return rpc_buffer_.get_buffer();
}

common::ThreadSpecificBuffer::Buffer* ObChunkServer::get_response_buffer() const {
  return response_buffer_.get_buffer();
}

const common::ThreadSpecificBuffer* ObChunkServer::get_thread_specific_rpc_buffer() const {
  return &rpc_buffer_;
}

const common::ObServer& ObChunkServer::get_self() const {
  return self_;
}

const common::ObServer& ObChunkServer::get_root_server() const {
  return param_.get_root_server();
}

const common::ObClientManager& ObChunkServer::get_client_manager() const {
  return client_manager_;
}

const ObChunkServerParam& ObChunkServer::get_param() const {
  return param_;
}

ObChunkServerParam& ObChunkServer::get_param() {
  return param_;
}

ObChunkServerStatManager& ObChunkServer::get_stat_manager() {
  return stat_;
}

const ObTabletManager& ObChunkServer::get_tablet_manager() const {
  return tablet_manager_;
}

ObTabletManager& ObChunkServer::get_tablet_manager() {
  return tablet_manager_;
}

ObRootServerRpcStub& ObChunkServer::get_rs_rpc_stub() {
  return rs_rpc_stub_;
}

int ObChunkServer::set_self(const char* dev_name, const int32_t port) {
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


int ObChunkServer::initialize() {
  int ret = OB_SUCCESS;
  // do not handle batch packet.
  // process packet one by one.
  set_batch_process(false);

  // read configure item value from configure file.
  // this step is the very first thing.
  ret = param_.load_from_config();

  // set listen port
  if (OB_SUCCESS == ret) {
    ret = set_listen_port(param_.get_chunk_server_port());
  }

  if (OB_SUCCESS == ret) {
    ret = set_dev_name(param_.get_dev_name());
    if (OB_SUCCESS == ret) {
      ret = set_self(param_.get_dev_name(),
                     param_.get_chunk_server_port());
    }
  }

  // task queue and work thread count
  if (OB_SUCCESS == ret) {
    ret = set_default_queue_size(param_.get_task_queue_size());
  }

  if (OB_SUCCESS == ret) {
    ret = set_thread_count(param_.get_task_thread_count());
  }

  if (OB_SUCCESS == ret) {
    ret = set_min_left_time(param_.get_task_left_time());
  }

  // set packet factory object.
  if (OB_SUCCESS == ret) {
    ret = set_packet_factory(&packet_factory_);
  }

  // initialize client_manager_ for server remote procedure call.
  if (OB_SUCCESS == ret) {
    ret = client_manager_.initialize(get_transport(), get_packet_streamer());
  }

  if (OB_SUCCESS == ret) {
    ret = rs_rpc_stub_.init(param_.get_root_server(), &client_manager_);
  }

  if (OB_SUCCESS == ret) {
    ret = tablet_manager_.init(&param_);
  }

  // server initialize, including start transport,
  // listen port, accept socket data from client
  if (OB_SUCCESS == ret) {
    ret = ObSingleServer::initialize();
  }

  if (OB_SUCCESS == ret) {
    ret = service_.initialize(this);
  }

  return ret;
}

int ObChunkServer::start_service() {
  TBSYS_LOG(INFO, "start service...");
  // finally, start service, handle the request from client.
  return service_.start();
}

void ObChunkServer::wait_for_queue() {
  ObSingleServer::wait_for_queue();
}

void ObChunkServer::destroy() {
  ObSingleServer::destroy();
  tablet_manager_.destroy();
  service_.destroy();
  //TODO maybe need more destroy
}

tbnet::IPacketHandler::HPRetCode ObChunkServer::handlePacket(
  tbnet::Connection* connection, tbnet::Packet* packet) {
  tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::FREE_CHANNEL;
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

int ObChunkServer::do_request(ObPacket* base_packet) {
  int ret = OB_SUCCESS;
  ObPacket* ob_packet = base_packet;
  int32_t packet_code = ob_packet->get_packet_code();
  int32_t version = ob_packet->get_api_version();
  int32_t channel_id = ob_packet->getChannelId();
  ret = ob_packet->deserialize();

  if (OB_SUCCESS == ret) {
    ObDataBuffer* in_buffer = ob_packet->get_buffer();
    if (NULL == in_buffer) {
      TBSYS_LOG(ERROR, "in_buffer is NUll should not reach this");
    } else {
      tbnet::Connection* connection = ob_packet->get_connection();
      ThreadSpecificBuffer::Buffer* thread_buffer =
        response_buffer_.get_buffer();
      if (NULL != thread_buffer) {
        thread_buffer->reset();
        ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
        //TODO read thread stuff multi thread
        TBSYS_LOG(DEBUG, "handle packet, packe code is %d, packet:%p",
                  packet_code, ob_packet);
        ret = service_.do_request(packet_code,
                                  version, channel_id, connection, *in_buffer, out_buffer);
      } else {
        TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
      }
    }
  }

  return ret;
}


} // end namespace chunkserver
} // end namespace sb

