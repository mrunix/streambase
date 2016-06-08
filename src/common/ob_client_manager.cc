/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_client_manager.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include "ob_client_manager.h"
#include "wait_object.h"

namespace sb {
namespace common {

ObClientManager::ObClientManager()
  : inited_(false), max_request_timeout_(5000000), connmgr_(NULL), waitmgr_(NULL) {
}

ObClientManager::~ObClientManager() {
  destroy();
}

void ObClientManager::destroy() {
  if (NULL != connmgr_) {
    delete connmgr_;
    connmgr_ = NULL;
  }
  if (NULL != waitmgr_) {
    delete waitmgr_;
    waitmgr_ = NULL;
  }
}

int ObClientManager::initialize(tbnet::Transport* transport,
                                tbnet::IPacketStreamer* streamer, const int64_t max_request_timeout /*=5000000*/) {
  int rc = OB_SUCCESS;
  if (inited_) {
    TBSYS_LOG(ERROR, "ClientManager already initialized.");
    rc = OB_INIT_TWICE;
  }

  if (OB_SUCCESS == rc) {
    max_request_timeout_ = max_request_timeout;
    connmgr_ = new(std::nothrow) tbnet::ConnectionManager(transport, streamer, this);
    if (NULL == connmgr_) {
      TBSYS_LOG(ERROR, "cannot allocate ClientManager object.");
      rc = OB_ERROR;
    } else {
      // ConnectionManager use time in ms;
      connmgr_->setDefaultQueueTimeout(0, max_request_timeout_ / 1000);
    }
    waitmgr_ = new(std::nothrow) common::WaitObjectManager();
    if (NULL == waitmgr_) {
      TBSYS_LOG(ERROR, "cannot allocate WaitObjectManager object.");
      rc = OB_ERROR;
    }
  }

  inited_ = (OB_SUCCESS == rc);
  if (!inited_) { destroy(); }

  return rc;
}

tbnet::IPacketHandler::HPRetCode ObClientManager::handlePacket(
  tbnet::Packet* packet, void* args) {
  if (NULL != args && NULL != packet && packet->isRegularPacket()) {
    int64_t id = reinterpret_cast<int64_t>(args);
    waitmgr_->wakeup_wait_object(id, packet);
  } else {
    // post_packet set args to NULL, means donot handle response.
    // there is no client waiting for this response packet, free it.
    if (NULL != packet) {
      if (packet->isRegularPacket()) {
        TBSYS_LOG(INFO, "no client waiting this packet:code=%d", packet->getPCode());
      } else if (NULL != args) {
        tbnet::ControlPacket* ctrl_packet = static_cast<tbnet::ControlPacket*>(packet);
        if (NULL != ctrl_packet) {
          if (tbnet::ControlPacket::CMD_TIMEOUT_PACKET == ctrl_packet->getCommand()) {
            int64_t id = reinterpret_cast<int64_t>(args);
            TBSYS_LOG(INFO, "timeout packet (command=%d), args:%ld",
                      ctrl_packet->getCommand(), id);
            // wakeup client waiting for this request, no need to blocking until timeout
            waitmgr_->wakeup_wait_object(id, NULL);
          } else {
            // CMD_BAD_PACKET(1) or CMD_DISCONN_PACKET(3)
            TBSYS_LOG(INFO, "bad or disconnect packet (command=%d) ", ctrl_packet->getCommand());
          }
        } else {
          TBSYS_LOG(WARN, "packet (pcode=%d) is not regular packet, discard anyway. args:%ld",
                    packet->getPCode(), reinterpret_cast<int64_t>(args));
        }
      } else {
        TBSYS_LOG(DEBUG, "packet (pcode=%d) is not regular packet, discard anyway. "
                  "args is NULL, maybe post channel timeout packet ", packet->getPCode());
      }
    } else {
      TBSYS_LOG(WARN, "packet is NULL, unknown error. args:%ld", reinterpret_cast<int64_t>(args));
    }
  }
  return tbnet::IPacketHandler::FREE_CHANNEL;
}

/**
 * post_packet is async version of send_packet. donot wait for response packet.
 */
int ObClientManager::post_packet(const ObServer& server, ObPacket* packet) const {
  int rc = OB_SUCCESS;
  if (NULL == packet) {
    rc = OB_INVALID_ARGUMENT;
  } else if (!inited_) {
    rc = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "cannot post packet, ClientManager not initialized.");
    packet->free();
  } else {
    bool send_ok = connmgr_->sendPacket(server.get_ipv4_server_id(), packet, NULL, NULL);
    if (!send_ok) {
      rc = OB_PACKET_NOT_SENT;
      TBSYS_LOG(INFO, "cannot post packet, maybe send queue is full or disconnect.");
      packet->free();
    }
  }

  return rc;
}

int ObClientManager::post_request(const ObServer& server,
                                  const int32_t pcode, const int32_t version,
                                  const ObDataBuffer& in_buffer) const {
  int rc = OB_SUCCESS;
  ObPacket* packet = new(std::nothrow) ObPacket();
  if (NULL == packet) {
    rc = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    packet->set_packet_code(pcode);
    packet->setChannelId(0);
    packet->set_api_version(version);
    packet->set_data(in_buffer);
    rc = packet->serialize();
    if (OB_SUCCESS != rc) {
      TBSYS_LOG(WARN, "packet serialize error");
      packet->free();
      packet = NULL;
    } else {
      rc = post_packet(server, packet);
    }
  }

  return rc;
}

/*
 * send a packet to server %server and wait response packet
 * @param server send to server
 * @param packet send packet object, must be allocate on heap,
 * if send_packet failed, packet will be free by send_packet.
 * @param timeout max wait time interval
 * @param [out] response  response packet from remote server, allocated on heap,
 * must be free by user who call the send_packet. response not NULL when return success.
 * @return OB_SUCCESS on success or other on failure.
 */
int ObClientManager::send_packet(const ObServer& server,
                                 ObPacket* packet, const int64_t timeout, ObPacket*& response) const {
  response = NULL;
  int rc = OB_SUCCESS;
  if (NULL == packet) {
    rc = OB_INVALID_ARGUMENT;
  } else if (!inited_) {
    rc = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "cannot send packet, ClientManager not initialized.");
    packet->free();
    packet = NULL;
  }

  common::WaitObject* wait_object = NULL;
  if (OB_SUCCESS == rc) {
    wait_object =  waitmgr_->create_wait_object();
    if (NULL == wait_object) {
      TBSYS_LOG(ERROR, "cannot send packet, cannot create wait object");
      rc = OB_ERROR;
    }

  }

  if (OB_SUCCESS == rc) {
    if (timeout > max_request_timeout_) {
      max_request_timeout_ = timeout;
      connmgr_->setDefaultQueueTimeout(0, max_request_timeout_ / 1000);
    }
    // caution! wait_object set no free, it means response packet
    // not be free by wait_object, must be handled by user who call send_packet.
    // MODIFY: wait_object need free the response packet not handled.
    // wait_object->set_no_free();
    bool send_ok = connmgr_->sendPacket(server.get_ipv4_server_id(), packet, NULL,
                                        reinterpret_cast<void*>(wait_object->get_id()));
    if (send_ok) {
      send_ok = wait_object->wait(timeout);
      if (!send_ok) {
        TBSYS_LOG(WARN, "wait packet (%d)'s response timeout.", packet->get_packet_code());
        rc = OB_RESPONSE_TIME_OUT;
      } else {
        response = dynamic_cast<ObPacket*>(wait_object->get_response());
        // there's two situation on here.
        // 1. connect remote server failed, ControlPacket(timeout) raise up.
        // 2. timeout parameter of this function greater than default timeout
        // of ConnectManager(5000ms), Packet timeout raise up.
        // TODO, maybe need a new error code.
        rc = (NULL !=  response) ? OB_SUCCESS : OB_RESPONSE_TIME_OUT;
      }

    } else {
      packet->free();
      rc = OB_PACKET_NOT_SENT;
      TBSYS_LOG(ERROR, "cannot send packet, maybe send queue is full or disconnect.");
    }

    // do not free the response packet.
    waitmgr_->destroy_wait_object(wait_object);
    wait_object = NULL;

    if (OB_SUCCESS == rc && NULL != response) {
      rc = response->deserialize();
      if (OB_SUCCESS != rc) {
        TBSYS_LOG(ERROR, "response packet deserialize failed.");
        //response->free();
        response = NULL;
      }
    }

  }

  return rc;
}

/*
 * send_packet wrappered byte stream %in_buffer as a ObPacket send to remote server
 * and receive the reponse ObPacket translate to %out_buffer
 * @param server send to server
 * @param pcode  packet type
 * @param version packet version
 * @param timeout max wait time
 * @param in_buffer byte stream be sent
 * @param out_buffer response packet byte stream.
 * @return OB_SUCCESS on success or other on failure.
 * response data filled into end of out_buffer,
 * so function return successfully with:
 * response data pointer = out_buffer.get_data() + origin_position
 * response data size = out_buffer.get_position() - origin_position;
 */
int ObClientManager::send_request(
  const ObServer& server, const int32_t pcode,
  const int32_t version, const int64_t timeout,
  ObDataBuffer& in_buffer, ObDataBuffer& out_buffer) const {
  int rc = OB_SUCCESS;

  ObPacket* response = NULL;
  rc = send_request(server, pcode, version, timeout, in_buffer, response);

  // deserialize response packet to out_buffer
  if (OB_SUCCESS == rc && NULL != response) {
    // copy response's inner_buffer to out_buffer.
    int64_t data_length = response->get_data_length();
    ObDataBuffer* response_buffer = response->get_buffer();
    if (out_buffer.get_remain() < data_length) {
      TBSYS_LOG(ERROR, "insufficient memory in out_buffer");
      rc = OB_ERROR;
    } else {
      memcpy(out_buffer.get_data() + out_buffer.get_position(),
             response_buffer->get_data() + response_buffer->get_position(),
             data_length);
      out_buffer.get_position() += data_length;
    }

    if (NULL != response) {
      //response->free();
    }
  }

  return rc;
}

/*
 * like the overload function as above.
 * %in_out_buffer position will be reset to start
 * for store response packet's data.
 * response data filled into start of in_out_buffer
 * so function return successfully with:
 * response data  = in_out_buffer.get_data()
 * response data size = in_out_buffer.get_position()
 */
int ObClientManager::send_request(
  const ObServer& server, const int32_t pcode,
  const int32_t version, const int64_t timeout,
  ObDataBuffer& in_out_buffer) const {
  int rc = OB_SUCCESS;

  ObPacket* response = NULL;
  rc = send_request(server, pcode, version, timeout, in_out_buffer, response);

  // deserialize response packet to out_buffer
  if (OB_SUCCESS == rc && NULL != response) {
    // copy response's in_buffer to out_buffer.
    int64_t data_length = response->get_data_length();
    ObDataBuffer* response_buffer = response->get_buffer();
    // reset in_out_buffer;
    in_out_buffer.get_position() = 0;
    if (in_out_buffer.get_remain() < data_length) {
      TBSYS_LOG(ERROR, "insufficient memory in out_buffer");
      rc = OB_ERROR;
    } else if (data_length <= 0) {
      TBSYS_LOG(ERROR, "invalid data length, data_length=%ld", data_length);
      rc = OB_ERROR;
    } else {
      memcpy(in_out_buffer.get_data(),
             response_buffer->get_data() + response_buffer->get_position(),
             data_length);
      // reset postion
      in_out_buffer.get_position() += data_length;
    }

    if (NULL != response) {
      //response->free();
    }
  }

  return rc;
}

int ObClientManager::send_request(
  const ObServer& server, const int32_t pcode,
  const int32_t version, const int64_t timeout,
  ObDataBuffer& in_buffer, ObPacket*& response) const {
  int rc = OB_SUCCESS;

  ObPacket* packet = new(std::nothrow) ObPacket();
  if (NULL == packet) {
    rc = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    packet->set_packet_code(pcode);
    packet->setChannelId(0);
    packet->set_api_version(version);
    packet->set_data(in_buffer);
    packet->set_source_timeout(timeout);
  }

  if (OB_SUCCESS == rc) {
    rc = packet->serialize();
    if (OB_SUCCESS != rc)
      TBSYS_LOG(WARN, "packet serialize error");
  }

  // serialize failed
  if (OB_SUCCESS != rc && NULL != packet) {
    packet->free();
  }

  if (OB_SUCCESS == rc) {
    rc = send_packet(server, packet, timeout, response);
  }

  return rc;
}

} // end namespace chunkserver
} // end namespace sb




