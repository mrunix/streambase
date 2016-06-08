/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_client_manager.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_CLIENT_MANAGER_H_
#define OCEANBASE_COMMON_CLIENT_MANAGER_H_

#include "tbnet.h"

#include "data_buffer.h"
#include "ob_server.h"
#include "ob_packet.h"

namespace sb {

namespace common {
class WaitObjectManager;
class ObClientManager : public tbnet::IPacketHandler {
 public:
  ObClientManager();
  ~ObClientManager();
 public:
  int initialize(tbnet::Transport* transport, tbnet::IPacketStreamer* streamer,
                 const int64_t max_request_timeout = 5000000);
  int post_packet(const ObServer& server, ObPacket* packet) const;
  int post_request(const ObServer& server,  const int32_t pcode, const int32_t version,
                   const ObDataBuffer& in_buffer) const;
  int send_packet(const ObServer& server, ObPacket* packet,
                  const int64_t timeout, ObPacket*& response) const;
  int send_request(const ObServer& server, const int32_t pcode, const int32_t version,
                   const int64_t timeout, ObDataBuffer& in_buffer, ObDataBuffer& out_buffer) const;
  int send_request(const ObServer& server, const int32_t pcode, const int32_t version,
                   const int64_t timeout, ObDataBuffer& in_out_buffer) const;
  tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet* packet, void* args);
 private:
  int send_request(const ObServer& server, const int32_t pcode, const int32_t version,
                   const int64_t timeout, ObDataBuffer& in_buffer, ObPacket*& response) const;
  void destroy();
 private:
  int32_t inited_;
  mutable int64_t max_request_timeout_;
  tbnet::ConnectionManager* connmgr_;
  WaitObjectManager* waitmgr_;
};

} // end namespace chunkserver
} // end namespace sb


#endif //OCEANBASE_COMMON_CLIENT_MANAGER_H_


