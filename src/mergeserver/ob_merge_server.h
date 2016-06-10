/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_server.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_MERGESERVER_H_
#define OCEANBASE_MERGESERVER_MERGESERVER_H_

#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/ob_server.h"
#include "common/ob_timer.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "ob_merge_server_params.h"
#include "ob_merge_server_service.h"

namespace sb {
namespace mergeserver {
class ObMergeServer : public common::ObSingleServer {
 public:
  ObMergeServer();

 public:
  int initialize();
  int start_service();
  void destroy();
  tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection* connection,
                                                tbnet::Packet* packet);
  int do_request(common::ObPacket* base_packet);
  //
  bool handle_overflow_packet(common::ObPacket* base_packet);
  void handle_timeout_packet(common::ObPacket* base_packet);
 public:
  common::ThreadSpecificBuffer::Buffer* get_response_buffer() const;

  common::ThreadSpecificBuffer* get_rpc_buffer();

  const common::ObServer& get_self() const;

  const common::ObServer& get_root_server() const;

  const common::ObServer& get_update_server() const;

  common::ObTimer& get_timer();

  bool is_stoped() const;

  const common::ObClientManager& get_client_manager() const;

  const ObMergeServerParams& get_params() const ;

  void set_update_server(const sb::common::ObServer& ups);

 private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeServer);
  int init_root_server();
  int set_self(const char* dev_name, const int32_t port);
  // handle no response request add timeout as process time for monitor info
  void handle_no_response_request(common::ObPacket* base_packet);
 private:
  common::ObPacketFactory packet_factory_;
  ObMergeServerParams ms_params_;
  common::ObTimer task_timer_;
  common::ThreadSpecificBuffer response_buffer_;
  common::ThreadSpecificBuffer rpc_buffer_;
  common::ObClientManager client_manager_;
  common::ObServer self_;
  common::ObServer name_server_;
  common::ObServer update_server_;
  ObMergeServerService service_;
};
} /* mergeserver */
} /* sb */
#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_H_ */


