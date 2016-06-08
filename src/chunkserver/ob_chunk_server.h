/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_chunk_server.h
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     maoqi <maoqi@taobao.com>
 *     ruohai <ruohai@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_CHUNKSERVER_H_
#define OCEANBASE_CHUNKSERVER_CHUNKSERVER_H_

#include <pthread.h>
#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "ob_chunk_service.h"
#include "ob_chunk_server_param.h"
#include "ob_tablet_manager.h"
#include "ob_root_server_rpc.h"
#include "ob_chunk_server_stat.h"

namespace sb {
namespace chunkserver {
class ObChunkServer : public common::ObSingleServer {
 public:
  static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024 * 1024 * 2; //2MB
  static const int32_t RPC_BUFFER_SIZE = 1024 * 1024 * 2; //2MB
 public:
  ObChunkServer();
  ~ObChunkServer();
 public:
  /** called before start server */
  virtual int initialize();
  /** called after start transport and listen port*/
  virtual int start_service();
  virtual void wait_for_queue();
  virtual void destroy();

  virtual tbnet::IPacketHandler::HPRetCode handlePacket(
    tbnet::Connection* connection, tbnet::Packet* packet);

  virtual int do_request(common::ObPacket* base_packet);
 public:
  common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;
  common::ThreadSpecificBuffer::Buffer* get_response_buffer() const;

  const common::ThreadSpecificBuffer* get_thread_specific_rpc_buffer() const;

  const common::ObClientManager& get_client_manager() const;
  const common::ObServer& get_self() const;
  const common::ObServer& get_root_server() const;

  const ObChunkServerParam& get_param() const ;
  ObChunkServerParam& get_param() ;
  ObChunkServerStatManager& get_stat_manager();
  const ObTabletManager& get_tablet_manager() const ;
  ObTabletManager& get_tablet_manager() ;
  ObRootServerRpcStub& get_rs_rpc_stub();

 private:
  DISALLOW_COPY_AND_ASSIGN(ObChunkServer);
  int set_self(const char* dev_name, const int32_t port);
 private:
  // request service handler
  ObChunkService service_;
  ObTabletManager tablet_manager_;
  ObChunkServerParam param_;
  ObChunkServerStatManager stat_;

  // network objects.
  common::ObPacketFactory packet_factory_;
  common::ObClientManager  client_manager_;
  ObRootServerRpcStub rs_rpc_stub_;

  // thread specific buffers
  common::ThreadSpecificBuffer response_buffer_;
  common::ThreadSpecificBuffer rpc_buffer_;

  // server information
  common::ObServer self_;
};

#ifndef NO_STAT
#define OB_CHUNK_STAT(op,args...) \
    ObChunkServerMain::get_instance()->get_chunk_server().get_stat_manager().op(args)
#else
#define OB_CHUNK_STAT(op,args...)
#endif

} // end namespace chunkserver
} // end namespace sb


#endif //OCEANBASE_CHUNKSERVER_CHUNKSERVER_H_

