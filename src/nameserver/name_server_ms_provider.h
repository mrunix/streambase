/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * name_server_ms_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_MS_PROVIDER_H
#define _OB_ROOT_MS_PROVIDER_H 1

#include "common/roottable/ob_ms_provider.h"
#include "ob_chunk_server_manager.h"
#include "name_server_server_config.h"
#include "name_server_rpc_stub.h"
namespace sb {
namespace nameserver {
// thread safe simple ronud-robin merge server provider
class NameServerMsProvider: public common::ObMsProvider {
 public:
  NameServerMsProvider(ObChunkServerManager& server_manager);
  virtual ~NameServerMsProvider();
  void init(NameServerConfig& config, NameServerRpcStub& rpc_stub);
 public:
  int get_ms(common::ObServer& server);
  int get_ms(common::ObServer& server, const bool query_master_cluster);
  // not implement this abstract interface
  int get_ms(const common::ObScanParam& param, int64_t retry_num, common::ObServer& server) {
    UNUSED(param);
    UNUSED(retry_num);
    UNUSED(server);
    return common::OB_NOT_IMPLEMENT;
  }
 private:
  // get master cluster merge server
  int get_ms(const common::ObServer& master_rs, common::ObServer& server);
 private:
  bool init_;
  ObChunkServerManager& server_manager_;
  NameServerConfig* config_;
  NameServerRpcStub* rpc_stub_;
};
} // end namespace nameserver
} // end namespace sb

#endif /* _OB_ROOT_MS_PROVIDER_H */

