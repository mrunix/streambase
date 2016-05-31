/**
  * (C) 2007-2010 Taobao Inc.
  *
  * This program is free software; you can redistribute it and/or modify
  * it under the terms of the GNU General Public License version 2 as
  * published by the Free Software Foundation.
  *
  * Version: $Id$
  *
  * Authors:
  *   zhidong <xielun.szd@taobao.com>
  *     - some work details if you want
  */

#ifndef OB_ROOT_SQL_PROXY_H_
#define OB_ROOT_SQL_PROXY_H_

#include "name_server_ms_provider.h"

namespace sb {
namespace common {
class ObString;
}
namespace nameserver {
class NameServerRpcStub;
class ObChunkServerManager;
// thread safe sql proxy
class NameServerSQLProxy {
 public:
  NameServerSQLProxy(ObChunkServerManager& server_manager, NameServerServerConfig& config, NameServerRpcStub& rpc_stub);
  virtual ~NameServerSQLProxy();
 public:
  // exectue sql query
  int query(const int64_t retry_times, const int64_t timeout, const common::ObString& sql);
  int query(const bool query_master_cluster, const int64_t retry_times, const int64_t timeout, const common::ObString& sql);
 private:
  NameServerMsProvider ms_provider_;
  NameServerRpcStub& rpc_stub_;
};
}
}

#endif //OB_ROOT_SQL_PROXY_H_
