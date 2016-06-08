/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_common_rpc_stub.h for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#ifndef __OCEANBASE_COMMON_OB_COMMON_RPC_STUB_H__
#define __OCEANBASE_COMMON_OB_COMMON_RPC_STUB_H__

#include "ob_define.h"
#include "data_buffer.h"
#include "ob_server.h"
#include "ob_client_manager.h"
#include "ob_result.h"
#include "thread_buffer.h"
#include "ob_packet.h"
#include "ob_lease_common.h"
#include "ob_obi_role.h"

namespace sb {
namespace common {
class ObCommonRpcStub {
 public:
  ObCommonRpcStub();
  virtual ~ObCommonRpcStub();

  int init(const ObClientManager* client_mgr);

  // send commit log to Slave
  virtual int send_log(const ObServer& ups_slave, ObDataBuffer& log_data,
                       const int64_t timeout_us);

  // send renew lease request to Master, used by Slave
  virtual int renew_lease(const common::ObServer& ups_master,
                          const common::ObServer& slave_addr, const int64_t timeout_us);

  // send grant lease to Slave, used by Master
  virtual int grant_lease(const common::ObServer& slave, const ObLease& lease,
                          const int64_t timeout_us);
  //
  // send quit info to Master, called when Slave quit
  virtual int slave_quit(const common::ObServer& master, const common::ObServer& slave_addr,
                         const int64_t timeout_us);

  virtual int get_obi_role(const common::ObServer& rs, common::ObiRole& obi_role, const int64_t timeout_us);
 private:
  int get_thread_buffer_(ObDataBuffer& data_buff);

 private:
  static const int32_t DEFAULT_VERSION;

 private:
  ThreadSpecificBuffer thread_buffer_;
 protected:
  const ObClientManager* client_mgr_;
};
}
}

#endif // __OCEANBASE_COMMON_OB_COMMON_RPC_STUB_H__

