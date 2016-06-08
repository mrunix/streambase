/*
 * src/nameserver/.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for .
 *
 * Library: nameserver
 * Package: nameserver
 * Module :
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef OCEANBASE_ROOT_RPC_STUB_H_
#define OCEANBASE_ROOT_RPC_STUB_H_

#include "common/ob_fetch_runnable.h"
#include "common/ob_common_rpc_stub.h"
#include "common/ob_schema.h"
#include "common/ob_range.h"
namespace sb {
namespace nameserver {
class NameWorker;
class ObRootRpcStub : public common::ObCommonRpcStub {
 public:
  ObRootRpcStub();
  virtual ~ObRootRpcStub();
  int init(const common::ObClientManager* client_mgr, common::ThreadSpecificBuffer* tsbuffer);
  // synchronous rpc messages
  virtual int slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout);
  virtual int set_obi_role(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us);
  virtual int switch_schema(const common::ObServer& ups, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us);
  virtual int migrate_tablet(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObRange& range, bool keep_src, const int64_t timeout_us);
  virtual int create_tablet(const common::ObServer& cs, const common::ObRange& range, const int64_t mem_version, const int64_t timeout_us);
  virtual int get_last_frozen_version(const common::ObServer& ups, const int64_t timeout_us, int64_t& frozen_version);
  virtual int get_obi_role(const common::ObServer& master, const int64_t timeout_us, common::ObiRole& obi_role);
  // asynchronous rpc messages
  virtual int heartbeat_to_cs(const common::ObServer& cs, const int64_t lease_time, const int64_t frozen_mem_version);
  virtual int heartbeat_to_ms(const common::ObServer& ms, const int64_t lease_time, const int64_t schema_version, const common::ObiRole& role);
 private:
  int get_thread_buffer_(common::ObDataBuffer& data_buffer);
 private:
  static const int32_t DEFAULT_VERSION = 1;
  common::ThreadSpecificBuffer* thread_buffer_;
};
} /* nameserver */
} /* sb */

#endif /* end of include guard: OCEANBASE_ROOT_RPC_STUB_H_ */

