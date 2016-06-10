/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_rpc_stub.h for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#ifndef __OCEANBASE_UPDATESERVER_OB_UPS_RPC_STUB_H__
#define __OCEANBASE_UPDATESERVER_OB_UPS_RPC_STUB_H__

#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_server.h"
#include "common/ob_client_manager.h"
#include "common/ob_schema.h"
#include "common/ob_result.h"
#include "common/ob_common_rpc_stub.h"
#include "ob_ups_utils.h"
#include "ob_schema_mgrv2.h"
#include "ob_ups_fetch_runnable.h"

namespace sb {
namespace updateserver {
class ObUpsRpcStub : public common::ObCommonRpcStub {
 public:
  ObUpsRpcStub();
  virtual ~ObUpsRpcStub();

 public:
  // fetch schema from Root Server, used by UPS Master.
  virtual int fetch_schema(const common::ObServer& root_addr, const int64_t timestamp,
                           CommonSchemaManagerWrapper& schema, const int64_t timeout_us);
  // sync schema data from UPS Master, used by UPS Slave. It's called
  // when UPS Slave starts up or freeze_memtable.
  virtual int sync_schema(const common::ObServer& ups_master, ObSchemaManagerWrapper& schema,
                          const int64_t timeout_us);
  // register with Master, called by Slave when it starts up.
  virtual int slave_register_followed(const common::ObServer& master, const ObSlaveInfo& slave_info,
                                      ObUpsFetchParam& fetch_param, const int64_t timeout_us);
  virtual int slave_register_standalone(const common::ObServer& master,
                                        const uint64_t log_id, const uint64_t log_seq,
                                        uint64_t& log_id_res, uint64_t& log_seq_res, const int64_t timeout_us);
  // send freeze memtable resp.
  virtual int send_freeze_memtable_resp(const common::ObServer& name_server,
                                        const common::ObServer& ups_master, const int64_t schema_timestamp, const int64_t timeout_us);
  virtual int report_freeze(const common::ObServer& name_server,
                            const common::ObServer& ups_master, const int64_t frozen_version, const int64_t timeout_us);

  virtual int fetch_lsync(const common::ObServer& lsync, const uint64_t log_id, const uint64_t log_seq,
                          char*& log_data, int64_t& log_len, const int64_t timeout_us);

 private:
  int get_thread_buffer_(common::ObDataBuffer& data_buff);

 private:
  static const int32_t DEFAULT_VERSION = 1;
};
}
}

#endif //__OB_UPS_RPC_STUB_H__



