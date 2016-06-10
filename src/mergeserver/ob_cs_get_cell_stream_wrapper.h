/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cs_get_cell_stream_wrapper.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_CS_GET_CELL_STREAM_WRAPPER_H_
#define OCEANBASE_MERGESERVER_OB_CS_GET_CELL_STREAM_WRAPPER_H_

#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/thread_buffer.h"
#include "mergeserver/ob_ms_rpc_stub.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "mergeserver/ob_ms_schema_manager.h"
#include "mergeserver/ob_ms_tablet_location.h"
#include "mergeserver/ob_ms_get_cell_stream.h"
#include "mergeserver/ob_ms_scan_cell_stream.h"

namespace sb {
namespace mergeserver {
class ObMSUpsStreamWrapper {
 public:
  ObMSUpsStreamWrapper(const int64_t retry_times, const int64_t timeout,
                       const common::ObServer& name_server);

  virtual ~ObMSUpsStreamWrapper();

 public:
  /// @fn set update server vip for fake update list
  void set_update_server(const common::ObServer& server);

  /// @fn init for rpc proxy
  int init(const common::ThreadSpecificBuffer* rpc_buffer,
           const common::ObClientManager* rpc_frame);

  /// @fn get rpc proxy for read update server
  ObMergerRpcProxy* get_ups_rpc_proxy();

 private:
  /// @property this is a big one
  common::ObServer update_server_;
  common::ObServer merge_server_;
  ///
  ObMergerRpcStub  rpc_stub_;
  ObMergerRpcProxy rpc_proxy_;
  ObMergerSchemaManager* schema_mgr_;
  ObMergerTabletLocationCache location_cache_;
};

class ObMSGetCellStreamWrapper {
 public:
  /// @param timeout network timeout
  /// @param update_server address of update server
  ObMSGetCellStreamWrapper(ObMSUpsStreamWrapper& ups_stream);
  virtual ~ObMSGetCellStreamWrapper();

 public:
  /// @fn get cell stream used for join
  ObMSGetCellStream* get_ups_get_cell_stream();
  /// @fn get cell stream used for merge
  ObMSScanCellStream* get_ups_scan_cell_stream();

 private:
  ObMSGetCellStream get_cell_stream_;
  ObMSScanCellStream scan_cell_stream_;
};
}
}

#endif /* OCEANBASE_MERGESERVER_OB_CS_GET_CELL_STREAM_WRAPPER_H_ */


