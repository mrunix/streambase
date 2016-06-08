/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_client_wrapper.h for ...
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *
 */
#ifndef OCEANBASE_UPDATESERVER_CLIENTWRAPPER_H_
#define OCEANBASE_UPDATESERVER_CLIENTWRAPPER_H_

#include "mergeserver/ob_ms_get_cell_stream.h"
#include "mergeserver/ob_merge_join_agent.h"

namespace sb {
namespace updateserver {
using namespace common;
using namespace mergeserver;
class ObClientWrapper {
 public:
  // retry interval time
  static const int64_t RETRY_INTERVAL_TIME = 20; // 20 ms usleep

  //
  ObClientWrapper(const int64_t rpc_retry_times,
                  const int64_t rpc_timeout,
                  const ObServer& root_server,
                  const ObServer& update_server,
                  const ObServer& merge_server);

  ~ObClientWrapper();

  int init(ObMergerRpcStub* rpc_stub,
           ObMergerSchemaManager* schema,
           ObMergerTabletLocationCache* cache,
           ObMergerServiceMonitor* monitor = NULL);

  // 获取一批cell的值
  //通过ObMergeJoinAgent的get_cell得到所有结果值
  //get_param的中必须把end_version_设为ups活跃表版本号-1
  int get(const ObGetParam& get_param, const ObSchemaManagerV2& schema_mgr);

  int get_cell(sb::common::ObCellInfo * *cell);
  int next_cell();
  void clear();

  inline ObMergerRpcProxy& get_proxy() {
    return rpc_proxy_;
  }

 private:
  bool init_;
  ObMSGetCellStream* ups_stream_;
  ObMSGetCellStream* ups_join_stream_;
  ObMergerRpcProxy rpc_proxy_;
  ObMergeJoinAgent ups_rpc_agent_;
};
} //end of namespace updateserver
}//end of namespace sb

#endif //OCEANBASE_UPDATASERVER_CLIENT_WRAPPER


