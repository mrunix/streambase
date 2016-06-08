/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_join_agent.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_MERGE_JOIN_AGENT_H_
#define OCEANBASE_MERGESERVER_OB_MERGE_JOIN_AGENT_H_
#include "ob_merge_join_agent_imp.h"
#include "ob_cell_stream.h"
#include "common/ob_cell_array.h"
#include "common/ob_iterator.h"
#include "common/ob_vector.h"
#include "common/ob_schema.h"
#include "common/ob_string.h"
#include "common/ob_merger.h"
namespace sb {
namespace mergeserver {
/// @class  only encapsulate ObScanMergeJoinAgentImp and ObGetMergeJoinAgentImp
class ObMergeJoinAgent: public ObMergeJoinAgentImp {
 public:
  ObMergeJoinAgent(ObMergerRpcProxy& proxy);
  ~ObMergeJoinAgent();
 public:
  virtual int get_cell(sb::common::ObCellInfo * *cell);
  virtual int get_cell(sb::common::ObCellInfo * *cell, bool* is_row_changed);
  virtual int next_cell();
 public:
  /// @param max_memory_size 只有在chunkserver每日合并的时候，
  ///   将max_memory_size设置为大于0，表示合并过程中最多使用的内存大小，
  ///   如果以后一个tablet在合并过程中可以完全放入内存，可以取消该参数
  virtual int set_request_param(const sb::common::ObScanParam& scan_param,
                                ObUPSCellStream& ups_stream,
                                ObUPSCellStream& ups_join_stream,
                                const sb::common::ObSchemaManagerV2& schema_mgr,
                                const int64_t max_memory_size = -1);
  virtual int set_request_param(const sb::common::ObGetParam& get_param,
                                ObUPSCellStream& ups_stream,
                                ObUPSCellStream& ups_join_stream,
                                const sb::common::ObSchemaManagerV2& schema_mgr,
                                const int64_t max_memory_size = -1);
  virtual void clear();
  virtual bool is_request_fullfilled();
 private:
  ObScanMergeJoinAgentImp   scan_agent_;
  ObGetMergeJoinAgentImp    get_agent_;
  /// @property point to scan_agent_ or get_agent_
  ObMergeJoinAgentImp*          req_agent_;
};
}
}
#endif /* MERGESERVER_OB_MERGE_JOIN_AGENT_H_ */


