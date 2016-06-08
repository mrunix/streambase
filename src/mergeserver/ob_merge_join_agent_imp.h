/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_join_agent_imp.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_MERGE_JOIN_AGENT_IMP_H_
#define OCEANBASE_MERGESERVER_OB_MERGE_JOIN_AGENT_IMP_H_
#include "ob_cell_stream.h"
#include "ob_ms_rpc_proxy.h"
#include "ob_groupby_operator.h"
#include "common/ob_iterator.h"
#include "common/ob_vector.h"
#include "common/ob_schema.h"
#include "common/ob_string.h"
#include "common/ob_merger.h"
#include "common/ob_scanner.h"
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
namespace sb {
namespace common {
template <>
struct ob_vector_traits<int64_t>;
}
namespace mergeserver {
/// @class  do real jobs like merge, join; but it only represent stage result
///   ObMergeJoinAgent will merge stage result into final result
/// @author wushi(wushi.ly@taobao.com)  (9/17/2010)
class ObMergeJoinOperator : public common::ObCellArray {
 public:
  ObMergeJoinOperator(ObMergerRpcProxy& rpc_proxy);
  ~ObMergeJoinOperator();
 public:
  /// @fn set request parameter
  /// @param max_memory_size if the intermediate results take memory size more than this
  ///   stop process, and continue when user call this function next time
  int set_request_param(const sb::common::ObScanParam& scan_param,
                        ObUPSCellStream& ups_stream,
                        ObUPSCellStream& ups_join_stream,
                        const sb::common::ObSchemaManagerV2& schema_mgr,
                        const int64_t max_memory_size);
  int set_request_param(const sb::common::ObGetParam& get_param,
                        ObUPSCellStream& ups_stream,
                        ObUPSCellStream& ups_join_stream,
                        const sb::common::ObSchemaManagerV2& schema_mgr,
                        const int64_t max_memory_size);
  /// @fn clear all result stored
  void clear();
  /// @fn reset intermediate result
  void prepare_next_round();
  /// @fn check if result has been finised yet
  bool is_request_finished()const ;

  virtual int next_cell();
 private:
  const sb::common::ObSchemaManagerV2* schema_mgr_;
  ObUPSCellStream*  ups_stream_;
  ObUPSCellStream*  ups_join_stream_;
  int64_t       max_memory_size_;
  bool          request_finished_;
  bool          cs_request_finished_;
  static char ugly_fake_get_param_rowkey_;
  sb::common::ObGetParam fake_get_param_;
  const sb::common::ObScanParam* scan_param_;
  const sb::common::ObGetParam*  get_param_;
  /// @property when get, req_param_ == get_param_; when scan req_param_ == & fake_get_param_
  const sb::common::ObGetParam* req_param_;

 private:
  ObMergerTabletLocation                cur_cs_addr_;
  sb::common::ObStringBuf        cur_row_key_buf_;
  /// properties for get request
  sb::common::ObMerger           merger_;
  bool                                  merger_iterator_moved_;
  sb::common::ObGetParam         cur_get_param_;
  int64_t                               got_cell_num_;
  /// properties for scan request
  sb::common::ObScanParam        cur_scan_param_;
  sb::common::ObMemBuffer        cs_scan_buffer_;
  sb::common::ObMemBuffer        ups_scan_buffer_;
  sb::common::ObScanner          cur_cs_result_;
  ObMergerRpcProxy*                     rpc_proxy_;
  bool                                  param_contain_duplicated_columns_;
  int32_t                               column_id_idx_map_[sb::common::OB_MAX_COLUMN_NUMBER];
  /// properties for join
  int64_t cur_rpc_offset_beg_;
  sb::common::ObReadParam        cur_join_read_param_;
  bool                                  is_need_query_ups_;
  bool                                  is_need_join_;
  ObCellArray                           join_param_array_;
  sb::common::ObVector<int64_t>  join_row_width_vec_;
  sb::common::ObVector<int64_t>  join_offset_vec_;

 private:
  int do_merge_join();
  int merge();
  int merge_next_row(int64_t& cur_row_beg, int64_t& cur_row_end);
  int apply_whole_row(const sb::common::ObCellInfo& cell, const int64_t row_beg);
  int prepare_join_param_();
  int join();
  void initialize();
  int get_next_rpc_result();
  int get_next_get_rpc_result();
  int get_next_scan_rpc_result();
  template<typename IteratorT>
  int join_apply(const sb::common::ObCellInfo& cell,
                 IteratorT& dst_off_beg,
                 IteratorT& dst_off_end);
  void move_to_next_row(const sb::common::ObGetParam& row_spec_arr,
                        const int64_t row_spec_arr_size,
                        int64_t& cur_row_beg,
                        int64_t& cur_row_end);
 private:
  sb::common::ObCellInfo join_apply_cell_adjusted_;
};

/// @class merge and join agent implementation
class ObMergeJoinAgentImp : public sb::common::ObIterator {
 public:
  virtual ~ObMergeJoinAgentImp() {
  }
  int get_cell(sb::common::ObCellInfo**) {
    return sb::common::OB_SUCCESS;
  }
  int get_cell(sb::common::ObCellInfo**, bool*) {
    return sb::common::OB_SUCCESS;
  }
  int next_cell() {
    return sb::common::OB_SUCCESS;
  }
  /// @fn set request parameter
  virtual int set_request_param(const sb::common::ObScanParam& scan_param,
                                ObUPSCellStream& ups_stream,
                                ObUPSCellStream& ups_join_stream,
                                const sb::common::ObSchemaManagerV2& schema_mgr,
                                const int64_t max_memory_size) = 0;
  virtual int set_request_param(const sb::common::ObGetParam& get_param,
                                ObUPSCellStream& ups_stream,
                                ObUPSCellStream& ups_join_stream,
                                const sb::common::ObSchemaManagerV2& schema_mgr,
                                const int64_t max_memory_size) = 0;
  virtual void clear() = 0;
  virtual bool is_request_fullfilled() = 0;
};

/// @class  just encapsulate ObMergeJoinOperator
class ObGetMergeJoinAgentImp : public ObMergeJoinAgentImp {
 public:
  ObGetMergeJoinAgentImp(ObMergerRpcProxy& proxy)
    : merge_join_operator_(proxy) {
  }
  ~ObGetMergeJoinAgentImp();
 public:
  virtual int get_cell(sb::common::ObCellInfo * *cell);
  virtual int get_cell(sb::common::ObCellInfo * *cell, bool* is_row_changed);
  virtual int next_cell();
  /// @fn set request parameter
  virtual int set_request_param(const sb::common::ObScanParam& scan_param,
                                ObUPSCellStream& ups_stream,
                                ObUPSCellStream& ups_join_stream,
                                const sb::common::ObSchemaManagerV2& schema_mgr,
                                const int64_t max_memory_size);
  virtual int set_request_param(const sb::common::ObGetParam& get_param,
                                ObUPSCellStream& ups_stream,
                                ObUPSCellStream& ups_join_stream,
                                const sb::common::ObSchemaManagerV2& schema_mgr,
                                const int64_t max_memory_size);
  virtual void clear();
  virtual bool is_request_fullfilled();
 private:
  ObMergeJoinOperator merge_join_operator_;
};

/// @class encapsulate ObMergeJoinOperator and implement order by, limit,
///   and multiple times scan intermediate result
class ObScanMergeJoinAgentImp : public ObMergeJoinAgentImp {
 public:
  ObScanMergeJoinAgentImp(ObMergerRpcProxy& proxy);
  ~ObScanMergeJoinAgentImp();
 public:
  virtual int get_cell(sb::common::ObCellInfo * *cell);
  virtual int get_cell(sb::common::ObCellInfo * *cell, bool* is_row_changed);
  virtual int next_cell();
  virtual int set_request_param(const sb::common::ObScanParam& scan_param,
                                ObUPSCellStream& ups_stream,
                                ObUPSCellStream& ups_join_stream,
                                const sb::common::ObSchemaManagerV2& schema_mgr,
                                const int64_t max_memory_size);
  virtual int set_request_param(const sb::common::ObGetParam& get_param,
                                ObUPSCellStream& ups_stream,
                                ObUPSCellStream& ups_join_stream,
                                const sb::common::ObSchemaManagerV2& schema_mgr,
                                const int64_t max_memory_size);
  virtual void clear();
  virtual bool is_request_fullfilled();

  static void set_return_uncomplete_result(bool return_uncomplete_result) {
    return_uncomplete_result_  = return_uncomplete_result;
  }
 private:
  static bool return_uncomplete_result_;
  int filter_org_row_(const sb::common::ObCellArray& cells, const int64_t row_beg,
                      const int64_t row_end, const sb::common::ObSimpleFilter& filter,
                      bool& result);
  int jump_limit_offset_(sb::common::ObIterator& cells, const int64_t jump_cell_num);
  int prepare_final_result_();
  int prepare_final_result_process_intermediate_result_();
  const sb::common::ObScanParam* param_;
  ObMergeJoinOperator merge_join_operator_;
  ObGroupByOperator   groupby_operator_;
  common::ObCellArray*    pfinal_result_;
  ObUPSCellStream*        ups_stream_;
  int64_t             max_avail_mem_size_;

  int64_t             limit_offset_;
  int64_t             limit_count_;
  int64_t             max_avail_cell_num_;
  sb::common::ObCellArray::OrderDesc orderby_desc_[sb::common::OB_MAX_COLUMN_NUMBER];
};
}
}
#endif /* MERGESERVER_OB_MERGE_JOIN_AGENT_IMP_H_ */


