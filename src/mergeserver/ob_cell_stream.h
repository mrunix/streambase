/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cell_stream.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_CELL_STREAM_H_
#define OCEANBASE_MERGESERVER_OB_CELL_STREAM_H_

#include "common/ob_iterator.h"
#include "common/ob_cache.h"
#include "common/ob_cell_array.h"
#include "common/ob_read_common_data.h"
#include "ob_row_cell_vec.h"
#include "ob_ms_tablet_location_item.h"

namespace sb {
namespace mergeserver {
class ObMSGetCellArray {
 public:
  ObMSGetCellArray();
  ObMSGetCellArray(common::ObCellArray& cell_array);
  ObMSGetCellArray(const sb::common::ObGetParam& get_param);
  virtual ~ObMSGetCellArray() {
  }

  /// @fn get cell number
  int64_t get_cell_size()const;
  /// @fn get cell according to operator []
  const sb::common::ObCellInfo& operator[](int64_t offset)const;

 private:
  common::ObCellArray* cell_array_;
  const sb::common::ObGetParam* get_param_;
  sb::common::ObCellInfo default_cell_;
};

typedef enum ServerType {
  CHUNK_SERVER = 100,
  UPDATE_SERVER = 101,
  UNKNOW_SERVER = 102,
} server_type;
/// @class  encapsulate stream read of ups, cs or something else
/// @author wushi(wushi.ly@taobao.com)  (9/17/2010)
class ObUPSCellStream : public sb::common::ObIterator {
 public:
  virtual ~ObUPSCellStream() {
  }
 public:
  virtual void reset() = 0;
  /// @fn prepare to do get query, may just prepare,
  ///   whether doing actions was decided by implementation
  /// @note 不使用ObGetParam是因为在join的时候上层调用的get可能包含很多cell，拆分由ObCellStream完成
  virtual int get(const sb::common::ObReadParam& read_param,
                  ObMSGetCellArray& get_cells, const ObMergerTabletLocation& cs_addr) = 0;
  /// @fn prepare to do scan query, may just prepare,
  ///   whether doing actions was decided by implementation
  virtual int scan(const sb::common::ObScanParam& scan_param,
                   const ObMergerTabletLocation& cs_addr) = 0;

  /// @fn set cache to  if (OB_SUCCESS == err)
  ///    cached content is [tableid,rowkey]->(ObScanner of the row)
  virtual void set_cache(sb::common::ObVarCache<>& cache) {
    UNUSED(cache);
  }

  /// @fn get a row in the cache, 在这个函数中处理每日合并join的cache和bloomfilter
  ///
  /// @param key    需要查询的cell
  /// @param result ObScanner串行化后的结果
  ///   -# 如果命中cache，直接返回cache项
  ///   -# 如果bloomfilter发现rowkey对应的记录没有发生修改，那么返回一个特殊结果，结果中包含0个cell
  virtual int get_cache_row(const sb::common::ObCellInfo& key,  ObRowCellVec*& result) {
    UNUSED(key);
    UNUSED(result);
    return sb::common::OB_ENTRY_NOT_EXIST;
  }
};
}
}


#endif /* MERGESERVER_OB_CELL_STREAM_H_ */


