/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_common_param.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_COMMON_PARAM_H_
#define OCEANBASE_COMMON_COMMON_PARAM_H_

#include "ob_define.h"
#include "ob_object.h"
#include "ob_range.h"

namespace sb {
namespace common {
struct ObCellInfo {
  ObCellInfo() : table_name_(), table_id_(OB_INVALID_ID),
    row_key_(), column_id_(OB_INVALID_ID),
    column_name_(), value_() {}

  ObString table_name_;
  uint64_t table_id_;
  ObString row_key_;
  uint64_t column_id_;
  ObString column_name_;
  ObObj value_;

  void reset() {
    table_name_.assign(NULL, 0);
    table_id_ = OB_INVALID_ID;
    row_key_.assign(NULL, 0);
    column_id_ = OB_INVALID_ID;
    column_name_.assign(NULL, 0);
    value_.reset();
  }
};

/// @class ObReadParam  OB read parameter, API should not concern these parameters,
///   and mergeserver will directly ignore these parameters
class ObReadParam {
 public:
  ObReadParam();
  virtual ~ObReadParam();

  /// @fn get data whose timestamp is newer or as new as the given timestamp,
  ///   -# when reading cs, if not setted, the result is decided by the server;
  ///   -# when reading ups, this parameter must be setted
  void set_version_range(const ObVersionRange& range);
  ObVersionRange get_version_range(void) const;

  /// @fn when reading cs, indicating whether the result (including intermediate result,
  /// like sstable block readed from sstable) of this operation should be cached.
  ///
  /// ups just ignores this parameter
  void set_is_result_cached(const bool cached);
  bool get_is_result_cached()const;

  void set_is_read_consistency(const bool cons);
  bool get_is_read_consistency()const;

  void reset(void);

  /// serailize or deserialization
  NEED_SERIALIZE_AND_DESERIALIZE;

 protected:
  // RESERVE_PARAM_FIELD
  int serialize_reserve_param(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_reserve_param(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_reserve_param_serialize_size(void) const;

 private:
  int8_t is_read_master_;
  int8_t is_result_cached_;
  ObVersionRange version_range_;
};
} /* common */
} /* sb */

#endif /* end of include guard: OCEANBASE_COMMON_COMMON_PARAM_H_ */

