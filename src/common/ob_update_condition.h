/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_update_condition.h for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_UPDATE_CONDITION_H__
#define __OCEANBASE_CHUNKSERVER_OB_UPDATE_CONDITION_H__

#include "ob_define.h"
#include "ob_string.h"
#include "ob_object.h"
#include "ob_simple_condition.h"
#include "ob_vector.h"
#include "ob_string_buf.h"
#include "ob_action_flag.h"

namespace sb {
namespace common {
struct ObCondInfo {
  ObString table_name_;
  ObString row_key_;
  ObString column_name_;
  ObLogicOperator op_type_;
  ObObj value_;
};

template <>
struct ob_vector_traits<ObCondInfo> {
  typedef ObCondInfo* pointee_type;
  typedef ObCondInfo value_type;
  typedef const ObCondInfo const_value_type;
  typedef value_type* iterator;
  typedef const value_type* const_iterator;
  typedef int32_t difference_type;
};

class ObUpdateCondition {
 public:
  ObUpdateCondition();
  ~ObUpdateCondition();

  void reset();

 public:
  int add_cond(const ObString& table_name, const ObString& row_key, const ObString& column_name,
               const ObLogicOperator op_type, const ObObj& value);

  int add_cond(const ObString& table_name, const ObString& row_key, const bool is_exist);

 public:
  int64_t get_count(void) const {
    return conditions_.size();
  }

  const ObCondInfo* operator[](const int64_t index) const;

  NEED_SERIALIZE_AND_DESERIALIZE;

 private:
  int add_cond_info_(const ObCondInfo& cond_info);
  int copy_cond_info_(const ObCondInfo& src_cond_info, ObCondInfo& dst_cond_info);

  int serialize_cond_info_(char* buf, const int64_t buf_len, int64_t& pos,
                           const ObCondInfo& cond_info) const;
  int deserialize_cond_info_(const char* buf, const int64_t buf_len, int64_t& pos,
                             ObCondInfo& cond_info);
  int64_t get_serialize_size_(const ObCondInfo& cond_info) const;
 private:
  ObVector<ObCondInfo> conditions_;
  ObStringBuf string_buf_;
};
}
}

#endif //__OB_UPDATE_CONDITION_H__


