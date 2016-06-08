/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_simple_filter.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef OB_SIMPLE_FILTER_H_
#define OB_SIMPLE_FILTER_H_

#include "ob_vector.h"
#include "ob_string_buf.h"
#include "ob_simple_condition.h"

namespace sb {
namespace common {
class ObCellArray;
// the container of all logic and conditions using for logic calculate
// when add condition item, deeply copy the right operand object especially for string
class ObSimpleFilter {
 public:
  ObSimpleFilter();
  // will be removed after scan_param refactor
  ObSimpleFilter(const ObSimpleFilter& other);
  virtual ~ObSimpleFilter();

 public:
  /// add a condition by column name used by client api
  int add_cond(const ObString& column_name, const ObLogicOperator& cond_op,
               const ObObj& cond_value);

 public:
  /// add a condition by column index used as inner interface
  int add_cond(const uint64_t column_index, const ObLogicOperator& cond_op,
               const ObObj& cond_value);

  /// random access condition item
  const ObSimpleCond* operator [](const int64_t index) const;

  /// do check all conditions
  /// require condition.column_index + row_begin <= row_end
  int check(const ObCellArray& cells, const int64_t row_begin, const int64_t row_end,
            bool& result) const;

  /// reset for reusing this instance
  void reset(void);

  /// get condition count
  int64_t get_count(void) const;

  /// operator = overload
  int operator = (const ObSimpleFilter& other);

  /// operator == overload
  bool operator == (const ObSimpleFilter& other) const;

  /// serailize or deserialization
  NEED_SERIALIZE_AND_DESERIALIZE;

 private:
  /// add a condition item
  int add_cond(const ObSimpleCond& cond);
  /// deep copy obj cond_value to store_value
  int copy_obj(const ObObj& cond_value, ObObj& store_value);

 private:
  /// string buffer for cond value obj string
  ObStringBuf string_buffer_;
  /// all conditions for filter
  ObVector<ObSimpleCond> conditions_;
};

inline int64_t ObSimpleFilter::get_count(void) const {
  return conditions_.size();
}
}
}


#endif //OB_SIMPLE_FILTER_H_


