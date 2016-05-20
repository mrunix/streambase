/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scalar_aggregate.cc
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_scalar_aggregate.h"
#include "common/utility.h"

using namespace sb::sql;
using namespace sb::common;

ObScalarAggregate::ObScalarAggregate()
  : is_first_row_(true), is_input_empty_(false) {
}

ObScalarAggregate::~ObScalarAggregate() {
}

void ObScalarAggregate::reset() {
  merge_groupby_.reset();
  is_first_row_ = true;
  is_input_empty_ = false;
  ObSingleChildPhyOperator::reset();
}


void ObScalarAggregate::reuse() {
  merge_groupby_.reuse();
  is_first_row_ = true;
  is_input_empty_ = false;
  ObSingleChildPhyOperator::reuse();
}

int ObScalarAggregate::set_child(int32_t child_idx, ObPhyOperator& child_operator) {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::set_child(child_idx, child_operator))) {
    TBSYS_LOG(WARN, "failed to set single child, err=%d", ret);
  } else if (OB_SUCCESS != (ret = merge_groupby_.set_child(0, *child_op_))) {
    TBSYS_LOG(WARN, "failed to set child, err=%d", ret);
  }
  return ret;
}

int ObScalarAggregate::open() {
  is_first_row_ = true;
  is_input_empty_ = false;
  return merge_groupby_.open();
}

int ObScalarAggregate::close() {
  is_first_row_ = true;
  is_input_empty_ = false;
  return merge_groupby_.close();
}

int ObScalarAggregate::get_next_row(const ObRow*& row) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_first_row_)) {
    is_first_row_ = false;
    ret = merge_groupby_.get_next_row(row);
    if (OB_ITER_END == ret) {
      is_input_empty_ = true;
      // @see http://bugfree.corp.taobao.com/bug/200752
      TBSYS_LOG(DEBUG, "scalar aggregate returns single row for the empty input set");
      ret = merge_groupby_.get_row_for_empty_set(row);
    }
  } else {
    if (OB_UNLIKELY(is_input_empty_)) {
      ret = OB_ITER_END;
    } else {
      ret = merge_groupby_.get_next_row(row);
    }
  }
  return ret;
}

int ObScalarAggregate::get_row_desc(const common::ObRowDesc*& row_desc) const {
  return merge_groupby_.get_row_desc(row_desc);
}

int ObScalarAggregate::add_aggr_column(const ObSqlExpression& expr) {
  return merge_groupby_.add_aggr_column(expr);
}

namespace sb {
namespace sql {
REGISTER_PHY_OPERATOR(ObScalarAggregate, PHY_SCALAR_AGGREGATE);
}
}

int64_t ObScalarAggregate::to_string(char* buf, const int64_t buf_len) const {
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ScalarAggr(aggr_cols=[");
  for (int64_t i = 0; i < merge_groupby_.aggr_columns_.count(); ++i) {
    const ObSqlExpression& expr = merge_groupby_.aggr_columns_.at(i);
    pos += expr.to_string(buf + pos, buf_len - pos);
    if (i != merge_groupby_.aggr_columns_.count() - 1) {
      databuff_printf(buf, buf_len, pos, ",");
    }
  } // end for
  databuff_printf(buf, buf_len, pos, "])\n");
  if (NULL != child_op_) {
    int64_t pos2 = child_op_->to_string(buf + pos, buf_len - pos);
    pos += pos2;
  }
  return pos;
}

DEFINE_SERIALIZE(ObScalarAggregate) {
  int ret = OB_SUCCESS;
  if ((ret = merge_groupby_.serialize(buf, buf_len, pos)) != OB_SUCCESS) {
    TBSYS_LOG(WARN, "fail to serialize ObScalarAggregate. ret=%d", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObScalarAggregate) {
  int ret = OB_SUCCESS;
  if ((ret = merge_groupby_.deserialize(buf, data_len, pos)) != OB_SUCCESS) {
    TBSYS_LOG(WARN, "fail to deserialize ObScalarAggregate. ret=%d", ret);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObScalarAggregate) {
  int64_t size = 0;
  size = merge_groupby_.get_serialize_size();
  return size;
}

PHY_OPERATOR_ASSIGN(ObScalarAggregate) {
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObScalarAggregate);
  reset();
  ret = merge_groupby_.assign(&o_ptr->merge_groupby_);
  return ret;
}

