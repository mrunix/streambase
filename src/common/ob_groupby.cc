/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_groupby.cc for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_groupby.h"
#include "ob_define.h"
#include "murmur_hash.h"
#include "ob_action_flag.h"
using namespace sb;
using namespace sb::common;
sb::common::ObAggregateColumn::ObAggregateColumn() {
  org_column_idx_ = -1;
  as_column_idx_ = -1;
  func_type_ = AGG_FUNC_MIN;
}


sb::common::ObAggregateColumn::~ObAggregateColumn() {
}

sb::common::ObAggregateColumn::ObAggregateColumn(ObString& org_column_name, ObString& as_column_name,
                                                 const int64_t as_column_idx, const ObAggregateFuncType& func_type) {
  org_column_name_ = org_column_name;
  as_column_name_ = as_column_name;
  org_column_idx_ = -1;
  as_column_idx_ = as_column_idx;
  func_type_ = func_type;
}

sb::common::ObAggregateColumn::ObAggregateColumn(const int64_t org_column_idx,  const int64_t as_column_idx,
                                                 const ObAggregateFuncType& func_type) {
  org_column_idx_ = org_column_idx;
  as_column_idx_ = as_column_idx;
  func_type_ = func_type;
}

int sb::common::ObAggregateColumn::calc_aggregate_val(ObObj& aggregated_val, const ObObj& new_val)const {
  int err = OB_SUCCESS;
  ObObj mutation;
  switch (func_type_) {
  case SUM:
    if (ObNullType == aggregated_val.get_type()) {
      aggregated_val = new_val;
    } else if (ObNullType != new_val.get_type()) {
      mutation = new_val;
      err = mutation.set_add(true);
      if (OB_SUCCESS == err) {
        err = aggregated_val.apply(mutation);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "calculate aggregate value error [aggregated_val.type:%d,mutation.type:%d]",
                    aggregated_val.get_type(), mutation.get_type());
        }
      } else {
        TBSYS_LOG(WARN, "fail to set add property of mutation [mutation.type:%d]", mutation.get_type());
      }
    }
    break;
  case COUNT:
    mutation.set_int(1, true);
    err = aggregated_val.apply(mutation);
    break;
  case MAX:
    if ((ObNullType != new_val.get_type())
        && ((new_val > aggregated_val) || aggregated_val.get_type() == ObNullType)
       ) {
      aggregated_val = new_val;
    }
    break;
  case MIN:
    if ((ObNullType != new_val.get_type())
        && ((new_val < aggregated_val) || aggregated_val.get_type() == ObNullType)
       ) {
      aggregated_val = new_val;
    }
    break;
  default:
    TBSYS_LOG(WARN, "unsupported aggregate function type [type:%d]", func_type_);
    err = OB_NOT_SUPPORTED;
    break;
  }
  return err;
}


bool sb::common::ObAggregateColumn::operator==(const ObAggregateColumn& other)const {
  bool result = false;
  result = (org_column_name_ == other.org_column_name_
            && org_column_idx_ == other.org_column_idx_
            && as_column_name_ == other.as_column_name_
            && as_column_idx_ == other.as_column_idx_
            && func_type_ == other.func_type_);
  return result;
}

int sb::common::ObAggregateColumn::get_first_aggregate_obj(const ObObj& first_obj,
                                                           ObObj& agg_obj)const {
  int err = OB_SUCCESS;
  switch (func_type_) {
  case MAX:
  case MIN:
  case SUM:
    agg_obj = first_obj;
    break;
  case COUNT:
    agg_obj.set_int(1);
    break;
  default:
    TBSYS_LOG(WARN, "unsupported aggregate function type [type:%d]", func_type_);
    err = OB_NOT_SUPPORTED;
    break;
  }
  return err;
}

int sb::common::ObAggregateColumn::init_aggregate_obj(sb::common::ObObj& agg_obj)const {
  int err = OB_SUCCESS;
  switch (func_type_) {
  case MAX:
  case MIN:
    agg_obj.set_null();
    break;
  case SUM:
  case COUNT:
    agg_obj.set_int(0);
    break;
  default:
    TBSYS_LOG(WARN, "unsupported aggregate function type [type:%d]", func_type_);
    err = OB_NOT_SUPPORTED;
    break;
  }
  return err;
}

void sb::common::ObGroupKey::initialize() {
  hash_val_ = 0;
  key_type_ = INVALID_KEY_TYPE;
  group_by_param_ = NULL;
  cell_array_ = NULL;
  row_beg_ = -1;
  row_end_ = -1;
}

sb::common::ObGroupKey::ObGroupKey() {
  initialize();
}

sb::common::ObGroupKey::~ObGroupKey() {
  initialize();
}

int sb::common::ObGroupKey::init(const ObCellArray& cell_array, const ObGroupByParam& param,
                                 const int64_t row_beg, const int64_t row_end, const int32_t type) {
  int err = OB_SUCCESS;
  if (ORG_KEY != type && AGG_KEY != type) {
    TBSYS_LOG(WARN, "param error, unknow key type [type:%d]", type);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    if (row_beg < 0
        || row_end < 0
        || row_beg > row_end
        || row_end >= cell_array.get_cell_size()) {
      TBSYS_LOG(WARN, "param error [row_beg:%ld,row_end:%ld,cell_array.get_cell_size():%ld]",
                row_beg, row_end, cell_array.get_cell_size());
      err = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err) {
    cell_array_ = &cell_array;
    group_by_param_ = &param;
    row_beg_ = row_beg;
    row_end_ = row_end;
    key_type_ = type;
    err = param.calc_group_key_hash_val(*this, hash_val_);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to calc hash value of current key [err:%d]", err);
    }
  }
  if (OB_SUCCESS != err) {
    initialize();
  }
  return err;
}

bool sb::common::ObGroupKey::operator ==(const ObGroupKey& other)const {
  return ObGroupByParam::is_equal(*this, other);
}

sb::common::ObGroupByParam::ObGroupByParam():
  group_by_columns_(sizeof(group_by_columns_buf_) / sizeof(ColumnInfo), group_by_columns_buf_),
  return_columns_(sizeof(return_columns_buf_) / sizeof(ColumnInfo), return_columns_buf_),
  aggregate_columns_(sizeof(aggregate_columns_buf_) / sizeof(ObAggregateColumn), aggregate_columns_buf_) {
  column_num_ = 0;
  using_id_ = false;
  using_name_ = false;
}


sb::common::ObGroupByParam::~ObGroupByParam() {
}


void sb::common::ObGroupByParam::clear() {
  column_num_ = 0;
  buffer_.reset();
  group_by_columns_.clear();
  return_columns_.clear();
  aggregate_columns_.clear();
  using_id_ = false;
  using_name_ = false;
  ColumnInfo* cptr = NULL;
  cptr = new(group_by_columns_buf_)ColumnInfo[OB_MAX_COLUMN_NUMBER];
  cptr = new(return_columns_buf_)ColumnInfo[OB_MAX_COLUMN_NUMBER];
  ObAggregateColumn* aptr = NULL;
  aptr = new(aggregate_columns_buf_)ObAggregateColumn[OB_MAX_COLUMN_NUMBER];
}

int sb::common::ObGroupByParam::add_groupby_column(const ObString& column_name) {
  int err = OB_SUCCESS;
  ObString stored_column_name;
  ColumnInfo group_by_column;
  if (return_columns_.get_array_index() > 0 || aggregate_columns_.get_array_index() > 0) {
    TBSYS_LOG(WARN, "wrong order; add column step is 1. groupby columns; 2. return columns; 3. aggregate columns;");
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err) {
    err = buffer_.write_string(column_name, &stored_column_name);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to store column name into local buffer [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err) {
    group_by_column.as_column_idx_ = column_num_;
    group_by_column.column_name_ = stored_column_name;
    group_by_column.org_column_idx_ = -1;
    if (!group_by_columns_.push_back(group_by_column)) {
      err = OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "fail to push_back group by column [err:%d]", err);
    } else {
      column_num_ ++;
    }
  }
  return err;
}

int sb::common::ObGroupByParam::add_groupby_column(const int64_t column_idx) {
  int err = OB_SUCCESS;
  ColumnInfo group_by_column;
  if (return_columns_.get_array_index() > 0 || aggregate_columns_.get_array_index() > 0) {
    TBSYS_LOG(WARN, "wrong order; add column step is 1. groupby columns; 2. return columns; 3. aggregate columns;");
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err) {
    group_by_column.as_column_idx_ = column_num_;
    group_by_column.column_name_.assign(NULL, 0);
    group_by_column.org_column_idx_ = column_idx;
    if (!group_by_columns_.push_back(group_by_column)) {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "fail to push_back group by column [err:%d]", err);
    } else {
      column_num_ ++;
    }
  }
  return err;
}

int sb::common::ObGroupByParam::add_return_column(const ObString& column_name) {
  int err = OB_SUCCESS;
  ObString stored_column_name;
  ColumnInfo return_column;
  if (aggregate_columns_.get_array_index() > 0) {
    TBSYS_LOG(WARN, "wrong order; add column step is 1. groupby columns; 2. return columns; 3. aggregate columns;");
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err) {
    err = buffer_.write_string(column_name, &stored_column_name);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to store column name into local buffer [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err) {
    return_column.as_column_idx_ = column_num_;
    return_column.column_name_ = stored_column_name;
    return_column.org_column_idx_ = -1;
    if (!return_columns_.push_back(return_column)) {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "fail to push_back return column [err:%d]", err);
    } else {
      column_num_ ++;
    }
  }
  return err;
}

int sb::common::ObGroupByParam::add_return_column(const int64_t column_idx) {
  int err = OB_SUCCESS;
  ColumnInfo return_column;
  if (aggregate_columns_.get_array_index() > 0) {
    TBSYS_LOG(WARN, "wrong order; add column step is 1. groupby columns; 2. return columns; 3. aggregate columns;");
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err) {
    return_column.as_column_idx_ = column_num_;
    return_column.column_name_.assign(NULL, 0);
    return_column.org_column_idx_ = column_idx;
    if (!return_columns_.push_back(return_column)) {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "fail to push_back return column [err:%d]", err);
    } else {
      column_num_ ++;
    }
  }
  return err;
}

ObString sb::common::ObGroupByParam::COUNT_ROWS_COLUMN_NAME = ObString(strlen("*"), strlen("*"),
    const_cast<char*>("*"));

int sb::common::ObGroupByParam::add_aggregate_column(const ObString& org_column_name, const ObString& as_column_name,
                                                     const ObAggregateFuncType aggregate_func) {
  int err = OB_SUCCESS;
  ObString stored_org_column_name;
  ObString stored_as_column_name;
  if (aggregate_func >= AGG_FUNC_END || aggregate_func <= AGG_FUNC_MIN) {
    TBSYS_LOG(WARN, "param error, unrecogonized aggregate function type [type:%d]", aggregate_func);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    err = buffer_.write_string(org_column_name, &stored_org_column_name);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to store org_column_name to local buffer [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err) {
    err = buffer_.write_string(as_column_name, &stored_as_column_name);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to store as_column_name to local buffer [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err) {
    ObAggregateColumn aggregate_column(stored_org_column_name, stored_as_column_name, column_num_, aggregate_func);
    if (!aggregate_columns_.push_back(aggregate_column)) {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "fail to push_back aggregate_column [err:%d,column_num_:%ld]", err, column_num_);
    } else {
      column_num_ ++;
    }
  }
  return err;
}

int sb::common::ObGroupByParam::add_aggregate_column(const int64_t org_column_idx, const ObAggregateFuncType aggregate_func) {
  int err = OB_SUCCESS;
  if (aggregate_func >= AGG_FUNC_END || aggregate_func <= AGG_FUNC_MIN) {
    TBSYS_LOG(WARN, "param error, unrecogonized aggregate function type [type:%d]", aggregate_func);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    ObAggregateColumn aggregate_column(org_column_idx, column_num_, aggregate_func);
    if (!aggregate_columns_.push_back(aggregate_column)) {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "fail to push_back aggregate_column [err:%d,column_num_:%ld]", err, column_num_);
    } else {
      column_num_ ++;
    }
  }
  return err;
}



int sb::common::ObGroupByParam::calc_group_key_hash_val(const ObGroupKey& key, uint32_t& val) {
  uint32_t hash_val = 0;
  int err = OB_SUCCESS;
  if (NULL == key.get_cell_array() || NULL == key.get_groupby_param()) {
    TBSYS_LOG(WARN, "param error [key.get_cell_array():%p,key.get_groupby_param():%p]",
              key.get_cell_array(), key.get_groupby_param());
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    switch (key.get_key_type()) {
    case ObGroupKey::AGG_KEY:
      err = key.get_groupby_param()->calc_agg_group_key_hash_val(*key.get_cell_array(), key.get_row_beg(),
                                                                 key.get_row_end(), hash_val);
      break;
    case ObGroupKey::ORG_KEY:
      err = key.get_groupby_param()->calc_org_group_key_hash_val(*key.get_cell_array(), key.get_row_beg(),
                                                                 key.get_row_end(), hash_val);
      break;
    default:
      TBSYS_LOG(WARN, "param error, unregonized key type [key.get_key_type():%d]", key.get_key_type());
      err = OB_INVALID_ARGUMENT;
    }
  }
  val = hash_val;
  return err;
}

bool sb::common::ObGroupByParam::is_equal(const ObGroupKey& left, const ObGroupKey& right) {
  bool result = false;
  int err = OB_SUCCESS;
  if (NULL == left.get_cell_array()
      || NULL == left.get_groupby_param()
      || 0 > left.get_row_beg()
      || 0 > left.get_row_end()
      || left.get_row_beg() > left.get_row_end()
      || NULL == right.get_cell_array()
      || NULL == right.get_groupby_param()
      || 0 > right.get_row_beg()
      || 0 > right.get_row_end()
      || right.get_row_beg() > right.get_row_end()
      || left.get_groupby_param() != right.get_groupby_param()
     ) {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid agument ["
              "left.get_cell_array():%p,left.get_groupby_param():%p,left.get_row_beg():%ld,left.get_row_end():%ld,"
              "right.get_cell_array():%p,right.get_groupby_param():%p,right.get_row_beg():%ld,right.get_row_end():%ld,"
              "]"
              , left.get_cell_array(), left.get_groupby_param(), left.get_row_beg(), left.get_row_end()
              , right.get_cell_array(), right.get_groupby_param(), right.get_row_beg(), right.get_row_end()
             );
  }
  if (OB_SUCCESS == err) {
    if (left.get_row_end() >= left.get_cell_array()->get_cell_size()
        || right.get_row_end() >= right.get_cell_array()->get_cell_size()) {
      TBSYS_LOG(WARN, "invalid agument [left.get_cell_array()->get_cell_size():%d,right.get_cell_array()->get_cell_size():%d]",
                left.get_row_end() >= left.get_cell_array()->get_cell_size(),
                right.get_row_end() >= right.get_cell_array()->get_cell_size());
      err = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err) {
    result = true;
    const ObGroupByParam* param = left.get_groupby_param();
    if (left.get_hash_val() != right.get_hash_val()) {
      result = false;
    }
    for (int64_t i = 0; OB_SUCCESS == err && result && i < param->group_by_columns_.get_array_index(); i++) {
      int64_t left_idx = param->get_target_cell_idx(left, i);
      int64_t right_idx = param->get_target_cell_idx(right, i);
      if (left_idx < 0 || right_idx < 0) {
        TBSYS_LOG(WARN, "unexpected error [left_idx:%ld,right_idx:%ld]", left_idx, right_idx);
        err = (right_idx < 0) ? right_idx : left_idx;
        result = false;
      } else {
        if (left.get_cell_array()->operator [](left_idx).value_
            != right.get_cell_array()->operator [](right_idx).value_) {
          result = false;
        }
      }
    }
  }
  return result;
}


int64_t sb::common::ObGroupByParam::get_target_cell_idx(const ObGroupKey& key, const int64_t groupby_idx)const {
  int64_t max_idx = key.get_row_end() - key.get_row_beg();
  int64_t idx = 0;
  int64_t err = OB_SUCCESS;
  if (groupby_idx < 0 || groupby_idx >= group_by_columns_.get_array_index()) {
    TBSYS_LOG(WARN, "param error [groupby_idx:%ld,group_by_columns_.size():%ld]", groupby_idx, group_by_columns_.get_array_index());
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    switch (key.get_key_type()) {
    case ObGroupKey::AGG_KEY:
      idx = group_by_columns_.at(groupby_idx)->as_column_idx_;
      break;
    case ObGroupKey::ORG_KEY:
      idx = group_by_columns_.at(groupby_idx)->org_column_idx_;
      break;
    default:
      TBSYS_LOG(WARN, "unrecogonized key type [type:%d]", key.get_key_type());
      err = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS == err) {
      if (idx > max_idx) {
        TBSYS_LOG(WARN, "unexpected error [idx:%ld,row_beg:%ld,row_end:%ld]", idx, key.get_row_beg(), key.get_row_end());
      } else {
        idx += key.get_row_beg();
      }
    }
  }
  if (OB_SUCCESS == err) {
    if (idx >= key.get_cell_array()->get_cell_size()) {
      err = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "unexpected error, get idx bigger than size of cell array");
    } else {
      err = idx;
    }
  }
  return err;
}

int sb::common::ObGroupByParam::calc_org_group_key_hash_val(const ObCellArray& cells, const int64_t row_beg,
                                                            const int64_t row_end, uint32_t& val)const {
  uint32_t hash_val = 0;
  int64_t err = OB_SUCCESS;
  if (row_beg < 0
      || row_end < 0
      || row_beg >= cells.get_cell_size()
      || row_end >= cells.get_cell_size()
     ) {
    TBSYS_LOG(WARN, "param error [row_beg:%ld,row_end:%ld, cells.get_cell_size():%ld]", row_beg, row_end,
              cells.get_cell_size());
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    ObString str_val;
    int64_t max_idx = row_end - row_beg;
    for (int64_t i = 0; OB_SUCCESS == err && i < group_by_columns_.get_array_index(); i++) {
      if (group_by_columns_.at(i)->org_column_idx_ > max_idx || group_by_columns_.at(i)->org_column_idx_ < 0) {
        TBSYS_LOG(WARN, "param error [max_idx:%ld,group_by_column_idx:%ld,org_column_idx:%ld]", max_idx, i,
                  group_by_columns_.at(i)->org_column_idx_);
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err) {
        hash_val = cells[row_beg + group_by_columns_.at(i)->org_column_idx_].value_.murmurhash2(hash_val);
      }
    }
  }
  val = hash_val;
  return err;
}

int sb::common::ObGroupByParam::calc_agg_group_key_hash_val(const ObCellArray& cells, const int64_t row_beg,
                                                            const int64_t row_end, uint32_t& val)const {
  uint32_t hash_val = 0;
  int64_t err = OB_SUCCESS;
  if (row_beg < 0
      || row_end < 0
      || row_beg >= cells.get_cell_size()
      || row_end >= cells.get_cell_size()
     ) {
    TBSYS_LOG(WARN, "param error [row_beg:%ld,row_end:%ld, cells.get_cell_size():%ld]", row_beg, row_end,
              cells.get_cell_size());
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    ObString str_val;
    int64_t max_idx = row_end - row_beg;
    for (int64_t i = 0; OB_SUCCESS == err && i < group_by_columns_.get_array_index(); i++) {
      if (group_by_columns_.at(i)->as_column_idx_ > max_idx || group_by_columns_.at(i)->as_column_idx_ < 0) {
        TBSYS_LOG(WARN, "param error [max_idx:%ld,groupby_column_idx:%ld,as_column_idx:%ld]", max_idx, i,
                  group_by_columns_.at(i)->as_column_idx_);
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err) {
        hash_val = cells[row_beg + group_by_columns_.at(i)->as_column_idx_].value_.murmurhash2(hash_val);
      }
    }
  }
  val = hash_val;
  return err;
}



int sb::common::ObGroupByParam::aggregate(const ObCellArray& org_cells,  const int64_t org_row_beg,
                                          const int64_t org_row_end, ObCellArray& aggregate_cells,
                                          const int64_t aggregate_row_beg, const int64_t aggregate_row_end)const {
  int err = OB_SUCCESS;
  bool first_row_of_group = false;
  int64_t max_org_idx = org_row_end - org_row_beg;
  ObCellInfo* cell_out = NULL;
  if (aggregate_row_beg == aggregate_cells.get_cell_size()) {
    first_row_of_group = true;
  }
  if (aggregate_row_end - aggregate_row_beg + 1 != column_num_) {
    TBSYS_LOG(WARN, "param error [expected_aggregate_row_width:%ld,real_aggregate_row_width:%ld]",
              column_num_, aggregate_row_end - aggregate_row_beg + 1);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err && first_row_of_group) { /// append groupby return and aggregate columns
    for (int64_t i = 0;  OB_SUCCESS == err && i < group_by_columns_.get_array_index(); i++) {
      if (group_by_columns_.at(i)->org_column_idx_ > max_org_idx) {
        TBSYS_LOG(WARN, "unexpected error [groupby_column_idx:%ld,org_column_idx:%ld,max_org_idx:%ld]",
                  i, group_by_columns_.at(i)->org_column_idx_, max_org_idx);
        err = OB_ERR_UNEXPECTED;
      } else {
        err = aggregate_cells.append(org_cells[org_row_beg + group_by_columns_.at(i)->org_column_idx_], cell_out);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to append group by columns into aggregate cell array [err:%d]", err);
        }
      }
    }

    for (int64_t i = 0;  OB_SUCCESS == err && i < return_columns_.get_array_index(); i++) {
      if (return_columns_.at(i)->org_column_idx_ > max_org_idx) {
        TBSYS_LOG(WARN, "unexpected error [return_column_idx:%ld,org_column_idx:%ld,max_org_idx:%ld]",
                  i, return_columns_.at(i)->org_column_idx_, max_org_idx);
        err = OB_ERR_UNEXPECTED;
      } else {
        err = aggregate_cells.append(org_cells[org_row_beg + return_columns_.at(i)->org_column_idx_], cell_out);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to append return columns into aggregate cell array [err:%d]", err);
        }
      }
    }

    ObCellInfo first_agg_cell;
    first_agg_cell = org_cells[org_row_beg];
    for (int64_t i = 0; OB_SUCCESS == err && i < aggregate_columns_.get_array_index(); i++) {
      if (aggregate_columns_.at(i)->get_org_column_idx() > max_org_idx) {
        TBSYS_LOG(WARN, "unexpected error [aggregate_column_idx:%ld,org_column_idx:%ld,max_org_idx:%ld]",
                  i, aggregate_columns_.at(i)->get_org_column_idx(), max_org_idx);
        err = OB_ERR_UNEXPECTED;
      } else {
        err = aggregate_columns_.at(i)->get_first_aggregate_obj(org_cells[org_row_beg + aggregate_columns_.at(i)->get_org_column_idx()].value_,
                                                                first_agg_cell.value_);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to calc first aggregate value of aggregate cell [aggregeate_idx:%ld]", i);
        } else {
          err = aggregate_cells.append(first_agg_cell, cell_out);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to append aggregate columns into aggregate cell array [err:%d]", err);
          }
        }
      }
    }
  }
  /// process aggregate columns
  if (OB_SUCCESS == err && !first_row_of_group) {
    for (int64_t i = 0;  OB_SUCCESS == err && i < aggregate_columns_.get_array_index(); i++) {
      if (aggregate_columns_.at(i)->get_org_column_idx() > max_org_idx) {
        TBSYS_LOG(WARN, "unexpected error [aggregate_column_idx:%ld,org_column_idx:%ld,max_org_idx:%ld]",
                  i, aggregate_columns_.at(i)->get_org_column_idx(), max_org_idx);
        err = OB_ERR_UNEXPECTED;
      } else {
        ObObj& target_obj = aggregate_cells[aggregate_row_beg + aggregate_columns_.at(i)->get_as_column_idx()].value_;
        err =  aggregate_columns_.at(i)->calc_aggregate_val(target_obj,
                                                            org_cells[org_row_beg + aggregate_columns_.at(i)->get_org_column_idx()].value_);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to aggregate value [err:%d,org_idx:%ld,as_idx:%ld,func_type:%d]",
                    err, aggregate_columns_.at(i)->get_org_column_idx(), aggregate_columns_.at(i)->get_as_column_idx(),
                    aggregate_columns_.at(i)->get_func_type());
        }
      }
    }
  }
  return err;
}

int sb::common::ObGroupByParam::serialize(char* buf, const int64_t buf_len, int64_t& pos) const {
  int64_t err = OB_SUCCESS;
  err = serialize_helper(buf, buf_len, pos);
  if (0 <= err) {
    err = OB_SUCCESS;
  }
  return err;
}

int sb::common::ObGroupByParam::deserialize_groupby_columns(const char* buf, const int64_t buf_len, int64_t& pos) {
  int err = OB_SUCCESS;
  int64_t prev_pos = pos;
  ObObj cur_obj;
  int64_t int_val;
  ObString str_val;
  while (OB_SUCCESS == err && pos < buf_len) {
    prev_pos = pos;
    err = cur_obj.deserialize(buf, buf_len, pos);
    if (OB_SUCCESS == err) {
      if (cur_obj.get_ext() != 0
          || (cur_obj.get_type() != ObIntType && cur_obj.get_type() != ObVarcharType)) {
        pos = prev_pos;
        break;
      }
      if (!using_id_ && !using_name_) {
        if (cur_obj.get_type() == ObIntType) {
          using_id_ = true;
        } else {
          using_name_ = true;
        }
      }
      if (using_id_) {
        err = cur_obj.get_int(int_val);
        if (OB_SUCCESS == err) {
          err = add_groupby_column(int_val);
        } else {
          TBSYS_LOG(WARN, "fail to get int from obj [err:%d]", err);
        }
      } else {
        err = cur_obj.get_varchar(str_val);
        if (OB_SUCCESS == err) {
          err = add_groupby_column(str_val);
        } else {
          TBSYS_LOG(WARN, "fail to get varchar from obj [err:%d]", err);
        }
      }
    } else {
      TBSYS_LOG(WARN, "fail to deserialize groupby column [column_idx:%ld,err:%d]", group_by_columns_.get_array_index(), err);
    }
  }
  return err;
}

int sb::common::ObGroupByParam::deserialize_return_columns(const char* buf, const int64_t buf_len, int64_t& pos) {
  int err = OB_SUCCESS;
  int64_t prev_pos = pos;
  ObObj cur_obj;
  int64_t int_val;
  ObString str_val;
  while (OB_SUCCESS == err && pos < buf_len) {
    prev_pos = pos;
    err = cur_obj.deserialize(buf, buf_len, pos);
    if (OB_SUCCESS == err) {
      if (cur_obj.get_ext() != 0
          || (cur_obj.get_type() != ObIntType && cur_obj.get_type() != ObVarcharType)) {
        pos = prev_pos;
        break;
      }
      if (!using_id_ && !using_name_) {
        if (cur_obj.get_type() == ObIntType) {
          using_id_ = true;
        } else {
          using_name_ = true;
        }
      }
      if (using_id_) {
        err = cur_obj.get_int(int_val);
        if (OB_SUCCESS == err) {
          err = add_return_column(int_val);
        } else {
          TBSYS_LOG(WARN, "fail to get int from obj [err:%d]", err);
        }
      } else {
        err = cur_obj.get_varchar(str_val);
        if (OB_SUCCESS == err) {
          err = add_return_column(str_val);
        } else {
          TBSYS_LOG(WARN, "fail to get varchar from obj [err:%d]", err);
        }
      }
    } else {
      TBSYS_LOG(WARN, "fail to deserialize return column [column_idx:%ld,err:%d]", return_columns_.get_array_index(), err);
    }
  }
  return err;
}

int sb::common::ObGroupByParam::deserialize_aggregate_columns(const char* buf, const int64_t buf_len, int64_t& pos) {
  int err = OB_SUCCESS;
  int64_t prev_pos = pos;
  ObObj cur_obj;
  int64_t func_type = 0;
  int64_t org_column_idx = -1;
  ObString org_column_name;
  ObString as_column_name;
  while (OB_SUCCESS == err && pos < buf_len) {
    prev_pos = pos;
    err = cur_obj.deserialize(buf, buf_len, pos);
    if (OB_SUCCESS == err) {
      if (cur_obj.get_ext() != 0
          || (cur_obj.get_type() != ObIntType)) {
        pos = prev_pos;
        break;
      }
      /// func type
      err = cur_obj.get_int(func_type);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "fail to get int value from obj [err:%d]", err);
      }
      /// as column name
      if (OB_SUCCESS == err) {
        err = cur_obj.deserialize(buf, buf_len, pos);
      }
      if (OB_SUCCESS == err) {
        err = cur_obj.get_varchar(as_column_name);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to get varchar from obj [err:%d]", err);
        }
      }
      /// org info
      if (OB_SUCCESS == err) {
        err = cur_obj.deserialize(buf, buf_len, pos);
      }
      if (OB_SUCCESS == err) {
        if (!using_id_ && !using_name_) {
          if (cur_obj.get_type() == ObIntType) {
            using_id_ = true;
          } else {
            using_name_ = true;
          }
        }
        if (using_id_) {
          err = cur_obj.get_int(org_column_idx);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to get int from obj [err:%d]", err);
          }
        } else {
          err = cur_obj.get_varchar(org_column_name);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to get varchar from obj [err:%d]", err);
          }
        }
      }
      /// add column
      if (OB_SUCCESS == err) {
        if (using_id_) {
          err = add_aggregate_column(org_column_idx, static_cast<ObAggregateFuncType>(func_type));
        } else {
          err = add_aggregate_column(org_column_name, as_column_name, static_cast<ObAggregateFuncType>(func_type));
        }
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to add aggregate column [idx:%ld,err:%d]", column_num_, err);
        }
      }
    } else {
      TBSYS_LOG(WARN, "fail to deserialize return column [column_idx:%ld,err:%d]", return_columns_.get_array_index(), err);
    }
  }
  return err;
}


int sb::common::ObGroupByParam::deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
  int err = OB_SUCCESS;
  int64_t prev_pos = pos;
  ObObj cur_obj;
  if (NULL == buf || 0 >= data_len) {
    TBSYS_LOG(WARN, "param error [buf:%p,data_len:%ld]", buf, data_len);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    clear();
    while (OB_SUCCESS == err && pos < data_len) {
      prev_pos = pos;
      err = cur_obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == err) {
        switch (cur_obj.get_ext()) {
        case ObActionFlag::GROUPBY_GRO_COLUMN_FIELD:
          err = deserialize_groupby_columns(buf, data_len, pos);
          break;
        case ObActionFlag::GROUPBY_AGG_COLUMN_FIELD:
          err = deserialize_aggregate_columns(buf, data_len, pos);
          break;
        case ObActionFlag::GROUPBY_RET_COLUMN_FIELD:
          err = deserialize_return_columns(buf, data_len, pos);
          break;
        default:
          pos = prev_pos;
          break;
        }
        if (pos == prev_pos) {
          break;
        }
      }
    }
  }
  if (OB_SUCCESS == err) {
    if (column_num_ > 0 && aggregate_columns_.get_array_index() <= 0) {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "there is no aggregate column, but has groupby or return column");
    }
  }
  return err;
}

int64_t sb::common::ObGroupByParam::get_serialize_size(void) const {
  int64_t pos;
  return serialize_helper(NULL, 0, pos);
}

int64_t sb::common::ObGroupByParam::groupby_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t& pos) const {
  int err = OB_SUCCESS;
  ObString str_val;
  ObObj    obj;
  int64_t need_size = 0;
  /// group by columns
  if (OB_SUCCESS == err && group_by_columns_.get_array_index() > 0) {
    obj.set_ext(ObActionFlag::GROUPBY_GRO_COLUMN_FIELD);
    need_size += obj.get_serialize_size();
    if (NULL != buf) {
      err = obj.serialize(buf, buf_len, pos);
    }
  }
  for (int64_t i = 0; OB_SUCCESS == err && i < group_by_columns_.get_array_index(); i++) {
    if (!using_id_ && !using_name_) {
      if (0 != group_by_columns_.at(i)->column_name_.length()
          && NULL != group_by_columns_.at(i)->column_name_.ptr()) {
        using_name_ = true;
      } else {
        using_id_ = true;
      }
    }
    if (using_name_) {
      if (0 == group_by_columns_.at(i)->column_name_.length()
          || NULL == group_by_columns_.at(i)->column_name_.ptr()) {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "argument error, using only name [group_by_column_idx:%ld]", i);
      } else {
        obj.set_varchar(group_by_columns_.at(i)->column_name_);
        need_size += obj.get_serialize_size();
        if (NULL != buf) {
          err = obj.serialize(buf, buf_len, pos);
        }
      }
    } else {
      if (0 > group_by_columns_.at(i)->org_column_idx_) {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "argument error, using only id [group_by_column_idx:%ld]", i);
      } else {
        obj.set_int(group_by_columns_.at(i)->org_column_idx_);
        need_size += obj.get_serialize_size();
        if (NULL != buf) {
          err = obj.serialize(buf, buf_len, pos);
        }
      }
    }
  }
  if (OB_SUCCESS == err) {
    err = need_size;
  }
  return err;
}

int64_t sb::common::ObGroupByParam::return_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t& pos) const {
  int err = OB_SUCCESS;
  ObString str_val;
  ObObj    obj;
  int64_t need_size = 0;
  /// return columns
  if (OB_SUCCESS == err && return_columns_.get_array_index() > 0) {
    obj.set_ext(ObActionFlag::GROUPBY_RET_COLUMN_FIELD);
    need_size += obj.get_serialize_size();
    if (NULL != buf) {
      err = obj.serialize(buf, buf_len, pos);
    }
  }
  for (int64_t i = 0; OB_SUCCESS == err && i < return_columns_.get_array_index(); i++) {
    if (!using_id_ && !using_name_) {
      if (0 != return_columns_.at(i)->column_name_.length()
          && NULL != return_columns_.at(i)->column_name_.ptr()) {
        using_name_ = true;
      } else {
        using_id_ = true;
      }
    }
    if (using_name_) {
      if (0 == return_columns_.at(i)->column_name_.length()
          || NULL == return_columns_.at(i)->column_name_.ptr()) {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "argument error, using only name [return_column_idx:%ld]", i);
      } else {
        obj.set_varchar(return_columns_.at(i)->column_name_);
        need_size += obj.get_serialize_size();
        if (NULL != buf) {
          err = obj.serialize(buf, buf_len, pos);
        }
      }
    } else {
      if (0 > return_columns_.at(i)->org_column_idx_) {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "argument error, using only id [return_column_idx:%ld]", i);
      } else {
        obj.set_int(return_columns_.at(i)->org_column_idx_);
        need_size += obj.get_serialize_size();
        if (NULL != buf) {
          err = obj.serialize(buf, buf_len, pos);
        }
      }
    }
  }
  if (OB_SUCCESS == err) {
    err = need_size;
  }
  return err;
}

int64_t sb::common::ObGroupByParam::aggregate_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t& pos) const {
  int err = OB_SUCCESS;
  ObString str_val;
  ObObj    obj;
  int64_t need_size = 0;
  ObString empty_str;
  /// aggregate columns
  if (OB_SUCCESS == err && aggregate_columns_.get_array_index() > 0) {
    obj.set_ext(ObActionFlag::GROUPBY_AGG_COLUMN_FIELD);
    need_size += obj.get_serialize_size();
    if (NULL != buf) {
      err = obj.serialize(buf, buf_len, pos);
    }
  }
  for (int64_t i = 0; OB_SUCCESS == err && i < aggregate_columns_.get_array_index(); i++) {
    /// func type
    obj.set_int(aggregate_columns_.at(i)->get_func_type());
    need_size += obj.get_serialize_size();
    if (NULL != buf) {
      err = obj.serialize(buf, buf_len, pos);
    }
    if (OB_SUCCESS == err) {
      if (!using_id_ && !using_name_) {
        if (0 != aggregate_columns_.at(i)->get_org_column_name().length()
            && NULL != aggregate_columns_.at(i)->get_org_column_name().ptr()) {
          using_name_ = true;
        } else {
          using_id_ = true;
        }
      }
      if (using_name_) {
        if (0 == aggregate_columns_.at(i)->get_org_column_name().length()
            || NULL == aggregate_columns_.at(i)->get_org_column_name().ptr()
            || 0 == aggregate_columns_.at(i)->get_as_column_name().length()
            || NULL == aggregate_columns_.at(i)->get_as_column_name().ptr()) {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "argument error, using only name [aggregate_column_idx:%ld]", i);
        } else {
          obj.set_varchar(aggregate_columns_.at(i)->get_as_column_name());
          need_size += obj.get_serialize_size();
          if (NULL != buf) {
            err = obj.serialize(buf, buf_len, pos);
          }
          if (OB_SUCCESS == err) {
            obj.set_varchar(aggregate_columns_.at(i)->get_org_column_name());
            need_size += obj.get_serialize_size();
            if (NULL != buf) {
              err = obj.serialize(buf, buf_len, pos);
            }
          }
        }
      } else {
        if (0 > aggregate_columns_.at(i)->get_org_column_idx()) {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "argument error, using only id [aggregate_column_idx:%ld]", i);
        } else {
          obj.set_varchar(empty_str); /// when using index, just serialize an empty ObString
          need_size += obj.get_serialize_size();
          if (NULL != buf) {
            err = obj.serialize(buf, buf_len, pos);
          }
          if (OB_SUCCESS == err) {
            obj.set_int(aggregate_columns_.at(i)->get_org_column_idx());
            need_size += obj.get_serialize_size();
            if (NULL != buf) {
              err = obj.serialize(buf, buf_len, pos);
            }
          }
        }
      }
    }
  }
  if (OB_SUCCESS == err) {
    err = need_size;
  }
  return err;
}

int64_t sb::common::ObGroupByParam::serialize_helper(char* buf, const int64_t buf_len, int64_t& pos) const {
  int err = OB_SUCCESS;
  ObString str_val;
  ObObj    obj;
  int64_t need_size = 0;
  if (column_num_ > 0 && aggregate_columns_.get_array_index() == 0) {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "there is no aggregate column, but has groupby or return column");
  }
  if (OB_SUCCESS == err) {
    err = groupby_columns_serialize_helper(buf, buf_len, pos);
    if (0 <= err) {
      need_size += err;
      err = OB_SUCCESS;
    }
    if (OB_SUCCESS == err) {
      err = return_columns_serialize_helper(buf, buf_len, pos);
      if (0 <= err) {
        need_size += err;
        err = OB_SUCCESS;
      }
    }
    if (OB_SUCCESS == err) {
      err = aggregate_columns_serialize_helper(buf, buf_len, pos);
      if (0 <= err) {
        need_size += err;
        err = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCCESS == err) {
    err = need_size;
  }
  return err;
}


bool sb::common::ObGroupByParam::operator == (const ObGroupByParam& other)const {
  bool result = true;
  if (column_num_ != other.column_num_) {
    result = false;
  }
  if (result && group_by_columns_.get_array_index() != other.group_by_columns_.get_array_index()) {
    result = false;
  }
  if (result && return_columns_.get_array_index() != other.return_columns_.get_array_index()) {
    result = false;
  }
  if (result && aggregate_columns_.get_array_index() != other.aggregate_columns_.get_array_index()) {
    result = false;
  }
  for (uint32_t i = 0; result && i < group_by_columns_.get_array_index(); i++) {
    if (!((*group_by_columns_.at(i)) == *(other.group_by_columns_.at(i)))) {
      result = false;
    }
  }
  for (uint32_t i = 0; result && i < return_columns_.get_array_index(); i++) {
    if (!((*return_columns_.at(i)) == (*other.return_columns_.at(i)))) {
      result = false;
    }
  }
  for (uint32_t i = 0; result && i < aggregate_columns_.get_array_index(); i++) {
    if (!((*aggregate_columns_.at(i)) == (*other.aggregate_columns_.at(i)))) {
      result = false;
    }
  }
  return result;
}


int64_t sb::common::ObGroupByParam::find_column(const sb::common::ObString& column_name)const {
  bool find_column = false;
  int64_t idx = 0;
  for (uint32_t i = 0; !find_column && i < group_by_columns_.get_array_index(); i ++, idx ++) {
    if (group_by_columns_.at(i)->column_name_ == column_name) {
      find_column = true;
      break;
    }
  }
  for (uint32_t i = 0; !find_column && i < return_columns_.get_array_index(); i ++, idx ++) {
    if (return_columns_.at(i)->column_name_ == column_name) {
      find_column = true;
      break;
    }
  }

  for (uint32_t i = 0; !find_column && i < aggregate_columns_.get_array_index(); i ++, idx ++) {
    if (aggregate_columns_.at(i)->get_as_column_name() == column_name) {
      find_column = true;
      break;
    }
  }
  if (!find_column) {
    idx = -1;
  }
  return idx;
}


int  sb::common::ObGroupByParam::get_aggregate_column_name(const int64_t column_idx, ObString& column_name)const {
  int err = OB_SUCCESS;
  if (column_idx < 0 || column_idx >= column_num_) {
    TBSYS_LOG(WARN, "agument error [column_idx:%ld,column_num_:%ld]", column_idx, column_num_);
    err  = OB_ARRAY_OUT_OF_RANGE;
  }
  if (OB_SUCCESS == err) {
    if (column_idx < group_by_columns_.get_array_index()) {
      column_name = group_by_columns_.at(column_idx)->column_name_;
    } else if (column_idx < group_by_columns_.get_array_index() + return_columns_.get_array_index()) {
      column_name = return_columns_.at(column_idx - group_by_columns_.get_array_index())->column_name_;
    } else {
      column_name = aggregate_columns_.at(column_idx
                                          - group_by_columns_.get_array_index()
                                          - return_columns_.get_array_index())->get_as_column_name();
    }
  }
  return err;
}

