/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scan_param.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "ob_action_flag.h"
#include "ob_malloc.h"
#include "ob_scan_param.h"

namespace sb {
namespace common {
void ObScanParam::reset(void) {
  table_id_ = OB_INVALID_ID;
  scan_flag_.scan_direction_ = FORWARD;
  scan_flag_.read_mode_ = PREREAD;
  scan_size_ = 0;
  ObReadParam::reset();
  table_name_.assign(NULL, 0);
  range_.table_id_ = OB_INVALID_ID;
  range_.start_key_.assign(NULL, 0);
  range_.end_key_.assign(NULL, 0);
  clear_column();
  orderby_column_name_list_.clear();
  orderby_column_id_list_.clear();
  orderby_order_list_.clear();
  condition_filter_.reset();
  group_by_param_.clear();
  limit_offset_ = 0;
  limit_count_ = 0;
}

// ObScanParam
ObScanParam::ObScanParam() : table_id_(OB_INVALID_ID),
  table_name_(), range_(), scan_size_(0), scan_flag_() {
  limit_offset_ = 0;
  limit_count_ = 0;
  column_list_.init(OB_MAX_COLUMN_NUMBER, column_names_);
  column_id_list_.init(OB_MAX_COLUMN_NUMBER, column_ids_);
  orderby_column_name_list_.init(OB_MAX_COLUMN_NUMBER, orderby_column_names_);
  orderby_column_id_list_.init(OB_MAX_COLUMN_NUMBER, orderby_column_ids_);
  orderby_order_list_.init(OB_MAX_COLUMN_NUMBER, orderby_orders_);
}

ObScanParam::~ObScanParam() {
}

void ObScanParam::safe_copy(const ObScanParam& other) {
  memcpy(this, &other, sizeof(ObScanParam));
  column_list_.init(OB_MAX_COLUMN_NUMBER, column_names_, other.column_list_.get_array_index());
  column_id_list_.init(OB_MAX_COLUMN_NUMBER, column_ids_, other.column_id_list_.get_array_index());
}

void ObScanParam::set(const uint64_t& table_id, const ObString& table_name, const ObRange& range) {
  table_id_ = table_id;
  table_name_ = table_name;
  range_ = range;
  range_.table_id_ = table_id;
}

int ObScanParam::add_orderby_column(const ObString& column_name, Order order) {
  int err = OB_SUCCESS;
  if (orderby_column_id_list_.get_array_index() > 0) {
    TBSYS_LOG(WARN, "use only column id or column name, not both");
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    if (!orderby_column_name_list_.push_back(column_name)) {
      err = OB_ARRAY_OUT_OF_RANGE;
    } else {
      orderby_order_list_.push_back(order);
    }
  }
  return err;
}

int ObScanParam::add_orderby_column(const int64_t column_idx, Order order) {
  int err = OB_SUCCESS;
  if (orderby_column_name_list_.get_array_index() > 0) {
    TBSYS_LOG(WARN, "use only column id or column name, not both");
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    if (!orderby_column_id_list_.push_back(column_idx)) {
      err = OB_ARRAY_OUT_OF_RANGE;
    } else {
      orderby_order_list_.push_back(order);
    }
  }
  return err;
}

int64_t ObScanParam::get_orderby_column_size()const {
  return orderby_order_list_.get_array_index();
}


void ObScanParam::get_orderby_column(ObString const*& names, uint8_t const*& orders,
                                     int64_t& column_size)const {
  column_size = get_orderby_column_size();
  orders = orderby_orders_;
  if (0 == orderby_column_name_list_.get_array_index()) {
    names = NULL;
  } else {
    names = orderby_column_names_;
  }
}

void ObScanParam::get_orderby_column(int64_t const*& column_idxs, uint8_t const*& orders,
                                     int64_t& column_size)const {
  column_size = get_orderby_column_size();
  orders = orderby_orders_;
  if (0 == orderby_column_id_list_.get_array_index()) {
    column_idxs = NULL;
  } else {
    column_idxs = orderby_column_ids_;
  }
}

int ObScanParam::set_limit_info(const int64_t offset, int64_t count) {
  int err = OB_SUCCESS;
  if (offset < 0 || count < 0) {
    TBSYS_LOG(WARN, "param error [offset:%ld,count:%ld]", offset, count);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    limit_offset_ = offset;
    limit_count_ = count;
  }
  return err;
}

void ObScanParam::get_limit_info(int64_t& offset, int64_t& count) const {
  offset = limit_offset_;
  count = limit_count_;
}

int ObScanParam::add_column(const ObString& column_name) {
  int rc = OB_SUCCESS;
  if (column_id_list_.get_array_index() > 0) {
    rc = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "using column id or column name, not both");
  }

  if (OB_SUCCESS == rc && !column_list_.push_back(column_name)) {
    rc = OB_ARRAY_OUT_OF_RANGE;
  }

  return rc;
}


int ObScanParam::add_column(const uint64_t& column_id) {
  int rc = OB_SUCCESS;
  if (column_list_.get_array_index() > 0) {
    rc = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "using column id or column name, not both");
  }

  if (OB_SUCCESS == rc && !column_id_list_.push_back(column_id)) {
    rc = OB_ARRAY_OUT_OF_RANGE;
  }

  return rc;
}

void ObScanParam::clear_column(void) {
  column_id_list_.clear();
  column_list_.clear();
}

int ObScanParam::add_cond(const ObString& column_name, const ObLogicOperator& cond_op,
                          const ObObj& cond_value) {
  int ret = OB_SUCCESS;
  const ObString* columns = get_column_name();
  if (NULL == columns) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "check scan column failed:columns[%p]", columns);
  } else {
    bool find = false;
    int64_t size = get_column_name_size();
    for (int64_t i = 0; i < size; ++i) {
      if (column_name == columns[i]) {
        find = true;
        break;
      }
    }

    if (true == find) {
      UNUSED(column_name);
      UNUSED(cond_op);
      UNUSED(cond_value);
      ret = condition_filter_.add_cond(column_name, cond_op, cond_value);
    } else {
      TBSYS_LOG(ERROR, "not find condition column in scan columns:column[%.*s]",
                column_name.length(), column_name.ptr());
      ret = OB_ERROR;
    }
  }
  return ret;
}

const ObSimpleFilter& ObScanParam::get_filter_info(void)const {
  return condition_filter_;
}

ObSimpleFilter& ObScanParam::get_filter_info(void) {
  return condition_filter_;
}

int ObScanParam::add_groupby_column(const ObString& column_name) {
  return group_by_param_.add_groupby_column(column_name);
}

int ObScanParam::add_groupby_column(const int64_t column_idx) {
  return group_by_param_.add_groupby_column(column_idx);
}

int ObScanParam::add_return_column(const ObString& column_name) {
  return group_by_param_.add_return_column(column_name);
}

int ObScanParam::add_return_column(const int64_t column_idx) {
  return group_by_param_.add_return_column(column_idx);
}

int ObScanParam::add_aggregate_column(const ObString& org_column_name,
                                      const ObString& as_column_name, const ObAggregateFuncType  aggregate_func) {
  return group_by_param_.add_aggregate_column(org_column_name, as_column_name, aggregate_func);
}

int ObScanParam::add_aggregate_column(const int64_t org_column_idx, const ObAggregateFuncType  aggregate_func) {
  return group_by_param_.add_aggregate_column(org_column_idx,  aggregate_func);
}


const ObGroupByParam& ObScanParam::get_group_by_param()const {
  return group_by_param_;
}

ObGroupByParam& ObScanParam::get_group_by_param() {
  return group_by_param_;
}

// BASIC_PARAM_FIELD
int ObScanParam::serialize_basic_param(char* buf, const int64_t buf_len, int64_t& pos) const {
  ObObj obj;
  obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
  int ret = obj.serialize(buf, buf_len, pos);
  // read param info
  if (OB_SUCCESS == ret) {
    ret = ObReadParam::serialize(buf, buf_len, pos);
  }

  // table name or id
  if (OB_SUCCESS == ret) {
    if (table_name_.length() > 0) {
      if (OB_INVALID_ID == table_id_) {
        obj.set_varchar(table_name_);
      } else {
        ret = OB_ERROR;
      }
    } else {
      if (OB_INVALID_ID != table_id_) {
        obj.set_int(table_id_);
      } else {
        ret = OB_ERROR;
      }
    }
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "Both table_name and table_id are set");
    } else {
      ret = obj.serialize(buf, buf_len, pos);
    }
  }

  // scan range
  if (OB_SUCCESS == ret) {
    obj.set_int(range_.border_flag_.get_data());
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS == ret) {
      obj.set_varchar(range_.start_key_);
      ret = obj.serialize(buf, buf_len, pos);
    }
    if (OB_SUCCESS == ret) {
      obj.set_varchar(range_.end_key_);
      ret = obj.serialize(buf, buf_len, pos);
    }
  }

  // scan direction
  if (OB_SUCCESS == ret) {
    const int64_t* int64_p = reinterpret_cast<const int64_t*>(&scan_flag_);
    obj.set_int(*int64_p);
    ret = obj.serialize(buf, buf_len, pos);
    // scan size
    if (OB_SUCCESS == ret) {
      obj.set_int(scan_size_);
      ret = obj.serialize(buf, buf_len, pos);
    }
  }
  return ret;
}

int ObScanParam::deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos) {
  int64_t int_value = 0;
  int ret = ObReadParam::deserialize(buf, data_len, pos);
  // table name or table id
  ObObj obj;
  ObString str_value;
  if (OB_SUCCESS == ret) {
    ret = obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS == ret) {
      if (ObIntType == obj.get_type()) {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret) {
          table_id_ = int_value;
          range_.table_id_ = int_value;
        }
      } else {
        ret = obj.get_varchar(str_value);
        if (OB_SUCCESS == ret) {
          table_name_ = str_value;
          range_.table_id_ = 0;
        }
      }
    }
  }

  // scan range
  if (OB_SUCCESS == ret) {
    // border flag
    if (OB_SUCCESS == ret) {
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret) {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret) {
          range_.border_flag_.set_data(int_value);
        }
      }
    }

    // start key
    if (OB_SUCCESS == ret) {
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret) {
        ret = obj.get_varchar(str_value);
        if (OB_SUCCESS == ret) {
          range_.start_key_ = str_value;
        }
      }
    }

    // end key
    if (OB_SUCCESS == ret) {
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret) {
        ret = obj.get_varchar(str_value);
        if (OB_SUCCESS == ret) {
          range_.end_key_ = str_value;
        }
      }
    }
  }
  // scan direction
  if (OB_SUCCESS == ret) {
    ret = obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS == ret) {
      int64_t* flag_value = reinterpret_cast<int64_t*>(&scan_flag_);
      ret = obj.get_int(*flag_value);
      /*
      ret = obj.get_int(int_value);
      if (OB_SUCCESS == ret)
      {
        scan_direction_ = (Direction)int_value;
      }
      */
    }
  }
  // scan size
  if (OB_SUCCESS == ret) {
    ret = obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS == ret) {
      ret = obj.get_int(int_value);
      if (OB_SUCCESS == ret) {
        scan_size_ = int_value;
      }
    }
  }
  return ret;
}

int64_t ObScanParam::get_basic_param_serialize_size(void) const {
  int64_t total_size = 0;
  ObObj obj;
  // BASIC_PARAM_FIELD
  obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
  total_size += obj.get_serialize_size();

  /// READ PARAM
  total_size += ObReadParam::get_serialize_size();

  // table name id
  if (table_name_.length() > 0) {
    obj.set_varchar(table_name_);
  } else {
    obj.set_int(table_id_);
  }
  total_size += obj.get_serialize_size();

  // scan range
  obj.set_int(range_.border_flag_.get_data());
  total_size += obj.get_serialize_size();
  obj.set_varchar(range_.start_key_);
  total_size += obj.get_serialize_size();
  obj.set_varchar(range_.end_key_);
  total_size += obj.get_serialize_size();

  // scan sequence
  const int64_t* int64_p = reinterpret_cast<const int64_t*>(&scan_flag_);
  obj.set_int(*int64_p);
  total_size += obj.get_serialize_size();
  // scan size
  obj.set_int(scan_size_);
  total_size += obj.get_serialize_size();
  return total_size;
}

// COLUMN_PARAM_FIELD
int ObScanParam::serialize_column_param(char* buf, const int64_t buf_len, int64_t& pos) const {
  ObObj obj;
  obj.set_ext(ObActionFlag::COLUMN_PARAM_FIELD);
  int ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS == ret) {
    int64_t column_size = column_list_.get_array_index();
    int64_t column_id_size = column_id_list_.get_array_index();
    if ((column_size > 0) && (column_id_size > 0)) {
      TBSYS_LOG(ERROR, "check column size failed");
      ret = OB_ERROR;
    }

    if (OB_SUCCESS == ret) {
      for (int64_t i = 0; i < column_size; ++i) {
        obj.set_varchar(column_names_[i]);
        ret = obj.serialize(buf, buf_len, pos);
        if (ret != OB_SUCCESS) {
          break;
        }
      }

      for (int64_t i = 0; i < column_id_size; ++i) {
        obj.set_int(column_ids_[i]);
        ret = obj.serialize(buf, buf_len, pos);
        if (ret != OB_SUCCESS) {
          break;
        }
      }
    }
  }
  return ret;
}

int ObScanParam::deserialize_column_param(const char* buf, const int64_t data_len, int64_t& pos) {
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  ObString str_value;
  ObObj obj;
  int64_t old_pos = pos;
  while ((OB_SUCCESS == (ret = obj.deserialize(buf, data_len, pos)))
         && (ObExtendType != obj.get_type())) {
    old_pos = pos;
    if (ObIntType == obj.get_type()) {
      ret = obj.get_int(int_value);
      if (OB_SUCCESS == ret) {
        if (!column_id_list_.push_back(int_value)) {
          ret = OB_ERROR;
        }
      }
    } else {
      ret = obj.get_varchar(str_value);
      if (OB_SUCCESS == ret) {
        if (!column_list_.push_back(str_value)) {
          ret = OB_ERROR;
        }
      }
    }
    if (OB_SUCCESS != ret) {
      break;
    }
  }
  // modify last pos for the last ext_type obj
  pos = old_pos;
  return ret;
}

int64_t ObScanParam::get_column_param_serialize_size(void) const {
  ObObj obj;
  int64_t total_size = 0;
  // COLUMN_PARAM_FIELD
  obj.set_ext(ObActionFlag::COLUMN_PARAM_FIELD);
  total_size += obj.get_serialize_size();
  int64_t column_size = column_list_.get_array_index();
  int64_t column_id_size = column_id_list_.get_array_index();
  // scan name
  for (int64_t i = 0; i < column_size; ++i) {
    obj.set_varchar(column_names_[i]);
    total_size += obj.get_serialize_size();
  }
  // scan id
  for (int64_t i = 0; i < column_id_size; ++i) {
    obj.set_int(column_ids_[i]);
    total_size += obj.get_serialize_size();
  }
  return total_size;
}

// SORT_PARAM_FIELD
int ObScanParam::serialize_sort_param(char* buf, const int64_t buf_len, int64_t& pos) const {
  ObObj obj;
  obj.set_ext(ObActionFlag::SORT_PARAM_FIELD);
  int ret = obj.serialize(buf, buf_len, pos);
  int64_t orderby_column_size = orderby_column_name_list_.get_array_index();
  int64_t orderby_column_id_size = orderby_column_id_list_.get_array_index();
  if (OB_SUCCESS == ret) {
    if ((orderby_column_size > 0) && (orderby_column_id_size > 0)) {
      TBSYS_LOG(ERROR, "check column size failed");
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret) {
    for (int64_t i = 0; i < orderby_column_size; i++) {
      obj.set_varchar(orderby_column_names_[i]);
      ret = obj.serialize(buf, buf_len, pos);
      if (ret != OB_SUCCESS) {
        break;
      }
      obj.set_int(orderby_orders_[i]);
      ret = obj.serialize(buf, buf_len, pos);
      if (ret != OB_SUCCESS) {
        break;
      }
    }

    for (int64_t i = 0; i < orderby_column_id_size; i++) {
      obj.set_int(orderby_column_ids_[i]);
      ret = obj.serialize(buf, buf_len, pos);
      if (ret != OB_SUCCESS) {
        break;
      }
      obj.set_int(orderby_orders_[i]);
      ret = obj.serialize(buf, buf_len, pos);
      if (ret != OB_SUCCESS) {
        break;
      }
    }
  }
  return ret;
}

int ObScanParam::deserialize_sort_param(const char* buf, const int64_t data_len, int64_t& pos) {
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  ObString str_value;
  int64_t old_pos = pos;
  ObObj obj;
  while ((OB_SUCCESS == (ret = obj.deserialize(buf, data_len, pos)))
         && (ObExtendType != obj.get_type())) {
    old_pos = pos;
    // column
    if (ObIntType == obj.get_type()) {
      ret = obj.get_int(int_value);
      if (OB_SUCCESS == ret) {
        if (!orderby_column_id_list_.push_back(int_value)) {
          ret = OB_ERROR;
          break;
        }
      }
    } else {
      ret = obj.get_varchar(str_value);
      if (!orderby_column_name_list_.push_back(str_value)) {
        ret = OB_ERROR;
        break;
      }
    }

    // orderby
    if (OB_SUCCESS == ret) {
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret) {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret) {
          if (!orderby_order_list_.push_back(int_value)) {
            ret = OB_ERROR;
            break;
          }
        }
      }
    }
    if (OB_SUCCESS != ret) {
      break;
    }
  }
  // modify last pos for the last ext_type obj
  pos = old_pos;
  return ret;
}

int64_t ObScanParam::get_sort_param_serialize_size(void) const {
  int64_t total_size = 0;
  ObObj obj;
  /// SORT PARAM FIELD
  obj.set_ext(ObActionFlag::SORT_PARAM_FIELD);
  total_size += obj.get_serialize_size();

  // not check again
  int64_t orderby_order_size = orderby_order_list_.get_array_index();
  int64_t orderby_column_size = orderby_column_name_list_.get_array_index();
  int64_t orderby_column_id_size = orderby_column_id_list_.get_array_index();
  // name
  for (int64_t i = 0; i < orderby_column_size; i++) {
    obj.set_varchar(orderby_column_names_[i]);
    total_size += obj.get_serialize_size();
  }
  // id
  for (int64_t i = 0; i < orderby_column_id_size; i++) {
    obj.set_int(orderby_column_ids_[i]);
    total_size += obj.get_serialize_size();
  }

  // orderby
  for (int64_t i = 0; i < orderby_order_size; ++i) {
    obj.set_int(orderby_orders_[i]);
    total_size += obj.get_serialize_size();
  }
  return total_size;
}

int ObScanParam::serialize_limit_param(char* buf, const int64_t buf_len, int64_t& pos) const {
  ObObj obj;
  obj.set_ext(ObActionFlag::LIMIT_PARAM_FIELD);
  int ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS == ret) {
    obj.set_int(limit_offset_);
    ret = obj.serialize(buf, buf_len, pos);
  }

  if (OB_SUCCESS == ret) {
    obj.set_int(limit_count_);
    ret = obj.serialize(buf, buf_len, pos);
  }
  return ret;
}

int ObScanParam::deserialize_limit_param(const char* buf, const int64_t data_len, int64_t& pos) {
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  ObString str_value;
  ObObj obj;
  // offset
  ret = obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS == ret) {
    ret = obj.get_int(int_value);
    if (OB_SUCCESS == ret) {
      limit_offset_ = int_value;
    }
  }
  // count
  if (OB_SUCCESS == ret) {
    ret = obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS == ret) {
      ret = obj.get_int(int_value);
      if (OB_SUCCESS == ret) {
        limit_count_ = int_value;
      }
    }
  }
  return ret;
}

int64_t ObScanParam::get_limit_param_serialize_size(void) const {
  int64_t total_size = 0;
  ObObj obj;
  // LIMIT_PARAM_FIELD
  obj.set_ext(ObActionFlag::LIMIT_PARAM_FIELD);
  total_size += obj.get_serialize_size();
  // offset
  obj.set_int(limit_offset_);
  total_size += obj.get_serialize_size();
  // count
  obj.set_int(limit_count_);
  total_size += obj.get_serialize_size();
  return total_size;
}

int ObScanParam::serialize_groupby_param(char* buf, const int64_t buf_len, int64_t& pos) const {
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_ext(ObActionFlag::GROUPBY_PARAM_FIELD);
  ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS == ret && group_by_param_.get_aggregate_row_width() > 0) {
    ret = group_by_param_.serialize(buf, buf_len, pos);
  }
  return ret;
}

int ObScanParam::deserialize_groupby_param(const char* buf, const int64_t data_len, int64_t& pos) {
  return group_by_param_.deserialize(buf, data_len, pos);
}

int64_t ObScanParam::get_groupby_param_serialize_size(void) const {
  ObObj obj;
  obj.set_ext(ObActionFlag::GROUPBY_PARAM_FIELD);
  int64_t total_size = obj.get_serialize_size();
  if (group_by_param_.get_aggregate_row_width() > 0) {
    total_size += group_by_param_.get_serialize_size();
  }
  return total_size;
}

int ObScanParam::serialize_filter_param(char* buf, const int64_t buf_len, int64_t& pos) const {
  ObObj obj;
  obj.set_ext(ObActionFlag::FILTER_PARAM_FIELD);
  int ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS == ret) {
    ret = condition_filter_.serialize(buf, buf_len, pos);
  }
  return ret;
}

int ObScanParam::deserialize_filter_param(const char* buf, const int64_t data_len, int64_t& pos) {
  return condition_filter_.deserialize(buf, data_len, pos);
}

int64_t ObScanParam::get_filter_param_serialize_size(void) const {
  ObObj obj;
  obj.set_ext(ObActionFlag::FILTER_PARAM_FIELD);
  int64_t total_size = obj.get_serialize_size();
  total_size += condition_filter_.get_serialize_size();
  return total_size;
}

int ObScanParam::serialize_end_param(char* buf, const int64_t buf_len, int64_t& pos) const {
  ObObj obj;
  obj.set_ext(ObActionFlag::END_PARAM_FIELD);
  return obj.serialize(buf, buf_len, pos);
}

int ObScanParam::deserialize_end_param(const char* buf, const int64_t data_len, int64_t& pos) {
  // no data
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  return 0;
}

int64_t ObScanParam::get_end_param_serialize_size(void) const {
  ObObj obj;
  obj.set_ext(ObActionFlag::END_PARAM_FIELD);
  return obj.get_serialize_size();
}

DEFINE_SERIALIZE(ObScanParam) {
  int ret = OB_SUCCESS;
  if (orderby_column_name_list_.get_array_index() > 0
      && orderby_column_id_list_.get_array_index() > 0) {
    TBSYS_LOG(WARN, "use column id or column name, but not both");
    ret = OB_INVALID_ARGUMENT;
  }

  if (column_list_.get_array_index() > 0
      && column_id_list_.get_array_index() > 0) {
    TBSYS_LOG(WARN, "use column id or column name, but not both");
    ret = OB_INVALID_ARGUMENT;
  }

  if ((orderby_column_name_list_.get_array_index() > 0
       && orderby_column_name_list_.get_array_index() != orderby_order_list_.get_array_index())
      || (orderby_column_id_list_.get_array_index() > 0
          && orderby_column_id_list_.get_array_index() != orderby_order_list_.get_array_index())
     ) {
    TBSYS_LOG(WARN, "sizeof order list not coincident with column id/name list");
    ret = OB_ERR_UNEXPECTED;
  }

  // RESERVE_PARAM_FIELD
  if (OB_SUCCESS == ret) {
    ret = ObReadParam::serialize_reserve_param(buf, buf_len, pos);
  }

  // BASIC_PARAM_FIELD
  if (OB_SUCCESS == ret) {
    ret = serialize_basic_param(buf, buf_len, pos);
  }

  // COLUMN_PARAM_FIELD
  if (OB_SUCCESS == ret) {
    ret = serialize_column_param(buf, buf_len, pos);
  }

  // FILTER_PARAM_FILED
  if (OB_SUCCESS == ret) {
    ret = serialize_filter_param(buf, buf_len, pos);
  }

  // GROUPBY_PARAM_FIELD
  if (OB_SUCCESS == ret) {
    ret = serialize_groupby_param(buf, buf_len, pos);
  }

  // SORT_PARAM_FIELD
  if (OB_SUCCESS == ret) {
    ret = serialize_sort_param(buf, buf_len, pos);
  }

  // LIMIT_PARAM_FIELD
  if (OB_SUCCESS == ret) {
    ret = serialize_limit_param(buf, buf_len, pos);
  }

  // END_PARAM_FIELD
  if (OB_SUCCESS == ret) {
    ret = serialize_end_param(buf, buf_len, pos);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObScanParam) {
  // reset contents
  reset();
  ObObj obj;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == ret) {
    do {
      ret = obj.deserialize(buf, data_len, pos);
    } while (OB_SUCCESS == ret && ObExtendType != obj.get_type());

    if (OB_SUCCESS == ret && ObActionFlag::END_PARAM_FIELD != obj.get_ext()) {
      switch (obj.get_ext()) {
      case ObActionFlag::RESERVE_PARAM_FIELD: {
        ret = ObReadParam::deserialize_reserve_param(buf, data_len, pos);
        break;
      }
      case ObActionFlag::BASIC_PARAM_FIELD: {
        ret = deserialize_basic_param(buf, data_len, pos);
        break;
      }
      case ObActionFlag::COLUMN_PARAM_FIELD: {
        ret = deserialize_column_param(buf, data_len, pos);
        break;
      }
      case ObActionFlag::SORT_PARAM_FIELD: {
        ret = deserialize_sort_param(buf, data_len, pos);
        break;
      }
      case ObActionFlag::LIMIT_PARAM_FIELD: {
        ret = deserialize_limit_param(buf, data_len, pos);
        break;
      }
      case ObActionFlag::FILTER_PARAM_FIELD: {
        ret = deserialize_filter_param(buf, data_len, pos);
        break;
      }
      case ObActionFlag::GROUPBY_PARAM_FIELD: {
        ret = deserialize_groupby_param(buf, data_len, pos);
        break;
      }
      default: {
        // deserialize next cell
        ret = obj.deserialize(buf, data_len, pos);
        break;
      }
      }
    } else {
      break;
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObScanParam) {
  int64_t total_size = get_basic_param_serialize_size();
  total_size += ObReadParam::get_reserve_param_serialize_size();
  total_size += get_column_param_serialize_size();
  total_size += get_filter_param_serialize_size();
  total_size += get_groupby_param_serialize_size();
  total_size += get_sort_param_serialize_size();
  total_size += get_limit_param_serialize_size();
  total_size += get_end_param_serialize_size();
  return total_size;
}
} /* common */
} /* sb */

