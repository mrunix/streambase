/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_simple_filter.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "ob_define.h"
#include "ob_cell_array.h"
#include "ob_common_param.h"
#include "ob_simple_filter.h"

using namespace sb::common;

ObSimpleFilter::ObSimpleFilter() {
}

ObSimpleFilter::ObSimpleFilter(const ObSimpleFilter& other) {
  *this = other;
}

ObSimpleFilter::~ObSimpleFilter() {
}

// deep copy column name and obj string
int ObSimpleFilter::add_cond(const ObString& column_name, const ObLogicOperator& cond_op,
                             const ObObj& cond_value) {
  ObString store_name;
  int ret = string_buffer_.write_string(column_name, &store_name);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "store column name failed:src[%.*s], ret[%d]",
              column_name.length(), column_name.ptr(), ret);
  }

  ObObj store_value;
  if (OB_SUCCESS == ret) {
    ret = copy_obj(cond_value, store_value);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "copy obj failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret) {
    ObSimpleCond cond(store_name, cond_op, store_value);
    ret = add_cond(cond);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "add the condition item failed:ret[%d]", ret);
    }
  }
  return ret;
}

// deep copy obj string type
int ObSimpleFilter::copy_obj(const ObObj& cond_value, ObObj& store_value) {
  int ret = OB_SUCCESS;
  store_value = cond_value;
  if (cond_value.get_type() == ObVarcharType) {
    ObString src_string;
    ret = cond_value.get_varchar(src_string);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "get obj string failed:ret[%d]", ret);
    } else {
      ObString store_ptr;
      ret = string_buffer_.write_string(src_string, &store_ptr);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "store obj string failed:src[%.*s], ret[%d]",
                  src_string.length(), src_string.ptr(), ret);
      } else {
        store_value.set_varchar(store_ptr);
      }
    }
  }
  return ret;
}

// only deep copy obj string
int ObSimpleFilter::add_cond(const uint64_t column_index, const ObLogicOperator& cond_op,
                             const ObObj& cond_value) {
  ObObj store_obj;
  int ret = copy_obj(cond_value, store_obj);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "copy obj failed:ret[%d]", ret);
  } else {
    ObSimpleCond cond;
    ret = cond.set(column_index, cond_op, store_obj);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "set condtion content failed:ret[%d]", ret);
    } else {
      ret = add_cond(cond);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "add the condition item failed:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObSimpleFilter::add_cond(const ObSimpleCond& cond) {
  int ret = conditions_.push_back(cond);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "push back the condition item failed:ret[%d]", ret);
  }
  return ret;
}

const ObSimpleCond* ObSimpleFilter::operator [](const int64_t index) const {
  ObSimpleCond* ret = NULL;
  if ((index < 0) || (index >= conditions_.size())) {
    TBSYS_LOG(ERROR, "check index failed:index[%ld], count[%ld]",
              index, (int64_t)conditions_.size());
  } else {
    ret = &conditions_[index];
  }
  return ret;
}

int ObSimpleFilter::operator = (const ObSimpleFilter& other) {
  this->reset();
  int ret = OB_SUCCESS;
  ObSimpleCond cond;
  int64_t size = other.conditions_.size();
  for (int64_t i = 0; i < size; ++i) {
    cond = other.conditions_[i];
    if (cond.get_column_index() != ObSimpleCond::INVALID_INDEX) {
      ret = add_cond(cond.get_column_name(), cond.get_logic_operator(),
                     cond.get_right_operand());
    } else {
      ret = add_cond(cond.get_column_index(), cond.get_logic_operator(),
                     cond.get_right_operand());
    }
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "add condition failed:pos[%ld], ret[%d]", i, ret);
      break;
    }
  }
  return ret;
}

bool ObSimpleFilter::operator == (const ObSimpleFilter& other) const {
  int64_t size = conditions_.size();
  bool ret = (size == other.conditions_.size());
  if (true == ret) {
    for (int64_t i = 0; i < size; ++i) {
      ret = (conditions_[i] == other.conditions_[i]);
      if (false == ret) {
        TBSYS_LOG(DEBUG, "check the %ldth condition failed", i);
        break;
      }
    }
  }
  return ret;
}

int ObSimpleFilter::check(const ObCellArray& cells, const int64_t row_begin,
                          const int64_t row_end, bool& result) const {
  int ret = OB_SUCCESS;
  result = true;
  ObSimpleCond item;
  int64_t index = 0;
  int64_t size = conditions_.size();
  for (int64_t i = 0; i < size; ++i) {
    item = conditions_[i];
    index = item.get_column_index() + row_begin;
    if (index <= row_end) {
      result = item.calc(const_cast<ObCellArray&>(cells)[index].value_);
      // only support &&
      if (false == result) {
        break;
      }
    } else {
      TBSYS_LOG(ERROR, "check cell index failed:index[%ld], begin[%ld], end[%ld]",
                item.get_column_index(), row_begin, row_end);
      ret = OB_ERROR;
      break;
    }
  }
  return ret;
}

void ObSimpleFilter::reset(void) {
  conditions_.clear();
  string_buffer_.reset();
}

DEFINE_SERIALIZE(ObSimpleFilter) {
  int ret = OB_SUCCESS;
  int64_t size = conditions_.size();
  for (int64_t i = 0; i < size; ++i) {
    ret = conditions_[i].serialize(buf, buf_len, pos);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "the %ldth condition serialize failed:ret[%d]", i, ret);
      break;
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSimpleFilter) {
  reset();
  ObObj temp;
  ObSimpleCond cond;
  int ret = OB_SUCCESS;
  int64_t temp_pos = pos;
  while ((temp_pos < data_len)
         && (OB_SUCCESS == (ret = temp.deserialize(buf, data_len, temp_pos)))
         && (ObExtendType != temp.get_type())) {
    ret = cond.deserialize(buf, data_len, pos);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "the condition deserialize failed:ret[%d]", ret);
      break;
    }
    // add the condition item
    ret = add_cond(cond);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "add condition failed:ret[%d]", ret);
      break;
    }
    temp_pos = pos;
  }
  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObSimpleFilter) {
  int64_t total_size = 0;
  int64_t size = conditions_.size();
  for (int64_t i = 0; i < size; ++i) {
    total_size += conditions_[i].get_serialize_size();
  }
  return total_size;
}



