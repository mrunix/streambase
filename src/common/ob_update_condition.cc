/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_update_condition.cc for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#include "ob_update_condition.h"

namespace sb {
namespace common {
ObUpdateCondition::ObUpdateCondition() {
}

ObUpdateCondition::~ObUpdateCondition() {
}

void ObUpdateCondition::reset() {
  conditions_.clear();
  string_buf_.reset();
}

int ObUpdateCondition::add_cond(const ObString& table_name, const ObString& row_key,
                                const ObString& column_name, const ObLogicOperator op_type, const ObObj& value) {
  int err = OB_SUCCESS;
  ObCondInfo cond_info;

  cond_info.table_name_ = table_name;
  cond_info.row_key_ = row_key;
  cond_info.column_name_ = column_name;
  cond_info.op_type_ = op_type;
  cond_info.value_ = value;

  err = add_cond_info_(cond_info);
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "failed to add cond info, err=%d", err);
  }

  return err;
}

int ObUpdateCondition::add_cond(const ObString& table_name, const ObString& row_key,
                                const bool is_exist) {
  int err = OB_SUCCESS;
  ObCondInfo cond_info;

  cond_info.table_name_ = table_name;
  cond_info.row_key_ = row_key;
  cond_info.column_name_.assign(NULL, 0);
  if (!is_exist) {
    cond_info.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  } else {
    cond_info.value_.set_ext(ObActionFlag::OP_ROW_EXIST);
  }

  err = add_cond_info_(cond_info);
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "failed to add cond info, err=%d", err);
  }

  return err;
}

int ObUpdateCondition::add_cond_info_(const ObCondInfo& cond_info) {
  int err = OB_SUCCESS;
  ObCondInfo dst_cond_info;

  err = copy_cond_info_(cond_info, dst_cond_info);

  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "failed to copy cond info, err=%d", err);
    err = OB_ERROR;
  } else {
    err = conditions_.push_back(dst_cond_info);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to push back cond info, err=%d", err);
    }
  }

  return err;
}

int ObUpdateCondition::copy_cond_info_(const ObCondInfo& src_cond_info, ObCondInfo& dst_node_info) {
  int err = OB_SUCCESS;

  err = string_buf_.write_string(src_cond_info.table_name_, &dst_node_info.table_name_);
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "failed to copy table name, err=%d", err);
  } else {
    err = string_buf_.write_string(src_cond_info.row_key_, &dst_node_info.row_key_);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to copy row key, err=%d", err);
    }
  }

  if (OB_SUCCESS == err) {
    err = string_buf_.write_string(src_cond_info.column_name_, &dst_node_info.column_name_);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to copy column name, err=%d", err);
    } else {
      err = string_buf_.write_obj(src_cond_info.value_, &dst_node_info.value_);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to copy value, err=%d", err);
      } else {
        dst_node_info.op_type_ = src_cond_info.op_type_;
      }
    }
  }

  return err;
}

const ObCondInfo* ObUpdateCondition::operator[](const int64_t index) const {
  ObCondInfo* ret = NULL;
  if ((index < 0) || (index >= conditions_.size())) {
    TBSYS_LOG(WARN, "invalid param, index=%ld, size=%ld", index, (int64_t) conditions_.size());
  } else {
    ret = &conditions_[index];
  }

  return ret;
}

int ObUpdateCondition::serialize(char* buf, const int64_t buf_len, int64_t& pos) const {
  int err = OB_SUCCESS;

  if (NULL == buf || buf_len < 0 || pos > buf_len) {
    TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
    err = OB_INVALID_ARGUMENT;
  } else {
    int64_t count = conditions_.size();
    ObObj tmp_obj;
    tmp_obj.set_ext(ObActionFlag::UPDATE_COND_FIELD);

    for (int64_t i = 0; OB_SUCCESS == err && i < count; ++i) {
      err = tmp_obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to serialize update cond field, err=%d", err);
      } else {
        err = serialize_cond_info_(buf, buf_len, pos, conditions_[i]);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "failed to serialize cond info, i=%ld, err=%d", i, err);
        }
      }
    }
  }

  return err;
}

int ObUpdateCondition::deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
  int err = OB_SUCCESS;
  if (NULL == buf || data_len < 0 || pos > data_len) {
    TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld", buf, data_len, pos);
    err = OB_INVALID_ARGUMENT;
  } else {
    reset();

    ObObj obj;
    int64_t temp_pos = pos;

    if (pos < data_len) {
      err = obj.deserialize(buf, data_len, pos);
    }

    while (OB_SUCCESS == err && ObExtendType == obj.get_type() && temp_pos < data_len) {
      if (ObActionFlag::UPDATE_COND_FIELD != obj.get_ext()) {
        pos = temp_pos; // rollback to last pos
        break;
      } else {
        ObCondInfo cond_info;
        err = deserialize_cond_info_(buf, data_len, pos, cond_info);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "failed to deserialize cond info, err=%d", err);
        } else {
          err = add_cond_info_(cond_info);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "failed to add cond info, err=%d", err);
          }
        }
      }

      if (OB_SUCCESS == err) {
        temp_pos = pos;
        if (pos < data_len) {
          err = obj.deserialize(buf, data_len, pos);
        }
      }
    }
  }

  return err;
}

int64_t ObUpdateCondition::get_serialize_size(void) const {
  int64_t store_size = 0;
  int64_t count = conditions_.size();
  ObObj tmp_obj;
  tmp_obj.set_ext(ObActionFlag::UPDATE_COND_FIELD);

  store_size = count * tmp_obj.get_serialize_size();
  for (int64_t i = 0; i < count; ++i) {
    store_size += get_serialize_size_(conditions_[i]);
  }

  return store_size;
}

int ObUpdateCondition::serialize_cond_info_(char* buf, const int64_t buf_len, int64_t& pos,
                                            const ObCondInfo& cond_info) const {
  int err = OB_SUCCESS;

  if (NULL == buf || buf_len <= 0 || pos >= buf_len) {
    TBSYS_LOG(ERROR, "should not reach");
    err = OB_ERROR;
  } else {
    // serialize table name
    err = cond_info.table_name_.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to serialize table name, err=%d", err);
    } else {
      // serialize row key
      err = cond_info.row_key_.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to serialize row key, err=%d", err);
      }
    }

    // serialize value
    if (OB_SUCCESS == err) {
      err = cond_info.value_.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to serialize row key, err=%d", err);
      }
    }

    // serialize column name and op type if it's not extend type
    int tmp_type = cond_info.value_.get_type();
    if (OB_SUCCESS == err && ObExtendType != tmp_type) {
      err = cond_info.column_name_.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to serialize, err=%d", err);
      } else {
        ObObj tmp_obj;
        tmp_obj.set_int(cond_info.op_type_);
        err = tmp_obj.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "failed to serialize op type, err=%d", err);
        }
      }
    }
  }

  return err;
}

int ObUpdateCondition::deserialize_cond_info_(const char* buf, const int64_t buf_len, int64_t& pos,
                                              ObCondInfo& cond_info) {
  int err = OB_SUCCESS;

  if (NULL == buf || buf_len <= 0 || pos >= buf_len) {
    TBSYS_LOG(ERROR, "should not reach");
    err = OB_ERROR;
  } else {
    // deserialize table name
    err = cond_info.table_name_.deserialize(buf, buf_len, pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to deserialize table name, err=%d", err);
    } else {
      // deserialize row key
      err = cond_info.row_key_.deserialize(buf, buf_len, pos);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to deserialize row key, err=%d", err);
      }
    }

    // deserialize value
    if (OB_SUCCESS == err) {
      err = cond_info.value_.deserialize(buf, buf_len, pos);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to deserialize value, err=%d", err);
      }
    }

    if (OB_SUCCESS == err) {
      if (ObExtendType != cond_info.value_.get_type()) {
        err = cond_info.column_name_.deserialize(buf, buf_len, pos);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "failed to deserialize column name, err=%d", err);
        } else {
          ObObj tmp_obj;
          err = tmp_obj.deserialize(buf, buf_len, pos);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "failed to deserialize op type, err=%d", err);
          } else {
            int64_t tmp_int = 0;;
            err = tmp_obj.get_int(tmp_int);
            if (OB_SUCCESS != err) {
              TBSYS_LOG(WARN, "invalid op type, err=%d", err);
            } else {
              cond_info.op_type_ = (ObLogicOperator) tmp_int;
            }
          }
        }
      }
    }
  }

  return err;
}

int64_t ObUpdateCondition::get_serialize_size_(const ObCondInfo& cond_info) const {
  int64_t store_size = 0;

  store_size = cond_info.table_name_.get_serialize_size() + cond_info.row_key_.get_serialize_size();

  store_size += cond_info.value_.get_serialize_size();

  if (ObExtendType != cond_info.value_.get_type()) {
    store_size += cond_info.column_name_.get_serialize_size();
    ObObj tmp_obj;
    tmp_obj.set_int(cond_info.op_type_);
    store_size += tmp_obj.get_serialize_size();
  }

  return store_size;
}
}
}



