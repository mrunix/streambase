/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scanner.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "ob_scanner.h"

#include <new>

#include "tblog.h"
#include "ob_malloc.h"
#include "ob_define.h"
#include "serialization.h"
#include "ob_action_flag.h"

using namespace sb::common;

ObScanner::TableReader::TableReader() {
  cur_table_id_ = OB_INVALID_ID;
  id_name_type_ = UNKNOWN;
  is_row_changed_ = false;
}

ObScanner::TableReader::~TableReader() {
}

int ObScanner::TableReader::read_cell(const char* buf, const int64_t data_len, int64_t& pos,
                                      ObCellInfo& cell_info, ObObj& last_obj) {
  int ret = OB_SUCCESS;

  bool get_column = false;
  int64_t next_pos = pos;

  is_row_changed_ = false;

  if (NULL == buf || 0 >= data_len) {
    TBSYS_LOG(WARN, "invalid arguments, buf=%p data_len=%ld pos=%ld", buf, data_len, pos);
    ret = OB_INVALID_ARGUMENT;
  } else if (pos >= data_len) {
    ret = OB_ITER_END;
  } else {
    // deserialize table name or table id if existed
    ret = last_obj.deserialize(buf, data_len, next_pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "deserialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, data_len, next_pos);
    } else {
      if (ObExtendType == last_obj.get_type() && ObActionFlag::TABLE_NAME_FIELD == last_obj.get_ext()) {
        ret = ObScanner::deserialize_int_or_varchar_(buf, data_len, next_pos,
                                                     reinterpret_cast<int64_t&>(cur_table_id_), cur_table_name_, last_obj);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(WARN, "deserialize_int_or_varchar_ error, ret=%d", ret);
        } else {
          if (cur_table_name_.length() > 0 && NULL != cur_table_name_.ptr()) {
            if (ID == id_name_type_) {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=NAME before=ID");
            } else {
              id_name_type_ = NAME;
            }
          } else if (OB_INVALID_ID != cur_table_id_) {
            if (NAME == id_name_type_) {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=ID before=NAME");
            } else {
              id_name_type_ = ID;
            }
          } else {
            TBSYS_LOG(WARN, "Table Name and Table ID are both invalid when deserializing");
          }
        }

        if (OB_SUCCESS == ret) {
          is_row_changed_ = true;

          ret = last_obj.deserialize(buf, data_len, next_pos);
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(WARN, "deserialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          }
        }
      }
    }

    // deserialize row key if existed
    if (OB_SUCCESS == ret) {
      if (ObExtendType == last_obj.get_type() && ObActionFlag::ROW_KEY_FIELD == last_obj.get_ext()) {
        ret = ObScanner::deserialize_varchar_(buf, data_len, next_pos, cur_row_key_, last_obj);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(WARN, "deserialize_varchar_ error, ret=%d", ret);
        } else {
          is_row_changed_ = true;

          ret = last_obj.deserialize(buf, data_len, next_pos);
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(WARN, "deserialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          }
        }
      }
    }

    //deserialize colume
    if (OB_SUCCESS == ret) {
      if (ObExtendType == last_obj.get_type()
          && (ObActionFlag::OP_ROW_DOES_NOT_EXIST == last_obj.get_ext()
              || ObActionFlag::OP_NOP == last_obj.get_ext()
              || ObActionFlag::OP_DEL_ROW == last_obj.get_ext()
              || ObActionFlag::OP_DEL_TABLE == last_obj.get_ext())) {
        cell_info.value_.set_ext(last_obj.get_ext());
        get_column = true;
      } else if (ObIntType == last_obj.get_type() || ObVarcharType == last_obj.get_type()) {
        if (ObIntType == last_obj.get_type()) {
          ret = last_obj.get_int(reinterpret_cast<int64_t&>(cell_info.column_id_));
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(WARN, "deserialize column id error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          } else if (OB_INVALID_ID == cell_info.column_id_) {
            TBSYS_LOG(WARN, "deserialized column id is invalid");
          } else {
            if (NAME == id_name_type_) {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=ID before=NAME");
            } else {
              id_name_type_ = ID;
            }
          }
        } else {
          ret = last_obj.get_varchar(cell_info.column_name_);
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(WARN, "deserialize column name error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          } else if (NULL == cell_info.column_name_.ptr() || 0 == cell_info.column_name_.length()) {
            TBSYS_LOG(WARN, "deserialized column name is invalid");
          } else {
            if (ID == id_name_type_) {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=NAME before=ID");
            } else {
              id_name_type_ = NAME;
            }
          }
        }
        if (OB_SUCCESS == ret) {
          ret = last_obj.deserialize(buf, data_len, next_pos);
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(WARN, "deserialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          } else {
            cell_info.value_ = last_obj;
            get_column = true;
          }
        }
      }
    }

    if (OB_SUCCESS == ret && get_column) {
      if ((0 == cur_table_name_.length() || NULL == cur_table_name_.ptr())
          && OB_INVALID_ID == cur_table_id_) {
        TBSYS_LOG(WARN, "invalid status, table name and table id are both invalid");
        ret = OB_ERROR;
      } else {
        cell_info.table_name_ = cur_table_name_;
        cell_info.table_id_ = cur_table_id_;
        cell_info.row_key_ = cur_row_key_;
      }
    }

    if (OB_SUCCESS == ret) {
      pos = next_pos;
    }

    if (OB_SUCCESS == ret && !get_column) {
      // the current obobj can not be recognized
      ret = OB_UNKNOWN_OBJ;
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

ObScanner::Iterator::Iterator() : scanner_(NULL), cur_mem_block_(NULL),
  cur_pos_(0), next_pos_(0), iter_size_counter_(0),
  cur_cell_info_(), new_row_cell_(false) {
}

ObScanner::Iterator::~Iterator() {
  scanner_ = NULL;
  cur_mem_block_ = NULL;
  cur_pos_ = 0;
  next_pos_ = 0;
  iter_size_counter_ = 0;
}

ObScanner::Iterator::Iterator(const ObScanner::Iterator& other) {
  *this = other;
}

ObScanner::Iterator::Iterator(const ObScanner* scanner,
                              const ObScanner::MemBlock* mem_block,
                              int64_t size_counter, int64_t cur_pos) : scanner_(scanner), cur_mem_block_(mem_block),
  cur_pos_(cur_pos), next_pos_(cur_pos), iter_size_counter_(size_counter),
  cur_cell_info_(), new_row_cell_(false) {
  if (!is_iterator_end_()) {
    if (OB_SUCCESS != get_cell_(cur_cell_info_, next_pos_)) {
      TBSYS_LOG(ERROR, "get cell fail next_pos=%ld cur_pos=%ld", next_pos_, cur_pos_);
    }
  }
}

void ObScanner::Iterator::update_cur_mem_block_() {
  if (iter_size_counter_ < scanner_->cur_size_counter_) {
    iter_size_counter_ += (next_pos_ - cur_pos_);
  }
  if (NULL != cur_mem_block_
      && next_pos_ >= cur_mem_block_->cur_pos
      && NULL != scanner_
      && iter_size_counter_ <= scanner_->cur_size_counter_
      && NULL != cur_mem_block_->next) {
    cur_mem_block_ = cur_mem_block_->next;
    next_pos_ = 0;
  }
}

ObScanner::Iterator& ObScanner::Iterator::operator ++ () {
  /// ObCellInfo cell_info;
  update_cur_mem_block_();
  int64_t next_pos = next_pos_;
  if (iter_size_counter_ < scanner_->cur_size_counter_
      && OB_SUCCESS != get_cell_(cur_cell_info_, next_pos_)) {
    TBSYS_LOG(ERROR, "get cell fail next_pos=%ld cur_pos=%ld", next_pos, cur_pos_);
  } else {
    cur_pos_ = next_pos;
  }
  return *this;
}

ObScanner::Iterator ObScanner::Iterator::operator ++ (int) {
  ObScanner::Iterator ret = *this;
  ++ *this;
  return ret;
}

bool ObScanner::Iterator::operator == (const ObScanner::Iterator& other) const {
  bool bret = false;
  if (other.scanner_ == scanner_
      && other.cur_mem_block_ == cur_mem_block_
      && other.cur_pos_ == cur_pos_
      && other.iter_size_counter_ == iter_size_counter_) {
    bret = true;
  }
  return bret;
}

bool ObScanner::Iterator::operator != (const ObScanner::Iterator& other) const {
  return !(*this == other);
}

ObScanner::Iterator& ObScanner::Iterator::operator = (const ObScanner::Iterator& other) {
  scanner_ = other.scanner_;
  cur_mem_block_ = other.cur_mem_block_;
  cur_pos_ = other.cur_pos_;
  iter_size_counter_ = other.iter_size_counter_;
  reader_ = other.reader_;
  next_pos_ = other.next_pos_;
  cur_cell_info_ = other.cur_cell_info_;
  new_row_cell_ = other.new_row_cell_;
  return *this;
}

int ObScanner::Iterator::get_cell(ObCellInfo& cell_info) {
  ///int64_t next_pos = cur_pos_;
  ///return get_cell_(cell_info, next_pos);
  ObCellInfo* cell_out = NULL;
  int err = OB_SUCCESS;
  err = get_cell(&cell_out, NULL);
  if (NULL != cell_out) {
    cell_info = *cell_out;
  }
  return err;
}


bool ObScanner::Iterator::is_iterator_end_() {
  return (NULL == scanner_
          || 0 == scanner_->cur_size_counter_
          || iter_size_counter_ >= scanner_->cur_size_counter_);
}

int ObScanner::Iterator::get_cell(ObCellInfo** cell_info, bool* is_row_changed/* = NULL*/) {
  int err = OB_SUCCESS;
  if (is_iterator_end_()) {
    err = OB_ITER_END;
  }
  if (OB_SUCCESS == err && NULL == cell_info) {
    err  = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    *cell_info = &cur_cell_info_;
    if (NULL != is_row_changed) {
      *is_row_changed = new_row_cell_;
    }
  }
  return err;
  /// int ret = OB_SUCCESS;
  /// new_row_cell_ = false;
  /// if (NULL == cell_info)
  /// {
  ///   ret = OB_ERROR;
  /// }
  /// else if (OB_SUCCESS != get_cell(cur_cell_info_))
  /// {
  ///   ret = OB_ERROR;
  /// }
  /// else
  /// {
  ///   *cell_info = &cur_cell_info_;
  ///   if (NULL != is_row_changed)
  ///   {
  ///     *is_row_changed = new_row_cell_;
  ///   }
  /// }
  /// return ret;
}

int ObScanner::Iterator::get_cell_(ObCellInfo& cell_info, int64_t& next_pos) {
  int ret = OB_SUCCESS;
  if (NULL == scanner_
      || NULL == cur_mem_block_
      || iter_size_counter_ >= scanner_->cur_size_counter_) {
    ret = OB_ERROR;
  } else {
    ret = get_cell_(cur_mem_block_->memory, cur_mem_block_->cur_pos, next_pos, cell_info, next_pos);
  }
  return ret;
}

int ObScanner::Iterator::get_cell_(const char* data, const int64_t data_len, const int64_t cur_pos,
                                   ObCellInfo& cell_info, int64_t& next_pos) {
  int ret = OB_SUCCESS;

  next_pos = cur_pos;

  if (NULL == data || 0 >= data_len) {
    ret = OB_ERROR;
  } else {
    ///cell_info.reset();
    ObObj tmp_obj;
    ret = reader_.read_cell(data, data_len, next_pos, cell_info, tmp_obj);
    new_row_cell_ = reader_.is_row_changed();
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObScanner::ObScanner() : head_memblock_(NULL), cur_memblock_(NULL), rollback_memblock_(NULL),
  cur_size_counter_(0), rollback_size_counter_(0), cur_table_name_(), cur_table_id_(OB_INVALID_ID), cur_row_key_(),
  tmp_table_name_(), tmp_table_id_(OB_INVALID_ID), tmp_row_key_(),
  mem_size_limit_(DEFAULT_MAX_SERIALIZE_SIZE), iter_(), first_next_(false),
  string_buf_() {
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  id_name_type_ = UNKNOWN;
  has_row_key_flag_ = false;
}

ObScanner::~ObScanner() {
  MemBlock* iter = head_memblock_;
  while (NULL != iter) {
    MemBlock* tmp = iter->next;
    ob_free(iter);
    iter = tmp;
  }
}

void ObScanner::reset() {
  if (NULL != head_memblock_) {
    MemBlock* iter = head_memblock_;
    while (NULL != iter) {
      iter->reset();
      iter = iter->next;
    }
  }
  cur_memblock_ = head_memblock_;
  rollback_memblock_ = cur_memblock_;
  cur_size_counter_ = 0;
  rollback_size_counter_ = 0;
  cur_table_name_.assign(NULL, 0);
  cur_table_id_ = OB_INVALID_ID;
  cur_row_key_.assign(NULL, 0);
  tmp_table_name_.assign(NULL, 0);
  tmp_table_id_ = OB_INVALID_ID;
  tmp_row_key_.assign(NULL, 0);
  iter_ = begin();
  first_next_ = false;
  string_buf_.clear();

  range_.reset();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  id_name_type_ = UNKNOWN;
  last_row_key_.assign(NULL, 0);
  id_name_type_ = UNKNOWN;
  has_row_key_flag_ = false;
}

void ObScanner::clear() {
  if (NULL != head_memblock_) {
    MemBlock* iter = head_memblock_;
    while (NULL != iter) {
      MemBlock* tmp = iter->next;
      ob_free(iter);
      iter = tmp;
    }
    head_memblock_ = NULL;
  }
  cur_memblock_ = head_memblock_;
  rollback_memblock_ = cur_memblock_;
  cur_size_counter_ = 0;
  rollback_size_counter_ = 0;
  cur_table_name_.assign(NULL, 0);
  cur_table_id_ = OB_INVALID_ID;
  cur_row_key_.assign(NULL, 0);
  tmp_table_name_.assign(NULL, 0);
  tmp_table_id_ = OB_INVALID_ID;
  tmp_row_key_.assign(NULL, 0);
  iter_ = begin();
  first_next_ = false;
  string_buf_.clear();

  range_.reset();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  id_name_type_ = UNKNOWN;
  last_row_key_.assign(NULL, 0);
  id_name_type_ = UNKNOWN;
  has_row_key_flag_ = false;
}

int64_t ObScanner::set_mem_size_limit(const int64_t limit) {
  if (0 > limit
      || OB_MAX_PACKET_LENGTH < limit) {
    TBSYS_LOG(WARN, "invlaid limit_size=%ld cur_limit_size=%ld",
              limit, mem_size_limit_);
  } else if (0 == limit) {
    // 0 means using the default value
  } else {
    mem_size_limit_ = limit;
  }
  return mem_size_limit_;
}

int ObScanner::add_cell(const ObCellInfo& cell_info) {
  int ret = OB_SUCCESS;

  bool has_add = false;
  if (NULL == cur_memblock_) {
    ret = get_memblock_(DEFAULT_MAX_SERIALIZE_SIZE);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "get_memblock_ error, ret=%d", ret);
    }
  } else {
    ret = append_serialize_(cell_info);
    if (OB_SUCCESS != ret && OB_SIZE_OVERFLOW != ret) {
      ret = OB_SUCCESS;
      ret = get_memblock_(DEFAULT_MAX_SERIALIZE_SIZE);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "get_memblock_ error, ret=%d", ret);
      }
    } else {
      has_add = true;
    }
  }

  if (OB_SUCCESS == ret && !has_add) {
    ret = append_serialize_(cell_info);
    if (OB_SIZE_OVERFLOW == ret) {
      TBSYS_LOG(DEBUG, "exceed the memory limit");
    } else if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "append_serialize_ error, ret=%d", ret);
    }
  }

  return ret;
}

int ObScanner::rollback() {
  int ret = OB_SUCCESS;
  if (NULL == rollback_memblock_) {
    ret = OB_SUCCESS;
  } else {
    rollback_memblock_->cur_pos = rollback_memblock_->rollback_pos;
    cur_memblock_ = rollback_memblock_;
    cur_size_counter_ = rollback_size_counter_;
  }
  return ret;
}

DEFINE_SERIALIZE(ObScanner)
//int ObScanner::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t next_pos = pos;

  if (OB_SUCCESS == ret) {
    ///obj.reset();
    obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret) {
    ///obj.reset();
    obj.set_int(is_request_fullfilled_ ? 1 : 0);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret) {
    ///obj.reset();
    obj.set_int(fullfilled_item_num_);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret) {
    ///obj.reset();
    obj.set_int(data_version_);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (has_range_) {
    if (OB_SUCCESS == ret) {
      ///obj.reset();
      obj.set_ext(ObActionFlag::TABLET_RANGE_FIELD);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }

    if (OB_SUCCESS == ret) {
      ///obj.reset();
      obj.set_int(range_.border_flag_.get_data());
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }

    if (OB_SUCCESS == ret) {
      ///obj.reset();
      obj.set_varchar(range_.start_key_);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }

    if (OB_SUCCESS == ret) {
      ///obj.reset();
      obj.set_varchar(range_.end_key_);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
  }

  if (cur_size_counter_ > 0) {
    if (OB_SUCCESS == ret) {
      ///obj.reset();
      obj.set_ext(ObActionFlag::TABLE_PARAM_FIELD);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }

    if (OB_SUCCESS == ret) {
      MemBlock* iter = head_memblock_;
      while (NULL != iter && (next_pos + iter->cur_pos) < buf_len) {
        memcpy(buf + next_pos, iter->memory, iter->cur_pos);
        next_pos += iter->cur_pos;
        iter = iter->next;
      }
      if (NULL != iter) {
        TBSYS_LOG(WARN, "ObScanner buffer to serialize is not enough, buf_len=%ld", buf_len);
        ret = OB_BUF_NOT_ENOUGH;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    ///obj.reset();
    obj.set_ext(ObActionFlag::END_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret) {
    pos = next_pos;
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObScanner)
//int64_t ObScanner::get_serialize_size(void) const
{
  int64_t ret = 0;
  ObObj obj;

  ///obj.reset();
  obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
  ret += obj.get_serialize_size();

  ///obj.reset();
  obj.set_int(is_request_fullfilled_ ? 1 : 0);
  ret += obj.get_serialize_size();

  ///obj.reset();
  obj.set_int(fullfilled_item_num_);
  ret += obj.get_serialize_size();

  ///obj.reset();
  obj.set_int(data_version_);
  ret += obj.get_serialize_size();

  if (has_range_) {
    ///obj.reset();
    obj.set_ext(ObActionFlag::TABLET_RANGE_FIELD);
    ret += obj.get_serialize_size();

    ///obj.reset();
    obj.set_int(range_.border_flag_.get_data());
    ret += obj.get_serialize_size();

    ///obj.reset();
    obj.set_varchar(range_.start_key_);
    ret += obj.get_serialize_size();

    ///obj.reset();
    obj.set_varchar(range_.end_key_);
    ret += obj.get_serialize_size();
  }

  if (cur_size_counter_ > 0) {
    ///obj.reset();
    obj.set_ext(ObActionFlag::TABLE_PARAM_FIELD);
    ret += obj.get_serialize_size();

    ret += cur_size_counter_;
  }

  ///obj.reset();
  obj.set_ext(ObActionFlag::END_PARAM_FIELD);
  ret += obj.get_serialize_size();

  return ret;
}

DEFINE_DESERIALIZE(ObScanner)
//int ObScanner::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || 0 >= data_len - pos) {
    TBSYS_LOG(WARN, "invalid param buf=%p data_len=%ld pos=%ld", buf, data_len, pos);
    ret = OB_ERROR;
  } else {
    ObObj param_id;
    ObObjType obj_type;
    int64_t param_id_type;
    bool is_end = false;
    int64_t new_pos = pos;

    clear();

    ret = param_id.deserialize(buf, data_len, new_pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }

    while (OB_SUCCESS == ret && new_pos <= data_len && !is_end) {
      obj_type = param_id.get_type();
      // read until reaching a ObExtendType ObObj
      while (OB_SUCCESS == ret && ObExtendType != obj_type) {
        ret = param_id.deserialize(buf, data_len, new_pos);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
        } else {
          obj_type = param_id.get_type();
        }
      }

      if (OB_SUCCESS == ret) {
        ret = param_id.get_ext(param_id_type);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(WARN, "ObObj type is not ObExtendType, invalid status, ret=%d", ret);
        } else {
          switch (param_id_type) {
          case ObActionFlag::BASIC_PARAM_FIELD:
            ret = deserialize_basic_(buf, data_len, new_pos, param_id);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(WARN, "deserialize_basic_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                        ret, buf, data_len, new_pos);
            }
            break;
          case ObActionFlag::TABLE_PARAM_FIELD:
            ret = deserialize_table_(buf, data_len, new_pos, param_id);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(WARN, "deserialize_table_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                        ret, buf, data_len, new_pos);
            }
            break;
          case ObActionFlag::END_PARAM_FIELD:
            is_end = true;
            break;
          default:
            ret = param_id.deserialize(buf, data_len, new_pos);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
            }
            break;
          }
        }
      }
    }

    if (OB_SUCCESS == ret) {
      pos = new_pos;
    }
  }
  return ret;
}

int ObScanner::append_serialize_(const ObCellInfo& cell_info) {
  int ret = OB_SUCCESS;

  char* buf = cur_memblock_->memory;
  const int64_t buf_len = cur_memblock_->memory_size;
  int64_t pos = cur_memblock_->cur_pos;
  bool is_serialize_table = false;
  bool is_serialize_row = false;
  ObObj obj;

  tmp_table_name_ = cur_table_name_;
  tmp_table_id_ = cur_table_id_;
  tmp_row_key_ = cur_row_key_;

  // serialize table_name or table_id
  if (OB_SUCCESS == ret) {
    if (cell_info.table_name_ != cur_table_name_
        || cell_info.table_id_ != cur_table_id_) {
      is_serialize_table = true;
      cur_memblock_->rollback_pos = cur_memblock_->cur_pos;
      rollback_memblock_ = cur_memblock_;
      rollback_size_counter_ = cur_size_counter_;

      ret = serialize_table_name_or_id_(buf, buf_len, pos, cell_info);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "serialize_table_name_or_id_ error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
    }
  }

  // serialize row_key
  if (OB_SUCCESS == ret) {
    if (!has_row_key_flag_ || cell_info.row_key_ != cur_row_key_) {
      is_serialize_row = true;
      if (!is_serialize_table) {
        cur_memblock_->rollback_pos = cur_memblock_->cur_pos;
        rollback_memblock_ = cur_memblock_;
        rollback_size_counter_ = cur_size_counter_;
      }

      ret = serialize_row_key_(buf, buf_len, pos, cell_info);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "serialize_row_key_ error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
    }
  }

  // serialize column
  if (OB_SUCCESS == ret) {
    if (ObExtendType == cell_info.value_.get_type()
        && (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info.value_.get_ext()
            || ObActionFlag::OP_NOP == cell_info.value_.get_ext()
            || ObActionFlag::OP_DEL_ROW == cell_info.value_.get_ext()
            || ObActionFlag::OP_DEL_TABLE == cell_info.value_.get_ext())) {
      ///obj.reset();
      obj.set_ext(cell_info.value_.get_ext());
      ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
    } else if (ObExtendType != cell_info.value_.get_type()) {
      ret = serialize_column_(buf, buf_len, pos, cell_info);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "serialize_column_ error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
    } else {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "ObCellInfo value extend type is invalid, Type=%d",
                cell_info.value_.get_type());
    }
  }

  if (OB_SUCCESS == ret) {
    if ((cur_size_counter_ + (pos - cur_memblock_->cur_pos)) > mem_size_limit_) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      cur_size_counter_ += pos - cur_memblock_->cur_pos;
      cur_memblock_->cur_pos = pos;

      cur_table_name_ = tmp_table_name_;
      cur_table_id_ = tmp_table_id_;
      cur_row_key_ = tmp_row_key_;

      last_row_key_ = cur_row_key_;
      if (is_serialize_row) {
        has_row_key_flag_ = true;
      }
    }
  }

  return ret;
}

int ObScanner::serialize_table_name_or_id_(char* buf, const int64_t buf_len, int64_t& pos,
                                           const ObCellInfo& cell_info) {
  int ret = OB_SUCCESS;

  ObObj obj;

  obj.set_ext(ObActionFlag::TABLE_NAME_FIELD);
  ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
              ret, buf, buf_len, pos);
  } else {
    if (cell_info.table_name_.length() > 0
        && OB_INVALID_ID != cell_info.table_id_) {
      TBSYS_LOG(WARN, "table_name and table_id both exist, table_name=%.*s table_id=%lu",
                cell_info.table_name_.length(), cell_info.table_name_.ptr(), cell_info.table_id_);
      ret = OB_ERROR;
    } else {
      ///obj.reset();
      if (cell_info.table_name_.length() > 0 && NULL != cell_info.table_name_.ptr()) {
        obj.set_varchar(cell_info.table_name_);
        if (ID == id_name_type_) {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=NAME before=ID");
        } else {
          id_name_type_ = NAME;
        }
      } else if (OB_INVALID_ID != cell_info.table_id_) {
        tmp_table_id_ = cell_info.table_id_;
        obj.set_int(cell_info.table_id_);
        if (NAME == id_name_type_) {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=ID before=NAME");
        } else {
          id_name_type_ = ID;
        }
      } else {
        TBSYS_LOG(WARN, "Table Name and Table ID are both invalid");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret) {
        int64_t cur_pos = pos;

        ret = obj.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, pos);
        }

        if (cell_info.table_name_.length() > 0) {
          ObObj tmp_obj;
          ret = tmp_obj.deserialize(buf, pos, cur_pos);
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p pos=%ld cur_pos=%ld",
                      ret, buf, pos, cur_pos);
          } else {
            ret = tmp_obj.get_varchar(tmp_table_name_);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObScanner::serialize_row_key_(char* buf, const int64_t buf_len, int64_t& pos,
                                  const ObCellInfo& cell_info) {
  int ret = OB_SUCCESS;

  ObObj obj;

  obj.set_ext(ObActionFlag::ROW_KEY_FIELD);
  ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
              ret, buf, buf_len, pos);
  } else {
    ///obj.reset();
    obj.set_varchar(cell_info.row_key_);

    int64_t cur_pos = pos;

    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                ret, buf, buf_len, pos);
    } else {
      ObObj tmp_obj;
      ret = tmp_obj.deserialize(buf, pos, cur_pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p pos=%ld cur_pos=%ld",
                  ret, buf, pos, cur_pos);
      } else {
        ret = tmp_obj.get_varchar(tmp_row_key_);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
        }
      }
    }
  }

  return ret;
}

int ObScanner::serialize_column_(char* buf, const int64_t buf_len, int64_t& pos,
                                 const ObCellInfo& cell_info) {
  int ret = OB_SUCCESS;

  ObObj obj;

  if (cell_info.column_name_.length() > 0
      && OB_INVALID_ID != cell_info.column_id_) {
    TBSYS_LOG(WARN, "column_name and column_id both exist, column_name=%.*s column_id=%lu",
              cell_info.column_name_.length(), cell_info.column_name_.ptr(), cell_info.column_id_);
    ret = OB_ERROR;
  } else {
    ///obj.reset();
    if (cell_info.column_name_.length() > 0 && NULL != cell_info.column_name_.ptr()) {
      obj.set_varchar(cell_info.column_name_);
      if (ID == id_name_type_) {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=NAME before=ID");
      } else {
        id_name_type_ = NAME;
      }
    } else if (OB_INVALID_ID != cell_info.column_id_) {
      obj.set_int(cell_info.column_id_);
      if (NAME == id_name_type_) {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=ID before=NAME");
      } else {
        id_name_type_ = ID;
      }
    } else {
      TBSYS_LOG(WARN, "Column Name and Column ID are both invalid");
    }

    if (OB_SUCCESS == ret) {
      ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      } else {
        ret = cell_info.value_.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, pos);
        }
      }
    }
  }

  return ret;
}

int ObScanner::get_memblock_(const int64_t size) {
  int ret = OB_SUCCESS;

  int64_t memory_size = DEFAULT_MEMBLOCK_SIZE;
  if (size > DEFAULT_MEMBLOCK_SIZE) {
    memory_size = size;
  }

  if (NULL == head_memblock_) {
    head_memblock_ = new(ob_malloc(memory_size + sizeof(MemBlock))) MemBlock(memory_size);
    if (NULL == head_memblock_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      cur_memblock_ = head_memblock_;
      rollback_memblock_ = head_memblock_;
    }
  } else if (size + cur_memblock_->cur_pos > cur_memblock_->memory_size) {
    // 可能出现rollback的情况,之后的memblock不需要重新申请,reset即可
    if (NULL == cur_memblock_->next) {
      cur_memblock_->next = new(ob_malloc(memory_size + sizeof(MemBlock))) MemBlock(memory_size);
    }
    if (NULL == cur_memblock_->next) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      cur_memblock_->next->reset();
      cur_memblock_ = cur_memblock_->next;
    }
  }
  return ret;
}

int64_t ObScanner::get_valid_data_size_(const char* data, const int64_t data_len, int64_t& pos, ObObj& last_obj) {
  int ret = OB_SUCCESS;
  int64_t valid_data_size = 0;
  int64_t cur_pos = pos;
  ObCellInfo ci;

  TableReader reader;

  ret = reader.read_cell(data, data_len, pos, ci, last_obj);
  while (OB_SUCCESS == ret) {
    if (OB_SUCCESS == ret) {
      valid_data_size += (pos - cur_pos);
      cur_pos = pos;
      ret = reader.read_cell(data, data_len, pos, ci, last_obj);
    }
  }

  if (OB_UNKNOWN_OBJ == ret || OB_ITER_END == ret) {
    // when reading unknown obobj, it's common case and leave it to outer code to process
    ret = OB_SUCCESS;
  }

  if (OB_SUCCESS == ret) {
    ret = string_buf_.write_string(reader.get_cur_row_key(), &last_row_key_);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "write_string error, ret=%d", ret);
    }
  }

  if (OB_SUCCESS != ret) {
    valid_data_size = -1;
  }

  return valid_data_size;
}

int ObScanner::deserialize_basic_(const char* buf, const int64_t data_len, int64_t& pos, ObObj& last_obj) {
  int ret = OB_SUCCESS;

  int64_t value = 0;

  // deserialize isfullfilled
  ret = deserialize_int_(buf, data_len, pos, value, last_obj);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  } else {
    is_request_fullfilled_ = (value == 0 ? false : true);
  }

  // deserialize filled item number
  if (OB_SUCCESS == ret) {
    ret = deserialize_int_(buf, data_len, pos, value, last_obj);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    } else {
      fullfilled_item_num_ = value;
    }
  }

  // deserialize data_version
  if (OB_SUCCESS == ret) {
    ret = deserialize_int_(buf, data_len, pos, value, last_obj);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    } else {
      data_version_ = value;
    }
  }

  if (OB_SUCCESS == ret) {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    } else {
      if (ObExtendType != last_obj.get_type()) {
        // maybe new added field
      } else {
        int64_t type = last_obj.get_ext();
        if (ObActionFlag::TABLET_RANGE_FIELD == type) {
          int64_t border_flag = 0;
          ObString start_row;
          ObString end_row;
          ret = deserialize_int_(buf, data_len, pos, border_flag, last_obj);
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, pos);
          }

          if (OB_SUCCESS == ret) {
            ret = deserialize_varchar_(buf, data_len, pos, start_row, last_obj);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(WARN, "deserialize_varchar_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                        ret, buf, data_len, pos);
            }
          }

          if (OB_SUCCESS == ret) {
            ret = deserialize_varchar_(buf, data_len, pos, end_row, last_obj);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(WARN, "deserialize_varchar_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                        ret, buf, data_len, pos);
            }
          }

          if (OB_SUCCESS == ret) {
            range_.table_id_ = 0;
            range_.border_flag_.set_data(static_cast<int8_t>(border_flag));
            string_buf_.write_string(start_row, &(range_.start_key_));
            string_buf_.write_string(end_row, &(range_.end_key_));
            has_range_ = true;
          }

          // after reading all neccessary fields,
          // filter unknown ObObj
          if (OB_SUCCESS == ret) {
            ret = last_obj.deserialize(buf, data_len, pos);
            while (OB_SUCCESS == ret && ObExtendType != last_obj.get_type()) {
              ret = last_obj.deserialize(buf, data_len, pos);
            }
          }
        } else {
          // maybe another param field
        }
      }
    }
  }

  return ret;
}

int ObScanner::deserialize_table_(const char* buf, const int64_t data_len, int64_t& pos, ObObj& last_obj) {
  int ret = OB_SUCCESS;

  int64_t cur_pos = pos;
  int64_t valid_data_size = get_valid_data_size_(buf, data_len, pos, last_obj);

  if (valid_data_size < 0) {
    TBSYS_LOG(WARN, "get valid data size fail buf=%p data_len=%ld pos=%ld", buf, data_len, pos);
    ret = OB_ERROR;
  } else {
    ret = get_memblock_(valid_data_size);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "get_memblock_ error, ret=%d", ret);
    } else {
      memcpy(cur_memblock_->memory, buf + cur_pos, valid_data_size);
      cur_memblock_->cur_pos = valid_data_size;
      cur_size_counter_ = valid_data_size;
    }
  }

  return ret;
}

int ObScanner::deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
                                int64_t& value, ObObj& last_obj) {
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  } else {
    if (ObIntType != last_obj.get_type()) {
      TBSYS_LOG(WARN, "ObObj type is not Int, Type=%d", last_obj.get_type());
    } else {
      ret = last_obj.get_int(value);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObScanner::deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                    ObString& value, ObObj& last_obj) {
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  } else {
    if (ObVarcharType != last_obj.get_type()) {
      TBSYS_LOG(WARN, "ObObj type is not Varchar, Type=%d", last_obj.get_type());
    } else {
      ret = last_obj.get_varchar(value);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObScanner::deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                           int64_t& int_value, ObString& varchar_value, ObObj& last_obj) {
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  } else {
    if (ObIntType == last_obj.get_type()) {
      ret = last_obj.get_int(int_value);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
      }
    } else if (ObVarcharType == last_obj.get_type()) {
      ret = last_obj.get_varchar(varchar_value);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
      }
    } else {
      TBSYS_LOG(WARN, "ObObj type is not int or varchar, Type=%d", last_obj.get_type());
      ret = OB_ERROR;
    }
  }

  return ret;
}

ObScanner::Iterator ObScanner::begin() const {
  return ObScanner::Iterator(this, head_memblock_);
}

ObScanner::Iterator ObScanner::end() const {
  return ObScanner::Iterator(this, cur_memblock_, cur_size_counter_,
                             (NULL == cur_memblock_) ? 0 : cur_memblock_->cur_pos);
}

int ObScanner::reset_iter() {
  int ret = OB_SUCCESS;
  first_next_ = false;
  iter_ = begin();
  return ret;
}

int ObScanner::next_cell() {
  int ret = OB_SUCCESS;
  if (!first_next_) {
    iter_ = begin();
    if (iter_.is_iterator_end_()) {
      ret = OB_ITER_END;
    } else {
      first_next_ = true;
    }
  } else if ((++iter_).is_iterator_end_()) {
    ret = OB_ITER_END;
  } else {
    // do nothing
  }
  return ret;
}

int ObScanner::get_cell(sb::common::ObCellInfo** cell) {
  int ret = OB_SUCCESS;
  ret = iter_.get_cell(cell);
  return ret;
}

int ObScanner::get_cell(sb::common::ObCellInfo** cell, bool* is_row_changed) {
  int ret = OB_SUCCESS;
  ret = iter_.get_cell(cell, is_row_changed);
  return ret;
}

bool ObScanner::is_empty() const {
  return (0 == cur_size_counter_);
}

int ObScanner::get_last_row_key(ObString& row_key) const {
  int ret = OB_SUCCESS;

  if (last_row_key_.length() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    row_key = last_row_key_;
  }

  return ret;
}

int ObScanner::set_is_req_fullfilled(const bool& is_fullfilled, const int64_t fullfilled_item_num) {
  int err = OB_SUCCESS;
  if (0 > fullfilled_item_num) {
    TBSYS_LOG(WARN, "param error [fullfilled_item_num:%ld]", fullfilled_item_num);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err) {
    fullfilled_item_num_ = fullfilled_item_num;
    is_request_fullfilled_ = is_fullfilled;
  }
  return err;
}

int ObScanner::get_is_req_fullfilled(bool& is_fullfilled, int64_t& fullfilled_item_num) const {
  int err = OB_SUCCESS;
  is_fullfilled = is_request_fullfilled_;
  fullfilled_item_num = fullfilled_item_num_;
  return err;
}

int ObScanner::set_range(const ObRange& range) {
  int ret = OB_SUCCESS;

  if (has_range_) {
    TBSYS_LOG(WARN, "range has been setted before");
    ret = OB_ERROR;
  } else {
    range_.table_id_ = range.table_id_;
    range_.border_flag_.set_data(range.border_flag_.get_data());

    ret = string_buf_.write_string(range.start_key_, &(range_.start_key_));
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "write_string error, ret=%d", ret);
    } else {
      ret = string_buf_.write_string(range.end_key_, &(range_.end_key_));
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "write_string error, ret=%d", ret);
      }
    }
    has_range_ = true;
  }

  return ret;
}

int ObScanner::set_range_shallow_copy(const ObRange& range) {
  int ret = OB_SUCCESS;

  if (has_range_) {
    TBSYS_LOG(WARN, "range has been setted before");
    ret = OB_ERROR;
  } else {
    range_ = range;
    has_range_ = true;
  }

  return ret;
}

int ObScanner::get_range(ObRange& range) const {
  int ret = OB_SUCCESS;

  if (!has_range_) {
    TBSYS_LOG(WARN, "range has not been setted");
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    range = range_;
  }

  return ret;
}


