/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_sstable_block_getter.cc
 *
 * Authors:
 *     huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/ob_action_flag.h"
#include "ob_sstable_getter.h"
#include "ob_sstable_block_getter.h"
#include "ob_sstable_block_index_v2.h"

namespace sb {
namespace sstable {
using namespace common;

ObSSTableBlockGetter::ObSSTableBlockGetter(const ObScanColumnIndexes& column_index)
  : inited_(false), handled_del_row_(false), sstable_data_store_style_(OB_SSTABLE_STORE_DENSE),
    column_cursor_(0), current_column_count_(0), row_cursor_(NULL),
    index_buf_(DEFAULT_INDEX_BUF_SIZE), query_column_indexes_(column_index) {
  memset(current_columns_, 0 , sizeof(current_columns_));
}

ObSSTableBlockGetter::~ObSSTableBlockGetter() {

}

int ObSSTableBlockGetter::get_current_column_index(
  const int64_t cursor, int64_t& column_id, int64_t& column_index) const {
  int ret = OB_SUCCESS;

  if (sstable_data_store_style_ == OB_SSTABLE_STORE_DENSE) {
    ret = query_column_indexes_.get_column(cursor, column_id, column_index);
  } else if (sstable_data_store_style_ == OB_SSTABLE_STORE_SPARSE) {
    column_id = ObScanColumnIndexes::INVALID_COLUMN;
    int64_t data_column_id = ObScanColumnIndexes::INVALID_COLUMN;
    column_index = ObScanColumnIndexes::NOT_EXIST_COLUMN;

    ret = query_column_indexes_.get_column_id(cursor, column_id);
    for (int64_t i = 0; i < current_column_count_ && OB_SUCCESS == ret; ++i) {
      ret = current_ids_[i].get_int(data_column_id);
      if (OB_SUCCESS == ret && data_column_id == column_id) {
        column_index = i;
      }
    }
  }

  return ret;
}

int ObSSTableBlockGetter::load_current_row(const_iterator row_index) {
  int ret               = OB_SUCCESS;
  current_column_count_ = OB_MAX_COLUMN_NUMBER;

  ret = reader_.get_row(sstable_data_store_style_, row_index,
                        current_row_key_, current_ids_, current_columns_, current_column_count_);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "read current row error, store style=%ld, current row cursor=%ld,%ld",
              sstable_data_store_style_, row_index->offset_, row_index->size_);
  }

  return ret;
}

int ObSSTableBlockGetter::store_sparse_column(const int64_t column_index) {
  int ret                 = OB_SUCCESS;
  bool is_del_row         = false;
  int64_t data_column_id  = OB_INVALID_ID;

  if (0 == column_cursor_ && !handled_del_row_) {
    ret = current_ids_[column_cursor_].get_int(data_column_id);
    if (OB_SUCCESS == ret) {
      if (OB_DELETE_ROW_COLUMN_ID == static_cast<uint64_t>(data_column_id)) {
        is_del_row = true;
      }
    } else {
      TBSYS_LOG(WARN, "failed to get column id from obj");
    }
  }

  if (OB_SUCCESS == ret) {
    if (is_del_row) {
      /**
       * delete row is a special column with column_id == 0 and only
       * exists sparse format sstable, we could check whether it's
       * delete row op first, if it's delete row op, we only return
       * the cell with delete row op, ignore the other column.
       */
      current_cell_info_.column_id_ = data_column_id;
      current_cell_info_.value_ = current_columns_[column_cursor_];
      handled_del_row_ = true;

      if (1 == current_column_count_) {
        /**
         * if there is only one delete row op in sstable for current
         * row, skip the next columns, in this case we don't fill the
         * extra NOP for non-existent columns.
         */
        column_cursor_ = query_column_indexes_.get_column_count();
      }
    } else {
      ++column_cursor_;
      handled_del_row_ = true;
      if (column_index == ObScanColumnIndexes::NOT_EXIST_COLUMN) {
        current_cell_info_.value_.set_ext(ObActionFlag::OP_NOP);
      } else {
        current_cell_info_.value_ = current_columns_[column_index];
      }
    }
  }

  return ret;
}

int ObSSTableBlockGetter::store_current_cell(
  const int64_t column_id, const int64_t column_index) {
  int ret = OB_SUCCESS;

  current_cell_info_.table_id_ = 0;
  current_cell_info_.row_key_ = current_row_key_;
  current_cell_info_.column_id_ = column_id;

  if (column_index == ObScanColumnIndexes::ROWKEY_COLUMN) {
    // TODO store row key
    current_cell_info_.value_.set_varchar(current_row_key_);
  } else if (column_index == ObScanColumnIndexes::NOT_EXIST_COLUMN) {
    //only dense format return null cell if column non-existent
    if (sstable_data_store_style_ == OB_SSTABLE_STORE_DENSE) {
      current_cell_info_.value_.set_null();
    } else {
      ret = store_sparse_column(column_index);
    }
  } else {
    if (column_index >= current_column_count_) {
      TBSYS_LOG(ERROR, "column_index=%ld > current_column_count_=%ld",
                column_index, current_column_count_);
      ret = OB_ERROR;
    } else {
      if (sstable_data_store_style_ == OB_SSTABLE_STORE_DENSE) {
        current_cell_info_.value_ = current_columns_[column_index];
      } else {
        ret = store_sparse_column(column_index);
      }
    }
  }

  return ret;
}

int ObSSTableBlockGetter::store_and_advance_column() {
  int ret               = OB_SUCCESS;
  int64_t column_index  = ObScanColumnIndexes::INVALID_COLUMN;
  int64_t column_id     = OB_INVALID_ID;

  if (OB_SUCCESS != (ret = get_current_column_index(
                             column_cursor_, column_id, column_index))) {
    TBSYS_LOG(ERROR, "get column index error, ret = %d, cursor=%ld, id= %ld, index=%ld",
              ret, column_cursor_, column_id, column_index);
  } else if (OB_SUCCESS != (ret = store_current_cell(column_id, column_index))) {
    TBSYS_LOG(ERROR, "store current cell error, ret = %d, cursor=%ld, id=%ld, index=%ld",
              ret, column_cursor_, column_id, column_index);
  } else if (sstable_data_store_style_ == OB_SSTABLE_STORE_DENSE) {
    ++column_cursor_;
  }

  return ret;
}

/**
 * NOTE: column data pair is like this:
 *    --------------------------
 *    |column_id_obj|column_obj|
 *    --------------------------
 * sstable sparse format is like this:
 *     -------------------------------------------------------
 *     |row_key|column data pair|column data pair|   ...     |
 *     -------------------------------------------------------
 * sparse format is used for ups, currently we assumpt that
 * there is only one column group in sparse format sstable, and
 * we only get full row once.
 */
int ObSSTableBlockGetter::next_cell() {
  int ret = OB_SUCCESS;

  if (!inited_) {
    TBSYS_LOG(WARN, "ObSSTableBlockGetter doesn't initialize");
    ret = OB_NOT_INIT;
  } else if (NULL == row_cursor_ || query_column_indexes_.get_column_count() == 0) {
    TBSYS_LOG(WARN, "not initialized, cursor=%p, column_count=%ld",
              row_cursor_, query_column_indexes_.get_column_count() == 0);
    ret = OB_NOT_INIT;
  } else {
    if (column_cursor_ < query_column_indexes_.get_column_count()) {
      ret = store_and_advance_column();
    } else {
      // we iterator over current row
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int ObSSTableBlockGetter::get_cell(ObCellInfo** cell) {
  int ret = OB_SUCCESS;

  if (NULL == cell) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!inited_) {
    TBSYS_LOG(WARN, "ObSSTableBlockGetter doesn't initialize");
    ret = OB_ERROR;
  } else if (OB_SUCCESS == ret) {
    *cell = &current_cell_info_;
  }

  return ret;
}

void ObSSTableBlockGetter::clear() {
  inited_ = false;
  handled_del_row_ = false;
  sstable_data_store_style_ = OB_SSTABLE_STORE_DENSE;
  row_cursor_ = NULL;
  column_cursor_ = 0;
  current_column_count_ = 0;
  current_cell_info_.reset();
  current_row_key_.assign(NULL, 0);
  reader_.reset();
}

int ObSSTableBlockGetter::init(const ObString& row_key,
                               const char* buf,
                               const int64_t data_len,
                               const int64_t store_style) {
  int32_t ret = OB_SUCCESS;
  int64_t pos = 0;

  if (NULL == buf || data_len <= 0
      || NULL == row_key.ptr() || 0 == row_key.length()) {
    TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, "
              "row_key_ptr=%p, row_key_len=%d",
              buf, data_len, row_key.ptr(), row_key.length());
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret) {
    clear();
    sstable_data_store_style_ = store_style;
    current_row_key_ = row_key;

    ret = index_buf_.ensure_space(DEFAULT_INDEX_BUF_SIZE, ObModIds::OB_SSTABLE_EGT_SCAN);
    if (OB_SUCCESS == ret) {
      ret = reader_.deserialize(index_buf_.get_buffer(), index_buf_.get_buffer_size(),
                                buf, data_len, pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "deserialize error, ret=%d", ret);
        ret = OB_DESERIALIZE_ERROR;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    inited_ = true;
    row_cursor_ = reader_.find(row_key);
    if (row_cursor_ != reader_.end()) {
      //just the rowkey, load it
      ret = load_current_row(row_cursor_);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "load row error, ret=%d", ret);
        inited_ = false;
      }
    } else {
      TBSYS_LOG(DEBUG, "not find the row key:, ret=%d", ret);
      hex_dump(row_key.ptr(), row_key.length(), true);
      ret = OB_SEARCH_NOT_FOUND;
    }
  }

  return ret;
}
}//end namespace sstable
}//end namespace sb
