/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * row_checker.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "common/ob_crc64.h"
#include "common/ob_define.h"
#include "updateserver/ob_ups_utils.h"
#include "cellinfo_builder.h"
#include "utils.h"
#include "row_checker.h"

using namespace sb;
using namespace common;
using namespace updateserver;

RowChecker::RowChecker() {
  read_result_.create(OB_MAX_COLUMN_NUMBER);
  last_is_del_row_ = false;
  rowkey_read_set_.create(RK_SET_SIZE);
  add_rowkey_times_ = 0;
}

RowChecker::~RowChecker() {
}

void RowChecker::add_cell(const ObCellInfo* ci) {
  ObString rowkey_info;
  rowkey_info.assign_ptr(ROWKEY_INFO_ROWKEY, strlen(ROWKEY_INFO_ROWKEY));
  if (rowkey_info != ci->row_key_) {
    ObCellInfo* new_ci = (ObCellInfo*)ci_mem_tank_.alloc(sizeof(ObCellInfo));
    *new_ci = *ci;
    ci_mem_tank_.write_obj(ci->value_, &(new_ci->value_));
    if (SEED_COLUMN_ID == ci->column_id_
        && NULL == cur_row_key_.ptr()) {
      cur_row_key_.assign_ptr(NULL, 0);
      ci_mem_tank_.write_string(ci->row_key_, &cur_row_key_);
    }
    CellinfoBuilder::merge_obj(CellinfoBuilder::UPDATE, new_ci, read_result_);
    last_is_del_row_ = (ObActionFlag::OP_DEL_ROW == ci->value_.get_ext());
  }
}

int64_t RowChecker::cell_num() const {
  return read_result_.size();
}

bool RowChecker::check_row(CellinfoBuilder& cb, const ObSchema& schema) {
  bool bret = true;
  ObCellInfo* seed_ci = NULL;
  read_result_.get(SEED_COLUMN_ID, seed_ci);
  if (NULL == seed_ci) {
    TBSYS_LOG(WARN, "not seed column cellinfo [%s]", ::print_cellinfo(seed_ci));
    bret = false;
  } else {
    int64_t seed_end = 0;
    seed_ci->value_.get_int(seed_end);
    CellinfoBuilder::result_set_t check_result;
    check_result.create(OB_MAX_COLUMN_NUMBER);
    PageArena<char> allocer;
    cb.get_result(cur_row_key_, schema, SEED_START + 1, seed_end, check_result, allocer);

    CellinfoBuilder::result_iter_t iter;
    for (iter = read_result_.begin(); iter != read_result_.end(); ++iter) {
      if (SEED_COLUMN_ID == iter->second->column_id_
          || C_TIME_COLUMN_ID == iter->second->column_id_
          || M_TIME_COLUMN_ID == iter->second->column_id_) {
        continue;
      } else {
        ObCellInfo* tmp_ci = NULL;
        check_result.get(iter->first, tmp_ci);
        if (NULL == tmp_ci) {
          if (ObNullType != iter->second->value_.get_type()) {
            TBSYS_LOG(WARN, "value not equal [%s] [%s]", ::print_cellinfo(iter->second), ::print_cellinfo(tmp_ci));
            bret = false;
            break;
          }
        } else if (tmp_ci->value_.get_type() == ObExtendType
                   && tmp_ci->value_.get_ext() == iter->second->value_.get_ext()) {
          continue;
        } else if (!(tmp_ci->value_ == iter->second->value_)) {
          TBSYS_LOG(WARN, "value not equal [%s] [%s]", ::print_cellinfo(iter->second), ::print_cellinfo(tmp_ci));
          bret = false;
          break;
        }
      }
    }
    if (last_is_del_row_) {
      TBSYS_LOG(INFO, "[row_is_deleted]");
    }
  }
  //assert(bret);
  read_result_.clear();
  ci_mem_tank_.clear();
  cur_row_key_.assign_ptr(NULL, 0);
  last_is_del_row_ = false;
  return bret;
}

const ObString& RowChecker::get_cur_rowkey() const {
  return cur_row_key_;
}

bool RowChecker::is_prefix_changed(const ObString& row_key) {
  bool bret = false;
  if (NULL == last_row_key_.ptr() && NULL != row_key.ptr()) {
    bret = true;
  } else if (NULL != last_row_key_.ptr() && NULL == row_key.ptr()) {
    bret = true;
  } else if (0 != memcmp(last_row_key_.ptr(), row_key.ptr(), RowkeyBuilder::I64_STR_LENGTH)) {
    bret = true;
  }
  return bret;
}

void RowChecker::add_rowkey(const ObString& row_key) {
  ObString rowkey_info;
  rowkey_info.assign_ptr(ROWKEY_INFO_ROWKEY, strlen(ROWKEY_INFO_ROWKEY));
  if (rowkey_info != row_key) {
    ObString new_rk;
    rk_mem_tank_.write_string(row_key, &new_rk);
    rowkey_read_set_.set(new_rk, 1, 1);
    last_row_key_ = new_rk;
    add_rowkey_times_++;
  }
}

bool RowChecker::check_rowkey(RowkeyBuilder& rb, const int64_t* prefix_ptr) {
  bool bret = true;
  PageArena<char> allocer;
  ObHashMap<ObString, uint64_t> rowkey_check_set;
  rowkey_check_set.create(RK_SET_SIZE);
  bool prefix_changed = true;
  for (int64_t i = 0; i < add_rowkey_times_; i++) {
    ObString rowkey_check = rb.get_rowkey4checkall(allocer, prefix_changed, prefix_ptr);
    prefix_changed = false;
    if (HASH_OVERWRITE_SUCC == rowkey_check_set.set(rowkey_check, 1, 1)) {
      i--;
    }
  }

  ObHashMap<ObString, uint64_t>::iterator iter;
  for (iter = rowkey_read_set_.begin(); iter != rowkey_read_set_.end(); iter++) {
    uint64_t tmp = 0;
    if (HASH_EXIST != rowkey_check_set.get(iter->first, tmp)) {
      bret = false;
      TBSYS_LOG(WARN, "read_rowkey=[%.*s] not found in check_set",
                iter->first.length(), iter->first.ptr());
    } else {
      rowkey_check_set.erase(iter->first);
    }
  }
  if (bret) {
    if (0 != rowkey_check_set.size()) {
      bret = false;
      for (iter = rowkey_check_set.begin(); iter != rowkey_check_set.end(); iter++) {
        TBSYS_LOG(WARN, "rowkey=[%.*s] not in result_set", iter->first.length(), iter->first.ptr());
      }
    }
  }
  rowkey_read_set_.clear();
  add_rowkey_times_ = 0;
  return bret;
}

const ObString& RowChecker::get_last_rowkey() const {
  return last_row_key_;
}

int64_t RowChecker::rowkey_num() const {
  return rowkey_read_set_.size();
}


