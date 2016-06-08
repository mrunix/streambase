/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_scan_column_indexes.cc
 *
 * Authors:
 *     huating <huating.zmq@taobao.com>
 *
 */

#include "ob_scan_column_indexes.h"
#include "ob_sstable_schema.h"

using namespace sb::common;

namespace sb {
namespace sstable {
ObScanColumnIndexes::ObScanColumnIndexes() {
  memset(column_index_array_, 0, sizeof(column_index_array_));
  column_index_list_.init(OB_MAX_COLUMN_NUMBER, column_index_array_, 0);
  memset(column_id_array_, 0, sizeof(column_id_array_));
  column_id_list_.init(OB_MAX_COLUMN_NUMBER, column_id_array_, 0);
}

ObScanColumnIndexes::~ObScanColumnIndexes() {
}

void ObScanColumnIndexes::reset() {
  memset(column_index_array_, 0, sizeof(column_index_array_));
  column_index_list_.clear();
  memset(column_id_array_, 0, sizeof(column_id_array_));
  column_id_list_.clear();
}

int ObScanColumnIndexes::add_column_id(const int64_t index, const int64_t column_id) {
  int ret = OB_SUCCESS;
  if (index < 0 || column_id < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (get_column_count() >= OB_MAX_COLUMN_NUMBER) {
    ret = OB_SIZE_OVERFLOW;
  } else if (!column_index_list_.push_back(index)) {
    ret = OB_SIZE_OVERFLOW;
  } else if (!column_id_list_.push_back(column_id)) {
    ret = OB_SIZE_OVERFLOW;
  }

  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "add column error, ret = %d, index=%ld, column_id=%ld,"
              "index list size=%ld, id list size =%ld ", ret , index, column_id,
              column_index_list_.get_array_index(), column_id_list_.get_array_index());
  }

  return ret;
}

int ObScanColumnIndexes::get_column_index(const int64_t index, int64_t& column_index) const {
  int ret = OB_SUCCESS;
  column_index = INVALID_COLUMN;
  if (index < 0 || index >= get_column_count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    column_index = *column_index_list_.at(index);
  }

  return ret;
}

int ObScanColumnIndexes::get_column_id(const int64_t index, int64_t& column_id) const {
  int ret = OB_SUCCESS;
  column_id = INVALID_COLUMN;
  if (index < 0 || index >= column_id_list_.get_array_index()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    column_id = *column_id_list_.at(index);
  }

  return ret;
}

int ObScanColumnIndexes::get_column(const int64_t index,
                                    int64_t& column_id, int64_t& column_index) const {
  int ret = OB_SUCCESS;
  column_id = INVALID_COLUMN;
  if (index < 0 || index >= column_id_list_.get_array_index()
      || index >= column_index_list_.get_array_index()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    column_id = *column_id_list_.at(index);
    column_index = *column_index_list_.at(index);
  }

  return ret;
}

} // end namespace sstable
} // end namespace sb



