/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_seq_sstable_scanner.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     huating <huating.zmq@taobao.com>
 *
 */

#include "ob_seq_sstable_scanner.h"
#include "common/ob_read_common_data.h"
#include "ob_sstable_reader.h"
#include "ob_sstable_scanner.h"

using namespace sb::common;

namespace sb {
namespace sstable {

ObSeqSSTableScanner::ObSeqSSTableScanner()
  : cur_iter_idx_(0), cur_iter_status_(NOT_START),
    block_cache_(NULL), block_index_cache_(NULL) {
  memset(sstable_readers_, 0, sizeof(sstable_readers_));
  sstable_array_.init(MAX_SSTABLE_SCANNER_NUM, sstable_readers_, 0);
}

ObSeqSSTableScanner::~ObSeqSSTableScanner() {
  cleanup();
}

void ObSeqSSTableScanner::cleanup() {
  sstable_scanner_.cleanup();
}

// ObSeqSSTableScanner stored pointer of ObScanParam but not param's value
// this usage maybe a problem when user free %param before call next_cell
// TODO: ObSeqSSTableScanner need store %param for itself.
int ObSeqSSTableScanner::set_scan_param(
  const common::ObScanParam& param,
  ObBlockCache& block_cache,
  ObBlockIndexCache& block_index_cache) {
  int ret = OB_SUCCESS;

  if (NULL == param.get_range() || param.get_range()->empty()) {
    TBSYS_LOG(ERROR, "input scan param range is empty, cannot scan.");
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG) {
      char range_buf[OB_RANGE_STR_BUFSIZ];
      param.get_range()->to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(DEBUG, "scan input param range, range=%s", range_buf);
    }

    block_cache_ = &block_cache;
    block_index_cache_ = &block_index_cache;
    scan_param_.safe_copy(param);
    cur_iter_idx_ = 0;
    cur_iter_status_ = NOT_START;
    sstable_array_.clear();
  }

  return ret;
}

int ObSeqSSTableScanner::add_sstable_reader(ObSSTableReader* reader) {
  int ret = OB_SUCCESS;
  if (NULL == reader) {
    TBSYS_LOG(ERROR, "add sstable reader error reader is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else if (sstable_array_.get_array_index() >= MAX_SSTABLE_SCANNER_NUM) {
    TBSYS_LOG(ERROR, "add sstable reader error, sstable reader array full, size=%ld",
              sstable_array_.get_array_index());
    ret = OB_SIZE_OVERFLOW;
  } else if (!sstable_array_.push_back(reader)) {
    TBSYS_LOG(ERROR, "add sstable reader push to sstable array error, size=%ld",
              sstable_array_.get_array_index());
    ret = OB_SIZE_OVERFLOW;
  }


  return ret;
}

int ObSeqSSTableScanner::next_cell() {
  int ret = OB_SUCCESS;
  // has no sstable readers, return OB_ITER_END for empty dataset;
  if (OB_UNLIKELY(sstable_array_.get_array_index() == 0)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(cur_iter_idx_ < 0 || cur_iter_idx_ > sstable_array_.get_array_index())) {
    TBSYS_LOG(ERROR, "cur_iter_idx_ = %d > sstable array size=%ld",
              cur_iter_idx_, sstable_array_.get_array_index());
    ret = OB_ERROR_OUT_OF_RANGE;
  } else {
    switch (cur_iter_status_) {
    case NOT_START:
      cur_iter_idx_ = 0;
      ret = sstable_scanner_.set_scan_param(scan_param_,
                                            *sstable_array_.at(cur_iter_idx_), *block_cache_, *block_index_cache_);
      TBSYS_LOG(DEBUG, "begin to scan first sstable id =%ld, ret=%d",
                (*sstable_array_.at(cur_iter_idx_))->get_sstable_id().sstable_file_id_, ret);
      if (OB_SUCCESS == ret)
        ret = sstable_scanner_.next_cell();
      cur_iter_status_ = PROGRESS;
      break;
    case PROGRESS:
      ret = sstable_scanner_.next_cell();
      break;
    case END_OF_DATA:
      ret = OB_ITER_END;
      break;
    case ERROR_STATUS:
    default:
      ret = OB_ERROR;
      break;
    }

    if (OB_ITER_END == ret) {
      // skip to next scanner;
      ++cur_iter_idx_;
      if (PROGRESS == cur_iter_status_
          && cur_iter_idx_ < sstable_array_.get_array_index()) {
        sstable_scanner_.cleanup();
        ret = sstable_scanner_.set_scan_param(scan_param_,
                                              *sstable_array_.at(cur_iter_idx_), *block_cache_, *block_index_cache_);
        TBSYS_LOG(DEBUG, "begin to scan %d th sstable id =%ld, ret=%d", cur_iter_idx_,
                  (*sstable_array_.at(cur_iter_idx_))->get_sstable_id().sstable_file_id_, ret);
        if (OB_SUCCESS == ret)
          ret = sstable_scanner_.next_cell();
      } else {
        ret = OB_ITER_END;
        cur_iter_status_ = END_OF_DATA;
      }
    }

    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      cur_iter_status_ = ERROR_STATUS;
    }

  }
  return ret;
}


int ObSeqSSTableScanner::get_cell(ObCellInfo** cell) {
  bool is_row_changed = false;
  return get_cell(cell, &is_row_changed);
}

int ObSeqSSTableScanner::get_cell(ObCellInfo** cell, bool* is_row_changed) {
  int ret = OB_SUCCESS;
  if (cell) *cell = NULL;
  if (NULL == cell) {
    ret = OB_INVALID_ARGUMENT;
  }

  if (sstable_array_.get_array_index() <= 0
      || cur_iter_idx_ < 0
      || cur_iter_idx_ > sstable_array_.get_array_index()) {
    ret = OB_ERROR_OUT_OF_RANGE;
  }

  if (OB_SUCCESS == ret) {
    if (PROGRESS == cur_iter_status_)
      ret = sstable_scanner_.get_cell(cell, is_row_changed);
    else
      ret = OB_ERROR;
  }

  return ret;
}

} // end namespace sstable
} // end namespace sb



