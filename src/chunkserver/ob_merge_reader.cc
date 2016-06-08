/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_merge_reader.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     maoqi <maoqi@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *
 */

#include "ob_merge_reader.h"
#include "common/ob_define.h"
#include "ob_tablet_image.h"
#include "ob_tablet_manager.h"
#include "ob_tablet.h"
#include "common/utility.h"
#include "sstable/ob_sstable_reader.h"

using namespace sb::common;
using namespace sb::sstable;

namespace sb {
namespace chunkserver {
ObMergeReader::ObMergeReader(ObTabletManager& manager)
  :  initialized_(false), tablet_(NULL),
     tablet_image_(manager.get_serving_tablet_image()),
     manager_(manager) {
}

ObMergeReader::~ObMergeReader() {
  reset();
}

int ObMergeReader::scan(const sb::common::ObScanParam& scan_param) {
  TBSYS_LOG(DEBUG, "ObMergeReader::scan begin");
  int ret = scanner_.set_scan_param(scan_param, manager_.get_serving_block_cache(),
                                    manager_.get_serving_block_index_cache());
  if (OB_SUCCESS == ret) {
    ret = tablet_image_.acquire_tablet(*scan_param.get_range(),
                                       ObMultiVersionTabletImage::SCAN_FORWARD,
                                       scan_param.get_version_range().start_version_, tablet_);
    TBSYS_LOG(DEBUG, "ObMergeReader::scan acquire_tablet, ret=%d", ret);
    if (OB_SUCCESS == ret) tablet_->get_range().hex_dump();
  }

  if (OB_SUCCESS == ret && NULL != tablet_) {
    ObSSTableReader* sstable_reader_list[ObTablet::MAX_SSTABLE_PER_TABLET];
    int32_t size = ObTablet::MAX_SSTABLE_PER_TABLET;
    ret = tablet_->find_sstable(*scan_param.get_range(), sstable_reader_list, size);
    if (OB_SUCCESS == ret) {
      for (int32_t i = 0; i < size ; ++i) {
        ret = scanner_.add_sstable_reader(sstable_reader_list[i]);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(ERROR, "add sstable reader object to seq scanner failed(%d,%p).",
                    i, sstable_reader_list[i]);
          break;
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    initialized_ = true;
  }
  TBSYS_LOG(DEBUG, "ObMergeReader::scan finish, ret=%d,initialized_=%d", ret, initialized_);
  return ret;
}

void ObMergeReader::reset() {
  if (NULL != tablet_) {
    // release tablet;
    tablet_image_.release_tablet(tablet_);
    tablet_ = NULL;
  }

  if (initialized_) {
    scanner_.cleanup();
    initialized_ = false;
  }
}

int ObMergeReader::next_cell() {
  int ret = OB_SUCCESS;
  if (!initialized_) ret = OB_NOT_INIT;
  if (OB_SUCCESS == ret) ret = scanner_.next_cell();
  return ret;
}

int ObMergeReader::get_cell(ObCellInfo** cell) {
  int ret = OB_SUCCESS;
  if (!initialized_) ret = OB_NOT_INIT;
  if (OB_SUCCESS == ret) ret = scanner_.get_cell(cell);
  return ret;
}

int ObMergeReader::get_cell(ObCellInfo** cell, bool* is_row_changed) {
  int ret = OB_SUCCESS;
  if (!initialized_) ret = OB_NOT_INIT;
  if (OB_SUCCESS == ret) ret = scanner_.get_cell(cell, is_row_changed);
  return ret;
}

} // end namespace chunkserver
} // end namespace sb



