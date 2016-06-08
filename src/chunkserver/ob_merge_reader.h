/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_merge_reader.h
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     maoqi <maoqi@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_H_
#define OCEANBASE_CHUNKSERVER_H_

#include "common/ob_iterator.h"
#include "sstable/ob_seq_sstable_scanner.h"

namespace sb {
namespace chunkserver {

class ObMultiVersionTabletImage;
class ObTablet;
class ObTabletManager;

class ObMergeReader : public common::ObIterator {
 public:
  ObMergeReader(ObTabletManager& manager);
  virtual ~ObMergeReader() ;
 public:
  // Moves the cursor to next cell.
  // @return OB_SUCCESS if sucess, OB_ITER_END if iter ends, or other error code
  virtual int next_cell();
  // Gets the current cell.
  virtual int get_cell(common::ObCellInfo** cell);
  virtual int get_cell(common::ObCellInfo** cell, bool* is_row_changed);
 public:
  int scan(const sb::common::ObScanParam& scan_param);
  void reset();
 private:
  bool initialized_;
  ObTablet* tablet_;
  ObMultiVersionTabletImage& tablet_image_;
  ObTabletManager& manager_;
  sstable::ObSeqSSTableScanner scanner_;
};
} // end namespace chunkserver
} // end namespace sb


#endif //OCEANBASE_CHUNKSERVER_H_

