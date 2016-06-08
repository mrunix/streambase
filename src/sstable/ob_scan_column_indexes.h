/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_scan_column_indexes.h
 *
 * Authors:
 *     huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_SCAN_COLUMN_INDEXES_H_
#define OCEANBASE_SSTABLE_SCAN_COLUMN_INDEXES_H_

#include "common/ob_define.h"
#include "common/ob_array_helper.h"

namespace sb {
namespace sstable {
class ObSSTableSchema;
class ObScanColumnIndexes {
 public:
  static const int64_t NOT_EXIST_COLUMN = INT64_MAX;
  static const int64_t ROWKEY_COLUMN = INT64_MAX - 1;
  static const int64_t INVALID_COLUMN = INT64_MAX - 2;
 public:
  ObScanColumnIndexes();
  ~ObScanColumnIndexes();
 public:
  void reset();
  int add_column_id(const int64_t index, const int64_t column_id);
  int get_column_index(const int64_t index, int64_t& column_index) const;
  int get_column_id(const int64_t index, int64_t& column_id) const;
  int get_column(const int64_t index, int64_t& column_id, int64_t& column_index) const;
  inline int64_t get_column_count() const { return column_index_list_.get_array_index(); }

 private:
  int64_t column_index_array_[common::OB_MAX_COLUMN_NUMBER];
  int64_t column_id_array_[common::OB_MAX_COLUMN_NUMBER];
  common::ObArrayHelper<int64_t> column_index_list_;
  common::ObArrayHelper<int64_t> column_id_list_;
};

} // end namespace sstable
} // end namespace sb


#endif //OCEANBASE_SSTABLE_SCAN_COLUMN_INDEXES_H_

