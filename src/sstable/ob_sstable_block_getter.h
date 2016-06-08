/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_sstable_block_getter.h
 *
 * Authors:
 *     huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_GETTER_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_GETTER_H_

#include "common/ob_string.h"
#include "common/ob_iterator.h"
#include "ob_sstable_block_reader.h"

namespace sb {
namespace sstable {
class ObScanColumnIndexes;

class ObSSTableBlockGetter : public common::ObIterator {
 public:
  ObSSTableBlockGetter(const ObScanColumnIndexes& column_index);
  ~ObSSTableBlockGetter();

  /**
   * WARNING: this function must be called before function
   * get_cell(), next_cell() -> get_cell() -> next_cell() ->
   * get_cell()
   *
   * @return int
   * 1. success
   *    OB_SUCCESS
   *    OB_ITER_END finish to traverse the current row
   * 2. fail
   *     OB_ERROR
   *     OB_DESERIALIZE_ERROR failed to deserialize obj
   */
  int next_cell();

  /**
   * get current cell. must be called after next_cell()
   *
   * @param cell_info [out] store the cell result
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR or OB_INVALID_ARGUMENT
   */
  int get_cell(common::ObCellInfo** cell_info);

  /**
   * initialize block getter
   *
   * @param row_key row key which the block getter to find
   * @param buf block buffer which include the block data
   * @param data_len block data length
   * @param store_style sstable store style
   *
   * @return int
   * 1. success
   *    OB_SUCCESS
   *    OB_SEARCH_NOT_FOUND not find the row key in block
   * 2. fail
   *     OB_ERROR
   *     OB_INVALID_ARGUMENT invalid arguments
   *     OB_DESERIALIZE_ERROR failed to deserialize obj
   */
  int init(const common::ObString& row_key, const char* buf,
           const int64_t data_len, const int64_t store_style);

 private:
  typedef ObSSTableBlockReader::const_iterator const_iterator;
  typedef ObSSTableBlockReader::iterator iterator;

 private:
  int load_current_row(const_iterator row_index);
  int store_sparse_column(const int64_t column_index);
  int store_current_cell(const int64_t column_id, const int64_t column_index);
  int store_and_advance_column();
  int get_current_column_index(const int64_t cursor,
                               int64_t& column_id, int64_t& column_index) const;
  void clear();

 private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableBlockGetter);

 private:
  static const int64_t DEFAULT_INDEX_BUF_SIZE = 32 * 1024; // 32K

  bool inited_;                     //whether the block getter is initialized
  bool handled_del_row_;            //whether handled the first delete row op
  int64_t sstable_data_store_style_;//sstable store style
  int64_t column_cursor_;           //current column cursor
  int64_t current_column_count_;    //current column count
  const_iterator row_cursor_;       //current row cursor

  common::ObMemBuf index_buf_;      //row position index buffer

  common::ObCellInfo current_cell_info_;//current cell info to return for get_cell()
  common::ObString current_row_key_;  //current row key
  common::ObObj current_ids_[common::OB_MAX_COLUMN_NUMBER];
  common::ObObj current_columns_[common::OB_MAX_COLUMN_NUMBER];

  ObSSTableBlockReader reader_;
  const ObScanColumnIndexes& query_column_indexes_;
};
}//end namespace sstable
}//end namespace sb

#endif  //OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_GETTER_H_
