/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_sstable_scanner.h
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_SCANNER_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_SCANNER_H_

#include "common/ob_iterator.h"
#include "common/ob_merger.h"
#include "ob_block_index_cache.h"
#include "ob_sstable_scan_param.h"
#include "ob_scan_column_indexes.h"
#include "ob_column_group_scanner.h"

namespace sb {
namespace sstable {
class ObSSTableReader;
class ObBlockIndexCache;
class ObBlockCache;
class ObSSTableSchema;

class ObSSTableScanner : public common::ObMerger {
 public:
  ObSSTableScanner();
  virtual ~ObSSTableScanner();

  /**
   * move cursor to next cell, if no cell remain, return OB_ITER_END
   *
   * @return OB_SUCCESS on success, OB_ITER_END on success and end of data
   * otherwise on failure and error code.
   */
  int next_cell();

  /**
   * get current cell of iterator
   *
   * @param [out] cell info
   * @param [out] is_row_changed set true when row has changed.
   *
   * @return
   *  OB_SUCCESS on success
   *  OB_ITER_END on success and end of data
   *  OB_ERROR on failure.
   */
  int get_cell(sb::common::ObCellInfo** cell);
  int get_cell(sb::common::ObCellInfo** cell, bool* is_row_changed);

  /**
   * set scan parameters like input query range, sstable object.
   *
   * @param context input scan param, query range, columns, table, etc.
   * @param sstable_reader corresponds sstable for scanning.
   * @param block_cache block cache
   * @param block_index_cache block index cache
   *
   * @return
   *  OB_SUCCESS on success otherwise on failure.
   */
  int set_scan_param(
    const sb::common::ObScanParam& scan_param,
    const ObSSTableReader* const sstable_reader,
    ObBlockCache& block_cache,
    ObBlockIndexCache& block_index_cache);

  /**
   * cleanup internal objects, this method must be
   * invoke after scan done when reuse ObSSTableScanner.
   */
  void cleanup();


 private:
  bool column_group_exists(const uint64_t* group_array,
                           const int64_t group_size, const uint64_t group_id) const;

  /**
   * translate column ids in %ObScanParam to
   * column group array.
   *
   * @param scan_param input query column ids
   * @param sstable_reader corresponds sstable.
   * @param [out] group_array translate result.
   * @param [out] group_size size of group_array
   *
   * @return OB_SUCCESS on success otherwise on failure.
   */
  int trans_input_column_id(const common::ObScanParam& scan_param,
                            const ObSSTableSchema* schema, uint64_t* group_array, int64_t& group_size);

  /**
   * special case while column_id is 0, means we get whole row.
   * helper function used by trans_input_column_id.
   */
  int trans_input_whole_row(const common::ObScanParam& scan_param,
                            const ObSSTableSchema* schema, uint64_t* group_array, int64_t& group_size);

  /**
   * store input scan parameters by translate
   * ObScanParam to ObSSTableScanParam.
   * check some border flags like min , max value.
   */
  int trans_input_scan_range(const common::ObScanParam& scan_param) ;

  /**
   * call by set_scan_param, before next_cell or get_cell
   * initialize can reset status of itself,
   */
  int initialize(ObBlockCache& block_cache, ObBlockIndexCache& block_index_cache);

  int set_column_group_scanner(const uint64_t* group_array, const int64_t group_size,
                               const ObSSTableReader* const sstable_reader);

  /**
   * reset all members of scanner.
   * cache objects, scan_param_, base class members;
   * and reset column group scanner
   */
  void destroy_internal_scanners();


 private:
  // hold cache reference
  ObBlockIndexCache* block_index_cache_;
  ObBlockCache* block_cache_;

  ObSSTableScanParam scan_param_;
  char* internal_scanner_obj_ptr_;
  int64_t internal_scanner_obj_count_;

};
}//end namespace sstable
}//end namespace sb

#endif
