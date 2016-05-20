/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *         search_sstable.h is for what ...
 *
 *  Version: $Id: search_sstable.h 01/30/2013 05:12:46 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_sstable_scanner.h"
#include "compactsstablev2/ob_sstable_block_cache.h"
#include "compactsstablev2/ob_sstable_block_index_cache.h"
#include "compactsstablev2/ob_compact_sstable_reader.h"
#include "sql/ob_sstable_scan.h"
#include "common/ob_row_desc.h"
#include "common/ob_iterator_adaptor.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "common_func.h"

using sb::common::OB_SUCCESS;
using sb::common::ObRowkey;
using sb::common::ObRow;
using sb::common::ObNewRange;

// --------------------------------------------------------------
// Structures
// --------------------------------------------------------------
enum CmdType {
  ROW_SCAN = 0,
  ROW_GET = 1,
  CELL_SCAN = 2,
  CELL_GET = 3,
  SCAN_CELL = 4,
};

struct CmdLineParam {
  int64_t cmd_type;
  int64_t sstable_version;
  int64_t sstable_file_id;
  int64_t table_id;
  bool quiet;
  bool output;
  bool is_result_cached;
  bool is_async_read;
  const char* scan_range;
  const char* get_rowkey;
  const char* query_columns;
  const char* schema_file;
};


class CacheFactory {
 public:
  static const int64_t THREAD_BUFFER_SIZE = 2 * 1024 * 1024;
 public:
  CacheFactory() :
    compressed_buffer_(THREAD_BUFFER_SIZE),
    uncompressed_buffer_(THREAD_BUFFER_SIZE),
    block_cache_(fic_), block_index_cache_(fic_),
    compact_block_cache_(fic_), compact_block_index_cache_(fic_) {}
  ~CacheFactory() { destroy(); }
  static CacheFactory& get_instance() { return instance_; }
 public:
  void build_scan_context(sb::sql::ScanContext& scan_context) {
    scan_context.block_index_cache_ = &block_index_cache_;
    scan_context.block_cache_ = &block_cache_;
    scan_context.compact_context_.block_index_cache_ = &compact_block_index_cache_;
    scan_context.compact_context_.block_cache_ = &compact_block_cache_;
  }

  void init();
  void destroy();
  static CacheFactory instance_;
  sb::chunkserver::FileInfoCache fic_;
  sb::common::ThreadSpecificBuffer compressed_buffer_;
  sb::common::ThreadSpecificBuffer uncompressed_buffer_;
  sb::sstable::ObBlockCache block_cache_;
  sb::sstable::ObBlockIndexCache block_index_cache_;
  sb::compactsstablev2::ObSSTableBlockCache compact_block_cache_;
  sb::compactsstablev2::ObSSTableBlockIndexCache compact_block_index_cache_;
};

class RowScanOp {
 public:
  RowScanOp();
  ~RowScanOp();
 public:
  int open(const int64_t sstable_file_id, const int64_t sstable_version,
           const sb::sstable::ObSSTableScanParam& param);
  int get_next_row(const sb::common::ObRowkey*& row_key,
                   const sb::common::ObRow*& row_value);
 private:
  sb::common::ModuleArena arena_;
  sb::sstable::ObSSTableReader reader1_;
  sb::compactsstablev2::ObCompactSSTableReader reader2_;
  sb::sql::ObSSTableScanner scanner_;
  sb::compactsstablev2::ObCompactSSTableScanner compact_scanner_;
  sb::sql::ObRowkeyIterator* iterator_;
  sb::sql::ScanContext scan_context_;
};

class CellScanOp {
 public:
  CellScanOp();
  ~CellScanOp();
 public:
  int open(const int64_t sstable_file_id, const int64_t sstable_version,
           const sb::common::ObScanParam& param);
  int get_next_row(const sb::common::ObRowkey*& row_key,
                   const sb::common::ObRow*& row_value);

  inline int next_cell() {
    return scanner_.next_cell();
  }
  inline int get_cell(sb::common::ObCellInfo** cell, bool* is_row_changed) {
    return scanner_.get_cell(cell, is_row_changed);
  }
  inline int is_row_finished(bool* is_row_finished) {
    return scanner_.is_row_finished(is_row_finished);
  }
 private:
  int build_row_desc(const sb::common::ObScanParam& param,
                     const ObSSTableReader& reader);
  sb::common::ModuleArena arena_;
  sb::sstable::ObSSTableReader reader1_;
  sb::sstable::ObSSTableScanner scanner_;
  sb::common::ObRowDesc row_desc_;
  sb::common::ObRowIterAdaptor row_iter_;
};


// --------------------------------------------------------------
// helper functions;
// --------------------------------------------------------------
template <typename ScanParam>
int build_scan_param(const CmdLineParam& clp, ScanParam& param) {
  int ret = OB_SUCCESS;
  ObNewRange range;
  range.table_id_ = clp.table_id;
  int32_t query_column_size = 512;
  int32_t query_column_array[query_column_size];
  if (NULL == clp.query_columns || NULL == clp.scan_range || 0 >= clp.table_id) {
    fprintf(stderr, "invalid table=%ld, scan_range=%s, query_columns=%s\n",
            clp.table_id, clp.scan_range, clp.query_columns);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = parse_range_str(clp.scan_range, 1, range))) {
    fprintf(stderr, "parse_range_str ret=%d\n", ret);
  } else if (OB_SUCCESS != (ret = parse_number_range(clp.query_columns, query_column_array, query_column_size))) {
    fprintf(stderr, "parse query_column ret=%d\n", ret);
  } else {
    fprintf(stderr, "query range = %s\n", to_cstring(range));
    param.set_range(range);
    param.set_is_result_cached(clp.is_result_cached);
    if (clp.is_async_read) {
      param.set_read_mode(sb::common::ScanFlag::ASYNCREAD);
    } else {
      param.set_read_mode(sb::common::ScanFlag::SYNCREAD);
    }
    param.set_scan_direction(sb::common::ScanFlag::FORWARD);

    for (int64_t i = 0; OB_SUCCESS == ret && i < query_column_size; i++) {
      if (OB_SUCCESS != (ret = param.add_column(query_column_array[i]))) {
        fprintf(stderr, "scan param add column i=%ld, fail:ret[%d]\n", i, ret);
      }
    }

  }

  return ret;
}

template <typename ScanOp, typename ScanParam>
int scan_sstable_output_row(const CmdLineParam& clp) {
  int ret = OB_SUCCESS;
  ScanParam param;
  ScanOp scan_op;
  const ObRowkey* key = NULL;
  const ObRow* value = NULL;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  int64_t row_count = 0;
  if (OB_SUCCESS != (ret = build_scan_param(clp, param))) {
    fprintf(stderr, "build param range=%s, query columns=%s, err=%d\n",
            clp.scan_range, clp.query_columns, ret);
  } else if (OB_SUCCESS != (ret = scan_op.open(clp.sstable_file_id, clp.sstable_version, param))) {
    fprintf(stderr, "open scan op range=%s, query columns=%s, err=%d\n",
            clp.scan_range, clp.query_columns, ret);
  } else {
    while (OB_SUCCESS == (ret = scan_op.get_next_row(key, value))) {
      if (clp.output)
        fprintf(stderr, "get row:%s, value:%s\n", to_cstring(*key), to_cstring(*value));
      /*
      if (clp.output)
      {
        value->get_rowkey(key);
        fprintf(stderr, "value rowkey cell count:%ld, key:%s\n",
            value->get_row_desc()->get_rowkey_cell_count(), to_cstring(*key));
      }
      */

      ++row_count;
    }
    int64_t end_time = tbsys::CTimeUtil::getTime();
    fprintf(stderr, "scan row count=%ld, consume time =%ld\n", row_count, end_time - start_time);
  }
  return 0;
}


