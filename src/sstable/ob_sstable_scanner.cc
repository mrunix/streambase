/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_sstable_scanner.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     huating <huating.zmq@taobao.com>
 *
 */
#include "ob_sstable_scanner.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/ob_record_header.h"
#include "ob_sstable_reader.h"
#include "ob_sstable_writer.h"
#include "ob_blockcache.h"

using namespace sb::common;

namespace sb {
namespace sstable {
ObSSTableScanner::ObSSTableScanner()
  : block_index_cache_(NULL),
    block_cache_(NULL),
    internal_scanner_obj_ptr_(NULL),
    internal_scanner_obj_count_(0) {
}

ObSSTableScanner::~ObSSTableScanner() {
  cleanup();
}

void ObSSTableScanner::cleanup() {
  destroy_internal_scanners();
}

void ObSSTableScanner::destroy_internal_scanners() {
  ObColumnGroupScanner* scanner = NULL;

  if (NULL != internal_scanner_obj_ptr_ &&
      0 < internal_scanner_obj_count_) {
    for (int64_t i = 0; i < internal_scanner_obj_count_; ++i) {
      scanner = reinterpret_cast<ObColumnGroupScanner*>(
                  internal_scanner_obj_ptr_ + sizeof(ObColumnGroupScanner) * i);
      scanner->~ObColumnGroupScanner();
    }
    internal_scanner_obj_ptr_ = NULL;
    internal_scanner_obj_count_ = 0;
  }
}

int ObSSTableScanner::initialize(
  ObBlockCache& block_cache, ObBlockIndexCache& block_index_cache) {
  int iret = OB_SUCCESS;

  block_cache_ = &block_cache;
  block_index_cache_ = &block_index_cache;

  // reset members of object.
  scan_param_.reset();
  ObMerger::reset();

  return iret;
}

int ObSSTableScanner::next_cell() {
  return ObMerger::next_cell();
}

int ObSSTableScanner::get_cell(ObCellInfo** cell) {
  return ObMerger::get_cell(cell, NULL);
}

int ObSSTableScanner::get_cell(ObCellInfo** cell, bool* is_row_changed) {
  return ObMerger::get_cell(cell, is_row_changed);
}


int ObSSTableScanner::set_column_group_scanner(
  const uint64_t* group_array, const int64_t group_size,
  const ObSSTableReader* const sstable_reader) {
  int iret = OB_SUCCESS;
  ObColumnGroupScanner* column_group_scanner = NULL;
  uint64_t current_group_id = 0;
  int64_t group_seq = 0;

  internal_scanner_obj_ptr_ = GET_TSI(ModuleArena)->alloc(
                                sizeof(ObColumnGroupScanner) * group_size);
  if (NULL == internal_scanner_obj_ptr_) {
    TBSYS_LOG(ERROR, "alloc memory for column group scanner failed,"
              "group_size=%ld", group_size);
    iret = OB_ALLOCATE_MEMORY_FAILED;
    common::ModuleArena* internal_buffer_arena = GET_TSI(common::ModuleArena);
    TBSYS_LOG(ERROR, "thread local page arena hold memory usage,"
              "total=%ld,used=%ld,pages=%ld", internal_buffer_arena->total(),
              internal_buffer_arena->used(), internal_buffer_arena->pages());
  } else {
    ObMerger::set_asc(!scan_param_.is_reverse_scan());

    for (; group_seq < group_size && OB_SUCCESS == iret; ++group_seq) {
      char* object_ptr = internal_scanner_obj_ptr_ + group_seq * sizeof(ObColumnGroupScanner);
      current_group_id = group_array[group_seq];

      column_group_scanner = new(object_ptr) ObColumnGroupScanner();
      if (OB_SUCCESS != (iret =
                           column_group_scanner->initialize(block_index_cache_, block_cache_))) {
        TBSYS_LOG(ERROR, "column group scanner initialize , iret=%d,"
                  "current_group_id=%ld, group_seq=%ld", iret, current_group_id, group_seq);
      } else if (OB_SUCCESS != (iret =
                                  column_group_scanner->set_scan_param(
                                    current_group_id, group_seq, &scan_param_, sstable_reader))) {
        TBSYS_LOG(ERROR, "column group scanner set scan parameter error, iret=%d,"
                  "current_group_id=%ld, group_seq=%ld", iret, current_group_id, group_seq);
      } else if (OB_SUCCESS != (iret = add_iterator(column_group_scanner))) {
        TBSYS_LOG(ERROR, "add iterator to ObMerger error, iret=%d,"
                  "current_group_id=%ld, group_seq=%ld", iret, current_group_id, group_seq);
      } else {
        TBSYS_LOG(DEBUG, "add %ldth group id=%ld to scan, ret = %d",
                  group_seq, group_array[group_seq], iret);
      }
    }

    internal_scanner_obj_count_ = group_seq;
  }

  return iret;
}

/**
 * 设置scan所用到的参数, 这时会初始化用到的变量
 * @param [in] context scan所用到的外部参数
 * @param [in] sstable_reader SSTableReader
 * @return
 * 1. 成功:
 *    a. OB_SUCCESS
 *    b. OB_BEYOND_THE_RANGE 超出查询范围
 * 2. 错误, 错误码如下:
 *    a. 查询blockindex失败
 */
int ObSSTableScanner::set_scan_param(
  const ObScanParam& scan_param,
  const ObSSTableReader* const sstable_reader,
  ObBlockCache& block_cache,
  ObBlockIndexCache& block_index_cache) {
  int iret = OB_SUCCESS;
  uint64_t group_array[OB_MAX_COLUMN_GROUP_NUMBER];
  int64_t group_size = OB_MAX_COLUMN_GROUP_NUMBER;

  if (NULL == sstable_reader) {
    TBSYS_LOG(ERROR , "invalid argument, sstable_reader is NULL.");
    iret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (iret = initialize(block_cache, block_index_cache))) {
    TBSYS_LOG(ERROR, "initialize column_group_scanner error ret=%d", iret);
  } else if (OB_SUCCESS != (iret = trans_input_scan_range(scan_param))) {
    TBSYS_LOG(ERROR , "trans_input_scan_range error, ret=%d,", iret);
  } else if (OB_SUCCESS != (iret = trans_input_column_id(scan_param,
                                                         sstable_reader->get_schema(), group_array, group_size))) {
    TBSYS_LOG(ERROR, "trans_input_column_id error, ret=%d", iret);
  } else if (OB_SUCCESS != (iret = set_column_group_scanner(
                                     group_array, group_size, sstable_reader))) {
    TBSYS_LOG(ERROR, "set_column_group_scanner error, ret=%d", iret);
  }
  return iret;
}

bool ObSSTableScanner::column_group_exists(const uint64_t* group_array,
                                           const int64_t group_size, const uint64_t group_id) const {
  bool ret = false;
  for (int64_t i = 0; i < group_size; ++i) {
    if (group_array[i] == group_id) {
      ret = true;
      break;
    }
  }
  return ret;
}

int ObSSTableScanner::trans_input_whole_row(
  const common::ObScanParam& scan_param,
  const ObSSTableSchema* schema,
  uint64_t* group_array, int64_t& group_size) {
  int iret = OB_SUCCESS;
  uint64_t table_id = scan_param.get_table_id();

  if (NULL == schema || NULL == group_array || group_size <= 0) {
    TBSYS_LOG(ERROR, "invalid param, schema=%p, table_id=%ld, group_array=%p, group_size=%ld.",
              schema, table_id, group_array, group_size);
    iret = OB_ERROR;
  } else {
    iret = schema->get_table_column_groups_id(table_id, group_array, group_size);
  }

  return iret;
}

int ObSSTableScanner::trans_input_column_id(
  const common::ObScanParam& scan_param,
  const ObSSTableSchema* schema,
  uint64_t* group_array, int64_t& group_size) {
  int iret = OB_SUCCESS;
  int64_t column_index = -1;
  int64_t column_id_size = scan_param.get_column_id_size();
  const uint64_t* const column_id_begin = scan_param.get_column_id();
  if (NULL == schema || NULL == column_id_begin) {
    TBSYS_LOG(ERROR, "invalid arguments, sstable schema "
              "object =%p, column_id =%p", schema, column_id_begin);
    iret = OB_ERROR;
  } else if (column_id_size == 1 && *column_id_begin == 0) {
    iret = trans_input_whole_row(scan_param, schema, group_array, group_size);
  } else {
    int64_t current_group_size = 0;
    uint64_t column_id = OB_INVALID_ID;
    uint64_t column_group_id = OB_INVALID_ID;

    bool has_invalid_column = false;

    for (int64_t i = 0; i < column_id_size && OB_SUCCESS == iret; ++i) {
      column_id = column_id_begin[i];
      column_index = schema->find_offset_first_column_group_schema(
                       scan_param.get_table_id(), column_id, column_group_id);
      if (column_index < 0 || OB_INVALID_ID == column_group_id) {
        has_invalid_column = true;
      } else if (!column_group_exists(group_array, current_group_size, column_group_id)) {
        if (current_group_size > group_size) {
          iret = OB_SIZE_OVERFLOW;
          TBSYS_LOG(ERROR, "input group size =%ld not "
                    "enough with current=%ld, i=%ld, group id=%ld",
                    group_size, current_group_size, i, column_group_id);
          break;
        }
        group_array[current_group_size++] = column_group_id;
      }
    }

    // all columns are invalid column,
    if (0 == current_group_size && has_invalid_column && group_size > 0) {
      const ObSSTableSchemaColumnDef* def = schema->get_column_def(0);
      if (NULL == def) {
        TBSYS_LOG(ERROR, "internal error, sstable has null schema.");
        iret = OB_ERROR;
      } else {
        group_array[current_group_size++] = def->column_group_id_;
      }
    }

    group_size = current_group_size;
  }
  return iret;
}

int ObSSTableScanner::trans_input_scan_range(const ObScanParam& scan_param) {
  int iret = OB_SUCCESS;
  scan_param_.assign(scan_param);

  if (!scan_param_.is_valid()) {
    TBSYS_LOG(ERROR, "input scan parmeter invalid, cannot scan any data.");
    iret = OB_INVALID_ARGUMENT;
  }

  const ObRange& input_range = *scan_param.get_range();
  ObString start_key = input_range.start_key_;
  ObString end_key   = input_range.end_key_;

  char range_buf[OB_RANGE_STR_BUFSIZ];

  if (OB_SUCCESS == iret && input_range.border_flag_.is_min_value()) { // only end_key valid
    if ((!input_range.border_flag_.is_max_value())
        && (NULL == end_key.ptr() || end_key.length() <= 0)) {
      input_range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(ERROR, "invalid end key, rang=%s", range_buf);
      iret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == iret && input_range.border_flag_.is_max_value()) { // only start_key valid
    if ((!input_range.border_flag_.is_min_value())
        && (NULL == start_key.ptr() || start_key.length() <= 0)) {
      input_range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(ERROR, "invalid start key, range=%s", range_buf);
      iret = OB_INVALID_ARGUMENT;
    }
  }

  return iret;
}

}//end namespace sstable
}//end namespace sb
