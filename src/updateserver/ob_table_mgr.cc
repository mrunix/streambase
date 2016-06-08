/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_mgr.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "common/ob_atomic.h"
#include "common/ob_probability_random.h"
#include "sstable/ob_sstable_trailer.h"
#include "sstable/ob_sstable_block_builder.h"
#include "ob_table_mgr.h"
#include "ob_update_server_main.h"

namespace sb {
namespace updateserver {
using namespace common;
using namespace hash;

ColumnFilter* ITableEntity::build_columnfilter(const ObScanParam& scan_param, ColumnFilter* column_filter) {
  ColumnFilter* ret = column_filter;
  if (NULL != column_filter) {
    int64_t column_size = scan_param.get_column_id_size();
    const uint64_t* cf_ids = scan_param.get_column_id();
    if (NULL == cf_ids) {
      ret = NULL;
    } else {
      column_filter->clear();
      for (int64_t i = 0; i < column_size; ++i) {
        if (OB_SUCCESS != column_filter->add_column(cf_ids[i])) {
          TBSYS_LOG(WARN, "failed to add the %ld-th column, cf_id=%lu", i, cf_ids[i]);
          ret = NULL;
          break;
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

MemTableEntityIterator::MemTableEntityIterator() : memtable_iter_(), is_multi_update_(false) {
}

MemTableEntityIterator::~MemTableEntityIterator() {
}

int MemTableEntityIterator::next_cell() {
  return memtable_iter_.next_cell();
}

int MemTableEntityIterator::get_cell(ObCellInfo** cell_info) {
  return memtable_iter_.get_cell(cell_info);
}

int MemTableEntityIterator::get_cell(ObCellInfo** cell_info, bool* is_row_changed) {
  return memtable_iter_.get_cell(cell_info, is_row_changed);
}

void MemTableEntityIterator::reset() {
  memtable_iter_.reset();
  is_multi_update_ = false;
}

MemTableIterator& MemTableEntityIterator::get_memtable_iter() {
  return memtable_iter_;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

MemTableUtils::MemTableUtils() : table_iter_(), trans_handle_() {
}

MemTableUtils::~MemTableUtils() {
}

ITableIterator& MemTableUtils::get_table_iter() {
  return table_iter_;
}

TableTransHandle& MemTableUtils::get_trans_handle() {
  return trans_handle_;
}

void MemTableUtils::reset() {
  table_iter_.reset();
}

////////////////////////////////////////////////////////////////////////////////////////////////////

MemTableEntity::MemTableEntity(TableItem& table_item) : ITableEntity(table_item), memtable_() {
  memtable_.inc_ref_cnt();
}

MemTableEntity::~MemTableEntity() {
}

ITableUtils* MemTableEntity::get_tsi_tableutils(const int64_t index) {
  MemTableUtils* ret = NULL;
  TableUtilsSet* table_utils_set = GET_TSI(TableUtilsSet);
  if (NULL != table_utils_set
      && index < MAX_TABLE_UTILS_NUM) {
    ret = &(table_utils_set->data[index]);
  } else {
    TBSYS_LOG(WARN, "get tsi table utils fail table_utils_set=%p index=%ld", table_utils_set, index);
  }
  return ret;
}

int MemTableEntity::get(TableTransHandle& trans_handle,
                        const uint64_t table_id,
                        const ObString& row_key,
                        ColumnFilter* column_filter,
                        ITableIterator* iter) {
  int ret = OB_SUCCESS;
  MemTableEntityIterator* sub_iter = dynamic_cast<MemTableEntityIterator*>(iter);
  sub_iter->reset();
  if (NULL == sub_iter) {
    TBSYS_LOG(WARN, "invalid param sub_iter=%p", sub_iter);
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool is_multi_update = false;
    ret = memtable_.get(trans_handle, table_id, row_key, sub_iter->get_memtable_iter(), is_multi_update, column_filter);
    sub_iter->set_multi_update(is_multi_update);
  }
  return ret;
}

int MemTableEntity::scan(TableTransHandle& trans_handle,
                         const ObScanParam& scan_param,
                         ITableIterator* iter) {
  int ret = OB_SUCCESS;
  MemTableEntityIterator* sub_iter = dynamic_cast<MemTableEntityIterator*>(iter);
  sub_iter->reset();
  const ObRange* scan_range = scan_param.get_range();
  ColumnFilter* column_filter = get_tsi_columnfilter();
  if (NULL == sub_iter
      || NULL == scan_range) {
    TBSYS_LOG(WARN, "invalid param sub_iter=%p scan_range=%p", sub_iter, scan_range);
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool reverse = (scan_param.get_scan_direction() == ObScanParam::BACKWARD);
    column_filter = build_columnfilter(scan_param, column_filter);
    ret = memtable_.scan(trans_handle, *scan_range, reverse, sub_iter->get_memtable_iter(), column_filter);
  }
  return ret;
}

int MemTableEntity::start_transaction(TableTransHandle& trans_handle) {
  int ret = OB_SUCCESS;
  ret = memtable_.start_transaction(READ_TRANSACTION, trans_handle);
  return ret;
}

int MemTableEntity::end_transaction(TableTransHandle& trans_handle) {
  int ret = OB_SUCCESS;
  ret = memtable_.end_transaction(trans_handle);
  return ret;
}

MemTable& MemTableEntity::get_memtable() {
  return memtable_;
}

void MemTableEntity::ref() {
  memtable_.inc_ref_cnt();
}

void MemTableEntity::deref() {
  if (0 == memtable_.dec_ref_cnt()) {
    SSTableID sst_id = memtable_.get_version();
    memtable_.clear();
    TBSYS_LOG(INFO, "clear memtable succ %s", sst_id.log_str());
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

SSTableEntityIterator::SSTableEntityIterator() : get_param_(), sstable_iter_(NULL), column_filter_(NULL), cur_ci_ptr_(NULL),
  is_row_changed_(false),
  row_has_changed_(false) {
}


//SSTableEntityIterator::SSTableEntityIterator() : get_param_(), sstable_iter_(NULL), column_filter_(NULL), cur_ci_ptr_(NULL),
//                                                 is_iter_end_(false), is_row_changed_(false), row_has_expected_column_(true),
//                                                 row_has_returned_column_(false),
//                                                 need_not_next_(false), is_sstable_iter_end_(false)
//{
//  cell_info_.column_id_ = OB_INVALID_ID;
//  cell_info_.value_.set_ext(ObActionFlag::OP_NOP);
//}

SSTableEntityIterator::~SSTableEntityIterator() {
}

int SSTableEntityIterator::next_cell() {
  int ret = OB_SUCCESS;
  if (NULL == sstable_iter_
      || NULL == column_filter_) {
    TBSYS_LOG(WARN, "invalid sstable_iter=%p column_filter=%p", sstable_iter_, column_filter_);
    ret = OB_ERROR;
  } else {
    while (OB_SUCCESS == (ret = sstable_iter_->next_cell())) {
      if (OB_SUCCESS != (ret = sstable_iter_->get_cell(&cur_ci_ptr_, &is_row_changed_))
          || NULL == cur_ci_ptr_) {
        ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
        break;
      }
      row_has_changed_ = is_row_changed_ ? true : row_has_changed_;
      // sstable不接受column_id为OB_INVALID_ID所以转成了0
      // 迭代出来后需要再修改成OB_INVALID_ID
      if (OB_FULL_ROW_COLUMN_ID == cur_ci_ptr_->column_id_) {
        cur_ci_ptr_->column_id_ = OB_INVALID_ID;
      }
      if (column_filter_->column_exist(cur_ci_ptr_->column_id_)) {
        is_row_changed_ = row_has_changed_;
        row_has_changed_ = false;
        break;
      }
    }
  }
  return ret;
}

//int SSTableEntityIterator::next_cell()
//{
//  int ret = OB_SUCCESS;
//  if (NULL == sstable_iter_
//      || NULL == column_filter_)
//  {
//    TBSYS_LOG(WARN, "invalid sstable_iter=%p column_filter=%p", sstable_iter_, column_filter_);
//    ret = OB_ERROR;
//  }
//  else if (is_iter_end_)
//  {
//    ret = OB_ITER_END;
//  }
//  else
//  {
//    while (OB_SUCCESS == ret)
//    {
//      if (need_not_next_
//          || OB_SUCCESS == (ret = sstable_iter_->next_cell()))
//      {
//        need_not_next_ = false;
//        cur_ci_ptr_ = NULL;
//        if (OB_SUCCESS == (ret = sstable_iter_->get_cell(&cur_ci_ptr_, &is_row_changed_))
//            && NULL != cur_ci_ptr_)
//        {
//          if (is_row_changed_)
//          {
//            row_has_returned_column_ = false;
//            if (!row_has_expected_column_)
//            {
//              // 如果上一行没有期待的列被过滤出 那么break
//              // 构造一个NOP
//              // 下一次不需要调用next_cell
//              need_not_next_ = true;
//              break;
//            }
//            else
//            {
//              // 否则表示新的一行开始 深拷贝这行的rowkey 并将row_has_expected_column_置为false
//              cell_info_.table_id_ = cur_ci_ptr_->table_id_;
//              string_buf_.reset();
//              if (OB_SUCCESS != (ret = string_buf_.write_string(cur_ci_ptr_->row_key_, &(cell_info_.row_key_))))
//              {
//                TBSYS_LOG(WARN, "write row_key fail ret=%d cell_info=[%s]", ret, print_cellinfo(cur_ci_ptr_));
//                break;
//              }
//              row_has_expected_column_ = false;
//            }
//          }
//          if (column_filter_->column_exist(cur_ci_ptr_->column_id_))
//          {
//            if (!row_has_returned_column_)
//            {
//              is_row_changed_ = true;
//            }
//            if (OB_FULL_ROW_COLUMN_ID != cur_ci_ptr_->column_id_)
//            {
//              row_has_expected_column_ = true;
//            }
//            row_has_returned_column_ = true;
//            break;
//          }
//          if (OB_FULL_ROW_COLUMN_ID == cur_ci_ptr_->column_id_)
//          {
//            row_has_returned_column_ = true;
//            break;
//          }
//        }
//        else
//        {
//          ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
//          break;
//        }
//      }
//      else
//      {
//        ret = (!need_not_next_ && is_sstable_iter_end_) ? OB_ITER_END : ret;
//      }
//    }
//    is_iter_end_ = (OB_ITER_END == ret);
//    is_sstable_iter_end_ = (OB_ITER_END == ret);
//    // (有新行开始 或 迭代结束) && 没有期待的列返回 那么需要构造一个NOP
//    if (((!row_has_returned_column_ && OB_SUCCESS == ret) || is_iter_end_)
//        && !row_has_expected_column_)
//    {
//      cur_ci_ptr_ = &cell_info_;
//      is_iter_end_ = false;
//      row_has_expected_column_ = true;
//      is_row_changed_ = true;
//      ret = OB_SUCCESS;
//    }
//  }
//  TBSYS_LOG(DEBUG, "this=%p ret=%d", this, ret);
//  return ret;
//}

int SSTableEntityIterator::get_cell(ObCellInfo** cell_info) {
  return get_cell(cell_info, NULL);
}

int SSTableEntityIterator::get_cell(ObCellInfo** cell_info, bool* is_row_changed) {
  int ret = OB_SUCCESS;
  if (NULL == cell_info) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == cur_ci_ptr_) {
    ret = OB_ERROR;
  } else {
    *cell_info = cur_ci_ptr_;
    if (NULL != is_row_changed) {
      *is_row_changed = is_row_changed_;
    }
    TBSYS_LOG(DEBUG, "this=%p %s is_row_changed=%s", this, print_cellinfo(*cell_info), STR_BOOL(is_row_changed_));
  }
  return ret;
}

void SSTableEntityIterator::reset() {
  sstable_iter_ = NULL;
  column_filter_ = NULL;
  cur_ci_ptr_ = NULL;
  //is_iter_end_ = false;
  is_row_changed_ = false;
  row_has_changed_ = false;
  //row_has_expected_column_ = true;
  //need_not_next_ = false;
  //is_sstable_iter_end_ = false;
  sst_scanner_.cleanup();
}

ObGetParam& SSTableEntityIterator::get_get_param() {
  return get_param_;
}

void SSTableEntityIterator::set_sstable_iter(ObIterator* sstable_iter) {
  sstable_iter_ = sstable_iter;
}

void SSTableEntityIterator::set_column_filter(ColumnFilter* column_filter) {
  column_filter_ = column_filter;
}

sstable::ObSSTableGetter& SSTableEntityIterator::get_sstable_getter() {
  return sst_getter_;
}

sstable::ObSeqSSTableScanner& SSTableEntityIterator::get_sstable_scanner() {
  return sst_scanner_;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

SSTableUtils::SSTableUtils() : table_iter_(), trans_handle_() {
}

SSTableUtils::~SSTableUtils() {
}

ITableIterator& SSTableUtils::get_table_iter() {
  return table_iter_;
}

TableTransHandle& SSTableUtils::get_trans_handle() {
  return trans_handle_;
}

void SSTableUtils::reset() {
  table_iter_.reset();
}

////////////////////////////////////////////////////////////////////////////////////////////////////

SSTableEntity::SSTableEntity(TableItem& table_item)
  : ITableEntity(table_item), sstable_id_(0), mod_(ObModIds::OB_SSTABLE_READER),
    allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_), sstable_reader_(NULL) {
}

SSTableEntity::~SSTableEntity() {
  destroy_sstable_meta();
}

ITableUtils* SSTableEntity::get_tsi_tableutils(const int64_t index) {
  SSTableUtils* ret = NULL;
  TableUtilsSet* table_utils_set = GET_TSI(TableUtilsSet);
  if (NULL != table_utils_set
      && index < MAX_TABLE_UTILS_NUM) {
    ret = &(table_utils_set->data[index]);
  } else {
    TBSYS_LOG(WARN, "get tsi table utils fail table_utils_set=%p index=%ld", table_utils_set, index);
  }
  return ret;
}

int SSTableEntity::get(TableTransHandle& trans_handle,
                       const uint64_t table_id,
                       const ObString& row_key,
                       ColumnFilter* column_filter,
                       ITableIterator* iter) {
  UNUSED(trans_handle);
  int ret = OB_SUCCESS;
  SSTableEntityIterator* sub_iter = dynamic_cast<SSTableEntityIterator*>(iter);
  sub_iter->reset();
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(ERROR, "get ups main fail");
    ret = OB_ERROR;
  } else if (NULL == sstable_reader_) {
    TBSYS_LOG(WARN, "invalid sstable_reader sstable_id=%lu", sstable_id_);
    ret = OB_ERROR;
  } else if (NULL == column_filter
             || NULL == sub_iter) {
    TBSYS_LOG(WARN, "invalid param column_filter=%p sub_iter=%p", column_filter, sub_iter);
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObTransferSSTableQuery& sstable_query = ups_main->get_update_server().get_sstable_query();
    ObGetParam& get_param = sub_iter->get_get_param();
    get_param.reset();
    ObCellInfo cell_info;
    cell_info.table_id_ = table_id;
    cell_info.row_key_ = row_key;
    cell_info.column_id_ = OB_FULL_ROW_COLUMN_ID;
    ObIterator* sstable_iter = &(sub_iter->get_sstable_getter());
    get_param.set_is_result_cached(true);
    if (OB_SUCCESS != (ret = get_param.add_only_one_cell(cell_info))) {
      TBSYS_LOG(WARN, "add cell to get_param fail ret=%d cell_info=[%s]", ret, print_cellinfo(&cell_info));
    } else if (OB_SUCCESS != (ret = sstable_query.get(get_param, *sstable_reader_, sstable_iter))
               || NULL == sstable_iter) {
      TBSYS_LOG(WARN, "sstable query get fail ret=%d sstable_iter=%p cell_info=[%s]", ret, sstable_iter, print_cellinfo(&cell_info));
    } else {
      sub_iter->set_sstable_iter(sstable_iter);
      sub_iter->set_column_filter(column_filter);
    }
  }
  return ret;
}

int SSTableEntity::scan(TableTransHandle& trans_handle,
                        const ObScanParam& scan_param,
                        ITableIterator* iter) {
  UNUSED(trans_handle);
  int ret = OB_SUCCESS;
  SSTableEntityIterator* sub_iter = dynamic_cast<SSTableEntityIterator*>(iter);
  sub_iter->reset();
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  ColumnFilter* column_filter = get_tsi_columnfilter();
  if (NULL == ups_main) {
    TBSYS_LOG(ERROR, "get ups main fail");
    ret = OB_ERROR;
  } else if (NULL == sstable_reader_) {
    TBSYS_LOG(WARN, "invalid sstable_reader=%p", sstable_reader_);
    ret = OB_ERROR;
  } else if (NULL == (column_filter = build_columnfilter(scan_param, column_filter))) {
    TBSYS_LOG(WARN, "build column_filter fail");
    ret = OB_ERROR;
  } else if (NULL == sub_iter) {
    TBSYS_LOG(WARN, "invalid sub_iter=%p");
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObScanParam scan_whole_row;
    scan_whole_row.safe_copy(scan_param);
    scan_whole_row.clear_column();
    scan_whole_row.add_column(OB_FULL_ROW_COLUMN_ID);
    ObTransferSSTableQuery& sstable_query = ups_main->get_update_server().get_sstable_query();
    ObIterator* sstable_iter = &(sub_iter->get_sstable_scanner());
    if (OB_SUCCESS != (ret = sstable_query.scan(scan_whole_row, *sstable_reader_, sstable_iter))
        || NULL == sstable_iter) {
      TBSYS_LOG(WARN, "sstable query scan fail ret=%d sstable_iter=%p", ret, sstable_iter);
    } else {
      sub_iter->set_sstable_iter(sstable_iter);
      sub_iter->set_column_filter(column_filter);
    }
  }
  return ret;
}

int SSTableEntity::start_transaction(TableTransHandle& trans_handle) {
  int ret = OB_SUCCESS;
  UNUSED(trans_handle);
  return ret;
}

int SSTableEntity::end_transaction(TableTransHandle& trans_handle) {
  int ret = OB_SUCCESS;
  UNUSED(trans_handle);
  return ret;
}

uint64_t SSTableEntity::get_sstable_id() const {
  return sstable_id_;
}

void SSTableEntity::set_sstable_id(const uint64_t sstable_id) {
  sstable_id_ = sstable_id;
}

void SSTableEntity::ref() {
}

void SSTableEntity::deref() {
}

int SSTableEntity::init_sstable_meta(ITableEntity::SSTableMeta& sst_meta) {
  int ret = OB_SUCCESS;
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == sstable_reader_) {
    if (NULL == ups_main) {
      TBSYS_LOG(ERROR, "get ups main fail");
      ret = OB_ERROR;
    } else {
      SSTableMgr& sstable_mgr = ups_main->get_update_server().get_sstable_mgr();
      char* buffer = allocator_.alloc(sizeof(sstable::ObSSTableReader));
      sstable_reader_ = new(buffer) sstable::ObSSTableReader(allocator_, sstable_mgr);
    }
  } else {
    sstable_reader_->reset();
  }
  SSTableID sst_id = sstable_id_;
  sstable::ObSSTableId ob_sstable_id(sstable_id_);
  if (NULL == sstable_reader_) {
    TBSYS_LOG(WARN, "invalid sstable_reader %s", sst_id.log_str());
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = sstable_reader_->open(ob_sstable_id))) {
    destroy_sstable_meta();
    TBSYS_LOG(WARN, "sstable_reader open fail ret=%d %s", ret, sst_id.log_str());
  } else {
    TBSYS_LOG(INFO, "init sstable meta succ %s sstable_reader=%p", sst_id.log_str(), sstable_reader_);
    sstable::ObBlockIndexPositionInfo info;
    info.sstable_file_id_ = sstable_id_;
    info.offset_ = sstable_reader_->get_trailer().get_block_index_record_offset();
    info.size_   = sstable_reader_->get_trailer().get_block_index_record_size();
    sst_meta.time_stamp = sstable_reader_->get_trailer().get_frozen_time();
  }
  return ret;
}

void SSTableEntity::destroy_sstable_meta() {
  if (NULL != sstable_reader_) {
    using namespace sstable;
    sstable_reader_->~ObSSTableReader();
    sstable_reader_ = NULL;
    allocator_.free();
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

TableItem::TableItem() : memtable_entity_(*this), sstable_entity_(*this), stat_(UNKNOW),
  ref_cnt_(1), clog_id_(OB_INVALID_ID), row_iter_(), time_stamp_(0),
  sstable_loaded_time_(0) {
}

TableItem::~TableItem() {
}

ITableEntity* TableItem::get_table_entity(int64_t& sstable_percent) {
  ITableEntity* ret = NULL;
  if (ACTIVE <= stat_
      && DUMPED >= stat_) {
    if (DUMPED != stat_
        || 0 == sstable_percent) {
      ret = &memtable_entity_;
    } else {
      int32_t percents[2] = {sstable_percent, 100 - sstable_percent};
      int32_t index = ObStalessProbabilityRandom::random(percents, 2, 100);
      switch (index) {
      case 0:
        ret = &sstable_entity_;
        break;
      case 1:
        ret = &memtable_entity_;
        break;
      default:
        TBSYS_LOG(WARN, "invalid percent index=%d sstable_percent=%ld", index, sstable_percent);
        ret = &memtable_entity_;
        break;
      }
    }
    ret = &memtable_entity_;
  } else if (DROPING == stat_
             || DROPED == stat_) {
    ret = &sstable_entity_;
  } else {
    ret = NULL;
  }
  return ret;
}

MemTable& TableItem::get_memtable() {
  if (DROPING < stat_) {
    TBSYS_LOG(WARN, "invalid stat=%d for get memtable", stat_);
  }
  return memtable_entity_.get_memtable();
}

int TableItem::init_sstable_meta() {
  int ret = OB_SUCCESS;
  ITableEntity::SSTableMeta sst_meta;
  ret = sstable_entity_.init_sstable_meta(sst_meta);
  if (OB_SUCCESS == ret) {
    time_stamp_ = sst_meta.time_stamp;
    sstable_loaded_time_ = tbsys::CTimeUtil::getTime();
  }
  return ret;
}

int64_t TableItem::get_time_stamp() const {
  return time_stamp_;
}

int64_t TableItem::get_sstable_loaded_time() const {
  return sstable_loaded_time_;
}

TableItem::Stat TableItem::get_stat() const {
  return stat_;
}

void TableItem::set_stat(const TableItem::Stat stat) {
  if (UNKNOW == stat_) {
    if (ACTIVE == stat
        || DROPED == stat) {
      stat_ = stat;
    }
  }
}

void TableItem::set_sstable_id(const uint64_t sstable_id) {
  sstable_entity_.set_sstable_id(sstable_id);
  memtable_entity_.get_memtable().set_version(sstable_id);
}

uint64_t TableItem::get_sstable_id() const {
  return sstable_entity_.get_sstable_id();
}

int TableItem::do_freeze(const uint64_t clog_id, const int64_t time_stamp) {
  int ret = OB_SUCCESS;
  if (FREEZING != stat_) {
    TBSYS_LOG(WARN, "invalid status=%d for do_freeze", stat_);
    ret = OB_ERROR;
  } else {
    ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
    if (NULL == ups_main) {
      TBSYS_LOG(WARN, "get ups main fail");
      ret = OB_ERROR;
    } else if (OB_INVALID_ID == clog_id) {
      TBSYS_LOG(WARN, "invalid param clog_id=%lu", clog_id);
      ret = OB_INVALID_ARGUMENT;
    } else {
      const char* compressor_name = ups_main->get_update_server().get_param().get_sstable_compressor_name();
      int64_t block_size = ups_main->get_update_server().get_param().get_sstable_block_size();
      if (NULL == compressor_name
          || 0 == strlen(compressor_name)) {
        compressor_name = DEFAULT_COMPRESSOR_NAME;
      }
      if (0 >= block_size) {
        block_size = ObUpdateServerParam::DEFAULT_SSTABLE_BLOCK_SIZE;
      }
      if (OB_SUCCESS != (ret = row_iter_.init(&(memtable_entity_.get_memtable()), compressor_name, block_size))) {
        TBSYS_LOG(WARN, "row_iter set schema handle fail ret=%d", ret);
      } else {
        TBSYS_LOG(INFO, "memtable rowiter init succ compressor_name=[%s] block_size=%ld",
                  compressor_name, block_size);
      }
    }
    clog_id_ = clog_id;
    time_stamp_ = time_stamp;
    stat_ = FROZEN;
  }
  return ret;
}

int TableItem::freeze_memtable() {
  int ret = OB_SUCCESS;
  if (ACTIVE != stat_) {
    TBSYS_LOG(WARN, "invalid status=%d for freeze", stat_);
    ret = OB_ERROR;
  } else {
    stat_ = FREEZING;
  }
  return ret;
}

int TableItem::do_dump() {
  int ret = OB_SUCCESS;
  if (DUMPING != stat_) {
    TBSYS_LOG(WARN, "invalid status=%d for do_dump", stat_);
    ret = OB_ERROR;
  } else {
    ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
    if (NULL == ups_main) {
      TBSYS_LOG(ERROR, "get ups main fail");
      ret = OB_ERROR;
    } else {
      SSTableMgr& sstable_mgr = ups_main->get_update_server().get_sstable_mgr();
      if (OB_SUCCESS != (ret = sstable_mgr.add_sstable(sstable_entity_.get_sstable_id(), clog_id_, time_stamp_, row_iter_))) {
        TBSYS_LOG(ERROR, "add sstable fail ret=%d", ret);
      } else if (OB_SUCCESS != (ret = this->init_sstable_meta())) {
        TBSYS_LOG(ERROR, "init sstable_meta fail ret=%d", ret);
      } else {
        SSTableID sst_id = sstable_entity_.get_sstable_id();
        ups_main->get_update_server().get_log_mgr().write_replay_point(clog_id_);
        TBSYS_LOG(INFO, "dump sstable succ ret=%d %s", ret, sst_id.log_str());
        stat_ = DUMPED;
      }
    }
    if (OB_SUCCESS != ret) {
      // dump 失败重新修改为frozen状态
      stat_ = FROZEN;
    }
  }
  return ret;
}

bool TableItem::dump_memtable() {
  bool bret = false;
  if (FROZEN == stat_) {
    bret = true;
    stat_ = DUMPING;
  }
  return bret;
}

int TableItem::do_drop() {
  int ret = OB_SUCCESS;
  if (DROPING != stat_) {
    TBSYS_LOG(WARN, "invalid status=%d for do_drop", stat_);
    ret = OB_ERROR;
  } else {
    row_iter_.destroy();
    memtable_entity_.deref();
    stat_ = DROPED;
  }
  return ret;
}

bool TableItem::drop_memtable() {
  bool bret = false;
  if (DUMPED == stat_) {
    bret = true;
    stat_ = DROPING;
  }
  return bret;
}

bool TableItem::erase_sstable() {
  bool bret = false;
  if (DUMPED <= stat_) {
    sstable_entity_.destroy_sstable_meta();
    SSTableID sst_id = get_sstable_id();
    TBSYS_LOG(INFO, "erase sstable succ table_item_stat=%d %s", stat_, sst_id.log_str());
    if (DROPED == stat_) {
      bret = true;
    } else {
      // 如果还没有drop 则修改状态为FROZEN 可以重新写sstable
      stat_ = FROZEN;
    }
  }
  return bret;
}

int64_t TableItem::inc_ref_cnt() {
  return atomic_inc((uint64_t*)&ref_cnt_);
}

int64_t TableItem::dec_ref_cnt() {
  return atomic_dec((uint64_t*)&ref_cnt_);
}

////////////////////////////////////////////////////////////////////////////////////////////////////

TableMgr::TableMgr() : inited_(false),
  sstable_scan_finished_(false),
  table_map_(sizeof(TableItemKey)),
  cur_major_version_(SSTableID::START_MAJOR_VERSION),
  cur_minor_version_(SSTableID::START_MINOR_VERSION),
  active_table_item_(NULL),
  frozen_memused_(0),
  frozen_memtotal_(0),
  frozen_rowcount_(0),
  last_major_freeze_time_(0),
  cur_warm_up_percent_(0) {
}

TableMgr::~TableMgr() {
  destroy();
}

int TableMgr::init() {
  int ret = OB_SUCCESS;
  if (inited_) {
    TBSYS_LOG(WARN, "have already inited");
    ret = OB_INIT_TWICE;
  } else if (NULL == (active_table_item_ = table_allocator_.allocate())) {
    TBSYS_LOG(WARN, "allocate table item fail");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = active_table_item_->get_memtable().init())) {
    TBSYS_LOG(WARN, "init memtable fail ret=%d", ret);
    table_allocator_.deallocate(active_table_item_);
  } else {
    active_table_item_->get_memtable().get_attr(memtable_attr_);
    memtable_attr_.extern_mem_total = this;
    active_table_item_->get_memtable().set_attr(memtable_attr_);
    active_table_item_->set_stat(TableItem::ACTIVE);
    cur_major_version_ = SSTableID::START_MAJOR_VERSION;
    cur_minor_version_ = SSTableID::START_MINOR_VERSION;
    sstable_scan_finished_ = false;
    frozen_memused_ = 0;
    frozen_memtotal_ = 0;
    frozen_rowcount_ = 0;
    inited_ = true;
  }
  return ret;
}

void TableMgr::destroy() {
  if (inited_) {
    if (true) {
      BtreeReadHandle handle;
      int btree_ret = ERROR_CODE_OK;
      if (ERROR_CODE_OK != (btree_ret = table_map_.get_read_handle(handle))) {
        TBSYS_LOG(WARN, "get read handle fail ret=%d", btree_ret);
      } else {
        table_map_.set_key_range(handle, table_map_.get_min_key(), 0, table_map_.get_max_key(), 0);
        TableItem* table_item = NULL;
        while (ERROR_CODE_OK == table_map_.get_next(handle, table_item)) {
          if (NULL != table_item) {
            table_allocator_.deallocate(table_item);
          }
        }
      }
    }
    table_map_.destroy();
    if (NULL != active_table_item_
        && !sstable_scan_finished_) {
      table_allocator_.deallocate(active_table_item_);
      active_table_item_ = NULL;
    }
    inited_ = false;
  }
  return;
}

int TableMgr::add_sstable(const uint64_t sstable_id) {
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_ID == sstable_id) {
    TBSYS_LOG(WARN, "invalid param sstable_id=%lu", sstable_id);
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (table_item = table_allocator_.allocate())) {
    TBSYS_LOG(WARN, "allocate table item fail");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_item->get_memtable().init())) {
    TBSYS_LOG(WARN, "init memtable fail ret=%d", ret);
    table_allocator_.deallocate(table_item);
  } else {
    table_item->get_memtable().set_attr(memtable_attr_);
    table_item->set_sstable_id(sstable_id);
    table_item->set_stat(TableItem::DROPED);
    if (OB_SUCCESS != (ret = table_item->init_sstable_meta())) {
      TBSYS_LOG(WARN, "init sstable meta fail ret=%d sstable_id=%lu", ret, sstable_id);
      table_allocator_.deallocate(table_item);
    } else {
      map_lock_.wrlock();
      const SSTableID sst_id = sstable_id;
      int btree_ret = table_map_.put(sst_id, table_item, false);
      if (ERROR_CODE_OK != btree_ret) {
        if (ERROR_CODE_KEY_REPEAT != btree_ret) {
          TBSYS_LOG(WARN, "put table item fail sstable_id=%lu", sstable_id);
          ret = OB_ERROR;
        } else {
          TableItem* tmp_table_item = NULL;
          table_map_.get(sst_id, tmp_table_item);
          if (NULL != tmp_table_item
              && TableItem::FROZEN == tmp_table_item->get_stat()) {
            tmp_table_item->set_stat(TableItem::DUMPED);
          }
        }
        table_allocator_.deallocate(table_item);
      } else {
        if (!sstable_scan_finished_) {
          if (cur_major_version_ < sst_id.major_version) {
            cur_major_version_ = sst_id.major_version;
            cur_minor_version_ = sst_id.minor_version_end;
          } else {
            if (cur_major_version_ ==  sst_id.major_version
                && cur_minor_version_ < sst_id.minor_version_end) {
              cur_minor_version_ = sst_id.minor_version_end;
            }
          }
        }
        TBSYS_LOG(INFO, "add sstable succ %s", sst_id.log_str());
      }
      map_lock_.unlock();
    }
  }
  return ret;
}

int TableMgr::erase_sstable(const uint64_t sstable_id) {
  int ret = OB_SUCCESS;
  int btree_ret = ERROR_CODE_OK;
  TableItem* table_item = NULL;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_ID == sstable_id) {
    TBSYS_LOG(WARN, "invalid param sstable_id=%lu", sstable_id);
    ret = OB_INVALID_ARGUMENT;
  } else {
    map_lock_.wrlock();
    const SSTableID sst_id = sstable_id;
    if (ERROR_CODE_OK != (btree_ret = table_map_.get(sst_id, table_item))
        || NULL == table_item
        || sstable_id != table_item->get_sstable_id()) {
      TBSYS_LOG(WARN, "get from table_map fail sstable_id=%lu table_item_sstable_id=%lu",
                sstable_id, (NULL == table_item) ? OB_INVALID_ID : table_item->get_sstable_id());
      ret = OB_ERROR;
    } else {
      if (table_item->erase_sstable()) {
        table_map_.remove(sst_id);
        TBSYS_LOG(INFO, "erase sstable, remove from map %s", sst_id.log_str());
        if (0 == table_item->dec_ref_cnt()) {
          table_allocator_.deallocate(table_item);
          TBSYS_LOG(INFO, "erase sstable, delete table_item=%p %s", table_item, sst_id.log_str());
        }
      }
    }
    map_lock_.unlock();
  }
  return ret;
}

int TableMgr::sstable_scan_finished(const int64_t minor_num_limit) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
    ret = OB_NOT_INIT;
  } else {
    map_lock_.wrlock();
    if (sstable_scan_finished_) {
      TBSYS_LOG(INFO, "sstable scan has already finished");
    } else {
      SSTableID sst_id;
      uint64_t tmp_major_version = cur_major_version_;
      uint64_t tmp_minor_version = cur_minor_version_;
      if (0 == table_map_.get_object_count()) {
        // do not modify version
      } else if ((uint64_t)minor_num_limit < (tmp_minor_version + 1)
                 || SSTableID::MAX_MINOR_VERSION < (tmp_minor_version + 1)) {
        tmp_major_version += 1;
        tmp_minor_version = SSTableID::START_MINOR_VERSION;
      } else {
        tmp_minor_version += 1;
      }
      sst_id.major_version = tmp_major_version;
      sst_id.minor_version_start = tmp_minor_version;
      sst_id.minor_version_end = tmp_minor_version;
      active_table_item_->set_sstable_id(SSTableID::get_id(tmp_major_version, tmp_minor_version, tmp_minor_version));
      int btree_ret = ERROR_CODE_OK;
      if (ERROR_CODE_OK != (btree_ret = table_map_.put(sst_id, active_table_item_, false))) {
        TBSYS_LOG(WARN, "put active_table_item to table_map fail ret=%d sstable_id=%lu", btree_ret, sst_id.id);
        ret = OB_ERROR;
      } else {
        cur_major_version_ = tmp_major_version;
        cur_minor_version_ = tmp_minor_version;
        sstable_scan_finished_ = true;
      }
    }
    map_lock_.unlock();
    if (OB_SUCCESS == ret) {
      ret = check_sstable_id();
    }
  }
  return ret;
}

int TableMgr::check_sstable_id() {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
    ret = OB_NOT_INIT;
  } else {
    map_lock_.rdlock();
    BtreeReadHandle handle;
    int btree_ret = ERROR_CODE_OK;
    if (ERROR_CODE_OK != (btree_ret = table_map_.get_read_handle(handle))) {
      TBSYS_LOG(WARN, "get read handle fail ret=%d", btree_ret);
      ret = OB_ERROR;
    } else {
      SSTableID sst_id_checker = 0;
      table_map_.set_key_range(handle, table_map_.get_min_key(), 0, table_map_.get_max_key(), 0);
      TableItem* table_item = NULL;
      while (ERROR_CODE_OK == table_map_.get_next(handle, table_item)) {
        if (NULL != table_item) {
          SSTableID cur_sst_id = table_item->get_sstable_id();
          if (!sst_id_checker.continous(cur_sst_id)) {
            TBSYS_LOG(WARN, "sstable id do not continous %s <--> %s", sst_id_checker.log_str(), cur_sst_id.log_str());
            ret = OB_ERROR;
            break;
          } else {
            sst_id_checker = table_item->get_sstable_id();
          }
        }
      }
    }
    map_lock_.unlock();
  }
  return ret;
}

int TableMgr::get_table_time_stamp(const uint64_t major_version, int64_t& time_stamp) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
    ret = OB_NOT_INIT;
  } else if (cur_major_version_ <= major_version) {
    TBSYS_LOG(WARN, "invalid major_version=%ld cur_major_version=%ld",
              major_version, cur_major_version_);
    ret = OB_UPS_INVALID_MAJOR_VERSION;
  } else {
    TableItemKey start_key;
    TableItemKey end_key;
    TableItemKey* start_key_ptr = &start_key;
    TableItemKey* end_key_ptr = &end_key;
    start_key.sst_id.major_version = major_version;
    start_key.sst_id.minor_version_start = SSTableID::MAX_MINOR_VERSION;
    start_key.sst_id.minor_version_end = SSTableID::MAX_MINOR_VERSION;
    end_key.sst_id.major_version = major_version;
    end_key.sst_id.minor_version_start = SSTableID::START_MINOR_VERSION;
    end_key.sst_id.minor_version_end = SSTableID::START_MINOR_VERSION;
    int start_exclude = 0;
    int end_exclude = 0;
    map_lock_.rdlock();
    BtreeReadHandle handle;
    int btree_ret = ERROR_CODE_OK;
    if (ERROR_CODE_OK != (btree_ret = table_map_.get_read_handle(handle))) {
      TBSYS_LOG(WARN, "get read handle fail ret=%d", btree_ret);
      ret = OB_ERROR;
    } else {
      table_map_.set_key_range(handle, start_key_ptr, start_exclude, end_key_ptr, end_exclude);
      TableItemKey key;
      TableItem* table_item = NULL;
      ret = OB_UPS_INVALID_MAJOR_VERSION;
      while (ERROR_CODE_OK == table_map_.get_next(handle, key, table_item)) {
        if (NULL != table_item) {
          time_stamp = table_item->get_time_stamp();
          ret = OB_SUCCESS;
        }
        break;
      }
    }
    map_lock_.unlock();
  }
  return ret;
}

int TableMgr::acquire_table(const ObVersionRange& version_range,
                            uint64_t& max_version,
                            TableList& table_list) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
    ret = OB_NOT_INIT;
  } else {
    SSTableID sst_id_start;
    SSTableID sst_id_end;
    sst_id_start.major_version = version_range.start_version_;
    sst_id_end.major_version = version_range.end_version_;
    int start_exclude = 1;
    int end_exclude = 1;
    if (version_range.border_flag_.inclusive_start()) {
      start_exclude = 0;
      sst_id_start.minor_version_start = SSTableID::START_MINOR_VERSION;
      sst_id_start.minor_version_end = SSTableID::START_MINOR_VERSION;
    } else {
      sst_id_start.minor_version_start = SSTableID::MAX_MINOR_VERSION;
      sst_id_start.minor_version_end = SSTableID::MAX_MINOR_VERSION;
    }
    if (version_range.border_flag_.inclusive_end()) {
      end_exclude = 0;
      sst_id_end.minor_version_start = SSTableID::MAX_MINOR_VERSION;
      sst_id_end.minor_version_end = SSTableID::MAX_MINOR_VERSION;
    } else {
      sst_id_end.minor_version_start = SSTableID::START_MINOR_VERSION;
      sst_id_end.minor_version_end = SSTableID::START_MINOR_VERSION;
    }
    if (version_range.border_flag_.is_min_value()
        || (sst_id_start.major_version > sst_id_end.major_version
            && !version_range.border_flag_.is_max_value())) {
      TBSYS_LOG(WARN, "invalid version_range=%s", range2str(version_range));
      ret = OB_UPS_ACQUIRE_TABLE_FAIL;
    } else {
      bool first = true;
      TableItem* table_item = NULL;
      table_list.clear();
      map_lock_.rdlock();
      BtreeReadHandle handle;
      int btree_ret = ERROR_CODE_OK;
      if (ERROR_CODE_OK != (btree_ret = table_map_.get_read_handle(handle))) {
        TBSYS_LOG(WARN, "get read handle fail ret=%d", btree_ret);
        ret = OB_UPS_ACQUIRE_TABLE_FAIL;
      } else {
        int64_t warm_up_percent = get_warm_up_percent_();
        SSTableID sst_id_checker = 0;
        TableItemKey start_key = sst_id_start;
        TableItemKey end_key = sst_id_end;
        TableItemKey* start_key_ptr = &start_key;
        TableItemKey* end_key_ptr = &end_key;
        if (version_range.border_flag_.is_max_value()) {
          end_key_ptr = table_map_.get_max_key();
        }
        TableItemKey key;
        table_map_.set_key_range(handle, start_key_ptr, start_exclude, end_key_ptr, end_exclude);
        while (ERROR_CODE_OK == table_map_.get_next(handle, key, table_item)) {
          if (first
              && key.sst_id.major_version != (uint64_t)version_range.start_version_) {
            TBSYS_LOG(WARN, "invalid version_range=[%s] min_major_version=%lu",
                      range2str(version_range), key.sst_id.major_version);
            ret = OB_INVALID_START_VERSION;
            break;
          }
          ITableEntity* table_entity = NULL;
          if (NULL == table_item
              || NULL == (table_entity = table_item->get_table_entity(warm_up_percent))) {
            TBSYS_LOG(WARN, "invalid table_item sstable_id=%lu", key.sst_id.id);
            ret = OB_UPS_ACQUIRE_TABLE_FAIL;
            break;
          } else if (active_table_item_ == table_item
                     && !version_range.border_flag_.is_max_value()) {
            TBSYS_LOG(WARN, "maybe acquire an active table for daily merge, will fail, version_range=[%s]", range2str(version_range));
            ret = OB_UPS_TABLE_NOT_FROZEN;
            break;
          } else if (0 != table_list.push_back(table_entity)) {
            TBSYS_LOG(WARN, "push to list fail sstable_id=%lu", key.sst_id.id);
            ret = OB_UPS_ACQUIRE_TABLE_FAIL;
            break;
          } else {
            table_entity->ref();
            table_item->inc_ref_cnt();
            max_version = key.sst_id.major_version;
            if (!sst_id_checker.continous(key.sst_id)) {
              TBSYS_LOG(WARN, "sstable id do not continous %s <--> %s", sst_id_checker.log_str(), key.sst_id.log_str());
              ret = OB_UPS_ACQUIRE_TABLE_FAIL;
              break;
            } else {
              sst_id_checker = key.sst_id;
            }
          }
          first = false;
        }
      }
      map_lock_.unlock();
      if (OB_SUCCESS != ret) {
        revert_table(table_list);
        table_list.clear();
      }
    }
  }
  return ret;
}

void TableMgr::revert_table(const TableList& table_list) {
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
  } else {
    TableList::const_iterator iter;
    for (iter = table_list.begin(); iter != table_list.end(); iter++) {
      ITableEntity* table_entity = *iter;
      if (NULL != table_entity) {
        table_entity->deref();
        map_lock_.rdlock();
        TableItem* table_item = &(table_entity->get_table_item());
        SSTableID sst_id = table_item->get_sstable_id();
        if (0 == table_entity->get_table_item().dec_ref_cnt()) {
          table_allocator_.deallocate(table_item);
          TBSYS_LOG(INFO, "erase sstable, delete table_item=%p %s", &table_item, sst_id.log_str());
        }
        map_lock_.unlock();
      }
    }
  }
  return;
}

TableItem* TableMgr::get_active_memtable() {
  TableItem* ret = NULL;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
  } else {
    map_lock_.rdlock();
    if (NULL == active_table_item_) {
      TBSYS_LOG(WARN, "active_table_item null pointer");
    } else {
      ret = active_table_item_;
      ret->get_memtable().inc_ref_cnt();
      ret->inc_ref_cnt();
    }
    map_lock_.unlock();
  }
  return ret;
}

void TableMgr::revert_active_memtable(TableItem* table_item) {
  revert_memtable_(table_item);
}

void TableMgr::revert_memtable_(TableItem* table_item) {
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
  } else if (NULL == table_item) {
    TBSYS_LOG(WARN, "invalid param table_item=%p", table_item);
  } else {
    SSTableID sst_id = table_item->get_memtable().get_version();
    if (0 == table_item->get_memtable().dec_ref_cnt()) {
      table_item->get_memtable().clear();
      TBSYS_LOG(INFO, "clear memtable succ %s", sst_id.log_str());
    }
    map_lock_.rdlock();
    if (0 == table_item->dec_ref_cnt()) {
      table_allocator_.deallocate(table_item);
      TBSYS_LOG(INFO, "erase sstable, delete table_item=%p %s", table_item, sst_id.log_str());
    }
    map_lock_.unlock();
  }
  return;
}

int TableMgr::replay_freeze_memtable(const uint64_t new_version,
                                     const uint64_t frozen_version,
                                     const uint64_t clog_id,
                                     const int64_t time_stamp) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_ID == new_version
             || OB_INVALID_ID == clog_id) {
    TBSYS_LOG(WARN, "invalid param new_version=%lu clog_id=%lu", new_version, clog_id);
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == active_table_item_) {
    TBSYS_LOG(WARN, "invalid active_table_item");
    ret = OB_ERROR;
  } else {
    map_lock_.wrlock();
    if (0 == active_table_item_->get_sstable_id()) {
      // 处理初始化后第一次遇到freeze日志的情况
      SSTableID sst_id = frozen_version;
      int btree_ret = table_map_.put(sst_id, active_table_item_, false);
      if (ERROR_CODE_OK == btree_ret) {
        active_table_item_->set_sstable_id(frozen_version);
      }
      sstable_scan_finished_ = true;
      TBSYS_LOG(INFO, "first replay frozen log %s btree_ret=%d", sst_id.log_str(), btree_ret);
    }
    TableItem* table_item2freeze = freeze_active_(new_version);
    if (NULL == table_item2freeze) {
      TBSYS_LOG(WARN, "freeze memtable fail");
      ret = OB_ERROR;
    } else if (0 == table_item2freeze->get_memtable().size()) {
      // 对于大小为0的table直接删除
      SSTableID sst_id = table_item2freeze->get_sstable_id();
      int btree_ret = table_map_.remove(sst_id);
      bool deallocated = false;
      if (0 == table_item2freeze->dec_ref_cnt()) {
        table_allocator_.deallocate(table_item2freeze);
        deallocated = true;
      }
      table_item2freeze = NULL;
      TBSYS_LOG(INFO, "drop empty frozen table %s btree_ret=%d deallocated=%s",
                sst_id.log_str(), btree_ret, STR_BOOL(deallocated));
    } else {
      table_item2freeze->inc_ref_cnt();
    }
    map_lock_.unlock();
    if (NULL != table_item2freeze) {
      SSTableID sst_id = table_item2freeze->get_sstable_id();
      if (OB_SUCCESS != (ret = table_item2freeze->do_freeze(clog_id, time_stamp))) {
        TBSYS_LOG(ERROR, "do freeze fail ret=%d clog_id=%lu", ret, clog_id);
      } else {
        frozen_memused_ += table_item2freeze->get_memtable().used();
        frozen_memtotal_ += table_item2freeze->get_memtable().total();
        frozen_rowcount_ += table_item2freeze->get_memtable().size();
        TBSYS_LOG(INFO, "replay freeze succ frozen_memused=%ld frozen_memtotal=%ld frozen_rowcount=%ld %s",
                  frozen_memused_, frozen_memtotal_, frozen_rowcount_, sst_id.log_str());
      }
      map_lock_.rdlock();
      if (0 == table_item2freeze->dec_ref_cnt()) {
        table_allocator_.deallocate(table_item2freeze);
        TBSYS_LOG(INFO, "erase sstable, delete table_item=%p %s", table_item2freeze, sst_id.log_str());
      }
      map_lock_.unlock();
    }
  }
  return ret;
}

int TableMgr::try_freeze_memtable(const int64_t mem_limit, const int64_t num_limit,
                                  const int64_t min_major_freeze_interval,
                                  uint64_t& new_version, uint64_t& frozen_version,
                                  uint64_t& clog_id, int64_t& time_stamp,
                                  bool& major_version_changed) {
  int ret = OB_SUCCESS;
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
    ret = OB_NOT_INIT;
  } else if (NULL == active_table_item_) {
    TBSYS_LOG(WARN, "invalid active_table_item");
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get ups_main fail");
    ret = OB_ERROR;
  } else if (0 >= active_table_item_->get_memtable().total()) {
    //        TBSYS_LOG(INFO, "active_mem_total=%ld need not freeze",
    //                  active_table_item_->get_memtable().total());
    ret = OB_EAGAIN;
  } else if (mem_limit >= active_table_item_->get_memtable().total()) {
    ret = OB_EAGAIN;
  } else if (((uint64_t)num_limit < (cur_minor_version_ + 1)
              || SSTableID::MAX_MINOR_VERSION < (cur_minor_version_ + 1))
             && (tbsys::CTimeUtil::getTime() - last_major_freeze_time_) < min_major_freeze_interval) {
    ret = OB_EAGAIN;
    TBSYS_LOG(WARN, "major freeze interval too small last_major_freeze_time=%ld cur_time=%ld min_major_freeze_interval=%ld",
              last_major_freeze_time_, tbsys::CTimeUtil::getTime(), min_major_freeze_interval);
  } else if (OB_SUCCESS != (ret = ups_main->get_update_server().get_log_mgr().switch_log_file(clog_id))) {
    TBSYS_LOG(WARN, "switch commit log fail ret=%d", ret);
  } else {
    map_lock_.wrlock();
    uint64_t tmp_major_version = cur_major_version_;
    uint64_t tmp_minor_version = cur_minor_version_;
    TableItem* table_item2freeze = NULL;
    if ((uint64_t)num_limit < (tmp_minor_version + 1)
        || SSTableID::MAX_MINOR_VERSION < (tmp_minor_version + 1)) {
      tmp_major_version += 1;
      tmp_minor_version = SSTableID::START_MINOR_VERSION;
      major_version_changed = true;
    } else {
      tmp_minor_version += 1;
      major_version_changed = false;
    }
    if (OB_SUCCESS == ret) {
      frozen_version = SSTableID::get_id(cur_major_version_, cur_minor_version_, cur_minor_version_);
      new_version = SSTableID::get_id(tmp_major_version, tmp_minor_version, tmp_minor_version);
      table_item2freeze = freeze_active_(new_version);
    }
    if (NULL == table_item2freeze) {
      TBSYS_LOG(WARN, "freeze memtable fail");
      ret = OB_ERROR;
    } else {
      table_item2freeze->inc_ref_cnt();
    }
    map_lock_.unlock();
    if (NULL != table_item2freeze) {
      SSTableID sst_id = table_item2freeze->get_sstable_id();
      time_stamp = tbsys::CTimeUtil::getTime();
      if (OB_SUCCESS != (ret = table_item2freeze->do_freeze(clog_id, time_stamp))) {
        TBSYS_LOG(ERROR, "do freeze fail ret=%d clog_id=%lu", ret, clog_id);
      } else {
        if (major_version_changed) {
          last_major_freeze_time_ = tbsys::CTimeUtil::getTime();
        }
        frozen_memused_ += table_item2freeze->get_memtable().used();
        frozen_memtotal_ += table_item2freeze->get_memtable().total();
        frozen_rowcount_ += table_item2freeze->get_memtable().size();
        TBSYS_LOG(INFO, "freeze succ last_major_freeze_time_=%ld frozen_memused=%ld frozen_memtotal=%ld frozen_rowcount=%ld %s",
                  last_major_freeze_time_, frozen_memused_, frozen_memtotal_, frozen_rowcount_, sst_id.log_str());
      }
      map_lock_.rdlock();
      if (0 == table_item2freeze->dec_ref_cnt()) {
        table_allocator_.deallocate(table_item2freeze);
        TBSYS_LOG(INFO, "erase sstable, delete table_item=%p %s", table_item2freeze, sst_id.log_str());
      }
      map_lock_.unlock();
    }
  }
  return ret;
}

TableItem* TableMgr::freeze_active_(const uint64_t new_version) {
  TableItem* table_item2freeze = NULL;
  TableItem* tmp_table_item = table_allocator_.allocate();
  if (NULL != tmp_table_item) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = tmp_table_item->get_memtable().init())) {
      TBSYS_LOG(WARN, "init memtable fail ret=%d", tmp_ret);
    } else {
      tmp_table_item->get_memtable().set_attr(memtable_attr_);
      tmp_table_item->set_stat(TableItem::ACTIVE);
      tmp_table_item->set_sstable_id(new_version);
      SSTableID sst_id = new_version;
      int btree_ret = ERROR_CODE_OK;
      if (OB_SUCCESS != (tmp_ret = active_table_item_->freeze_memtable())) {
        TBSYS_LOG(WARN, "freeze memtable fail ret=%d", tmp_ret);
      } else if (ERROR_CODE_OK != (btree_ret = table_map_.put(sst_id, tmp_table_item, false))) {
        TBSYS_LOG(WARN, "put to table_map fail ret=%d %s", btree_ret, sst_id.log_str());
      } else {
        table_item2freeze = active_table_item_;
        cur_major_version_ = sst_id.major_version;
        cur_minor_version_ = sst_id.minor_version_end;
        active_table_item_ = tmp_table_item;
      }
    }
    if (NULL == table_item2freeze) {
      table_allocator_.deallocate(tmp_table_item);
    }
  }
  return table_item2freeze;
}

bool TableMgr::try_dump_memtable() {
  bool bret = false;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
  } else {
    TableItem* table_item2dump = NULL;
    int btree_ret = ERROR_CODE_OK;
    map_lock_.rdlock();
    if (true) {
      BtreeReadHandle handle;
      if (ERROR_CODE_OK != (btree_ret = table_map_.get_read_handle(handle))) {
        TBSYS_LOG(WARN, "get read handle fail ret=%d", btree_ret);
      } else {
        table_map_.set_key_range(handle, table_map_.get_min_key(), 0, table_map_.get_max_key(), 0);
        TableItem* table_item = NULL;
        while (ERROR_CODE_OK == table_map_.get_next(handle, table_item)) {
          if (NULL != table_item
              && table_item->dump_memtable()) {
            table_item->inc_ref_cnt();
            table_item2dump = table_item;
            bret = true;
            break;
          }
        }
      }
    }
    map_lock_.unlock();
    if (NULL != table_item2dump) {
      table_item2dump->do_dump();
      SSTableID sst_id = table_item2dump->get_sstable_id();
      if (0 == table_item2dump->dec_ref_cnt()) {
        table_allocator_.deallocate(table_item2dump);
        TBSYS_LOG(INFO, "erase sstable, delete table_item=%p %s", table_item2dump, sst_id.log_str());
      }
    }
  }
  return bret;
}

bool TableMgr::try_drop_memtable(const int64_t mem_limit) {
  bool bret = false;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
  } else if (frozen_memtotal_ < mem_limit) {
    //TBSYS_LOG(INFO, "need not drop memtable frozen_size=%ld limit=%ld", frozen_memtotal_, mem_limit);
  } else {
    TableItem* table_item2drop = NULL;
    int64_t memused2drop = 0;
    int64_t memtotal2drop = 0;
    int64_t rowcount2drop = 0;
    int btree_ret = ERROR_CODE_OK;
    map_lock_.rdlock();
    if (true) {
      BtreeReadHandle handle;
      if (ERROR_CODE_OK != (btree_ret = table_map_.get_read_handle(handle))) {
        TBSYS_LOG(WARN, "get read handle fail ret=%d", btree_ret);
      } else {
        table_map_.set_key_range(handle, table_map_.get_min_key(), 0, table_map_.get_max_key(), 0);
        TableItem* table_item = NULL;
        while (ERROR_CODE_OK == table_map_.get_next(handle, table_item)) {
          if (NULL != table_item) {
            if (table_item->drop_memtable()) {
              cur_warm_up_percent_ = 0;
              memused2drop = table_item->get_memtable().used();
              memtotal2drop = table_item->get_memtable().total();
              rowcount2drop = table_item->get_memtable().size();
              table_item->inc_ref_cnt();
              table_item2drop = table_item;
              bret = true;
              break;
            }
          }
        }
      }
    }
    map_lock_.unlock();
    if (NULL != table_item2drop) {
      frozen_memused_ -= memused2drop;
      frozen_memtotal_ -= memtotal2drop;
      frozen_rowcount_ -= rowcount2drop;
      table_item2drop->do_drop();
      SSTableID sst_id = table_item2drop->get_sstable_id();
      if (0 == table_item2drop->dec_ref_cnt()) {
        table_allocator_.deallocate(table_item2drop);
        TBSYS_LOG(INFO, "erase sstable, delete table_item=%p %s", table_item2drop, sst_id.log_str());
      }
    } else {
      TBSYS_LOG(WARN, "need drop memtable for releasing memory, but no memtable can be dropped");
    }
    log_table_info();
  }
  return bret;
}

void TableMgr::try_erase_sstable(const int64_t time_limit) {
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
  } else {
    ObList<uint64_t> list2erase;
    int btree_ret = ERROR_CODE_OK;
    map_lock_.rdlock();
    if (true) {
      BtreeReadHandle handle;
      if (ERROR_CODE_OK != (btree_ret = table_map_.get_read_handle(handle))) {
        TBSYS_LOG(WARN, "get read handle fail ret=%d", btree_ret);
      } else {
        table_map_.set_key_range(handle, table_map_.get_min_key(), 0, table_map_.get_max_key(), 0);
        TableItem* table_item = NULL;
        uint64_t major_version2erase = 0;
        uint64_t sstable_id = 0;
        ObList<uint64_t> tmp_list;
        ObList<uint64_t>::iterator tmp_iter;
        while (ERROR_CODE_OK == table_map_.get_next(handle, table_item)) {
          if (NULL != table_item) {
            sstable_id = table_item->get_sstable_id();
            if (0 != major_version2erase
                && major_version2erase != SSTableID::get_major_version(sstable_id)) {
              for (tmp_iter = tmp_list.begin(); tmp_iter != tmp_list.end(); tmp_iter++) {
                list2erase.push_back(*tmp_iter);
              }
              major_version2erase = 0;
              tmp_list.clear();
              break;
            }
            if (TableItem::DROPED > table_item->get_stat()) {
              break;
            }
            if ((tbsys::CTimeUtil::getTime() - table_item->get_sstable_loaded_time()) > time_limit) {
              major_version2erase = SSTableID::get_major_version(sstable_id);
              tmp_list.push_back(sstable_id);
            } else if (0 != major_version2erase
                       && major_version2erase == SSTableID::get_major_version(sstable_id)) {
              tmp_list.push_back(sstable_id);
            } else {
              break;
            }
          } else {
            break;
          }
        }
      }
    }
    map_lock_.unlock();
    ObList<uint64_t>::iterator iter;
    for (iter = list2erase.begin(); iter != list2erase.end(); iter++) {
      ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main) {
        TBSYS_LOG(ERROR, "get ups main fail");
      } else {
        SSTableMgr& sstable_mgr = ups_main->get_update_server().get_sstable_mgr();
        bool remove_sstable_file = true;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = sstable_mgr.erase_sstable(*iter, remove_sstable_file))) {
          TBSYS_LOG(WARN, "erase sstable from sstable_mgr fail ret=%d sstable_id=%lu", tmp_ret, *iter);
        }
      }
      erase_sstable(*iter);
    }
  }
  return;
}

void TableMgr::log_table_info() {
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
  } else {
    map_lock_.rdlock();
    BtreeReadHandle handle;
    int btree_ret = ERROR_CODE_OK;
    if (ERROR_CODE_OK != (btree_ret = table_map_.get_read_handle(handle))) {
      TBSYS_LOG(WARN, "get read handle fail ret=%d", btree_ret);
    } else {
      TBSYS_LOG(INFO, "==========log table info start==========");
      table_map_.set_key_range(handle, table_map_.get_min_key(), 0, table_map_.get_max_key(), 0);
      TableItem* table_item = NULL;
      while (ERROR_CODE_OK == table_map_.get_next(handle, table_item)) {
        if (NULL != table_item) {
          SSTableID sst_id = table_item->get_sstable_id();
          if (TableItem::DUMPED >= table_item->get_stat()) {
            MemTableAttr memtable_attr;
            table_item->get_memtable().get_attr(memtable_attr);
            TBSYS_LOG(INFO, "[table_info] stat=%d %s timestamp=%ld sstable_loaded_time=%ld mem_total=%ld mem_used=%ld total_line=%ld mem_limit=%ld",
                      table_item->get_stat(), sst_id.log_str(),
                      table_item->get_time_stamp(),
                      table_item->get_sstable_loaded_time(),
                      table_item->get_memtable().total(),
                      table_item->get_memtable().used(),
                      table_item->get_memtable().size(),
                      memtable_attr.total_memlimit);
          } else {
            TBSYS_LOG(INFO, "[table_info] stat=%d %s timestamp=%ld",
                      table_item->get_stat(), sst_id.log_str(), table_item->get_time_stamp());
          }
        }
      }
      TBSYS_LOG(INFO, "==========log table info end==========");
    }
    map_lock_.unlock();
  }
}

int TableMgr::try_freeze_memtable(const FreezeType freeze_type,
                                  uint64_t& new_version, uint64_t& frozen_version,
                                  uint64_t& clog_id, int64_t& time_stamp,
                                  bool& major_version_changed) {
  int64_t mem_limit = ObUpdateServerParam::DEFAULT_ACTIVE_MEM_LIMIT_GB;
  int64_t num_limit = ObUpdateServerParam::DEFAULT_MINOR_NUM_LIMIT;
  int64_t min_major_freeze_interval = ObUpdateServerParam::DEFAULT_MIN_MAJOR_FREEZE_INTERVAL_S;

  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL != ups_main) {
    mem_limit = ups_main->get_update_server().get_param().get_active_mem_limit_gb();
    num_limit = ups_main->get_update_server().get_param().get_minor_num_limit();
    min_major_freeze_interval = ups_main->get_update_server().get_param().get_min_major_freeze_interval_s();
  }
  mem_limit *= (1024L * 1024L * 1024L);
  min_major_freeze_interval *= 1000000;

  if (FORCE_MINOR == freeze_type) {
    mem_limit = 0;
    min_major_freeze_interval = INT64_MAX;
    TBSYS_LOG(INFO, "try froce minor freeze");
  } else if (FORCE_MAJOR == freeze_type) {
    mem_limit = 0;
    num_limit = 0;
    TBSYS_LOG(INFO, "try froce major freeze");
  }

  return try_freeze_memtable(mem_limit, num_limit, min_major_freeze_interval,
                             new_version, frozen_version, clog_id, time_stamp, major_version_changed);
}

bool TableMgr::try_drop_memtable(const bool force) {
  int64_t mem_limit = INT64_MAX;
  if (force) {
    mem_limit = 0;
  } else {
    int64_t table_available_warn_size = get_table_available_warn_size();
    int64_t table_memory_limit = get_table_memory_limit();
    TableItem* table_item = this->get_active_memtable();
    if (NULL != table_item) {
      int64_t table_memory_total = table_item->get_memtable().total() + this->get_frozen_memtotal();
      this->revert_active_memtable(table_item);
      if ((table_memory_total + table_available_warn_size) > table_memory_limit) {
        TBSYS_LOG(INFO, "table_memory_total=%ld table_available_warn_size=%ld "
                  "table_memory_limit=%ld, will drop a frozen table",
                  table_memory_total, table_available_warn_size, table_memory_limit);
        mem_limit = 0;
      }
    }
  }
  return try_drop_memtable(mem_limit);
}

void TableMgr::try_erase_sstable(const bool force) {
  int64_t time_limit = ObUpdateServerParam::DEFAULT_SSTABLE_TIME_LIMIT_S;
  if (force) {
    time_limit = 0;
  } else {
    ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
    if (NULL != ups_main) {
      time_limit = ups_main->get_update_server().get_param().get_sstable_time_limit_s();
    }
    time_limit *= 1000000L;
  }
  try_erase_sstable(time_limit);
}

void TableMgr::set_memtable_attr(const MemTableAttr& memtable_attr) {
  memtable_attr_ = memtable_attr;
  TableItem* table_item = get_active_memtable();
  if (NULL != table_item) {
    table_item->get_memtable().set_attr(memtable_attr_);
    revert_active_memtable(table_item);
  }
}

int TableMgr::get_memtable_attr(MemTableAttr& memtable_attr) {
  int ret = OB_SUCCESS;
  TableItem* table_item = get_active_memtable();
  if (NULL == table_item) {
    ret = OB_ERROR;
  } else {
    table_item->get_memtable().get_attr(memtable_attr);
    revert_active_memtable(table_item);
  }
  return ret;
}

int64_t TableMgr::get_frozen_memused() const {
  return frozen_memused_;
}

int64_t TableMgr::get_frozen_memtotal() const {
  return frozen_memtotal_;
}

int64_t TableMgr::get_frozen_rowcount() const {
  return frozen_rowcount_;
}

int64_t TableMgr::get_extern_mem_total() {
  return frozen_memtotal_;
}

uint64_t TableMgr::get_active_version() {
  uint64_t ret = OB_INVALID_ID;
  TableItem* table_item = get_active_memtable();
  if (NULL != table_item) {
    SSTableID sst_id = table_item->get_memtable().get_version();
    ret = sst_id.major_version;
    revert_active_memtable(table_item);
  }
  return ret;
}

void TableMgr::dump_memtable2text(const common::ObString& dump_dir) {
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
  } else {
    map_lock_.rdlock();
    ObList<TableItem*> table_list;
    ObList<TableItem*>::iterator iter;
    BtreeReadHandle handle;
    int btree_ret = ERROR_CODE_OK;
    if (ERROR_CODE_OK != (btree_ret = table_map_.get_read_handle(handle))) {
      TBSYS_LOG(WARN, "get read handle fail ret=%d", btree_ret);
    } else {
      table_map_.set_key_range(handle, table_map_.get_min_key(), 0, table_map_.get_max_key(), 0);
      TableItem* table_item = NULL;
      while (ERROR_CODE_OK == table_map_.get_next(handle, table_item)) {
        if (NULL != table_item
            && TableItem::DUMPED >= table_item->get_stat()) {
          table_item->get_memtable().inc_ref_cnt();
          table_item->inc_ref_cnt();
          table_list.push_back(table_item);
        }
      }
    }
    map_lock_.unlock();
    for (iter = table_list.begin(); iter != table_list.end(); iter++) {
      TableItem* table_item = *iter;
      if (NULL != table_item) {
        table_item->get_memtable().dump2text(dump_dir);
        revert_memtable_(table_item);
      }
    }
  }
  return;
}

int TableMgr::clear_active_memtable() {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited this=%p", this);
    ret = OB_NOT_INIT;
  } else {
    int64_t total_before = 0;
    map_lock_.wrlock();
    if (NULL == active_table_item_) {
      ret = OB_ERROR;
    } else {
      total_before = active_table_item_->get_memtable().total();
      if (1 != active_table_item_->get_memtable().get_ref_cnt()) {
        TBSYS_LOG(WARN, "there is someone using memtable cannot clear ref_cnt=%ld",
                  active_table_item_->get_memtable().get_ref_cnt());
        ret = OB_UPS_TRANS_RUNNING;
      } else {
        active_table_item_->get_memtable().clear();
      }
    }
    map_lock_.unlock();
    TBSYS_LOG(INFO, "clear active memtable ret=%d total_before=%ld total_after=%ld",
              ret, total_before, active_table_item_->get_memtable().total());
  }
  return ret;
}

void TableMgr::set_warm_up_percent(const int64_t warm_up_percent) {
  cur_warm_up_percent_ = warm_up_percent;
}

int64_t TableMgr::get_warm_up_percent_() {
  return cur_warm_up_percent_;
}

void thread_read_prepare() {
  static common::ModulePageAllocator mod_allocator(ObModIds::OB_SSTABLE_EGT_SCAN);
  static const int64_t QUERY_INTERNAL_PAGE_SIZE = 2 * 1024 * 1024;
  common::ModuleArena* internal_buffer_arena = GET_TSI(common::ModuleArena);
  if (NULL == internal_buffer_arena) {
    TBSYS_LOG(ERROR, "cannot get tsi object of PageArena");
  } else {
    internal_buffer_arena->set_page_size(QUERY_INTERNAL_PAGE_SIZE);
    internal_buffer_arena->set_page_alloctor(mod_allocator);
    internal_buffer_arena->reuse();
  }
}

void thread_read_complete() {
  int status = OB_SUCCESS;
  sstable::ObThreadAIOBufferMgrArray* aio_buf_mgr_array = GET_TSI(sstable::ObThreadAIOBufferMgrArray);
  if (NULL != aio_buf_mgr_array) {
    status = aio_buf_mgr_array->wait_all_aio_buf_mgr_free(10 * 1000000);
    if (OB_AIO_TIMEOUT == status) {
      //TODO:stop current thread.
      TBSYS_LOG(WARN, "failed to wait all aio buffer manager free, stop current thread");
    }
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

void MajorFreezeDuty::runTimerTask() {
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  struct tm human_time;
  struct tm* human_time_ptr = NULL;
  time_t cur_time;
  time(&cur_time);
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get ups_main fail");
  } else if (NULL == (human_time_ptr = localtime_r(&cur_time, &human_time))) {
    TBSYS_LOG(WARN, "get local time fail errno=%u", errno);
  } else {
    ObUpdateServer& ups = ups_main->get_update_server();
    int duty_hour = ups.get_param().get_major_freeze_duty_time().tm_hour;
    int duty_min = ups.get_param().get_major_freeze_duty_time().tm_min;
    if (0 == human_time_ptr->tm_sec
        && duty_hour == human_time_ptr->tm_hour
        && duty_min == human_time_ptr->tm_min) {
      ups.submit_major_freeze();
      TBSYS_LOG(INFO, "submit major freeze cur_time=%d:%d duty_time=%d:%d",
                human_time_ptr->tm_hour, human_time_ptr->tm_min, duty_hour, duty_min);
    } else {
      TBSYS_LOG(DEBUG, "need not do major freeze cur_time=%d:%d duty_time=%d:%d",
                human_time_ptr->tm_hour, human_time_ptr->tm_min, duty_hour, duty_min);
    }
  }
  return;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

void HandleFrozenDuty::runTimerTask() {
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get ups_main fail");
  } else {
    ObUpdateServer& ups = ups_main->get_update_server();
    ups.submit_handle_frozen();
  }
}
}
}



