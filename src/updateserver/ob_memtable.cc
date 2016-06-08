/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_memtable.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "common/ob_trace_log.h"
#include "ob_memtable.h"
#include "ob_ups_utils.h"
#include "ob_ups_tmps.h"
#include "ob_update_server_main.h"

namespace sb {
namespace updateserver {
using namespace common;
using namespace hash;

const char* MemTable::MIN_STR = "\0";

MemTable::MemTable() : inited_(false), mem_tank_(), table_engine_(), table_bf_(),
  version_(0), ref_cnt_(0),
  checksum_before_mutate_(0), checksum_after_mutate_(0) {
}

MemTable::~MemTable() {
  if (inited_) {
    destroy();
  }
}

#ifdef __UNIT_TEST__
#ifndef _BTREE_ENGINE_
int MemTable::init(int64_t hash_item_num, int64_t index_item_num) {
  int ret = OB_SUCCESS;
  if (inited_) {
    TBSYS_LOG(WARN, "have already inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_engine_.init(&mem_tank_, hash_item_num, index_item_num))) {
    TBSYS_LOG(WARN, "init table engine fail ret=%d", ret);
  } else if (OB_SUCCESS != (ret = table_bf_.init(BLOOM_FILTER_NHASH, BLOOM_FILTER_NBYTE))) {
    TBSYS_LOG(WARN, "init table bloomfilter fail ret=%d", ret);
  } else {
    inited_ = true;
  }
  return ret;
}
#else
int MemTable::init(int64_t hash_item_num, int64_t index_item_num) {
  UNUSED(hash_item_num);
  UNUSED(index_item_num);
  return init();
}
#endif
#endif

int MemTable::init() {
  int ret = OB_SUCCESS;
  if (inited_) {
    TBSYS_LOG(WARN, "have already inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_engine_.init(&mem_tank_))) {
    TBSYS_LOG(WARN, "init table engine fail ret=%d", ret);
  } else if (OB_SUCCESS != (ret = table_bf_.init(BLOOM_FILTER_NHASH, BLOOM_FILTER_NBYTE))) {
    TBSYS_LOG(WARN, "init table bloomfilter fail ret=%d", ret);
  } else {
    inited_ = true;
  }
  return ret;
}

int MemTable::destroy() {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
  } else if (OB_SUCCESS != clear()) {
    TBSYS_LOG(WARN, "clear fail");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != table_engine_.destroy()) {
    TBSYS_LOG(WARN, "table engine destroy fail");
    ret = OB_ERROR;
  } else {
    inited_ = false;
  }
  return ret;
}

int MemTable::clear() {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != table_engine_.clear()) {
    TBSYS_LOG(WARN, "table engine clear fail");
    ret = OB_ERROR;
  } else {
    mem_tank_.clear();
    checksum_before_mutate_ = 0;
    checksum_after_mutate_ = 0;
  }
  return ret;
}

ObCellInfoNode* MemTable::copy_cell_(const ObCellInfo& cell_info) {
  ObCellInfoNode* ret = NULL;
  if (NULL != (ret = (ObCellInfoNode*)mem_tank_.alloc(sizeof(ObCellInfoNode)))) {
    ret->reset();
    //memset(ret, 0, sizeof(ObCellInfoNode));
    //ret->cell_info.table_id_ = cell_info.table_id_;
    ret->cell_info.column_id_ = cell_info.column_id_;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = mem_tank_.write_obj(cell_info.value_, &(ret->cell_info.value_)))) {
      TBSYS_LOG(WARN, "write obj fail ret=%d %s", tmp_ret, print_cellinfo(&cell_info));
      ret = NULL;
    }
  } else {
    TBSYS_LOG(WARN, "memtank alloc fail size=%lu", sizeof(ObCellInfoNode));
  }
  return ret;
}

//int MemTable::copy_row_key_(const ObCellInfo &cell_info, TEKey &key)
//{
//  int ret = OB_SUCCESS;
//  key.table_id = cell_info.table_id_;
//  ret = string_buf_.write_string(cell_info.row_key_, &(key.row_key));
//  return ret;
//}

void MemTable::update_value_(const TEKey& key, ObCellInfoNode* cell_info_node, TEValue& value, const int64_t mutate_timestamp) {
  if (NULL == value.list_head
      || is_delete_row_(cell_info_node->cell_info.value_)) {
    value.list_head = cell_info_node;
    value.list_tail = cell_info_node;
    value.cell_info_cnt = 1;
    value.cell_info_size = cell_info_node->size();
  } else {
    value.list_tail->next = cell_info_node;
    value.list_tail = cell_info_node;
    value.cell_info_cnt++;
    value.cell_info_size += cell_info_node->size();
    if (get_max_row_cell_num() < value.cell_info_cnt
        || MAX_ROW_SIZE < value.cell_info_size) {
      merge(key, value, mem_tank_);
    }
  }
  if (0 == value.create_time) {
    value.create_time = mutate_timestamp;
  }
  value.modify_time = mutate_timestamp;
  if (is_delete_row_(cell_info_node->cell_info.value_)) {
    value.row_stat = ROW_ST_NOT_EXIST;
    value.create_time = 0;
  } else {
    value.row_stat = ROW_ST_EXIST;
  }
  checksum_after_mutate_ = value.checksum(checksum_after_mutate_);
  checksum_after_mutate_ = cell_info_node->checksum(checksum_after_mutate_);
}

int MemTable::get_cur_value_(MemTableTransHandle& handle,
                             TEKey& cur_key,
                             const TEKey& prev_key,
                             const TEValue& prev_value,
                             TEValue& cur_value) {
  int ret = OB_SUCCESS;
  if (cur_key == prev_key) {
    cur_key = prev_key;
    cur_value = prev_value;
  } else {
    ret = table_engine_.get(handle, cur_key, cur_value);
    if (OB_ENTRY_NOT_EXIST == ret) {
      cur_value.reset();
      cur_value.row_stat = ROW_ST_NOT_EXIST;
      ObString tmp_row_key;
      ret = mem_tank_.write_string(cur_key.row_key, &tmp_row_key);
      if (OB_SUCCESS == ret) {
        cur_key.row_key = tmp_row_key;
        if (OB_SUCCESS != (ret = table_bf_.insert(cur_key.table_id, cur_key.row_key))) {
          TBSYS_LOG(WARN, "insert cur_key to bloomfilter fail ret=%d %s", ret, cur_key.log_str());
        } else {
          checksum_after_mutate_ = cur_key.checksum(checksum_after_mutate_);
        }
      }
      TBSYS_LOG(DEBUG, "copy row_key this=%p row_key %s", this, cur_key.log_str());
    } else {
      cur_value.update_count += 1;
    }
  }
  return ret;
}

int MemTable::get_cur_value_(TEKey& cur_key,
                             const TEKey& prev_key,
                             const TEValue& prev_value,
                             TEValue& cur_value) {
  int ret = OB_SUCCESS;
  if (cur_key == prev_key) {
    cur_key = prev_key;
    cur_value = prev_value;
  } else {
    ret = table_engine_.get(cur_key, cur_value);
    if (OB_ENTRY_NOT_EXIST == ret) {
      cur_value.reset();
      cur_value.row_stat = ROW_ST_NOT_EXIST;
      ObString tmp_row_key;
      ret = mem_tank_.write_string(cur_key.row_key, &tmp_row_key);
      if (OB_SUCCESS == ret) {
        cur_key.row_key = tmp_row_key;
        if (OB_SUCCESS != (ret = table_bf_.insert(cur_key.table_id, cur_key.row_key))) {
          TBSYS_LOG(WARN, "insert cur_key to bloomfilter fail ret=%d %s", ret, cur_key.log_str());
        } else {
          checksum_after_mutate_ = cur_key.checksum(checksum_after_mutate_);
        }
      }
      TBSYS_LOG(DEBUG, "copy row_key this=%p row_key %s", this, cur_key.log_str());
    } else {
      cur_value.update_count += 1;
    }
  }
  return ret;
}

int MemTable::ob_sem_handler_(MemTableTransHandle& handle,
                              ObCellInfoNode* cell_info_node,
                              TEKey& cur_key,
                              TEValue& cur_value,
                              const TEKey& prev_key,
                              const TEValue& prev_value,
                              const int64_t mutate_timestamp) {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = get_cur_value_(handle, cur_key, prev_key, prev_value, cur_value))) {
    update_value_(cur_key, cell_info_node, cur_value, mutate_timestamp);
    if (MAX_ROW_SIZE < cur_value.cell_info_size) {
      TBSYS_LOG(WARN, "row size overflow [%s] [%s]", cur_key.log_str(), cur_value.log_str());
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int MemTable::db_sem_handler_(MemTableTransHandle& handle,
                              ObCellInfoNode* cell_info_node,
                              TEKey& cur_key,
                              TEValue& cur_value,
                              const TEKey& prev_key,
                              const TEValue& prev_value,
                              const int64_t op_type,
                              const int64_t mutate_timestamp) {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = get_cur_value_(handle, cur_key, prev_key, prev_value, cur_value))) {
    if (is_insert_(op_type)) {
      if (ROW_ST_NOT_EXIST != cur_value.row_stat) {
        ret = OB_ENTRY_EXIST;
      }
    } else if (is_delete_row_(cell_info_node->cell_info.value_)
               || is_update_(op_type)) {
      if (ROW_ST_EXIST != cur_value.row_stat) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == ret) {
    update_value_(cur_key, cell_info_node, cur_value, mutate_timestamp);
  }
  return ret;
}

int MemTable::set(MemTableTransHandle& handle, ObUpsMutator& mutator, const bool check_checksum) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (check_checksum
             && checksum_before_mutate_ != mutator.get_memtable_checksum_before_mutate()) {
    handle_checksum_error(mutator);
    ret = OB_ERROR;
  } else {
    int64_t counter = 0;
    bool updated = false;
    TEKey cur_key;
    TEValue cur_value;
    TEKey prev_key;
    TEValue prev_value;
    CellInfoProcessor ci_proc;
    ObMutatorCellInfo* mutator_cell_info = NULL;
    ObCellInfo* cell_info = NULL;
    while (OB_SUCCESS == ret
           && OB_SUCCESS == (ret = mutator.next_cell())) {
      ObCellInfoNode* cell_info_node = NULL;
      if (OB_SUCCESS == (ret = mutator.get_cell(&mutator_cell_info))
          && NULL != mutator_cell_info) {
        cell_info = &(mutator_cell_info->cell_info);
        TBSYS_LOG(DEBUG, "trans set cell_info %s", print_cellinfo(cell_info));
        if (!ci_proc.analyse_syntax(*mutator_cell_info)) {
          ret = OB_ERROR;
        } else if (ci_proc.need_skip()) {
          continue;
        } else if (NULL != (cell_info_node = copy_cell_(*cell_info))
                   /*&& OB_SUCCESS == copy_row_key_(*cell_info, cur_key)*/) {
          //cell_info_node->cell_info.row_key_ = cur_key.row_key;
          cur_key.table_id = cell_info->table_id_;
          cur_key.row_key = cell_info->row_key_;
          if (0 == counter) {
            //FILL_TRACE_LOG("first cell info %s", print_cellinfo(cell_info));
          }
          if (cur_key != prev_key && updated) {
            ret = table_engine_.set(handle, prev_key, prev_value);
          }
          if (OB_SUCCESS == ret) {
            if (!ci_proc.is_db_sem()) {
              ret = ob_sem_handler_(handle, cell_info_node, cur_key, cur_value, prev_key, prev_value, mutator.get_mutate_timestamp());
            } else {
              ret = OB_NOT_SUPPORTED;
              TBSYS_LOG(WARN, "can not handle db sem now %s", print_cellinfo(cell_info));
              //ret = db_sem_handler_(handle, cell_info_node, cur_key, cur_value, prev_key, prev_value, ci_proc.get_op_type(), mutator.get_mutate_timestamp());
            }
            if (OB_SUCCESS == ret) {
              updated = true;
              prev_key = cur_key;
              prev_value = cur_value;
            }
          }
          ++counter;
        } else {
          TBSYS_LOG(WARN, "copy cell fail %s", print_cellinfo(cell_info));
          ret = OB_ERROR;
        }
      } else {
        TBSYS_LOG(WARN, "mutator get cell fail ret=%d", ret);
        ret = OB_ERROR;
      }
    }
    ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
    if (OB_SUCCESS == ret && updated) {
      ret = table_engine_.set(handle, cur_key, cur_value);
    }
    mutator.reset_iter();
    //FILL_TRACE_LOG("last cell info %s num=%ld ret=%d total=%ld", print_cellinfo(cell_info), counter, ret, total());
  }
  if (OB_ERROR == ret && mem_tank_.mem_over_limit()) {
    ret = OB_MEM_OVERFLOW;
  }
  if (OB_SUCCESS == ret) {
    if (check_checksum
        && checksum_after_mutate_ != mutator.get_memtable_checksum_after_mutate()) {
      handle_checksum_error(mutator);
      ret = OB_ERROR;
    } else {
      mutator.set_memtable_checksum_before_mutate(checksum_before_mutate_);
      mutator.set_memtable_checksum_after_mutate(checksum_after_mutate_);
    }
  }
  return ret;
}

void MemTable::handle_checksum_error(ObUpsMutator& mutator) {
  TBSYS_LOG(ERROR, "checksum wrong table_checksum_before=%ld mutator_checksum_before=%ld "
            "table_checksum_after=%ld mutator_checksum_after=%ld",
            checksum_before_mutate_, mutator.get_memtable_checksum_before_mutate(),
            checksum_after_mutate_, mutator.get_memtable_checksum_after_mutate());
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL != ups_main) {
    ups_main->get_update_server().get_role_mgr().set_state(ObRoleMgr::HOLD);
  }
  ObMutatorCellInfo* mutator_cell_info = NULL;
  while (OB_SUCCESS == mutator.next_cell()) {
    mutator.get_cell(&mutator_cell_info);
    if (NULL != mutator_cell_info) {
      TBSYS_LOG(INFO, "%s %s", print_obj(mutator_cell_info->op_type), print_cellinfo(&(mutator_cell_info->cell_info)));
    }
  }
}

int MemTable::set(ObUpsMutator& mutator, const bool check_checksum) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (check_checksum
             && checksum_before_mutate_ != mutator.get_memtable_checksum_before_mutate()) {
    handle_checksum_error(mutator);
    ret = OB_ERROR;
  } else {
    checksum_after_mutate_ = checksum_before_mutate_;

    int64_t counter = 0;
    bool updated = false;
    TEKey cur_key;
    TEValue cur_value;
    TEKey prev_key;
    TEValue prev_value;
    CellInfoProcessor ci_proc;
    ObMutatorCellInfo* mutator_cell_info = NULL;
    ObCellInfo* cell_info = NULL;
    while (OB_SUCCESS == ret
           && OB_SUCCESS == (ret = mutator.next_cell())) {
      ObCellInfoNode* cell_info_node = NULL;
      if (OB_SUCCESS == mutator.get_cell(&mutator_cell_info)
          && NULL != mutator_cell_info) {
        cell_info = &(mutator_cell_info->cell_info);
        TBSYS_LOG(DEBUG, "set cell_info %s", print_cellinfo(cell_info));
        if (!ci_proc.analyse_syntax(*mutator_cell_info)) {
          ret = OB_ERROR;
        } else if (ci_proc.need_skip()) {
          continue;
        } else if (NULL != (cell_info_node = copy_cell_(*cell_info))
                   /*&& OB_SUCCESS == copy_row_key_(*cell_info, cur_key)*/) {
          //cell_info_node->cell_info.row_key_ = cur_key.row_key;
          cur_key.table_id = cell_info->table_id_;
          cur_key.row_key = cell_info->row_key_;
          if (0 == counter) {
            //FILL_TRACE_LOG("first cell info %s", print_cellinfo(cell_info));
          }
          if (cur_key != prev_key && updated) {
            ret = table_engine_.set(prev_key, prev_value);
          }
          if (OB_SUCCESS == ret
              && OB_SUCCESS == (ret = get_cur_value_(cur_key, prev_key, prev_value, cur_value))) {
            update_value_(cur_key, cell_info_node, cur_value, mutator.get_mutate_timestamp());
            updated = true;
            prev_key = cur_key;
            prev_value = cur_value;
          }
          ++counter;
        } else {
          ret = OB_ERROR;
        }
      } else {
        ret = OB_ERROR;
      }
    }
    ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
    if (OB_SUCCESS == ret && updated) {
      ret = table_engine_.set(cur_key, cur_value);
    }
    mutator.reset_iter();
    //FILL_TRACE_LOG("last cell info %s num=%ld ret=%d total=%ld", print_cellinfo(cell_info), counter, ret, total());
  }
  if (OB_ERROR == ret && mem_tank_.mem_over_limit()) {
    ret = OB_MEM_OVERFLOW;
  }
  if (OB_SUCCESS == ret) {
    if (check_checksum
        && checksum_after_mutate_ != mutator.get_memtable_checksum_after_mutate()) {
      handle_checksum_error(mutator);
      ret = OB_ERROR;
    } else {
      checksum_before_mutate_ = checksum_after_mutate_;
    }
  }
  return ret;
}

int MemTable::get(MemTableTransHandle& handle,
                  const uint64_t table_id, const ObString& row_key,
                  MemTableIterator& iterator,
                  bool& is_multi_update,
                  ColumnFilter* column_filter/* = NULL*/) {
  int ret = OB_SUCCESS;
  TEKey key;
  key.table_id = table_id;
  key.row_key = row_key;
  TEValue value;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_engine_.get(handle, key, value))) {
    //TBSYS_LOG(WARN, "get from table engine fail %s", key.log_str());
    if (OB_ENTRY_NOT_EXIST == ret) {
      iterator.set_row_not_exist_(table_id, row_key);
      ret = OB_SUCCESS;
    }
  } else {
    iterator.set_(key, value, column_filter);
    is_multi_update = (1 < value.update_count);
  }
  return ret;
}

bool MemTable::need_query_sst(const uint64_t table_id, const ObString& row_key, bool dbsem) {
  int bret = false;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
  } else if (!dbsem) {
    // do nothing
  } else {
    TEKey key;
    key.table_id = table_id;
    key.row_key = row_key;
    TEValue value;
    if (OB_SUCCESS != table_engine_.get(key, value)
        || ROW_ST_UNKNOW == value.row_stat) {
      bret = true;
    }
  }
  return bret;
}

int MemTable::set_row_exist(const uint64_t table_id, const ObString& row_key, bool exist) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else {
    int8_t row_stat = exist ? ROW_ST_EXIST : ROW_ST_NOT_EXIST;
    TEKey key;
    key.table_id = table_id;
    key.row_key = row_key;
    TEValue value;
    table_engine_.get(key, value);
    switch (value.row_stat) {
    case ROW_ST_EXIST:
    case ROW_ST_NOT_EXIST:
      break;
    case ROW_ST_UNKNOW:
      value.row_stat = row_stat;
      break;
    default:
      TBSYS_LOG(WARN, "row stat error %s row_stat=%hhd query_stat=%hhd",
                key.log_str(), value.row_stat, row_stat);
      break;
    }
    table_engine_.set(key, value);
  }
  return ret;
}

int MemTable::scan(MemTableTransHandle& handle,
                   const ObRange& range,
                   const bool reverse,
                   MemTableIterator& iter,
                   ColumnFilter* column_filter/* = NULL*/) {
  int ret = OB_SUCCESS;

  TEKey start_key;
  TEKey end_key;
  int start_exclude = get_start_exclude(range);
  int end_exclude = get_end_exclude(range);
  int min_key = get_min_key(range);
  int max_key = get_max_key(range);

  start_key.table_id = get_table_id(range);
  start_key.row_key = get_start_key(range);;
  end_key.table_id = get_table_id(range);
  end_key.row_key = get_end_key(range);

  if (0 != min_key) {
    start_exclude = 0;
    min_key = 0;
    start_key.row_key.assign(const_cast<char*>(MIN_STR), strlen(MIN_STR));
  }

  TableEngineIterator te_iter;

  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != table_engine_.scan(handle,
                                              start_key, min_key, start_exclude,
                                              end_key, max_key, end_exclude,
                                              reverse, te_iter)) {
    ret = OB_ERROR;
  } else {
    iter.set_(te_iter, column_filter, get_table_id(range));
  }
  return ret;
}

int MemTable::start_transaction(const MemTableTransType type,
                                MemTableTransHandle& handle) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_engine_.start_transaction(type, handle))) {
    TBSYS_LOG(WARN, "table engine start transaction fail ret=%d", ret);
  }
  return ret;
}

int MemTable::end_transaction(MemTableTransHandle& handle, bool rollback) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_engine_.end_transaction(handle, rollback))) {
    TBSYS_LOG(WARN, "table engine end transaction fail ret=%d rollback=%d", ret, rollback);
  }
  return ret;
}

int MemTable::create_index() {
  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  MutexLocker locker(mutex);
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else {
    ret = table_engine_.create_index();
  }
  return ret;
}

int MemTable::start_mutation(MemTableTransHandle& handle) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_engine_.start_mutation(handle))) {
    TBSYS_LOG(WARN, "table engine start mutation fail ret=%d", ret);
  } else {
    checksum_after_mutate_ = checksum_before_mutate_;
  }
  return ret;
}

int MemTable::end_mutation(MemTableTransHandle& handle, bool rollback) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_engine_.end_mutation(handle, rollback))) {
    TBSYS_LOG(WARN, "table engine end mutation fail ret=%d rollback=%d", ret, rollback);
  } else {
    if (!rollback) {
      checksum_before_mutate_ = checksum_after_mutate_;
    } else {
      checksum_after_mutate_ = checksum_before_mutate_;
    }
  }
  return ret;
}

int MemTable::get_bloomfilter(TableBloomFilter& table_bf) const {
  return table_bf.deep_copy(table_bf_);
}

int MemTable::scan_all_start(TableEngineTransHandle& trans_handle, TableEngineIterator& iter) {
  int ret = OB_SUCCESS;
  TEKey empty_key;
  int min_key = 1;
  int max_key = 1;
  int start_exclude = 0;
  int end_exclude = 0;
  bool reverse = false;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_engine_.start_transaction(READ_TRANSACTION, trans_handle))) {
    TBSYS_LOG(WARN, "table engine start transaction fail ret=%d", ret);
  } else if (OB_SUCCESS != table_engine_.scan(trans_handle,
                                              empty_key, min_key, start_exclude,
                                              empty_key, max_key, end_exclude,
                                              reverse, iter)) {
    TBSYS_LOG(WARN, "table engine scan fail ret=%d", ret);
    table_engine_.end_transaction(trans_handle, false);
  } else {
    TBSYS_LOG(INFO, "scan all start succ");
  }
  return ret;
}

int MemTable::scan_all_end(TableEngineTransHandle& trans_handle) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else {
    ret = table_engine_.end_transaction(trans_handle, false);
    TBSYS_LOG(INFO, "scan all end ret=%d", ret);
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

MemTableIterator::MemTableIterator() : row_has_expected_column_(false), row_has_returned_column_(false),
  last_row_cell_(false), new_row_cell_(false),
  first_next_(false), is_iter_end_(false), is_row_changed_(false),
  row_not_exist_(false),
  ci_iter_(NULL), ci_iter_end_(NULL), te_iter_(),
  column_filter_(NULL), cur_cell_info_(), row_ext_info_(), table_id_(OB_INVALID_ID) {
}

MemTableIterator::~MemTableIterator() {
}

bool MemTableIterator::last_row_cell() const {
  return last_row_cell_;
}

int MemTableIterator::next_() {
  int ret = OB_SUCCESS;
  new_row_cell_ = false;
  bool need_get_ext_info = false;
  if (NULL == ci_iter_) {
    ret = OB_ITER_END;
  } else if (!first_next_) {
    first_next_ = true;
    new_row_cell_ = true;
  } else if (ci_iter_end_ == ci_iter_
             && row_ext_info_.have_ext_info()) {
    need_get_ext_info = true;
  } else if (ci_iter_end_ == ci_iter_) {
    ret = OB_ITER_END;
    TEKey te_key;
    TEValue te_value;
    if (OB_SUCCESS == te_iter_.next()
        && OB_SUCCESS == te_iter_.get_value(te_value)
        && OB_SUCCESS == te_iter_.get_key(te_key)
        && NULL != (ci_iter_ = te_value.list_head)) {
      new_row_cell_ = true;
      ci_iter_end_ = te_value.list_tail;
      cur_cell_info_.reset();
      cur_cell_info_.table_id_ = te_key.table_id;
      cur_cell_info_.row_key_ = te_key.row_key;
      row_ext_info_.set(te_key, te_value);
      ret = OB_SUCCESS;
    }
  } else {
    ci_iter_ = ci_iter_->next;
  }
  if (need_get_ext_info) {
    row_ext_info_.get_ext_info(cur_cell_info_);
  } else if (NULL != ci_iter_) {
    cur_cell_info_.column_id_ = ci_iter_->cell_info.column_id_;
    cur_cell_info_.value_ = ci_iter_->cell_info.value_;
  }
  last_row_cell_ = ((ci_iter_end_ == ci_iter_)
                    && !row_ext_info_.have_ext_info());
  return ret;
}

int MemTableIterator::next_cell() {
  int ret = OB_SUCCESS;
  if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (row_not_exist_) {
    last_row_cell_ = true;
    is_iter_end_ = true;
    ret = OB_SUCCESS;
  } else if (NULL == column_filter_) {
    ret = next_();
    is_iter_end_ = (OB_ITER_END == ret);
  } else {
    while (OB_SUCCESS == (ret = next_())) {
      if (new_row_cell_) {
        row_has_expected_column_ = false;
        row_has_returned_column_ = false;
      }
      if (column_filter_->column_exist(cur_cell_info_.column_id_)) {
        if (OB_INVALID_ID != cur_cell_info_.column_id_) {
          row_has_expected_column_ = true;
        }
        break;
      }
      if (last_row_cell_
          && !row_has_expected_column_) {
        cur_cell_info_.column_id_ = OB_INVALID_ID;
        cur_cell_info_.value_.set_ext(ObActionFlag::OP_NOP);
        break;
      }
    }
    if (OB_SUCCESS == ret) {
      if (!row_has_returned_column_) {
        is_row_changed_ = true;
      } else {
        is_row_changed_ = false;
      }
      row_has_returned_column_ = true;
      if (0 != table_id_
          && table_id_ != cur_cell_info_.table_id_) {
        ret = OB_ITER_END;
      }
    }
    is_iter_end_ = (OB_ITER_END == ret);
  }
  TBSYS_LOG(DEBUG, "this=%p ret=%d", this, ret);
  return ret;
}

int MemTableIterator::get_cell(sb::common::ObCellInfo** cell_info) {
  return get_cell(cell_info, NULL);
}

int MemTableIterator::get_cell(sb::common::ObCellInfo** cell_info, bool* is_row_changed) {
  int ret = OB_SUCCESS;
  if (is_iter_end_
      && !row_not_exist_) {
    ret = OB_ITER_END;
  } else if (NULL == cell_info) {
    TBSYS_LOG(WARN, "invalid param cell_info null pointer");
    ret = OB_ERROR;
  } else if (row_not_exist_) {
    *cell_info = &cur_cell_info_;
    if (NULL != is_row_changed) {
      *is_row_changed = true;
    }
    TBSYS_LOG(DEBUG, "this=%p get_cell %s is_row_changed=%s", this, print_cellinfo(*cell_info), STR_BOOL(is_row_changed_));
  } else if (NULL == ci_iter_) {
    TEKey te_key;
    TEValue te_value;
    te_iter_.get_key(te_key);
    te_iter_.get_value(te_value);
    TBSYS_LOG(WARN, "invalid ci_iter_=%p ci_iter_end_=%p te_key=[%s] te_value=[%s] cur_cell_info=[%s]",
              ci_iter_, ci_iter_end_, te_key.log_str(), te_value.log_str(), print_cellinfo(&cur_cell_info_));
    ret = OB_ERROR;
  } else {
    *cell_info = &cur_cell_info_;
    if (NULL != is_row_changed) {
      *is_row_changed = is_row_changed_;
    }
    TBSYS_LOG(DEBUG, "this=%p get_cell %s is_row_changed=%s", this, print_cellinfo(*cell_info), STR_BOOL(is_row_changed_));
  }
  return ret;
}

void MemTableIterator::set_row_not_exist_(const uint64_t table_id, const ObString& row_key) {
  row_not_exist_ = true;
  cur_cell_info_.table_id_ = table_id;
  cur_cell_info_.row_key_ = row_key;
  cur_cell_info_.column_id_ = OB_INVALID_ID;
  cur_cell_info_.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
}

void MemTableIterator::set_(const TEKey& te_key,
                            const TEValue& te_value,
                            ColumnFilter* column_filter) {
  ci_iter_ = te_value.list_head;
  ci_iter_end_ = te_value.list_tail;
  table_id_ = 0;
  te_iter_.reset();
  column_filter_ = column_filter;
  cur_cell_info_.reset();
  cur_cell_info_.table_id_ = te_key.table_id;
  cur_cell_info_.row_key_ = te_key.row_key;
  if (NULL != ci_iter_) {
    cur_cell_info_.column_id_ = ci_iter_->cell_info.column_id_;
    cur_cell_info_.value_ = ci_iter_->cell_info.value_;
    row_ext_info_.set(te_key, te_value);
  }
}

void MemTableIterator::set_(const TableEngineIterator& te_iter,
                            ColumnFilter* column_filter,
                            const uint64_t table_id) {
  ci_iter_ = NULL;
  ci_iter_end_ = NULL;
  table_id_ = table_id;
  te_iter_ = te_iter;
  TEKey te_key;
  TEValue te_value;
  column_filter_ = column_filter;
  if (OB_SUCCESS == te_iter_.get_value(te_value)
      && OB_SUCCESS == te_iter_.get_key(te_key)
      && NULL != (ci_iter_ = te_value.list_head)) {
    ci_iter_end_ = te_value.list_tail;
    cur_cell_info_.reset();
    cur_cell_info_.table_id_ = te_key.table_id;
    cur_cell_info_.row_key_ = te_key.row_key;
    cur_cell_info_.column_id_ = ci_iter_->cell_info.column_id_;
    cur_cell_info_.value_ = ci_iter_->cell_info.value_;
    row_ext_info_.set(te_key, te_value);
  }
}

void MemTableIterator::reset() {
  row_has_expected_column_ = false;
  row_has_returned_column_ = false;
  last_row_cell_ = false;
  new_row_cell_ = false;
  first_next_ = false;
  is_iter_end_ = false;
  is_row_changed_ = false;
  row_not_exist_ = false;
  ci_iter_ = NULL;
  ci_iter_end_ = NULL;
  te_iter_.reset();
  column_filter_ = NULL;
  cur_cell_info_.reset();
  table_id_ = OB_INVALID_ID;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

MemTableIterator::RowExtInfo::RowExtInfo() : te_key_(), te_value_(), got_map_(),
  create_time_column_id_(OB_INVALID_ID),
  modify_time_column_id_(OB_INVALID_ID) {
}

MemTableIterator::RowExtInfo::~RowExtInfo() {
}

void MemTableIterator::RowExtInfo::set(const TEKey& te_key, const TEValue& te_value) {
  te_key_ = te_key;
  te_value_ = te_value;
  got_map_.reset();
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  if (NULL != main) {
    const UpsSchemaMgr& sm = main->get_update_server().get_table_mgr().get_schema_mgr();
    create_time_column_id_ = sm.get_create_time_column_id(te_key.table_id);
    modify_time_column_id_ = sm.get_modify_time_column_id(te_key.table_id);
  }
  if (OB_INVALID_ID != create_time_column_id_) {
    got_map_.set(EXT_CREATE_TIME);
  }
  if (OB_INVALID_ID != modify_time_column_id_) {
    got_map_.set(EXT_MODIFY_TIME);
  }
}

bool MemTableIterator::RowExtInfo::have_ext_info() const {
  return ((ROW_ST_NOT_EXIST != te_value_.row_stat)
          && got_map_.any());
}

void MemTableIterator::RowExtInfo::get_ext_info(ObCellInfo& cell_info) {
  for (int64_t i = 0; i < EXT_INFO_NUM; i++) {
    if (got_map_.test(i)) {
      switch (i) {
      case EXT_CREATE_TIME:
        cell_info.column_id_ = create_time_column_id_;
        cell_info.value_.reset();
        cell_info.value_.set_createtime(te_value_.create_time);
        break;
      case EXT_MODIFY_TIME:
        cell_info.column_id_ = modify_time_column_id_;
        cell_info.value_.reset();
        cell_info.value_.set_modifytime(te_value_.modify_time);
        break;
      default:
        break;
      }
      got_map_.reset(i);
      break;
    }
  }
}
}
}



