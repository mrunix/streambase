/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_table_mgr.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "common/ob_trace_log.h"
#include "common/ob_row_compaction.h"
#include "ob_update_server_main.h"
#include "ob_ups_table_mgr.h"

namespace sb {
namespace updateserver {
using namespace sb::common;

ObUpsTableMgr :: ObUpsTableMgr() : log_buffer_(NULL),
  check_checksum_(false),
  has_started_(false) {
}

ObUpsTableMgr :: ~ObUpsTableMgr() {
  if (NULL != log_buffer_) {
    ob_free(log_buffer_);
    log_buffer_ = NULL;
  }
}

int ObUpsTableMgr :: init() {
  int err = OB_SUCCESS;

  err = table_mgr_.init();
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "failed to init memtable list, err=%d", err);
  } else if (NULL == (log_buffer_ = (char*)ob_malloc(LOG_BUFFER_SIZE))) {
    TBSYS_LOG(WARN, "malloc log_buffer fail size=%d", LOG_BUFFER_SIZE);
    err = OB_ERROR;
  }

  return err;
}

int ObUpsTableMgr::reg_table_mgr(SSTableMgr& sstable_mgr) {
  return sstable_mgr.reg_observer(&table_mgr_);
}

int ObUpsTableMgr :: get_active_memtable_version(uint64_t& version) {
  int ret = OB_SUCCESS;
  version = table_mgr_.get_active_version();
  if (OB_INVALID_ID == version) {
    ret = OB_ERROR;
  }
  return ret;
}

int ObUpsTableMgr :: get_last_frozen_memtable_version(uint64_t& version) {
  int ret = OB_SUCCESS;
  uint64_t active_version = table_mgr_.get_active_version();
  if (OB_INVALID_ID == active_version) {
    ret = OB_ERROR;
  } else {
    version = active_version - 1;
  }
  return ret;
}

int ObUpsTableMgr :: get_table_time_stamp(const uint64_t major_version, int64_t& time_stamp) {
  int ret = OB_SUCCESS;
  ret = table_mgr_.get_table_time_stamp(major_version, time_stamp);
  return ret;
}

int ObUpsTableMgr :: get_frozen_bloomfilter(const uint64_t version, TableBloomFilter& table_bf) {
  int ret = OB_SUCCESS;
  UNUSED(version);
  UNUSED(table_bf);
  TBSYS_LOG(WARN, "no get frozen bloomfilter impl now");
  return ret;
}

int ObUpsTableMgr :: start_transaction() {
  return start_transaction(WRITE_TRANSACTION, write_trans_handle_);
}

int ObUpsTableMgr :: start_transaction(const MemTableTransType type, UpsTableMgrTransHandle& handle) {
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  if (NULL == (table_item = table_mgr_.get_active_memtable())) {
    TBSYS_LOG(WARN, "failed to acquire active memtable");
    ret = OB_ERROR;
  } else {
    if (OB_SUCCESS == (ret = table_item->get_memtable().start_transaction(type, handle.trans_handle))) {
      handle.cur_memtable = table_item;
    } else {
      handle.cur_memtable = NULL;
      table_mgr_.revert_active_memtable(table_item);
    }
  }
  return ret;
}

int ObUpsTableMgr :: end_transaction(bool rollback) {
  return end_transaction(write_trans_handle_, rollback);
}

int ObUpsTableMgr :: end_transaction(UpsTableMgrTransHandle& handle, bool rollback) {
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  if (NULL == (table_item = handle.cur_memtable)) {
    TBSYS_LOG(WARN, "invalid param cur_memtable null pointer");
  } else {
    if (rollback) {
      ret = OB_ERROR;
    }
    if (OB_SUCCESS == ret) {
      ret = flush_commit_log_();
      FILL_TRACE_LOG("flush log ret=%d", ret);
    }
    if (OB_SUCCESS == ret) {
      ret = table_item->get_memtable().end_transaction(handle.trans_handle, rollback);
      FILL_TRACE_LOG("end transaction ret=%d", ret);
    }
    handle.cur_memtable = NULL;
    table_mgr_.revert_active_memtable(table_item);
  }
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "flush log or end transaction fail ret=%d, will kill self", ret);
    kill(getpid(), SIGTERM);
    ret = OB_RESPONSE_TIME_OUT;
  }
  return ret;
}

void ObUpsTableMgr :: store_memtable(const bool all) {
  bool need2dump = false;
  do {
    need2dump = table_mgr_.try_dump_memtable();
  } while (all && need2dump);
}

int ObUpsTableMgr :: freeze_memtable(const TableMgr::FreezeType freeze_type, uint64_t& frozen_version, bool& report_version_changed) {
  int ret = OB_SUCCESS;
  uint64_t new_version = 0;
  uint64_t new_log_file_id = 0;
  int64_t freeze_time_stamp = 0;
  if (OB_SUCCESS == (ret = table_mgr_.try_freeze_memtable(freeze_type, new_version, frozen_version,
                                                          new_log_file_id, freeze_time_stamp, report_version_changed))) {
    ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
    ObUpsMutator ups_mutator;
    ObMutatorCellInfo mutator_cell_info;
    CurFreezeParam freeze_param;
    CommonSchemaManagerWrapper schema_manager;
    ThreadSpecificBuffer my_thread_buffer;
    ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
    ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());

    freeze_param.param.active_version = new_version;
    freeze_param.param.frozen_version = frozen_version;
    freeze_param.param.new_log_file_id = new_log_file_id;
    freeze_param.param.time_stamp = freeze_time_stamp;
    freeze_param.param.op_flag = 0;

    ups_mutator.set_freeze_memtable();
    if (NULL == ups_main) {
      TBSYS_LOG(ERROR, "get updateserver main null pointer");
      ret = OB_ERROR;
    } else if (OB_SUCCESS != (ret = freeze_param.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
      TBSYS_LOG(WARN, "serialize freeze_param fail ret=%d", ret);
    } else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_mgr(schema_manager))) {
      TBSYS_LOG(WARN, "get schema mgr fail ret=%d", ret);
    } else if (OB_SUCCESS != (ret = schema_manager.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
      TBSYS_LOG(WARN, "serialize schema manager fail ret=%d", ret);
    } else {
      ObString str_freeze_param;
      str_freeze_param.assign_ptr(out_buff.get_data(), out_buff.get_position());
      mutator_cell_info.cell_info.value_.set_varchar(str_freeze_param);
      if (OB_SUCCESS != (ret = ups_mutator.get_mutator().add_cell(mutator_cell_info))) {
        TBSYS_LOG(WARN, "add cell to ups_mutator fail ret=%d", ret);
      } else {
        ret = flush_obj_to_log(OB_LOG_UPS_MUTATOR, ups_mutator);
      }
    }
    TBSYS_LOG(INFO, "write freeze_op log ret=%d new_version=%lu frozen_version=%lu new_log_file_id=%lu op_flag=%lx",
              ret, new_version, frozen_version, new_log_file_id, 0);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "will kill self");
      kill(getpid(), SIGTERM);
      ret = OB_RESPONSE_TIME_OUT;
    }
  }
  return ret;
}

void ObUpsTableMgr :: drop_memtable(const bool force) {
  table_mgr_.try_drop_memtable(force);
}

void ObUpsTableMgr :: erase_sstable(const bool force) {
  table_mgr_.try_erase_sstable(force);
}

int ObUpsTableMgr :: handle_freeze_log_(ObUpsMutator& ups_mutator) {
  int ret = OB_SUCCESS;

  ret = ups_mutator.get_mutator().next_cell();
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "next cell for freeze ups_mutator fail ret=%d", ret);
  }

  ObMutatorCellInfo* mutator_ci = NULL;
  ObCellInfo* ci = NULL;
  if (OB_SUCCESS == ret) {
    ret = ups_mutator.get_mutator().get_cell(&mutator_ci);
    if (OB_SUCCESS != ret
        || NULL == mutator_ci) {
      TBSYS_LOG(WARN, "get cell from freeze ups_mutator fail ret=%d", ret);
      ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
    } else {
      ci = &(mutator_ci->cell_info);
    }
  }

  ObString str_freeze_param;
  FreezeParamHeader* header = NULL;
  if (OB_SUCCESS == ret) {
    ret = ci->value_.get_varchar(str_freeze_param);
    header = (FreezeParamHeader*)str_freeze_param.ptr();
    if (OB_SUCCESS != ret
        || NULL == header
        || (int64_t)sizeof(FreezeParamHeader) >= str_freeze_param.length()) {
      TBSYS_LOG(WARN, "get freeze_param from freeze ups_mutator fail ret=%d header=%p length=%ld",
                ret, header, str_freeze_param.length());
      ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
    }
  }

  if (OB_SUCCESS == ret) {
    static int64_t cur_version = 0;
    if (cur_version <= header->version) {
      cur_version = header->version;
    } else {
      TBSYS_LOG(ERROR, "there is a old clog version=%ld follow a new clog version=%ld",
                header->version, cur_version);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret) {
    bool major_version_changed = false;
    switch (header->version) {
    // 回放旧日志中的freeze操作
    case 1: {
      FreezeParamV1* freeze_param_v1 = (FreezeParamV1*)(header->buf);
      uint64_t new_version = freeze_param_v1->active_version + 1;
      uint64_t new_log_file_id = freeze_param_v1->new_log_file_id;
      SSTableID new_sst_id;
      SSTableID frozen_sst_id;
      new_sst_id.major_version = new_version;
      new_sst_id.minor_version_start = SSTableID::START_MINOR_VERSION;
      new_sst_id.minor_version_end = SSTableID::START_MINOR_VERSION;
      frozen_sst_id = new_sst_id;
      frozen_sst_id.major_version -= 1;
      major_version_changed = true;
      ret = table_mgr_.replay_freeze_memtable(new_sst_id.id, frozen_sst_id.id, new_log_file_id);
      TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v1 active_version=%ld new_log_file_id=%ld op_flag=%lx ret=%d",
                freeze_param_v1->active_version, freeze_param_v1->new_log_file_id, freeze_param_v1->op_flag, ret);
      break;
    }
    case 2: {
      FreezeParamV2* freeze_param_v2 = (FreezeParamV2*)(header->buf);
      uint64_t new_version = freeze_param_v2->active_version;
      uint64_t frozen_version = freeze_param_v2->frozen_version;
      uint64_t new_log_file_id = freeze_param_v2->new_log_file_id;
      CommonSchemaManagerWrapper schema_manager;
      char* data = str_freeze_param.ptr();
      int64_t length = str_freeze_param.length();
      int64_t pos = sizeof(FreezeParamHeader) + sizeof(FreezeParamV2);
      if (OB_SUCCESS == (ret = schema_manager.deserialize(data, length, pos))) {
        ret = schema_mgr_.set_schema_mgr(schema_manager);
      }
      if (OB_SUCCESS == ret) {
        ret = table_mgr_.replay_freeze_memtable(new_version, frozen_version, new_log_file_id);
        if (OB_SUCCESS == ret) {
          SSTableID sst_id_new = new_version;
          SSTableID sst_id_frozen = frozen_version;
          if (sst_id_new.major_version != sst_id_frozen.major_version) {
            major_version_changed = true;
          }
        }
      }
      TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v2 active_version=%ld new_log_file_id=%ld op_flag=%lx ret=%d",
                freeze_param_v2->active_version, freeze_param_v2->new_log_file_id, freeze_param_v2->op_flag, ret);
      break;
    }
    case 3: {
      FreezeParamV3* freeze_param_v3 = (FreezeParamV3*)(header->buf);
      uint64_t new_version = freeze_param_v3->active_version;
      uint64_t frozen_version = freeze_param_v3->frozen_version;
      uint64_t new_log_file_id = freeze_param_v3->new_log_file_id;
      int64_t time_stamp = freeze_param_v3->time_stamp;
      CommonSchemaManagerWrapper schema_manager;
      char* data = str_freeze_param.ptr();
      int64_t length = str_freeze_param.length();
      int64_t pos = sizeof(FreezeParamHeader) + sizeof(FreezeParamV3);
      if (OB_SUCCESS == (ret = schema_manager.deserialize(data, length, pos))) {
        ret = schema_mgr_.set_schema_mgr(schema_manager);
      }
      if (OB_SUCCESS == ret) {
        ret = table_mgr_.replay_freeze_memtable(new_version, frozen_version, new_log_file_id, time_stamp);
        if (OB_SUCCESS == ret) {
          SSTableID sst_id_new = new_version;
          SSTableID sst_id_frozen = frozen_version;
          if (sst_id_new.major_version != sst_id_frozen.major_version) {
            major_version_changed = true;
          }
        }
      }
      TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v3 active_version=%ld new_log_file_id=%ld op_flag=%lx ret=%d",
                freeze_param_v3->active_version, freeze_param_v3->new_log_file_id, freeze_param_v3->op_flag, ret);
      break;
    }
    default:
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "freeze_param version error %d", header->version);
      break;
    }
    if (OB_SUCCESS == ret) {
      ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main) {
        TBSYS_LOG(WARN, "get ups main fail");
        ret = OB_ERROR;
      } else {
        if (major_version_changed) {
          ups_main->get_update_server().submit_report_freeze();
        }
        ups_main->get_update_server().submit_handle_frozen();
      }
    }
  }
  ups_mutator.get_mutator().reset_iter();
  log_table_info();
  return ret;
}

int ObUpsTableMgr :: replay(ObUpsMutator& ups_mutator) {
  int ret = OB_SUCCESS;
  if (ups_mutator.is_freeze_memtable()) {
    has_started_ = true;
    ret = handle_freeze_log_(ups_mutator);
  } else if (ups_mutator.is_drop_memtable()) {
    TBSYS_LOG(INFO, "ignore drop commit log");
  } else if (ups_mutator.is_first_start()) {
    has_started_ = true;
    ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
    if (NULL == main) {
      TBSYS_LOG(ERROR, "get updateserver main null pointer");
    } else {
      table_mgr_.sstable_scan_finished(main->get_update_server().get_param().get_minor_num_limit());
    }
    TBSYS_LOG(INFO, "handle first start flag log");
  } else {
    INC_STAT_INFO(UPS_STAT_APPLY_COUNT, 1);
    ret = set_mutator_(ups_mutator);
  }
  return ret;
}

template <typename T>
int ObUpsTableMgr :: flush_obj_to_log(const LogCommand log_command, T& obj) {
  int ret = OB_SUCCESS;
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(ERROR, "get updateserver main null pointer");
    ret = OB_ERROR;
  } else {
    ObUpsLogMgr& log_mgr = ups_main->get_update_server().get_log_mgr();
    int64_t serialize_size = 0;
    if (NULL == log_buffer_) {
      TBSYS_LOG(WARN, "log buffer malloc fail");
      ret = OB_ERROR;
    } else if (OB_SUCCESS != (ret = ups_serialize(obj, log_buffer_, LOG_BUFFER_SIZE, serialize_size))) {
      TBSYS_LOG(WARN, "obj serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
    } else {
      if (OB_SUCCESS != (ret = log_mgr.write_log(log_command, log_buffer_, serialize_size))) {
        TBSYS_LOG(WARN, "write log fail log_command=%d log_buffer_=%p serialize_size=%ld ret=%d",
                  log_command, log_buffer_, serialize_size, ret);
      } else if (OB_SUCCESS != (ret = log_mgr.flush_log())) {
        TBSYS_LOG(WARN, "flush log fail ret=%d", ret);
      } else {
        // do nothing
      }
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "write log fail ret=%d, will kill self", ret);
        kill(getpid(), SIGTERM);
        ret = OB_RESPONSE_TIME_OUT;
      }
    }
  }
  return ret;
}

int ObUpsTableMgr :: write_start_log() {
  int ret = OB_SUCCESS;
  if (has_started_) {
    TBSYS_LOG(INFO, "system has started need not write start flag");
  } else {
    ObUpsMutator ups_mutator;
    ups_mutator.set_first_start();
    ret = flush_obj_to_log(OB_LOG_UPS_MUTATOR, ups_mutator);
    TBSYS_LOG(INFO, "write first start flag ret=%d", ret);
  }
  return ret;
}

int ObUpsTableMgr :: set_mutator_(ObUpsMutator& mutator) {
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  TableEngineTransHandle trans_handle;
  if (NULL == (table_item = table_mgr_.get_active_memtable())) {
    TBSYS_LOG(WARN, "failed to acquire active memtable");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_item->get_memtable().start_transaction(WRITE_TRANSACTION, trans_handle))) {
    TBSYS_LOG(WARN, "start transaction fail ret=%d", ret);
  } else {
    //FILL_TRACE_LOG("start replay one mutator");
    if (OB_SUCCESS != (ret = table_item->get_memtable().start_mutation(trans_handle))) {
      TBSYS_LOG(WARN, "start mutation fail ret=%d", ret);
    } else {
      if (OB_SUCCESS != (ret = table_item->get_memtable().set(trans_handle, mutator, check_checksum_))) {
        TBSYS_LOG(WARN, "set to memtable fail ret=%d", ret);
      } else {
        TBSYS_LOG(DEBUG, "replay mutator succ");
      }
      table_item->get_memtable().end_mutation(trans_handle, OB_SUCCESS != ret);
    }
    table_item->get_memtable().end_transaction(trans_handle, OB_SUCCESS != ret);
    //FILL_TRACE_LOG("ret=%d", ret);
    //PRINT_TRACE_LOG();
  }
  if (NULL != table_item) {
    table_mgr_.revert_active_memtable(table_item);
  }
  return ret;
}

int ObUpsTableMgr :: apply(ObUpsMutator& ups_mutator) {
  return apply(write_trans_handle_, ups_mutator);
}

int ObUpsTableMgr :: apply(UpsTableMgrTransHandle& handle, ObUpsMutator& ups_mutator) {
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  if (NULL == (table_item = table_mgr_.get_active_memtable())) {
    TBSYS_LOG(WARN, "failed to acquire active memtable");
    ret = OB_ERROR;
  } else {
    MemTable* p_active_memtable = &(table_item->get_memtable());
    if (OB_SUCCESS != trans_name2id_(ups_mutator.get_mutator())) {
      TBSYS_LOG(WARN, "cellinfo do not pass valid check or trans name to id fail");
      ret = OB_SCHEMA_ERROR;
    } else if (OB_SUCCESS != prepare_sem_constraint_(p_active_memtable, ups_mutator.get_mutator())) {
      TBSYS_LOG(WARN, "prepare sem constraint fail");
      ret = OB_SCHEMA_ERROR;
    } else if (OB_SUCCESS != p_active_memtable->start_mutation(handle.trans_handle)) {
      TBSYS_LOG(WARN, "start mutation fail");
      ret = OB_ERROR;
    } else {
      ups_mutator.set_mutate_timestamp(tbsys::CTimeUtil::getTime());
      if (OB_SUCCESS != (ret = p_active_memtable->set(handle.trans_handle, ups_mutator))) {
        TBSYS_LOG(WARN, "set to memtable fail ret=%d", ret);
      } else {
        // 注意可能返回OB_EAGAIN
        ret = fill_commit_log_(ups_mutator);
      }
      if (OB_SUCCESS == ret) {
        bool rollback = false;
        ret = p_active_memtable->end_mutation(handle.trans_handle, rollback);
      } else {
        bool rollback = true;
        p_active_memtable->end_mutation(handle.trans_handle, rollback);
      }
    }
    table_mgr_.revert_active_memtable(table_item);
  }
  log_memtable_memory_info();
  return ret;
}

void ObUpsTableMgr :: log_memtable_memory_info() {
  static int64_t counter = 0;
  static const int64_t mod = 100000;
  if (0 == (counter++ % mod)) {
    log_table_info();
  }
}

void ObUpsTableMgr :: get_memtable_memory_info(TableMemInfo& mem_info) {
  TableItem* table_item = table_mgr_.get_active_memtable();
  if (NULL != table_item) {
    mem_info.memtable_used = table_item->get_memtable().used() + table_mgr_.get_frozen_memused();
    mem_info.memtable_total = table_item->get_memtable().total() + table_mgr_.get_frozen_memtotal();
    MemTableAttr memtable_attr;
    table_item->get_memtable().get_attr(memtable_attr);
    mem_info.memtable_limit = memtable_attr.total_memlimit;
    table_mgr_.revert_active_memtable(table_item);
  }
}

void ObUpsTableMgr::set_memtable_attr(const MemTableAttr& memtable_attr) {
  table_mgr_.set_memtable_attr(memtable_attr);
}

int ObUpsTableMgr::get_memtable_attr(MemTableAttr& memtable_attr) {
  return table_mgr_.get_memtable_attr(memtable_attr);
}

void ObUpsTableMgr :: update_memtable_stat_info() {
  TableItem* table_item = table_mgr_.get_active_memtable();
  if (NULL != table_item) {
    MemTableAttr memtable_attr;
    table_mgr_.get_memtable_attr(memtable_attr);
    int64_t active_limit = memtable_attr.total_memlimit;
    int64_t frozen_limit = memtable_attr.total_memlimit;
    int64_t active_total = table_item->get_memtable().total();
    int64_t frozen_total = table_mgr_.get_frozen_memtotal();
    int64_t active_used = table_item->get_memtable().used();
    int64_t frozen_used = table_mgr_.get_frozen_memused();
    int64_t active_row_count = table_item->get_memtable().size();
    int64_t frozen_row_count = table_mgr_.get_frozen_rowcount();

    SET_STAT_INFO(UPS_STAT_MEMTABLE_TOTAL, active_total + frozen_total);
    SET_STAT_INFO(UPS_STAT_MEMTABLE_USED, active_used + frozen_used);
    SET_STAT_INFO(UPS_STAT_TOTAL_LINE, active_row_count + frozen_row_count);

    SET_STAT_INFO(UPS_STAT_ACTIVE_MEMTABLE_LIMIT, active_limit);
    SET_STAT_INFO(UPS_STAT_ACTICE_MEMTABLE_TOTAL, active_total);
    SET_STAT_INFO(UPS_STAT_ACTIVE_MEMTABLE_USED, active_used);
    SET_STAT_INFO(UPS_STAT_ACTIVE_TOTAL_LINE, active_row_count);

    SET_STAT_INFO(UPS_STAT_FROZEN_MEMTABLE_LIMIT, frozen_limit);
    SET_STAT_INFO(UPS_STAT_FROZEN_MEMTABLE_TOTAL, frozen_total);
    SET_STAT_INFO(UPS_STAT_FROZEN_MEMTABLE_USED, frozen_used);
    SET_STAT_INFO(UPS_STAT_FROZEN_TOTAL_LINE, frozen_row_count);

    table_mgr_.revert_active_memtable(table_item);
  }
}

int ObUpsTableMgr :: set_schemas(const CommonSchemaManagerWrapper& schema_manager) {
  int ret = OB_SUCCESS;

  ret = schema_mgr_.set_schema_mgr(schema_manager);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "set_schemas error, ret=%d schema_version=%ld", ret, schema_manager.get_version());
  }

  return ret;
}

int ObUpsTableMgr :: switch_schemas(const CommonSchemaManagerWrapper& schema_manager) {
  int ret = OB_SUCCESS;

  int64_t serialize_size = 0;

  ret = schema_mgr_.set_schema_mgr(schema_manager);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "set_schemas error, ret=%d", ret);
  } else {
    ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
    if (NULL == main) {
      TBSYS_LOG(ERROR, "get updateserver main null pointer");
      ret = OB_ERROR;
    } else {
      if (NULL == log_buffer_) {
        TBSYS_LOG(WARN, "log buffer malloc fail");
        ret = OB_ERROR;
      }
    }

    if (OB_SUCCESS == ret) {
      ret = schema_manager.serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                  log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
      } else {
        ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
        ret = log_mgr.write_and_flush_log(OB_UPS_SWITCH_SCHEMA, log_buffer_, serialize_size);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(WARN, "write log fail log_buffer_=%p serialize_size=%ld ret=%d", log_buffer_, serialize_size, ret);
        }
      }
    }
  }

  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "schema_version=%ld", schema_manager.get_version());
  }

  return ret;
}

int ObUpsTableMgr :: create_index() {
  int ret = OB_SUCCESS;
  // need not create index
  TBSYS_LOG(WARN, "no create index impl now");
  return ret;
}

int ObUpsTableMgr :: get(const ObGetParam& get_param, ObScanner& scanner, const int64_t start_time, const int64_t timeout) {
  int ret = OB_SUCCESS;
  TableList* table_list = GET_TSI(TableList);
  uint64_t max_valid_version = 0;
  if (NULL == table_list) {
    TBSYS_LOG(WARN, "get tsi table_list fail");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_mgr_.acquire_table(get_param.get_version_range(), max_valid_version, *table_list))
             || 0 == table_list->size()) {
    TBSYS_LOG(WARN, "acquire table fail version_range=%s", range2str(get_param.get_version_range()));
    ret = (OB_SUCCESS == ret) ? OB_INVALID_START_VERSION : ret;
  } else {
    FILL_TRACE_LOG("version=%s table_num=%ld", range2str(get_param.get_version_range()), table_list->size());
    ObMerger merger;
    TableList::iterator iter;
    int64_t index = 0;
    SSTableID sst_id;
    for (iter = table_list->begin(); iter != table_list->end(); iter++, index++) {
      ITableEntity* table_entity = *iter;
      ITableUtils* table_utils = NULL;
      if (NULL == table_entity) {
        TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(get_param.get_version_range()));
        ret = OB_ERROR;
        break;
      }
      if (NULL == (table_utils = table_entity->get_tsi_tableutils(index))) {
        TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
        ret = OB_TOO_MANY_SSTABLE;
        break;
      }
      table_utils->reset();
      if (OB_SUCCESS != (ret = table_entity->start_transaction(table_utils->get_trans_handle()))) {
        TBSYS_LOG(WARN, "start transaction fail ret=%d", ret);
        break;
      }
      sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
      FILL_TRACE_LOG("get table %s ret=%d", sst_id.log_str(), ret);
    }

    if (OB_SUCCESS == ret) {
      ret = get_(*table_list, get_param, scanner, start_time, timeout);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "failed to get_ ret=%d", ret);
      } else {
        scanner.set_data_version(max_valid_version);
      }
    }

    for (iter = table_list->begin(), index = 0; iter != table_list->end(); iter++, index++) {
      ITableEntity* table_entity = *iter;
      ITableUtils* table_utils = NULL;
      if (NULL != table_entity
          && NULL != (table_utils = table_entity->get_tsi_tableutils(index))) {
        table_entity->end_transaction(table_utils->get_trans_handle());
      }
    }
    table_mgr_.revert_table(*table_list);
  }
  return ret;
}

int ObUpsTableMgr :: scan(const ObScanParam& scan_param, ObScanner& scanner, const int64_t start_time, const int64_t timeout) {
  int ret = OB_SUCCESS;
  TableList* table_list = GET_TSI(TableList);
  uint64_t max_valid_version = 0;
  const ObRange* scan_range = NULL;
  if (NULL == table_list) {
    TBSYS_LOG(WARN, "get tsi table_list fail");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = table_mgr_.acquire_table(scan_param.get_version_range(), max_valid_version, *table_list))
             || 0 == table_list->size()) {
    TBSYS_LOG(WARN, "acquire table fail version_range=%s", range2str(scan_param.get_version_range()));
    ret = (OB_SUCCESS == ret) ? OB_INVALID_START_VERSION : ret;
  } else if (NULL == (scan_range = scan_param.get_range())) {
    TBSYS_LOG(WARN, "invalid scan range");
    ret = OB_ERROR;
  } else {
    ColumnFilter cf;
    ColumnFilter* pcf = ITableEntity::build_columnfilter(scan_param, &cf);
    FILL_TRACE_LOG("%s columns=%s direction=%d version=%s table_num=%ld",
                   scan_range2str(*scan_range),
                   (NULL == pcf) ? "nil" : pcf->log_str(),
                   scan_param.get_scan_direction(),
                   range2str(scan_param.get_version_range()),
                   table_list->size());

    ObMerger merger;
    bool is_multi_update = false;
    merger.set_asc(scan_param.get_scan_direction() == ObScanParam::FORWARD);
    TableList::iterator iter;
    int64_t index = 0;
    SSTableID sst_id;
    for (iter = table_list->begin(); iter != table_list->end(); iter++, index++) {
      ITableEntity* table_entity = *iter;
      ITableUtils* table_utils = NULL;
      if (NULL == table_entity) {
        TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(scan_param.get_version_range()));
        ret = OB_ERROR;
        break;
      }
      if (NULL == (table_utils = table_entity->get_tsi_tableutils(index))) {
        TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
        ret = OB_TOO_MANY_SSTABLE;
        break;
      }
      table_utils->reset();
      if (OB_SUCCESS != (ret = table_entity->start_transaction(table_utils->get_trans_handle()))) {
        TBSYS_LOG(WARN, "start transaction fail ret=%d scan_range=%s", ret, scan_range2str(*scan_range));
        break;
      }
      if (OB_SUCCESS != (ret = table_entity->scan(table_utils->get_trans_handle(), scan_param, &(table_utils->get_table_iter())))) {
        TBSYS_LOG(WARN, "table entity scan fail ret=%d scan_range=%s", ret, scan_range2str(*scan_range));
        break;
      }
      if (OB_SUCCESS != (ret = merger.add_iterator(&(table_utils->get_table_iter())))) {
        TBSYS_LOG(WARN, "add iterator to merger fail ret=%d scan_range=%s", ret, scan_range2str(*scan_range));
        break;
      }
      is_multi_update |= table_utils->get_table_iter().is_multi_update();
      sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
      FILL_TRACE_LOG("scan table %s ret=%d", sst_id.log_str(), ret);
    }

    if (OB_SUCCESS == ret) {
      int64_t row_count = 0;
      ret = add_to_scanner_(merger, scanner, is_multi_update, row_count, start_time, timeout);
      FILL_TRACE_LOG("add to scanner scanner_size=%ld row_count=%ld ret=%d", scanner.get_size(), row_count, ret);
      if (OB_SIZE_OVERFLOW == ret) {
        if (row_count > 0) {
          scanner.set_is_req_fullfilled(false, row_count);
          ret = OB_SUCCESS;
        } else {
          TBSYS_LOG(WARN, "memory is not enough to add even one row");
          ret = OB_ERROR;
        }
      } else if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "failed to add data from ups_merger to scanner, ret=%d", ret);
      } else {
        scanner.set_is_req_fullfilled(true, row_count);
      }
      if (OB_SUCCESS == ret) {
        scanner.set_data_version(max_valid_version);
        ObRange range;
        range.table_id_ = scan_param.get_table_id();
        range.border_flag_.set_min_value();
        range.border_flag_.set_max_value();
        ret = scanner.set_range(range);
      }
    }

    for (iter = table_list->begin(), index = 0; iter != table_list->end(); iter++, index++) {
      ITableEntity* table_entity = *iter;
      ITableUtils* table_utils = NULL;
      if (NULL != table_entity
          && NULL != (table_utils = table_entity->get_tsi_tableutils(index))) {
        table_utils->reset();
        table_entity->end_transaction(table_utils->get_trans_handle());
      }
    }
    table_mgr_.revert_table(*table_list);
  }
  return ret;
}

int ObUpsTableMgr :: get_(TableList& table_list, const ObGetParam& get_param, ObScanner& scanner,
                          const int64_t start_time, const int64_t timeout) {
  int err = OB_SUCCESS;

  int64_t cell_size = get_param.get_cell_size();
  const ObCellInfo* cell = NULL;

  if (cell_size <= 0) {
    TBSYS_LOG(WARN, "invalid param, cell_size=%ld", cell_size);
    err = OB_INVALID_ARGUMENT;
  } else if (NULL == (cell = get_param[0])) {
    TBSYS_LOG(WARN, "invalid param first cellinfo");
    err = OB_ERROR;
  } else {
    int64_t last_cell_idx = 0;
    ObString last_row_key = cell->row_key_;
    uint64_t last_table_id = cell->table_id_;
    int64_t cell_idx = 1;

    while (OB_SUCCESS == err && cell_idx < cell_size) {
      if (NULL == (cell = get_param[cell_idx])) {
        TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", cell_idx);
        err = OB_ERROR;
      } else if (cell->row_key_ != last_row_key ||
                 cell->table_id_ != last_table_id) {
        err = get_row_(table_list, last_cell_idx, cell_idx - 1, get_param, scanner, start_time, timeout);
        if (OB_SIZE_OVERFLOW == err) {
          TBSYS_LOG(WARN, "allocate memory failed, first_idx=%ld, last_idx=%ld",
                    last_cell_idx, cell_idx - 1);
        } else if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "failed to get_row_, first_idx=%ld, last_idx=%ld, err=%d",
                    last_cell_idx, cell_idx - 1, err);
        } else {
          last_cell_idx = cell_idx;
          last_row_key = cell->row_key_;
          last_table_id = cell->table_id_;
        }
      }

      if (OB_SUCCESS == err) {
        ++cell_idx;
      }
    }

    if (OB_SUCCESS == err) {
      err = get_row_(table_list, last_cell_idx, cell_idx - 1, get_param, scanner, start_time, timeout);
      if (OB_SIZE_OVERFLOW == err) {
        TBSYS_LOG(WARN, "allocate memory failed, first_idx=%ld, last_idx=%ld",
                  last_cell_idx, cell_idx - 1);
      } else if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to get_row_, first_idx=%ld, last_idx=%ld, err=%d",
                  last_cell_idx, cell_idx - 1, err);
      }
    }

    FILL_TRACE_LOG("table_list_size=%ld get_param_cell_size=%ld get_param_row_size=%ld last_cell_idx=%ld cell_idx=%ld scanner_size=%ld ret=%d",
                   table_list.size(), get_param.get_cell_size(), get_param.get_row_size(), last_cell_idx, cell_idx, scanner.get_size(), err);
    if (OB_SIZE_OVERFLOW == err) {
      // wrap error
      scanner.set_is_req_fullfilled(false, last_cell_idx);
      err = OB_SUCCESS;
    } else if (OB_SUCCESS == err) {
      scanner.set_is_req_fullfilled(true, cell_idx);
    }
  }

  return err;
}

int ObUpsTableMgr :: get_row_(TableList& table_list, const int64_t first_cell_idx, const int64_t last_cell_idx,
                              const ObGetParam& get_param, ObScanner& scanner,
                              const int64_t start_time, const int64_t timeout) {
  int ret = OB_SUCCESS;
  int64_t cell_size = get_param.get_cell_size();
  const ObCellInfo* cell = NULL;
  ColumnFilter* column_filter = NULL;

  if (first_cell_idx > last_cell_idx) {
    TBSYS_LOG(WARN, "invalid param, first_cell_idx=%ld, last_cell_idx=%ld",
              first_cell_idx, last_cell_idx);
    ret = OB_INVALID_ARGUMENT;
  } else if (cell_size <= last_cell_idx) {
    TBSYS_LOG(WARN, "invalid status, cell_size=%ld, last_cell_idx=%ld",
              cell_size, last_cell_idx);
    ret = OB_ERROR;
  } else if (NULL == (cell = get_param[first_cell_idx])) {
    TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", first_cell_idx);
    ret = OB_ERROR;
  } else if (NULL == (column_filter = ITableEntity::get_tsi_columnfilter())) {
    TBSYS_LOG(WARN, "get tsi columnfilter fail");
    ret = OB_ERROR;
  } else {
    ObMerger merger;
    bool is_multi_update = false;
    uint64_t table_id = cell->table_id_;
    ObString row_key = cell->row_key_;
    column_filter->clear();
    for (int64_t i = first_cell_idx; i <= last_cell_idx; ++i) {
      if (NULL != get_param[i]) { // double check
        column_filter->add_column(get_param[i]->column_id_);
      }
    }

    TableList::iterator iter;
    int64_t index = 0;
    for (iter = table_list.begin(), index = 0; iter != table_list.end(); iter++, index++) {
      ITableEntity* table_entity = *iter;
      ITableUtils* table_utils = NULL;
      if (NULL == table_entity) {
        TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(get_param.get_version_range()));
        ret = OB_ERROR;
        break;
      }
      if (NULL == (table_utils = table_entity->get_tsi_tableutils(index))) {
        TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
        ret = OB_ERROR;
        break;
      }
      if (OB_SUCCESS != (ret = table_entity->get(table_utils->get_trans_handle(), table_id, row_key, column_filter, &(table_utils->get_table_iter())))) {
        TBSYS_LOG(WARN, "table entity get fail ret=%d table_id=%lu row_key=[%s] columns=[%s]",
                  ret, table_id, print_string(row_key), column_filter->log_str());
        break;
      }
      if (OB_SUCCESS != (ret = merger.add_iterator(&(table_utils->get_table_iter())))) {
        TBSYS_LOG(WARN, "add iterator to merger fail ret=%d", ret);
        break;
      }
      is_multi_update |= table_utils->get_table_iter().is_multi_update();
      TBSYS_LOG(DEBUG, "get row row_key=[%s] row_key_ptr=%p columns=[%s] iter=%p",
                print_string(row_key), row_key.ptr(), column_filter->log_str(), &(table_utils->get_table_iter()));
    }

    if (OB_SUCCESS == ret) {
      int64_t row_count = 0;
      ret = add_to_scanner_(merger, scanner, is_multi_update, row_count, start_time, timeout);
      if (OB_SIZE_OVERFLOW == ret) {
        // return ret
      } else if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "failed to add data from merger to scanner, ret=%d", ret);
      } else {
        // TODO (rizhao) add successful cell num to scanner
      }
    }
  }

  TBSYS_LOG(DEBUG, "[op=GET_ROW] [first_cell_idx=%ld] [last_cell_idx=%ld] [ret=%d]", first_cell_idx, last_cell_idx, ret);
  return ret;
}

int ObUpsTableMgr :: add_to_scanner_(ObMerger& ups_merger, ObScanner& scanner, const bool is_multi_update, int64_t& row_count,
                                     const int64_t start_time, const int64_t timeout) {
  int err = OB_SUCCESS;

  ObCellInfo* cell = NULL;
  bool is_row_changed = false;
  row_count = 0;
  ObRowCompaction* row_compaction = GET_TSI(ObRowCompaction);
  ObIterator* iter = NULL;
  if (NULL == row_compaction) {
    TBSYS_LOG(WARN, "get tsi row_compaction fail");
    iter = &ups_merger;
  } else if (!is_multi_update) {
    iter = &ups_merger;
  } else {
    row_compaction->set_iterator(&ups_merger);
    iter = row_compaction;
  }
  while (OB_SUCCESS == err && (OB_SUCCESS == (err = iter->next_cell())))
    //while (OB_SUCCESS == err && (OB_SUCCESS == (err = ups_merger.next_cell())))
  {
    err = iter->get_cell(&cell, &is_row_changed);
    //err = ups_merger.get_cell(&cell, &is_row_changed);
    if (OB_SUCCESS != err || NULL == cell) {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", err);
      err = OB_ERROR;
    } else {
      bool is_timeout = false;
      TBSYS_LOG(DEBUG, "from merger %s is_row_changed=%s", print_cellinfo(cell), STR_BOOL(is_row_changed));
      if (is_row_changed) {
        ++row_count;
      }
      if (is_row_changed
          && 1 < row_count
          && (start_time + timeout) < tbsys::CTimeUtil::getTime()) {
        TBSYS_LOG(WARN, "get or scan too long time, start_time=%ld timeout=%ld timeu=%ld row_count=%ld",
                  start_time, timeout, tbsys::CTimeUtil::getTime() - start_time, row_count - 1);
        err = OB_SIZE_OVERFLOW;
        is_timeout = true;
      } else {
        err = scanner.add_cell(*cell);
      }
      if (OB_SUCCESS != err) {
        row_count -= 1;
        if (1 > row_count) {
          TBSYS_LOG(WARN, "invalid row_count=%ld", row_count);
        }
      }
      if (OB_SIZE_OVERFLOW == err
          && !is_timeout) {
        TBSYS_LOG(WARN, "scanner memory is not enough, rollback last row");
        // rollback last row
        if (OB_SUCCESS != scanner.rollback()) {
          TBSYS_LOG(WARN, "failed to rollback");
          err = OB_ERROR;
        }
      } else if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to add cell, err=%d", err);
      }
    }
  }

  if (OB_ITER_END == err) {
    // wrap error
    err = OB_SUCCESS;
  }

  return err;
}

int ObUpsTableMgr :: clear_active_memtable() {
  return table_mgr_.clear_active_memtable();
}

int ObUpsTableMgr::trans_name2id_(ObMutator& mutator) {
  int ret = OB_SUCCESS;
  CellInfoProcessor ci_proc;
  while (OB_SUCCESS == ret
         && OB_SUCCESS == (ret = mutator.next_cell())) {
    ObMutatorCellInfo* mutator_ci = NULL;
    ObCellInfo* ci = NULL;
    if (OB_SUCCESS == mutator.get_cell(&mutator_ci)
        && NULL != mutator_ci) {
      const CommonTableSchema* table_schema = NULL;
      const CommonColumnSchema* column_schema = NULL;
      UpsSchemaMgr::SchemaHandle schema_handle;
      ci = &(mutator_ci->cell_info);
      if (!ci_proc.analyse_syntax(*mutator_ci)) {
        ret = OB_SCHEMA_ERROR;
      } else if (ci_proc.need_skip()) {
        continue;
      } else if (!ci_proc.cellinfo_check(*ci)) {
        ret = OB_SCHEMA_ERROR;
      } else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_handle(schema_handle))) {
        TBSYS_LOG(WARN, "get_schema_handle fail ret=%d", ret);
      } else {
        // 调用schema转化name2id
        if (NULL == (table_schema = schema_mgr_.get_table_schema(schema_handle, ci->table_name_))) {
          TBSYS_LOG(WARN, "get schema fail table_name=%.*s table_name_length=%d",
                    ci->table_name_.length(), ci->table_name_.ptr(), ci->table_name_.length());
          ret = OB_SCHEMA_ERROR;
        } else if (!CellInfoProcessor::cellinfo_check(*ci, *table_schema)) {
          ret = OB_SCHEMA_ERROR;
        } else if (common::ObActionFlag::OP_DEL_ROW == ci_proc.get_op_type()) {
          // do not trans column name
          ci->table_id_ = table_schema->get_table_id();
          ci->column_id_ = OB_INVALID_ID;
          ci->table_name_.assign_ptr(NULL, 0);
          ci->column_name_.assign_ptr(NULL, 0);
        } else if (NULL == (column_schema = schema_mgr_.get_column_schema(schema_handle, ci->table_name_, ci->column_name_))) {
          TBSYS_LOG(WARN, "get column schema fail table_name=%.*s table_id=%lu column_name=%.*s column_name_length=%d",
                    ci->table_name_.length(), ci->table_name_.ptr(), table_schema->get_table_id(),
                    ci->column_name_.length(), ci->column_name_.ptr(), ci->column_name_.length());
          ret = OB_SCHEMA_ERROR;
        }
        // 数据类型与合法性检查
        else if (!CellInfoProcessor::cellinfo_check(*ci, *column_schema)) {
          ret = OB_SCHEMA_ERROR;
        } else {
          ci->table_id_ = table_schema->get_table_id();
          ci->column_id_ = column_schema->get_id();
          ci->table_name_.assign_ptr(NULL, 0);
          ci->column_name_.assign_ptr(NULL, 0);
        }
        schema_mgr_.revert_schema_handle(schema_handle);
      }
    } else {
      ret = OB_ERROR;
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  mutator.reset_iter();
  return ret;
};

int ObUpsTableMgr::fill_commit_log_(ObUpsMutator& ups_mutator) {
  int ret = OB_SUCCESS;
  int64_t serialize_size = 0;
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  if (NULL == main) {
    TBSYS_LOG(ERROR, "get updateserver main null pointer");
    ret = OB_ERROR;
  } else if (NULL == log_buffer_) {
    TBSYS_LOG(WARN, "log buffer malloc fail");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = ups_mutator.serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size))) {
    TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
              log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
  } else {
    FILL_TRACE_LOG("ups_mutator serialize");
    ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
    if (OB_BUF_NOT_ENOUGH == (ret = log_mgr.write_log(OB_LOG_UPS_MUTATOR, log_buffer_, serialize_size))) {
      TBSYS_LOG(INFO, "log buffer full");
      ret = OB_EAGAIN;
    } else if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "write log fail ret=%d, will kill self", ret);
      kill(getpid(), SIGTERM);
      ret = OB_RESPONSE_TIME_OUT;
    }
  }
  FILL_TRACE_LOG("size=%ld ret=%d", serialize_size, ret);
  return ret;
};

int ObUpsTableMgr::flush_commit_log_() {
  int ret = OB_SUCCESS;
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  if (NULL == main) {
    TBSYS_LOG(ERROR, "get updateserver main null pointer");
    ret = OB_ERROR;
  } else {
    ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
    ret = log_mgr.flush_log();
  }
  // 只要不能刷新日志都要优雅退出
  //if (OB_SUCCESS != ret)
  //{
  //  TBSYS_LOG(WARN, "flush log fail ret=%d, will kill self", ret);
  //  kill(getpid(), SIGTERM);
  //}
  FILL_TRACE_LOG("ret=%d", ret);
  return ret;
};

int ObUpsTableMgr::prepare_sem_constraint_(MemTable* p_active_memtable, ObMutator& mutator) {
  int ret = OB_SUCCESS;
  CellInfoProcessor ci_proc;
  while (OB_SUCCESS == ret
         && OB_SUCCESS == (ret = mutator.next_cell())) {
    ObMutatorCellInfo* mutator_ci = NULL;
    ObCellInfo* ci = NULL;
    if (OB_SUCCESS == mutator.get_cell(&mutator_ci)
        && NULL != mutator_ci) {
      ci = &(mutator_ci->cell_info);
      if (!ci_proc.analyse_syntax(*mutator_ci)) {
        ret = OB_ERROR;
      } else if (ci_proc.need_skip()) {
        continue;
      } else if (p_active_memtable->need_query_sst(ci->table_id_, ci->row_key_, ci_proc.is_db_sem())) {
        // TODO 准备查询chunkserver的请求
      }
    } else {
      ret = OB_ERROR;
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  mutator.reset_iter();

  if (OB_SUCCESS == ret) {
    // TODO 同步查询chunkserver
  }

  if (OB_SUCCESS == ret) {
    // TODO 根据查询结果调用memtable的set_row_exist
  }
  return ret;
};

void ObUpsTableMgr::dump_memtable(const ObString& dump_dir) {
  table_mgr_.dump_memtable2text(dump_dir);
}

void ObUpsTableMgr::dump_schemas() {
  schema_mgr_.dump2text();
}

int ObUpsTableMgr::sstable_scan_finished(const int64_t minor_num_limit) {
  return table_mgr_.sstable_scan_finished(minor_num_limit);
}

int ObUpsTableMgr::check_sstable_id() {
  return table_mgr_.check_sstable_id();
}

void ObUpsTableMgr::log_table_info() {
  TBSYS_LOG(INFO, "replay checksum flag=%s", STR_BOOL(check_checksum_));
  table_mgr_.log_table_info();
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL != ups_main) {
    ups_main->get_update_server().get_sstable_mgr().log_sstable_info();
  }
}

void ObUpsTableMgr::set_warm_up_percent(const int64_t warm_up_percent) {
  table_mgr_.set_warm_up_percent(warm_up_percent);
}

bool get_key_prefix(const TEKey& te_key, TEKey& prefix_key) {
  bool bret = false;
  TEKey tmp_key = te_key;
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  if (NULL == main) {
    TBSYS_LOG(ERROR, "get updateserver main null pointer");
  } else {
    ObUpsTableMgr& tm = main->get_update_server().get_table_mgr();
    const CommonTableSchema* table_schema = NULL;
    UpsSchemaMgr::SchemaHandle schema_handle;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = tm.schema_mgr_.get_schema_handle(schema_handle))) {
      TBSYS_LOG(WARN, "get schema handle fail ret=%d", tmp_ret);
    } else {
      if (NULL == (table_schema = tm.schema_mgr_.get_table_schema(schema_handle, te_key.table_id))) {
        TBSYS_LOG(ERROR, "get schema fail table_id=%lu", te_key.table_id);
      } else {
        int32_t split_pos = table_schema->get_split_pos();
        if (split_pos > tmp_key.row_key.length()) {
          TBSYS_LOG(ERROR, "row key cannot be splited length=%d split_pos=%d",
                    tmp_key.row_key.length(), split_pos);
        } else {
          tmp_key.row_key.assign_ptr(tmp_key.row_key.ptr(), split_pos);
          prefix_key = tmp_key;
          bret = true;
        }
      }
      tm.schema_mgr_.revert_schema_handle(schema_handle);
    }
  }
  return bret;
}
}
}


