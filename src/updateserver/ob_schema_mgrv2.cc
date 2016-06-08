/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_mgrv2.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "ob_schema_mgrv2.h"

namespace sb {
namespace updateserver {
using namespace common;

CommonSchemaManagerWrapper::CommonSchemaManagerWrapper() : schema_mgr_buffer_(NULL), schema_mgr_impl_(NULL) {
  schema_mgr_buffer_ = ob_malloc(sizeof(CommonSchemaManager));
  schema_mgr_impl_ = new(schema_mgr_buffer_) CommonSchemaManager();
  if (NULL == schema_mgr_impl_) {
    TBSYS_LOG(WARN, "new schema mgr fail");
  }
}

CommonSchemaManagerWrapper::~CommonSchemaManagerWrapper() {
  if (NULL != schema_mgr_buffer_) {
    schema_mgr_impl_->~CommonSchemaManager();
    ob_free(schema_mgr_buffer_);
    schema_mgr_buffer_ = NULL;
    schema_mgr_impl_ = NULL;
  }
}

DEFINE_SERIALIZE(CommonSchemaManagerWrapper)
//int CommonSchemaManagerWrapper::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == schema_mgr_impl_) {
    ret = OB_ERROR;
  } else {
    ret = schema_mgr_impl_->serialize(buf, buf_len, pos);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(CommonSchemaManagerWrapper)
//int64_t CommonSchemaManagerWrapper::get_serialize_size(void) const
{
  int64_t ret = 0;
  if (NULL != schema_mgr_impl_) {
    ret = schema_mgr_impl_->get_serialize_size();
  }
  return ret;
}

DEFINE_DESERIALIZE(CommonSchemaManagerWrapper)
//int CommonSchemaManagerWrapper::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (NULL == schema_mgr_impl_) {
    ret = OB_ERROR;
  } else {
    schema_mgr_impl_->set_drop_column_group();
    ret = schema_mgr_impl_->deserialize(buf, data_len, pos);
  }
  return ret;
}

int64_t CommonSchemaManagerWrapper::get_version() const {
  int64_t ret = 0;
  if (NULL != schema_mgr_impl_) {
    ret = schema_mgr_impl_->get_version();
  }
  return ret;
}

bool CommonSchemaManagerWrapper::parse_from_file(const char* file_name, tbsys::CConfig& config) {
  bool bret = false;
  if (NULL != schema_mgr_impl_) {
    bret = schema_mgr_impl_->parse_from_file(file_name, config);
  }
  return bret;
}

const CommonSchemaManager* CommonSchemaManagerWrapper::get_impl() const {
  return schema_mgr_impl_;
}

int CommonSchemaManagerWrapper::set_impl(const CommonSchemaManager& schema_impl) const {
  int ret = OB_SUCCESS;
  if (NULL == schema_mgr_impl_) {
    ret = OB_ERROR;
  } else {
    *schema_mgr_impl_ = schema_impl;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

const UpsSchemaMgr::SchemaHandle UpsSchemaMgr::INVALID_SCHEMA_HANDLE = NULL;

UpsSchemaMgr::UpsSchemaMgr() : cur_schema_mgr_imp_(NULL), has_schema_(false) {
  if (NULL == (cur_schema_mgr_imp_ = new(std::nothrow) UpsSchemaMgrImp())) {
    TBSYS_LOG(ERROR, "new schema mgr imp fail");
  } else {
    cur_schema_mgr_imp_->inc_ref_cnt();
  }
}

UpsSchemaMgr::~UpsSchemaMgr() {
  if (NULL != cur_schema_mgr_imp_) {
    delete cur_schema_mgr_imp_;
    cur_schema_mgr_imp_ = NULL;
  }
}

int UpsSchemaMgr::set_schema_mgr(const CommonSchemaManagerWrapper& schema_manager) {
  int ret = OB_SUCCESS;
  UpsSchemaMgrImp* tmp_schema_mgr_imp = NULL;
  const CommonSchemaManager* schema_mgr = schema_manager.get_impl();
  if (NULL == schema_mgr) {
    TBSYS_LOG(WARN, "get schema mgr from schema_mgr_wrapper fail");
    ret = OB_ERROR;
  } else if (NULL == (tmp_schema_mgr_imp = new(std::nothrow) UpsSchemaMgrImp())) {
    TBSYS_LOG(WARN, "new tmp schema mgr imp fail");
    ret = OB_ERROR;
  } else {
    tmp_schema_mgr_imp->get_schema_mgr() = *schema_mgr;
    rwlock_.wrlock();
    UpsSchemaMgrImp* prev_schema_mgr_imp = cur_schema_mgr_imp_;
    tmp_schema_mgr_imp->inc_ref_cnt();
    cur_schema_mgr_imp_ = tmp_schema_mgr_imp;
    if (NULL != prev_schema_mgr_imp
        && 0 == prev_schema_mgr_imp->dec_ref_cnt()) {
      delete prev_schema_mgr_imp;
      prev_schema_mgr_imp = NULL;
    }
    rwlock_.unlock();
    has_schema_ = true;
  }
  return ret;
}

int UpsSchemaMgr::get_schema_mgr(CommonSchemaManagerWrapper& schema_manager) const {
  int ret = OB_SUCCESS;
  rwlock_.rdlock();
  if (NULL == cur_schema_mgr_imp_) {
    ret = OB_ERROR;
  } else {
    ret = schema_manager.set_impl(cur_schema_mgr_imp_->get_schema_mgr());
  }
  rwlock_.unlock();
  return ret;
}

int UpsSchemaMgr::get_schema_handle(SchemaHandle& schema_handle) const {
  int ret = OB_SUCCESS;
  rwlock_.rdlock();
  if (NULL == cur_schema_mgr_imp_) {
    ret = OB_ERROR;
  } else {
    cur_schema_mgr_imp_->inc_ref_cnt();
    schema_handle = cur_schema_mgr_imp_;
  }
  rwlock_.unlock();
  return ret;
}

void UpsSchemaMgr::revert_schema_handle(SchemaHandle& schema_handle) const {
  rwlock_.rdlock();
  if (NULL != schema_handle
      && 0 == schema_handle->dec_ref_cnt()) {
    delete schema_handle;
    schema_handle = NULL;
  }
  rwlock_.unlock();
}

uint64_t UpsSchemaMgr::get_create_time_column_id(const SchemaHandle& schema_handle, const uint64_t table_id) const {
  uint64_t ret = OB_INVALID_ID;
  if (INVALID_SCHEMA_HANDLE != schema_handle) {
    ret = schema_handle->get_schema_mgr().get_create_time_column_id(table_id);
  }
  return ret;
}

uint64_t UpsSchemaMgr::get_modify_time_column_id(const SchemaHandle& schema_handle, const uint64_t table_id) const {
  uint64_t ret = OB_INVALID_ID;
  if (INVALID_SCHEMA_HANDLE != schema_handle) {
    ret = schema_handle->get_schema_mgr().get_modify_time_column_id(table_id);
  }
  return ret;
}

uint64_t UpsSchemaMgr::get_create_time_column_id(const uint64_t table_id) const {
  uint64_t ret = OB_INVALID_ID;
  SchemaHandle schema_handle = INVALID_SCHEMA_HANDLE;
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS == (tmp_ret = get_schema_handle(schema_handle))) {
    ret = schema_handle->get_schema_mgr().get_create_time_column_id(table_id);
    revert_schema_handle(schema_handle);
  }
  return ret;
}

uint64_t UpsSchemaMgr::get_modify_time_column_id(const uint64_t table_id) const {
  uint64_t ret = OB_INVALID_ID;
  SchemaHandle schema_handle = INVALID_SCHEMA_HANDLE;
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS == (tmp_ret = get_schema_handle(schema_handle))) {
    ret = schema_handle->get_schema_mgr().get_modify_time_column_id(table_id);
    revert_schema_handle(schema_handle);
  }
  return ret;
}

const CommonTableSchema* UpsSchemaMgr::get_table_schema(const SchemaHandle& schema_handle, const uint64_t table_id) const {
  const CommonTableSchema* ret = NULL;
  if (INVALID_SCHEMA_HANDLE != schema_handle) {
    ret = schema_handle->get_schema_mgr().get_table_schema(table_id);
  }
  return ret;
}

const CommonTableSchema* UpsSchemaMgr::get_table_schema(const SchemaHandle& schema_handle, const ObString& table_name) const {
  const CommonTableSchema* ret = NULL;
  if (INVALID_SCHEMA_HANDLE != schema_handle) {
    ret = schema_handle->get_schema_mgr().get_table_schema(table_name);
  }
  return ret;
}

const CommonColumnSchema* UpsSchemaMgr::get_column_schema(const SchemaHandle& schema_handle,
                                                          const ObString& table_name,
                                                          const ObString& column_name) const {
  const CommonColumnSchema* ret = NULL;
  if (INVALID_SCHEMA_HANDLE != schema_handle) {
    ret = schema_handle->get_schema_mgr().get_column_schema(table_name, column_name);
  }
  return ret;
}

int UpsSchemaMgr::build_sstable_schema(const SchemaHandle schema_handle, sstable::ObSSTableSchema& sstable_schema) const {
  int ret = OB_SUCCESS;
  if (INVALID_SCHEMA_HANDLE == schema_handle) {
    ret = OB_ERROR;
  } else {
    const CommonSchemaManager& schema_mgr = schema_handle->get_schema_mgr();
    sstable::ObSSTableSchemaColumnDef column_info;
    const CommonColumnSchema* iter = NULL;
    for (iter = schema_mgr.column_begin(); iter != schema_mgr.column_end(); iter++) {
      if (NULL == iter) {
        TBSYS_LOG(WARN, "invalid column schema");
        ret = OB_ERROR;
        break;
      } else {
        column_info.reserved_ = 0;
        column_info.column_group_id_ = DEFAULT_COLUMN_GROUP_ID;
        column_info.column_name_id_ = iter->get_id();
        column_info.column_value_type_ = iter->get_type();
        column_info.table_id_ = iter->get_table_id();
        if (OB_SUCCESS != (ret = sstable_schema.add_column_def(column_info))) {
          TBSYS_LOG(WARN, "add_column_def fail ret=%d group_id=%hu column_id=%u value_type=%d table_id=%u",
                    ret, column_info.column_group_id_, column_info.column_name_id_,
                    column_info.column_value_type_, column_info.table_id_);
          break;
        }
      }
    }
  }
  return ret;
}

void UpsSchemaMgr::dump2text() const {
  const int64_t BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];
  snprintf(buffer, BUFFER_SIZE, "/tmp/ups_schemas.pid_%d.tim_%ld", getpid(), tbsys::CTimeUtil::getTime());
  FILE* fd = fopen(buffer, "w");
  if (NULL != fd) {
    SchemaHandle schema_handle = INVALID_SCHEMA_HANDLE;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS == (tmp_ret = get_schema_handle(schema_handle))) {
      const CommonSchemaManager& schema_mgr = schema_handle->get_schema_mgr();
      uint64_t cur_table_id = OB_INVALID_ID;
      const CommonColumnSchema* iter = NULL;
      for (iter = schema_mgr.column_begin(); iter != schema_mgr.column_end(); iter++) {
        if (NULL != iter) {
          if (iter->get_table_id() != cur_table_id) {
            const CommonTableSchema* table_schema = schema_mgr.get_table_schema(iter->get_table_id());
            if (NULL != table_schema) {
              fprintf(fd, "[TABLE_SCHEMA] table_id=%lu table_type=%d table_name=%s split_pos=%d rowkey_max_length=%d\n",
                      iter->get_table_id(), table_schema->get_table_type(), table_schema->get_table_name(),
                      table_schema->get_split_pos(), table_schema->get_rowkey_max_length());
            } else {
              fprintf(fd, "[TABLE_SCHEMA] table_id=%lu\n", iter->get_table_id());
            }
            cur_table_id = iter->get_table_id();
          }
          fprintf(fd, "              [COLUMN_SCHEMA] column_id=%lu column_name=%s column_type=%d size=%ld\n",
                  iter->get_id(), iter->get_name(), iter->get_type(), iter->get_size());
          iter->print_info();
        }
      }
      revert_schema_handle(schema_handle);
    }
  }
  fclose(fd);
}
}
}



