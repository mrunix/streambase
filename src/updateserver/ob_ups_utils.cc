/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_utils.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_malloc.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_action_flag.h"
#include "common/utility.h"
#include "ob_table_engine.h"
#include "ob_ups_utils.h"
#include "ob_update_server_main.h"
#include "ob_ups_stat.h"

namespace sb {
namespace updateserver {
using namespace sb::common;

const char* print_obj(const ObObj& obj) {
  static const int64_t BUFFER_SIZE = 128 * 1024;
  static __thread char buffers[2][BUFFER_SIZE];
  static __thread int64_t i = 0;
  char* buffer = buffers[i++ % 2];
  buffer[0] = '\0';
  switch (obj.get_type()) {
  case ObNullType:
    snprintf(buffer, BUFFER_SIZE, "obj_type=null");
    break;
  case ObIntType: {
    bool is_add = false;
    int64_t tmp = 0;
    obj.get_int(tmp, is_add);
    snprintf(buffer, BUFFER_SIZE, "obj_type=int value=%ld is_add=%s", tmp, STR_BOOL(is_add));
  }
  break;
  case ObFloatType: {
    bool is_add = false;
    float tmp = 0.0;
    obj.get_float(tmp, is_add);
    snprintf(buffer, BUFFER_SIZE, "obj_type=float value=%f is_add=%s", tmp, STR_BOOL(is_add));
  }
  break;
  case ObDoubleType: {
    bool is_add = false;
    double tmp = 0.0;
    obj.get_double(tmp, is_add);
    snprintf(buffer, BUFFER_SIZE, "obj_type=double value=%lf is_add=%s", tmp, STR_BOOL(is_add));
  }
  break;
  case ObDateTimeType: {
    bool is_add = false;
    ObDateTime tmp;
    obj.get_datetime(tmp, is_add);
    snprintf(buffer, BUFFER_SIZE, "obj_type=data_time value=%s is_add=%s", time2str(tmp), STR_BOOL(is_add));
  }
  break;
  case ObPreciseDateTimeType: {
    bool is_add = false;
    ObDateTime tmp;
    obj.get_precise_datetime(tmp, is_add);
    snprintf(buffer, BUFFER_SIZE, "obj_type=precise_data_time value=%s is_add=%s", time2str(tmp), STR_BOOL(is_add));
  }
  break;
  case ObVarcharType: {
    ObString tmp;
    obj.get_varchar(tmp);
    if (NULL != tmp.ptr()
        && 0 != tmp.length()
        && !str_isprint(tmp.ptr(), tmp.length())) {
      char hex_buffer[BUFFER_SIZE] = {'\0'};
      common::hex_to_str(tmp.ptr(), tmp.length(), hex_buffer, BUFFER_SIZE);
      snprintf(buffer, BUFFER_SIZE, "obj_type=var_char value=[0x %s] value_ptr=%p value_length=%d",
               hex_buffer, tmp.ptr(), tmp.length());
    } else {
      snprintf(buffer, BUFFER_SIZE, "obj_type=var_char value=[%.*s] value_ptr=%p value_length=%d",
               tmp.length(), tmp.ptr(), tmp.ptr(), tmp.length());
    }
  }
  break;
  case ObSeqType:
    snprintf(buffer, BUFFER_SIZE, "obj_type=seq");
    break;
  case ObCreateTimeType: {
    ObCreateTime tmp = 0;
    obj.get_createtime(tmp);
    snprintf(buffer, BUFFER_SIZE, "obj_type=create_time value=%s", time2str(tmp));
  }
  break;
  case ObModifyTimeType: {
    ObModifyTime tmp = 0;
    obj.get_modifytime(tmp);
    snprintf(buffer, BUFFER_SIZE, "obj_type=modify_time value=%s", time2str(tmp));
  }
  break;
  case ObExtendType: {
    int64_t tmp = 0;
    obj.get_ext(tmp);
    snprintf(buffer, BUFFER_SIZE, "obj_type=extend value=%ld", tmp);
  }
  break;
  default:
    break;
  }
  return buffer;
}

const char* print_string(const ObString& str) {
  static const int64_t BUFFER_SIZE = 16 * 1024;
  static __thread char buffer[BUFFER_SIZE] = "0x ";
  common::hex_to_str(str.ptr(), str.length(), &buffer[3], BUFFER_SIZE - 3);
  return buffer;
}

const char* print_cellinfo(const ObCellInfo* ci, const char* ext_info/* = NULL*/) {
  static const int64_t BUFFER_SIZE = 128 * 1024;
  static __thread char buffers[2][BUFFER_SIZE];
  static __thread int64_t i = 0;
  char* buffer = buffers[i++ % 2];
  buffer[0] = '\0';
  if (NULL != ci) {
    int32_t rowkey_len = 0;
    const char* rowkey_str = NULL;
    if (NULL != ci->row_key_.ptr()
        && 0 != ci->row_key_.length()
        && !str_isprint(ci->row_key_.ptr(), ci->row_key_.length())) {
      char hex_buffer[BUFFER_SIZE] = "0x ";
      common::hex_to_str(ci->row_key_.ptr(), ci->row_key_.length(), hex_buffer + strlen(hex_buffer), BUFFER_SIZE - strlen(hex_buffer));
      rowkey_str = hex_buffer;
      rowkey_len = strlen(hex_buffer);
    } else {
      rowkey_str = ci->row_key_.ptr();
      rowkey_len = ci->row_key_.length();
    }
    if (NULL == ext_info) {
      snprintf(buffer, BUFFER_SIZE, "table_id=%lu table_name=[%.*s] table_name_ptr=%p table_name_length=%d "
               "row_key=[%.*s] row_key_ptr=%p row_key_length=%d "
               "column_id=%lu column_name=[%.*s] column_name_ptr=%p column_name_length=%d "
               "%s",
               ci->table_id_, ci->table_name_.length(), ci->table_name_.ptr(), ci->table_name_.ptr(), ci->table_name_.length(),
               rowkey_len, rowkey_str, ci->row_key_.ptr(), ci->row_key_.length(),
               ci->column_id_, ci->column_name_.length(), ci->column_name_.ptr(), ci->column_name_.ptr(), ci->column_name_.length(),
               print_obj(ci->value_));
    } else {
      snprintf(buffer, BUFFER_SIZE, "%s table_id=%lu table_name=[%.*s] table_name_ptr=%p table_name_length=%d "
               "row_key=[%.*s] row_key_ptr=%p row_key_length=%d "
               "column_id=%lu column_name=[%.*s] column_name_ptr=%p column_name_length=%d "
               "%s",
               ext_info,
               ci->table_id_, ci->table_name_.length(), ci->table_name_.ptr(), ci->table_name_.ptr(), ci->table_name_.length(),
               rowkey_len, rowkey_str, ci->row_key_.ptr(), ci->row_key_.length(),
               ci->column_id_, ci->column_name_.length(), ci->column_name_.ptr(), ci->column_name_.ptr(), ci->column_name_.length(),
               print_obj(ci->value_));
    }
  }
  return buffer;
}

template <>
int ups_serialize<uint64_t>(const uint64_t& data, char* buf, const int64_t data_len, int64_t& pos) {
  return serialization::encode_vi64(buf, data_len, pos, (int64_t)data);
};

template <>
int ups_serialize<int64_t>(const int64_t& data, char* buf, const int64_t data_len, int64_t& pos) {
  return serialization::encode_vi64(buf, data_len, pos, data);
};

template <>
int ups_deserialize<uint64_t>(uint64_t& data, char* buf, const int64_t data_len, int64_t& pos) {
  return serialization::decode_vi64(buf, data_len, pos, (int64_t*)&data);
};

template <>
int ups_deserialize<int64_t>(int64_t& data, char* buf, const int64_t data_len, int64_t& pos) {
  return serialization::decode_vi64(buf, data_len, pos, &data);
};

ObSchemaManagerWrapper::ObSchemaManagerWrapper() : schema_mgr_buffer_(NULL), schema_mgr_impl_(NULL) {
  void* schema_mgr_buffer_ = ob_malloc(sizeof(ObSchemaManager));
  schema_mgr_impl_ = new(schema_mgr_buffer_) ObSchemaManager();
  if (NULL == schema_mgr_impl_) {
    TBSYS_LOG(WARN, "new schema mgr fail");
  }
}

ObSchemaManagerWrapper::~ObSchemaManagerWrapper() {
  if (NULL != schema_mgr_buffer_) {
    schema_mgr_impl_->~ObSchemaManager();
    ob_free(schema_mgr_buffer_);
    schema_mgr_buffer_ = NULL;
    schema_mgr_impl_ = NULL;
  }
}

const ObSchema* ObSchemaManagerWrapper::begin() const {
  const ObSchema* ret = NULL;
  if (NULL != schema_mgr_impl_) {
    ret = schema_mgr_impl_->begin();
  }
  return ret;
}

const ObSchema* ObSchemaManagerWrapper::end() const {
  const ObSchema* ret = NULL;
  if (NULL != schema_mgr_impl_) {
    ret = schema_mgr_impl_->end();
  }
  return ret;
}

DEFINE_SERIALIZE(ObSchemaManagerWrapper)
//int ObSchemaManagerWrapper::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == schema_mgr_impl_) {
    ret = OB_ERROR;
  } else {
    ret = schema_mgr_impl_->serialize(buf, buf_len, pos);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSchemaManagerWrapper)
//int64_t ObSchemaManagerWrapper::get_serialize_size(void) const
{
  int64_t ret = 0;
  if (NULL != schema_mgr_impl_) {
    ret = schema_mgr_impl_->get_serialize_size();
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSchemaManagerWrapper)
//int ObSchemaManagerWrapper::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (NULL == schema_mgr_impl_) {
    ret = OB_ERROR;
  } else {
    ret = schema_mgr_impl_->deserialize(buf, data_len, pos);
  }
  return ret;
}

int64_t ObSchemaManagerWrapper::get_version() const {
  int64_t ret = 0;
  if (NULL != schema_mgr_impl_) {
    ret = schema_mgr_impl_->get_version();
  }
  return ret;
}

bool ObSchemaManagerWrapper::parse_from_file(const char* file_name, tbsys::CConfig& config) {
  bool bret = false;
  if (NULL != schema_mgr_impl_) {
    bret = schema_mgr_impl_->parse_from_file(file_name, config);
  }
  return bret;
}

const ObSchemaManager* ObSchemaManagerWrapper::get_impl() const {
  return schema_mgr_impl_;
}

int ObSchemaManagerWrapper::set_impl(const ObSchemaManager& schema_impl) const {
  int ret = OB_SUCCESS;
  if (NULL == schema_mgr_impl_) {
    ret = OB_ERROR;
  } else {
    *schema_mgr_impl_ = schema_impl;
  }
  return ret;
}

const char* time2str(const int64_t time_us, const char* format) {
  static const int32_t BUFFER_SIZE = 1024;
  static __thread char buffer[2][BUFFER_SIZE];
  static __thread int64_t i = 0;
  buffer[i % 2][0] = '\0';
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  int64_t cur_second_time_us = time_us % 1000000;
  if (NULL != localtime_r(&time_s, &time_struct)) {
    int64_t pos = strftime(buffer[i % 2], BUFFER_SIZE, format, &time_struct);
    if (pos < BUFFER_SIZE) {
      snprintf(&buffer[i % 2][pos], BUFFER_SIZE - pos, " %ld %ld", cur_second_time_us, time_us);
    }
  }
  return buffer[i++ % 2];
}

const char* range2str(const ObVersionRange& range) {
  static const int64_t BUFFER_SIZE = 1024;
  static __thread char buffers[2][BUFFER_SIZE];
  static __thread int64_t i = 0;
  char* buffer = buffers[i++ % 2];
  buffer[0] = '\0';
  const char* start_bound = range.border_flag_.inclusive_start() ? "[" : "(";
  const char* end_bound = range.border_flag_.inclusive_end() ? "]" : ")";
  if (range.border_flag_.is_min_value()) {
    if (range.border_flag_.is_max_value()) {
      snprintf(buffer, BUFFER_SIZE, "%sMIN,MAX%s", start_bound, end_bound);
    } else {
      snprintf(buffer, BUFFER_SIZE, "%sMIN,%ld%s", start_bound, range.end_version_, end_bound);
    }
  } else {
    if (range.border_flag_.is_max_value()) {
      snprintf(buffer, BUFFER_SIZE, "%s%ld,MAX%s", start_bound, range.start_version_, end_bound);
    } else {
      snprintf(buffer, BUFFER_SIZE, "%s%ld,%ld%s", start_bound, range.start_version_, range.end_version_, end_bound);
    }
  }
  return buffer;
}

const char* scan_range2str(const common::ObRange& scan_range) {
  static const int64_t BUFFER_SIZE = 1024;
  static __thread char buffers[2][BUFFER_SIZE];
  static __thread int64_t i = 0;
  char* buffer = buffers[i++ % 2];
  buffer[0] = '\0';
  const char* start_bound = scan_range.border_flag_.inclusive_start() ? "[" : "(";
  const char* end_bound = scan_range.border_flag_.inclusive_end() ? "]" : ")";
  if (scan_range.border_flag_.is_min_value()) {
    if (scan_range.border_flag_.is_max_value()) {
      snprintf(buffer, BUFFER_SIZE, "table_id=%lu %sMIN,MAX%s", scan_range.table_id_, start_bound, end_bound);
    } else {
      char hex_buffer[BUFFER_SIZE] = {'\0'};
      hex_to_str(scan_range.end_key_.ptr(), scan_range.end_key_.length(), hex_buffer, BUFFER_SIZE);
      snprintf(buffer, BUFFER_SIZE, "table_id=%lu %sMIN,%s%s", scan_range.table_id_, start_bound, hex_buffer, end_bound);
    }
  } else {
    if (scan_range.border_flag_.is_max_value()) {
      char hex_buffer[BUFFER_SIZE] = {'\0'};
      hex_to_str(scan_range.start_key_.ptr(), scan_range.start_key_.length(), hex_buffer, BUFFER_SIZE);
      snprintf(buffer, BUFFER_SIZE, "table_id=%lu %s%s,MAX%s", scan_range.table_id_, start_bound, hex_buffer, end_bound);
    } else {
      char hex_buffers[2][BUFFER_SIZE];
      hex_buffers[0][0] = '\0';
      hex_to_str(scan_range.start_key_.ptr(), scan_range.start_key_.length(), hex_buffers[0], BUFFER_SIZE);
      hex_buffers[1][0] = '\0';
      hex_to_str(scan_range.end_key_.ptr(), scan_range.end_key_.length(), hex_buffers[1], BUFFER_SIZE);
      snprintf(buffer, BUFFER_SIZE, "table_id=%lu %s%s,%s%s", scan_range.table_id_, start_bound, hex_buffers[0], hex_buffers[1], end_bound);
    }
  }
  return buffer;
}

bool is_range_valid(const ObVersionRange& version_range) {
  bool bret = false;
  if (version_range.border_flag_.is_min_value()
      || version_range.border_flag_.is_max_value()) {
    bret = true;
  } else if (version_range.start_version_ < version_range.end_version_) {
    bret = true;
  } else if (version_range.start_version_ == version_range.end_version_) {
    if (version_range.border_flag_.inclusive_start()
        && version_range.border_flag_.inclusive_end()) {
      bret = true;
    }
  }
  return bret;
}

bool is_in_range(const int64_t key, const ObVersionRange& version_range) {
  bool bret = false;
  if (version_range.is_whole_range()) {
    bret = true;
  } else if (key > version_range.start_version_
             && version_range.border_flag_.is_max_value()) {
    bret = true;
  } else if (key < version_range.end_version_
             && version_range.border_flag_.is_min_value()) {
    bret = true;
  } else if (key > version_range.start_version_
             && key < version_range.end_version_) {
    bret = true;
  } else if (key == version_range.start_version_
             && version_range.border_flag_.inclusive_start()) {
    bret = true;
  } else if (key == version_range.end_version_
             && version_range.border_flag_.inclusive_end()) {
    bret = true;
  } else {
    // do nothing
  }
  return bret;
}

int precise_sleep(const int64_t microsecond) {
  int ret = OB_SUCCESS;
  if (0 < microsecond) {
    int64_t end_time = tbsys::CTimeUtil::getMonotonicTime() + microsecond;
    int64_t time2sleep = microsecond;
    struct timeval tv;
    do {
      tv.tv_sec = time2sleep / 1000000;
      tv.tv_usec = time2sleep % 1000000;
      int select_ret = select(0, NULL, NULL, NULL, &tv);
      if (0 == select_ret) {
        break;
      } else if (EINTR == errno) {
        int64_t cur_time = tbsys::CTimeUtil::getMonotonicTime();
        if (0 < (time2sleep = (end_time - cur_time))) {
          continue;
        } else {
          break;
        }
      } else {
        ret = OB_ERROR;
      }
    } while (end_time > tbsys::CTimeUtil::getMonotonicTime());
  }
  return ret;
}

bool str_isprint(const char* str, const int64_t length) {
  bool bret = false;
  if (NULL != str
      && 0 != length) {
    bret = true;
    for (int64_t i = 0; i < length; i++) {
      if (!isprint(str[i])) {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

const char* inet_ntoa_r(const uint64_t ipport) {
  static const int64_t BUFFER_SIZE = 32;
  static __thread char buffers[2][BUFFER_SIZE];
  static __thread int64_t i = 0;
  char* buffer = buffers[i++ % 2];
  buffer[0] = '\0';

  uint32_t ip = (uint32_t)(ipport & 0xffffffff);
  int port = (int)((ipport >> 32) & 0xffff);
  unsigned char* bytes = (unsigned char*) &ip;
  if (port > 0) {
    snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
  } else {
    snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:-1", bytes[0], bytes[1], bytes[2], bytes[3]);
  }

  return buffer;
}

int64_t get_max_row_cell_num() {
  int64_t ret = ObUpdateServerParam::DEFAULT_MAX_ROW_CELL_NUM;
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get updateserver main fail");
  } else {
    ret = ups_main->get_update_server().get_param().get_max_row_cell_num();
  }
  return ret;
}

int64_t get_table_available_warn_size() {
  int64_t ret = ObUpdateServerParam::DEFAULT_TABLE_AVAILABLE_WARN_SIZE;
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get updateserver main fail");
  } else {
    ret = ups_main->get_update_server().get_param().get_table_available_warn_size();
  }
  return ret;
}

int64_t get_table_available_error_size() {
  int64_t ret = ObUpdateServerParam::DEFAULT_TABLE_AVAILABLE_ERROR_SIZE;
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get updateserver main fail");
  } else {
    ret = ups_main->get_update_server().get_param().get_table_available_error_size();
  }
  return ret;
}

int64_t get_table_memory_limit() {
  int64_t ret = 0;
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get updateserver main fail");
  } else {
    ret = ups_main->get_update_server().get_param().get_table_memory_limit();
  }
  return ret;
}

bool ups_available_memory_warn_callback(const int64_t mem_size_available) {
  bool bret = true;
  static int64_t last_proc_time = 0;
  static const int64_t MIN_PROC_INTERVAL = 60L * 1000L * 1000L;
  int64_t table_available_error_size = ObUpdateServerParam::DEFAULT_TABLE_AVAILABLE_ERROR_SIZE_GB;
  if ((last_proc_time + MIN_PROC_INTERVAL) < tbsys::CTimeUtil::getTime()) {
    TBSYS_LOG(INFO, "available memory reach the warn-size=%ld last_proc_time=%ld, will try to free a memtable",
              mem_size_available, last_proc_time);
    ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
    if (NULL == ups_main) {
      TBSYS_LOG(WARN, "get updateserver main fail");
    } else {
      ups_main->get_update_server().submit_delay_drop();
      table_available_error_size = ups_main->get_update_server().get_param().get_table_available_error_size();
    }
    if (table_available_error_size >= mem_size_available) {
      TBSYS_LOG(ERROR, "available memory reach the error-size=%ld last_proc_time=%ld, will try to free a memtable",
                last_proc_time, mem_size_available);
    }
    last_proc_time = tbsys::CTimeUtil::getTime();
  }
  return bret;
}

const CacheWarmUpConf& get_warm_up_conf() {
  static const CacheWarmUpConf g_default_warm_up_conf;
  const CacheWarmUpConf* ret = &g_default_warm_up_conf;
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get updateserver main fail");
  } else {
    ret = &(ups_main->get_update_server().get_param().get_warm_up_conf());
  }
  return *ret;
}

void set_warm_up_percent(const int64_t warm_up_percent) {
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get updateserver main fail");
  } else {
    ups_main->get_update_server().get_table_mgr().set_warm_up_percent(warm_up_percent);
  }
}

void submit_force_drop() {
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get updateserver main fail");
  } else {
    ups_main->get_update_server().submit_force_drop();
  }
}

void schedule_warm_up_duty() {
  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();
  if (NULL == ups_main) {
    TBSYS_LOG(WARN, "get updateserver main fail");
  } else {
    ups_main->get_update_server().schedule_warm_up_duty();
  }
}

}
}



