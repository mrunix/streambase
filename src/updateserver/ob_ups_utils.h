/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_utils.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_UPS_UTILS_H_
#define  OCEANBASE_UPDATESERVER_UPS_UTILS_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "common/serialization.h"
#include "common/ob_schema.h"
#include "common/page_arena.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_file.h"

#define DEFAULT_TIME_FORMAT "%Y-%m-%d %H:%M:%S"

namespace sb {
namespace updateserver {
typedef common::ObSchemaManagerV2 CommonSchemaManager;
typedef common::ObColumnSchemaV2 CommonColumnSchema;
typedef common::ObTableSchema CommonTableSchema;

class TEKey;
class TEValue;
struct CacheWarmUpConf;
extern const char* print_obj(const common::ObObj& obj);
extern const char* print_string(const common::ObString& str);
extern const char* print_cellinfo(const common::ObCellInfo* ci, const char* ext_info = NULL);
extern const char* time2str(const int64_t time_s, const char* format = DEFAULT_TIME_FORMAT);
extern bool is_in_range(const int64_t key, const common::ObVersionRange& version_range);
extern bool is_range_valid(const common::ObVersionRange& version_range);
extern const char* range2str(const common::ObVersionRange& range);
extern const char* scan_range2str(const common::ObRange& scan_range);
extern int precise_sleep(const int64_t microsecond);
extern bool str_isprint(const char* str, const int64_t length);
extern const char* inet_ntoa_r(const uint64_t ipport);
extern int64_t get_max_row_cell_num();
extern int64_t get_table_available_warn_size();
extern int64_t get_table_available_error_size();
extern int64_t get_table_memory_limit();
extern bool ups_available_memory_warn_callback(const int64_t mem_size_available);
extern const CacheWarmUpConf& get_warm_up_conf();
extern void set_warm_up_percent(const int64_t warm_up_percent);
extern void submit_force_drop();
extern void schedule_warm_up_duty();

template <class T>
int ups_serialize(const T& data, char* buf, const int64_t data_len, int64_t& pos) {
  return data.serialize(buf, data_len, pos);
};

template <class T>
int ups_deserialize(T& data, char* buf, const int64_t data_len, int64_t& pos) {
  return data.deserialize(buf, data_len, pos);
};

template <>
extern int ups_serialize<uint64_t>(const uint64_t& data, char* buf, const int64_t data_len, int64_t& pos);
template <>
extern int ups_serialize<int64_t>(const int64_t& data, char* buf, const int64_t data_len, int64_t& pos);

template <>
extern int ups_deserialize<uint64_t>(uint64_t& data, char* buf, const int64_t data_len, int64_t& pos);
template <>
extern int ups_deserialize<int64_t>(int64_t& data, char* buf, const int64_t data_len, int64_t& pos);

class ObSchemaManagerWrapper {
 public:
  ObSchemaManagerWrapper();
  ~ObSchemaManagerWrapper();
  DISALLOW_COPY_AND_ASSIGN(ObSchemaManagerWrapper);
 public:
  const common::ObSchema* begin() const;
  const common::ObSchema* end() const;
  int64_t get_version() const;
  bool parse_from_file(const char* file_name, tbsys::CConfig& config);
  const common::ObSchemaManager* get_impl() const;
  int set_impl(const common::ObSchemaManager& schema_impl) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
 public:
  void* schema_mgr_buffer_;
  common::ObSchemaManager* schema_mgr_impl_;
};

struct TableMemInfo {
  int64_t memtable_used;
  int64_t memtable_total;
  int64_t memtable_limit;
  TableMemInfo() {
    memtable_used = 0;
    memtable_total = 0;
    memtable_limit = INT64_MAX;
  };
};

struct UpsPrivQueueConf {
  int64_t low_priv_network_lower_limit;
  int64_t low_priv_network_upper_limit;
  int64_t low_priv_adjust_flag;
  int64_t low_priv_cur_percent;
  int64_t low_priv_max_percent;

  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const {
    int ret = common::OB_SUCCESS;
    if ((pos + (int64_t)sizeof(*this)) > buf_len) {
      ret = common::OB_ERROR;
    } else {
      memcpy(buf + pos, this, sizeof(*this));
      pos += sizeof(*this);
    }
    return ret;
  }

  int deserialize(const char* buf, const int64_t buf_len, int64_t& pos) {
    int ret = common::OB_SUCCESS;
    if ((pos + (int64_t)sizeof(*this)) > buf_len) {
      ret = common::OB_ERROR;
    } else {
      memcpy(this, buf + pos, sizeof(*this));
      pos += sizeof(*this);
    }
    return ret;
  };

  int64_t get_serialize_size(void) const {
    return sizeof(*this);
  };
};

struct UpsMemoryInfo {
  const int64_t version;
  int64_t total_size;
  int64_t cur_limit_size;
  TableMemInfo table_mem_info;
  UpsMemoryInfo() : version(1), table_mem_info() {
    total_size = 0;
    cur_limit_size = INT64_MAX;
  };
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const {
    int ret = common::OB_SUCCESS;
    if ((pos + (int64_t)sizeof(*this)) > buf_len) {
      ret = common::OB_ERROR;
    } else {
      memcpy(buf + pos, this, sizeof(*this));
      pos += sizeof(*this);
    }
    return ret;
  };
  int deserialize(const char* buf, const int64_t buf_len, int64_t& pos) {
    int ret = common::OB_SUCCESS;
    if ((pos + (int64_t)sizeof(*this)) > buf_len) {
      ret = common::OB_ERROR;
    } else {
      memcpy(this, buf + pos, sizeof(*this));
      pos += sizeof(*this);
    }
    return ret;
  };
  int64_t get_serialize_size(void) const {
    return sizeof(*this);
  };
};

struct CacheWarmUpConf {
  static const int64_t STOP_PERCENT = 100; // 100%
  static const int64_t STEP_PERCENT = 1; // 1%
  static const int64_t DEFAULT_WARM_UP_TIME_S = 600; //10min
  static const int64_t MIN_WARM_UP_TIME_S = 10; // 10s
  static const int64_t MAX_WARM_UP_TIME_S = 1800; // 30min

  // 预热时间(默认10分钟)
  int64_t warm_up_time_s;
  // 读取sstable的比例两次步进的时间间隔(根据warm_up_time_us计算出)
  int64_t warm_up_step_interval_us;

  CacheWarmUpConf() : warm_up_time_s(DEFAULT_WARM_UP_TIME_S),
    warm_up_step_interval_us(DEFAULT_WARM_UP_TIME_S * 1000L * 1000L / (STOP_PERCENT / STEP_PERCENT)) {
  };

  bool check() {
    bool bret = false;
    if (0 == warm_up_time_s) {
      warm_up_step_interval_us = 0;
      bret = true;
    } else if (MIN_WARM_UP_TIME_S > warm_up_time_s) {
      TBSYS_LOG(WARN, "warm_up_time_s=%ld cannot smaller than %ld", warm_up_time_s, MIN_WARM_UP_TIME_S);
    } else if (MAX_WARM_UP_TIME_S < warm_up_time_s) {
      TBSYS_LOG(WARN, "warm_up_time_s=%ld cannot larger than %ld", warm_up_time_s, MAX_WARM_UP_TIME_S);
    } else {
      warm_up_step_interval_us = warm_up_time_s * 1000L * 1000L / (STOP_PERCENT / STEP_PERCENT);
      bret = true;
    }
    return bret;
  };
};
}
}

#endif //OCEANBASE_UPDATESERVER_UPS_UTILS_H_



