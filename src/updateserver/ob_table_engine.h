/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_engine.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_TABLE_ENGINE_H_
#define  OCEANBASE_UPDATESERVER_TABLE_ENGINE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "common/ob_read_common_data.h"
#include "common/utility.h"
#include "common/ob_crc64.h"
#include "ob_ups_utils.h"

namespace sb {
namespace updateserver {
const int64_t CELL_INFO_SIZE_UNIT = 1024L;

struct UpsCellInfo {
  uint64_t column_id_;
  common::ObObj value_;
  inline void reset() {
    column_id_ = common::OB_INVALID_ID;
    value_.reset();
  }
  inline int64_t checksum(const int64_t current) const {
    int64_t ret = current;
    ret = common::ob_crc64(ret, &column_id_, sizeof(column_id_));
    ret = value_.checksum(ret);
    return ret;
  }
};

struct ObCellInfoNode {
  UpsCellInfo cell_info;
  ObCellInfoNode* next;
  inline void reset() {
    cell_info.reset();
    next = NULL;
  }
  inline int64_t checksum(const int64_t current) const {
    return cell_info.checksum(current);
  }
  inline int64_t size() const {
    int64_t ret = sizeof(cell_info.column_id_) + sizeof(cell_info.value_);
    if (common::ObVarcharType == cell_info.value_.get_type()) {
      common::ObString varchar;
      cell_info.value_.get_varchar(varchar);
      ret += varchar.length();
    }
    // 按CELL_INFO_SIZE_UNIT向上取整
    ret = (ret + CELL_INFO_SIZE_UNIT - 1) & ~(CELL_INFO_SIZE_UNIT - 1);
    ret = ret / CELL_INFO_SIZE_UNIT;
    return ret;
  }
};

struct TEKey {
  uint64_t table_id;
  common::ObString row_key;

  TEKey() : table_id(common::OB_INVALID_ID), row_key() {
  };
  inline int64_t hash() const {
    return (common::murmurhash2(row_key.ptr(), row_key.length(), 0) + table_id);
  };
  inline int64_t checksum(const int64_t current) const {
    int64_t ret = current;
    ret = common::ob_crc64(ret, &table_id, sizeof(table_id));
    ret = common::ob_crc64(ret, row_key.ptr(), row_key.length());
    return ret;
  };
  const char* log_str() const {
    static const int32_t BUFFER_SIZE = 2048;
    static __thread char BUFFER[2][BUFFER_SIZE];
    static __thread int64_t i = 0;
    if (NULL != row_key.ptr()
        && 0 != row_key.length()
        && !isprint(row_key.ptr()[0])) {
      char hex_buffer[BUFFER_SIZE] = {'\0'};
      common::hex_to_str(row_key.ptr(), row_key.length(), hex_buffer, BUFFER_SIZE);
      snprintf(BUFFER[i % 2], BUFFER_SIZE, "table_id=%lu row_ptr=%p row_key=[0x %s] key_length=%d",
               table_id, row_key.ptr(), hex_buffer, row_key.length());
    } else {
      snprintf(BUFFER[i % 2], BUFFER_SIZE, "table_id=%lu row_ptr=%p row_key=[%.*s] key_length=%d",
               table_id, row_key.ptr(), row_key.length(), row_key.ptr(), row_key.length());
    }
    return BUFFER[i++ % 2];
  };
  inline bool operator == (const TEKey& other) const {
    return (table_id == other.table_id
            && row_key == other.row_key);
  };
  inline bool operator != (const TEKey& other) const {
    return (table_id != other.table_id
            || row_key != other.row_key);
  };
  inline int operator - (const TEKey& other) const {
    int ret = 0;
    if (table_id > other.table_id) {
      ret = 1;
    } else if (table_id < other.table_id) {
      ret = -1;
    } else {
      if (row_key > other.row_key) {
        ret = 1;
      } else if (row_key < other.row_key) {
        ret = -1;
      } else {
        ret = 0;
      }
    }
    return ret;
  };
};

const int8_t ROW_ST_UNKNOW = 0;
const int8_t ROW_ST_EXIST = 1;
const int8_t ROW_ST_NOT_EXIST = 2;

struct TEValue {
  int16_t cell_info_cnt;
  int8_t row_stat;
  int8_t update_count;
  int16_t cell_info_size; // 单位为1K
  int16_t reserve2;
  ObCellInfoNode* list_head;
  ObCellInfoNode* list_tail;
  int64_t create_time;
  int64_t modify_time;

  TEValue() {
    reset();
  };
  inline int64_t checksum(const int64_t current) const {
    int64_t ret = current;
    ret = common::ob_crc64(ret, &cell_info_cnt, sizeof(cell_info_cnt));
    ret = common::ob_crc64(ret, &row_stat, sizeof(row_stat));
    ret = common::ob_crc64(ret, &create_time, sizeof(create_time));
    ret = common::ob_crc64(ret, &modify_time, sizeof(modify_time));
    return ret;
  };
  void reset() {
    cell_info_cnt = 0;
    row_stat = ROW_ST_UNKNOW;
    update_count = 0;
    cell_info_size = 0;
    list_head = NULL;
    list_tail = NULL;
    create_time = 0;
    modify_time = 0;
  };
  const char* log_str() const {
    static const int32_t BUFFER_SIZE = 2048;
    static __thread char BUFFER[2][BUFFER_SIZE];
    static __thread int64_t i = 0;
    snprintf(BUFFER[i % 2], BUFFER_SIZE, "cell_info_cnt=%hd row_stat=%hhd update_count=%hhd cell_info_size=%hdKB list_head=%p list_tail=%p create_time=%s modify_time=%s",
             cell_info_cnt, row_stat, update_count, cell_info_size, list_head, list_tail, time2str(create_time), time2str(modify_time));
    return BUFFER[i++ % 2];
  };
};

enum TETransType {
  INVALID_TRANSACTION = 0,
  READ_TRANSACTION = 1,
  WRITE_TRANSACTION = 2,
};

extern bool get_key_prefix(const TEKey& te_key, TEKey& prefix_key);

typedef std::pair<TEKey, TEValue> TEKVPair;

class HashEngine;
class HashEngineIterator;
class HashEngineTransHandle;
class BtreeEngine;
class BtreeEngineIterator;
class BtreeEngineTransHandle;
}
}

#endif //OCEANBASE_UPDATESERVER_TABLE_ENGINE_H_



