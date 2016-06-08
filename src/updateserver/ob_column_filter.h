/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_column_filter.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_COLUMN_FILTER_H_
#define  OCEANBASE_UPDATESERVER_COLUMN_FILTER_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"

namespace sb {
namespace updateserver {
class ColumnFilter {
 public:
  inline ColumnFilter() : column_num_(0), is_sorted_(false) {
  };
  inline ~ColumnFilter() {
  };
 public:
  inline int add_column(const uint64_t column_id) {
    int ret = common::OB_SUCCESS;
    if (common::OB_MAX_COLUMN_NUMBER <= column_num_) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      columns_[column_num_] = column_id;
      column_num_ += 1;
      is_sorted_ = false;
    }
    return ret;
  };
  bool column_exist(const uint64_t column_id) {
    bool bret = false;
    if (common::OB_INVALID_ID == column_id) {
      bret = true;
    } else if (0 < column_num_) {
      if (!is_sorted_) {
        std::sort(columns_, &columns_[column_num_]);
        is_sorted_ = true;
      }

      if (common::OB_FULL_ROW_COLUMN_ID == columns_[0]) {
        bret = true;
      } else {
        bret = std::binary_search(columns_, &columns_[column_num_], column_id);
      }
    }
    return bret;
  };
  const char* log_str() const {
    int64_t pos = 0;
    log_buffer_[0] = '\0';
    for (int64_t i = 0; i < column_num_; ++i) {
      if (pos < LOG_BUFFER_SIZE) {
        pos += snprintf(log_buffer_ + pos, LOG_BUFFER_SIZE - pos, "%lu,", columns_[i]);
      } else {
        break;
      }
    }
    return log_buffer_;
  };
  void clear() {
    column_num_ = 0;
    is_sorted_ = false;
  };
 private:
  static const int64_t LOG_BUFFER_SIZE = 1024;
  mutable char log_buffer_[LOG_BUFFER_SIZE];
  uint64_t columns_[common::OB_MAX_COLUMN_NUMBER];
  int32_t column_num_;
  bool is_sorted_;
};
}
}

#endif //OCEANBASE_UPDATESERVER_COLUMN_FILTER_H_



