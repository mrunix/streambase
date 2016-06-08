/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_range.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_RANGE_H_
#define OCEANBASE_COMMON_OB_RANGE_H_

#include <tbsys.h>
#include "ob_define.h"
#include "ob_string.h"

namespace sb {
namespace common {
class ObBorderFlag {
 public:
  static const int8_t INCLUSIVE_START = 0x1;
  static const int8_t INCLUSIVE_END = 0x2;
  static const int8_t MIN_VALUE = 0x4;
  static const int8_t MAX_VALUE = 0x8;

 public:
  ObBorderFlag() : data_(0) {}
  virtual ~ObBorderFlag() {}

  inline void set_inclusive_start() { data_ |= INCLUSIVE_START; }

  inline void unset_inclusive_start() { data_ &= (~INCLUSIVE_START); }

  inline bool inclusive_start() const { return (data_ & INCLUSIVE_START) == INCLUSIVE_START; }

  inline void set_inclusive_end() { data_ |= INCLUSIVE_END; }

  inline void unset_inclusive_end() { data_ &= (~INCLUSIVE_END); }

  inline bool inclusive_end() const { return (data_ & INCLUSIVE_END) == INCLUSIVE_END; }

  inline void set_min_value() { data_ |= MIN_VALUE; }
  inline void unset_min_value() { data_ &= (~MIN_VALUE); }
  inline bool is_min_value() const { return (data_ & MIN_VALUE) == MIN_VALUE; }

  inline void set_max_value() { data_ |= MAX_VALUE; }
  inline void unset_max_value() { data_ &= (~MAX_VALUE); }
  inline bool is_max_value() const { return (data_ & MAX_VALUE) == MAX_VALUE; }

  inline void set_data(const int8_t data) { data_ = data; }
  inline int8_t get_data() const { return data_; }

  inline bool is_left_open_right_closed() const { return (!inclusive_start() && inclusive_end()) || is_max_value(); }
 private:
  int8_t data_;
};

struct ObVersionRange {
  ObBorderFlag border_flag_;
  int64_t start_version_;
  int64_t end_version_;

  // from MIN to MAX, complete set.
  inline bool is_whole_range() const {
    return (border_flag_.is_min_value()) && (border_flag_.is_max_value());
  }
};

struct ObRange {
  uint64_t table_id_;
  ObBorderFlag border_flag_;
  ObString start_key_;
  ObString end_key_;

  void reset() {
    table_id_ = OB_INVALID_ID;
    border_flag_.set_data(0);
    start_key_.assign(NULL, 0);
    end_key_.assign(NULL, 0);
  }

  // new compare func for tablet.range and scan_param.range
  int compare_with_endkey2(const ObRange& r) const {
    int cmp = 0;
    if (border_flag_.is_max_value()) {
      if (!r.border_flag_.is_max_value()) {
        cmp = 1;
      }
    } else if (r.border_flag_.is_max_value()) {
      cmp = -1;
    } else {
      cmp = end_key_.compare(r.end_key_);
      if (0 == cmp) {
        if (border_flag_.inclusive_end() && !r.border_flag_.inclusive_end()) {
          cmp = 1;
        } else if (!border_flag_.inclusive_end() && r.border_flag_.inclusive_end()) {
          cmp = -1;
        }
      }
    }
    return cmp;
  }

  inline int compare_with_endkey(const ObRange& r) const {
    int cmp = 0;
    if (table_id_ != r.table_id_) {
      cmp = (table_id_ < r.table_id_) ? -1 : 1;
    } else {
      if (border_flag_.is_max_value()) {
        // MAX_VALUE == MAX_VALUE;
        if (r.border_flag_.is_max_value()) {
          cmp = 0;
        } else {
          cmp = 1;
        }
      } else {
        if (r.border_flag_.is_max_value()) {
          cmp = -1;
        } else {
          cmp = end_key_.compare(r.end_key_);
        }
      }
    }
    return cmp;
  }

  inline int compare_with_startkey(const ObRange& r) const {
    int cmp = 0;
    if (table_id_ != r.table_id_) {
      cmp = (table_id_ < r.table_id_) ? -1 : 1;
    } else {
      if (border_flag_.is_min_value()) {
        // MIN_VALUE == MIN_VALUE;
        if (r.border_flag_.is_min_value()) {
          cmp = 0;
        } else {
          cmp = -1;
        }
      } else {
        if (r.border_flag_.is_min_value()) {
          cmp = 1;
        } else {
          cmp = start_key_.compare(r.start_key_);
        }
      }
    }
    return cmp;
  }

  int compare_with_startkey2(const ObRange& r) const {
    int cmp = 0;
    if (border_flag_.is_min_value()) {
      if (!r.border_flag_.is_min_value()) {
        cmp = -1;
      }
    } else if (r.border_flag_.is_min_value()) {
      cmp = 1;
    } else {
      cmp = start_key_.compare(r.start_key_);
      if (0 == cmp) {
        if (border_flag_.inclusive_start() && !r.border_flag_.inclusive_start()) {
          cmp = -1;
        } else if (!border_flag_.inclusive_start() && r.border_flag_.inclusive_start()) {
          cmp = 1;
        }
      }
    }
    return cmp;
  }

  inline bool empty() const {
    bool ret = false;
    if (border_flag_.is_min_value() || border_flag_.is_max_value()) {
      ret = false;
    } else {
      ret  = end_key_ < start_key_
             || ((end_key_ == start_key_)
                 && !((border_flag_.inclusive_end())
                      && border_flag_.inclusive_start()));
    }
    return ret;
  }

  // from MIN to MAX, complete set.
  inline bool is_whole_range() const {
    return (border_flag_.is_min_value()) && (border_flag_.is_max_value());
  }

  bool equal(const ObRange& r) const;

  bool intersect(const ObRange& r) const;

  void dump() const;

  bool check(void) const;

  void hex_dump(const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;

  int to_string(char* buffer, const int32_t length) const;

  NEED_SERIALIZE_AND_DESERIALIZE;
};

template <typename Allocator>
int deep_copy_range(Allocator& allocator, const ObRange& src, ObRange& dst) {
  int ret = OB_SUCCESS;

  ObString::obstr_size_t start_len = src.start_key_.length();
  ObString::obstr_size_t end_len = src.end_key_.length();

  char* copy_start_key_ptr = NULL;
  char* copy_end_key_ptr = NULL;

  dst.table_id_ = src.table_id_;
  dst.border_flag_ = src.border_flag_;


  if (NULL == (copy_start_key_ptr = allocator.alloc(start_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (NULL == (copy_end_key_ptr = allocator.alloc(end_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    memcpy(copy_start_key_ptr, src.start_key_.ptr(), start_len);
    dst.start_key_.assign_ptr(copy_start_key_ptr, start_len);
    memcpy(copy_end_key_ptr, src.end_key_.ptr(), end_len);
    dst.end_key_.assign_ptr(copy_end_key_ptr, end_len);
  }

  return ret;
}


} // end namespace common
} // end namespace sb

#endif //OCEANBASE_COMMON_OB_RANGE_H_


