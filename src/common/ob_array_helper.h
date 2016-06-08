/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_array_helper.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_ARRAY_HELPER_H_
#define OCEANBASE_COMMON_OB_ARRAY_HELPER_H_
#include <stdint.h>
#include <stdlib.h>

namespace sb {
namespace common {
template<class T>
class ObArrayHelper {
 public:
  ObArrayHelper(int64_t size, T* base_address, int64_t index = 0)
    : size_(size), index_(index), p_(base_address) {
  }
  ObArrayHelper()
    : size_(0), index_(0), p_(NULL) {
  }
  void init(int64_t size, T* base_address, int64_t index = 0) {
    size_ = size;
    p_ = base_address;
    index_ = index;
  }
  bool push_back(const T& value) {
    bool add_ok = true;
    if (index_ < size_ && p_ && index_ >= 0) {
      p_[index_++] = value;
    } else {
      add_ok = false;
    }
    return add_ok;
  }
  T* pop() {
    T* res = NULL;
    if (index_ > 0 && p_ && index_ <= size_) {
      res = &p_[--index_];
    }
    return res;
  }
  int64_t get_array_size() const {
    return size_;
  }
  int64_t get_array_index() const {
    return index_;
  }
  T* at(int64_t index) const {
    T* res = NULL;
    if (index < index_ && p_ && index >= 0) {
      res = &p_[index];
    }
    return res;
  }
  T* get_base_address() const {
    return p_;
  }
  void clear() {
    index_ = 0;
  }
 private:
  int64_t size_;
  int64_t index_;
  T* p_;
};
}
}
#endif

