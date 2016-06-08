/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_string_buf.h for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_STRING_BUF_H_
#define OCEANBASE_COMMON_OB_STRING_BUF_H_

#include "tblog.h"
#include "ob_string.h"
#include "ob_object.h"
#include "ob_memory_pool.h"

namespace sb {
namespace common {
// This class is not thread safe.
// ObStringBuf is used to store the ObString and ObObj object.
class ObStringBuf {
 public:
  ObStringBuf(const int32_t mod_id = 0);
  ~ObStringBuf();
  int clear();
  // only remain one memory block, clear block_head_ and block_tail_
  int reset();

 public:
  // Writes a string to buf.
  // @param [in] str the string object to be stored.
  // @param [out] stored_str records the stored ptr and length of string.
  // @return OB_SUCCESS if succeed, other error code if error occurs.
  int write_string(const ObString& str, ObString* stored_str);
  // Writes an obj to buf.
  // @param [in] obj the object to be stored.
  // @param [out] stored_obj records the stored obj
  // @return OB_SUCCESS if succeed, other error code if error occurs.
  int write_obj(const ObObj& obj, ObObj* stored_obj);

  inline int64_t used() const {
    return total_res_;
  };

  inline int64_t total() const {
    return total_virt_;
  };

 private:
  // Mem alloc/free methods
  int alloc_a_block_();
  int alloc_mem_(const int64_t size, void*& ptr);
  int free_mem_(void* ptr);

 private:
  static const int64_t DEF_MEM_BLOCK_SIZE = 2 * 1024L * 1024L;

 private:
  struct MemBlock {
    MemBlock* next;
    int32_t cur_pos;
    int32_t block_size;
    char data[0];
  };
 private:
  MemBlock* block_head_;
  MemBlock* block_tail_;
  int64_t total_virt_;
  int64_t total_res_;
  int64_t mod_id_;
};
}
}


#endif //__OB_STRING_BUF_H__


