/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_btree_engine_alloc.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ob_btree_engine_alloc.h"

using namespace sb::common;
using namespace sb::mergeserver;

const int32_t BtreeEngineAlloc::MALLOC_SIZE = (1024 * 1024);

BtreeEngineAlloc::BtreeEngineAlloc() : BtreeAlloc() {
  buffer_ = NULL;
  free_list_ = NULL;
  use_count_ = 0;
  free_count_ = 0;
  malloc_count_ = 0;
  init(sizeof(int64_t));
}

BtreeEngineAlloc::~BtreeEngineAlloc() {
  destroy();
}

void BtreeEngineAlloc::set_mem_tank(MemTank* mem_tank) {
  mem_tank_ = mem_tank;
}

void BtreeEngineAlloc::init(int32_t size) {
  destroy();
  size_ = size;
  // 不能小于8个. 在free_list上会用到
  if (size_ < static_cast<int32_t>(sizeof(int64_t))) {
    size_ = sizeof(int64_t);
  }
  buffer_ = NULL;
  free_list_ = NULL;
  use_count_ = 0;
  free_count_ = 0;
  malloc_count_ = 0;
}

int32_t BtreeEngineAlloc::get_alloc_size() {
  return size_;
}

char* BtreeEngineAlloc::alloc() {
  char* pret = NULL;
  // 从free list取
  if (free_list_ != NULL) {
    pret = free_list_;
    int64_t* addr = reinterpret_cast<int64_t*>(free_list_);
    free_list_ =  reinterpret_cast<char*>(*addr);
    free_count_ --;
  } else if ((buffer_ != NULL) && ((buffer_->last + size_) <= buffer_->end)) {
    // 从最后一个buffer上取
    pret = buffer_->last;
    buffer_->last += size_;
  } else {
    // malloc出一个新块, 每次1m
    // 分配出1m空间
    int32_t n = BtreeEngineAlloc::MALLOC_SIZE / size_;
    n = (n < 1 ? 1 : n) * size_ + sizeof(struct MemBuffer);
    char* pdata = NULL;
    if (NULL != mem_tank_) {
      pdata = reinterpret_cast<char*>(mem_tank_->engine_alloc(n));
    }
    if (NULL != pdata) {
      malloc_count_ ++;
      MemBuffer* mb = reinterpret_cast<MemBuffer*>(pdata);
      mb->last = pdata + sizeof(MemBuffer);
      mb->end = pdata + n;
      mb->next = buffer_;
      buffer_ = mb;
      // 从最后一个buffer上取
      pret = buffer_->last;
      buffer_->last += size_;
    }
  }
  memset(pret, 0, size_);
  use_count_  ++;
  return pret;
}

void BtreeEngineAlloc::release(const char* pdata) {
  if (pdata != NULL) {
    use_count_  --;
    free_count_ ++;

    // 释放掉, 用pdata自身的内存把链串起来
    int64_t* addr = reinterpret_cast<int64_t*>(const_cast<char*>(pdata));
    (*addr) = reinterpret_cast<int64_t>(free_list_);
    free_list_ = const_cast<char*>(pdata);
  }
}

void BtreeEngineAlloc::destroy() {
  if (buffer_ != NULL) {
    buffer_ = NULL;
    free_list_ = NULL;
    use_count_ = 0;
    free_count_ = 0;
    malloc_count_ = 0;
  }
}

int64_t BtreeEngineAlloc::get_use_count() {
  return use_count_;
}

int64_t BtreeEngineAlloc::get_free_count() {
  return free_count_;
}

int32_t BtreeEngineAlloc::get_malloc_count() {
  return malloc_count_;
}



