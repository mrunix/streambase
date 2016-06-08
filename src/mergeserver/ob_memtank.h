/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_memtank.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef  OCEANBASE_MERGER_MEMTANK_H_
#define  OCEANBASE_MERGER_MEMTANK_H_

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <bitset>
#include <algorithm>
#include "ob_atomic.h"
#include "common/ob_define.h"
#include "common/ob_string_buf.h"
#include "common/page_arena.h"

namespace sb {
namespace mergeserver {
class MemTank {
 public:
  MemTank(const int32_t mod_id = common::ObModIds::OB_MS_BTREE)
    : total_limit_(INT64_MAX),
      string_buf_(mod_id),
      mod_(mod_id),
      allocer_(common::ModuleArena::DEFAULT_PAGE_SIZE, mod_),
      indep_allocer_(common::ModuleArena::DEFAULT_PAGE_SIZE, mod_),
      engine_allocer_(common::ModuleArena::DEFAULT_PAGE_SIZE, mod_) {
  };

  ~MemTank() {
    clear();
  };

 public:
  int write_string(const common::ObString& str, common::ObString* stored_str) {
    int ret = common::OB_MEM_OVERFLOW;
    if (total() < total_limit_) {
      ret = string_buf_.write_string(str, stored_str);
    } else {
      log_error_(__FUNCTION__);
    }
    return ret;
  };
  int write_obj(const common::ObObj& obj, common::ObObj* stored_obj) {
    int ret = common::OB_MEM_OVERFLOW;
    if (total() < total_limit_) {
      ret = string_buf_.write_obj(obj, stored_obj);
    } else {
      log_error_(__FUNCTION__);
    }
    return ret;
  };
  void* alloc(const int32_t sz) {
    void* ret = NULL;
    if (total() < total_limit_) {
      ret = allocer_.alloc(sz);
    } else {
      log_error_(__FUNCTION__);
    }
    return ret;
  };
  void* indep_alloc(const int32_t sz) {
    void* ret = NULL;
    if (total() < total_limit_) {
      ret = indep_allocer_.alloc(sz);
    } else {
      log_error_(__FUNCTION__);
    }
    return ret;
  };
  void* engine_alloc(const int32_t sz) {
    void* ret = NULL;
    if (total() < total_limit_) {
      ret = engine_allocer_.alloc(sz);
    } else {
      log_error_(__FUNCTION__);
    }
    return ret;
  };
  void clear() {
    string_buf_.clear();
    allocer_.free();
    indep_allocer_.free();
    engine_allocer_.free();
  };
 public:
  bool mem_over_limit() const {
    return (total() >= total_limit_);
  };
  int64_t used() const {
    return (string_buf_.used() + allocer_.used() + indep_allocer_.used() + engine_allocer_.used());
  };
  int64_t total() const {
    return (string_buf_.total() + allocer_.total() + indep_allocer_.total() + engine_allocer_.total());
  };
  int64_t set_total_limit(const int64_t limit) {
    if (0 < limit) {
      total_limit_ = limit;
    }
    return total_limit_;
  };
  int64_t get_total_limit() const {
    return total_limit_;
  };
  void log_info() const {
    TBSYS_LOG(INFO, "MemTank report used=%ld total=%ld limit=%ld "
              "string_buf_used=%ld string_buf_total=%ld "
              "allocer_used=%ld allocer_total=%ld "
              "indep_allocer_used=%ld indep_allocer_total=%ld "
              "engine_allocer_used=%ld engine_allocer_total=%ld",
              used(), total(), get_total_limit(),
              string_buf_.used(), string_buf_.total(),
              allocer_.used(), allocer_.total(),
              indep_allocer_.used(), indep_allocer_.total(),
              engine_allocer_.used(), engine_allocer_.total());
  };
 private:
  void log_error_(const char* caller) const {
    TBSYS_LOG(ERROR, "memory over limited, caller=[%s] used=%ld total=%ld limit=%ld "
              "string_buf_used=%ld string_buf_total=%ld "
              "allocer_used=%ld allocer_total=%ld "
              "indep_allocer_used=%ld indep_allocer_total=%ld "
              "engine_allocer_used=%ld engine_allocer_total=%ld",
              caller, used(), total(), get_total_limit(),
              string_buf_.used(), string_buf_.total(),
              allocer_.used(), allocer_.total(),
              indep_allocer_.used(), indep_allocer_.total(),
              engine_allocer_.used(), engine_allocer_.total());
  };
 private:
  int64_t total_limit_;
  common::ObStringBuf string_buf_;
  common::ModulePageAllocator mod_;
  common::ModuleArena allocer_;
  common::ModuleArena indep_allocer_;
  common::ModuleArena engine_allocer_;
};
}
}

#endif //OCEANBASE_MERGER_MEMTANK_H_



