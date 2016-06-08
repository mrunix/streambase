/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * row_checker.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef _MIXED_TEST_ROW_CHECKER_
#define _MIXED_TEST_ROW_CHECKER_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/page_arena.h"
#include "common/ob_string.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_schema.h"
#include "common/ob_read_common_data.h"
#include "common/ob_mutator.h"
#include "updateserver/ob_memtank.h"
#include "rowkey_builder.h"
#include "cellinfo_builder.h"

using namespace sb;
using namespace sb::common;
using namespace sb::common::hash;
using namespace sb::updateserver;

class RowChecker {
  static const int64_t RK_SET_SIZE = 1024;
 public:
  RowChecker();
  ~RowChecker();
 public:
  void add_cell(const ObCellInfo* ci);
  bool check_row(CellinfoBuilder& cb, const ObSchema& schema);
  const ObString& get_cur_rowkey() const;
  int64_t cell_num() const;

  void add_rowkey(const ObString& row_key);
  bool check_rowkey(RowkeyBuilder& rb, const int64_t* prefix_ptr = NULL);
  bool is_prefix_changed(const ObString& row_key);
  const ObString& get_last_rowkey() const;
  int64_t rowkey_num() const;
 public:
  ObString cur_row_key_;
  CellinfoBuilder::result_set_t read_result_;
  MemTank ci_mem_tank_;

  bool last_is_del_row_;
  ObString last_row_key_;
  ObHashMap<ObString, uint64_t> rowkey_read_set_;
  MemTank rk_mem_tank_;
  int64_t add_rowkey_times_;
};

#endif // _MIXED_TEST_ROW_CHECKER_


