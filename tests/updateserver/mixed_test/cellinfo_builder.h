/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * cellinfo_builder.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef _MIXED_TEST_CELLINFO_BUILDER_
#define _MIXED_TEST_CELLINFO_BUILDER_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/page_arena.h"
#include "common/ob_string.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_schema.h"
#include "common/ob_read_common_data.h"
#include "common/ob_mutator.h"

#define SEED_COLUMN_NAME "seed_column"
#define SEED_COLUMN_ID 48
#define ROWKEY_INFO_COLUMN_NAME "rowkey_info"
#define ROWKEY_INFO_COLUMN_ID 49
#define ROWKEY_INFO_ROWKEY "####################rowkey_info"
#define C_TIME_COLUMN_ID 46
#define M_TIME_COLUMN_ID 47

using namespace sb;
using namespace sb::common;
using namespace sb::common::hash;

class CellinfoBuilder {
 public:
  enum {
    UPDATE = 1,
    INSERT = 2,
    ADD = 3,
    DEL_ROW = 4,
    DEL_CELL = 5,
  };
  typedef ObHashMap<uint64_t, ObCellInfo*> result_set_t;
  typedef result_set_t::iterator result_iter_t;
 public:
  CellinfoBuilder(const int64_t max_op_num);
  ~CellinfoBuilder();
 public:
  static int check_schema_mgr(const ObSchemaManager& schema_mgr);
  int get_mutator(const ObString& row_key, const ObSchema& schema, int64_t& cur_seed,
                  ObMutator& mutator, PageArena<char>& allocer);
  int get_result(const ObString& row_key, const ObSchema& schema,
                 const int64_t begin_seed, const int64_t end_seed,
                 result_set_t& result, PageArena<char>& allocer);
  static void merge_obj(const int64_t op_type, ObCellInfo* cell_info, result_set_t& result);
 private:
  void build_cell_(struct drand48_data& rand_data, const ObString& row_key, const ObSchema& schema,
                   ObObj& obj, int64_t& column_pos, int& op_type,
                   PageArena<char>& allocer);
  int build_operator_(struct drand48_data& rand_data, const ObString& row_key, const ObSchema& schema,
                      ObMutator& mutator, PageArena<char>& allocer);
  int calc_op_type_(const int64_t rand);
 private:
  const int64_t max_op_num_;
};

#endif // _MIXED_TEST_CELLINFO_BUILDER_


