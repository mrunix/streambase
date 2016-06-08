/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * mutator_builder.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef _MIXED_TEST_MUTATOR_BUILDER_
#define _MIXED_TEST_MUTATOR_BUILDER_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/page_arena.h"
#include "common/ob_string.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_schema.h"
#include "common/ob_read_common_data.h"
#include "common/ob_mutator.h"
#include "common/hash/ob_hashmap.h"
#include "common/hash/ob_hashutils.h"
#include "updateserver/ob_table_engine.h"
#include "../mock_client.h"
#include "cellinfo_builder.h"
#include "rowkey_builder.h"

using namespace sb;
using namespace sb::common;
using namespace sb::common::hash;
using namespace sb::updateserver;

class MutatorBuilder {
  typedef ObHashMap<TEKey, int64_t> seed_map_t;
 public:
  MutatorBuilder();
  ~MutatorBuilder();
 public:
  int init(const ObSchemaManager& schema_mgr,
           const int64_t prefix_start,
           const int64_t suffix_length,
           const char* serv_addr,
           const int serv_port,
           const int64_t table_start_version,
           const int64_t max_op_num = 32,
           const int64_t max_rownum_per_mutator = 128,
           const int64_t max_suffix_per_prefix = 32);
  void destroy();
  int init_prefix_end(const int32_t prefix_end);
  int build_mutator(ObMutator& mutator, PageArena<char>& allocer, const bool using_id);
  int build_get_param(ObGetParam& get_param, PageArena<char>& allocer,
                      const int64_t table_start_version, const bool using_id,
                      const int64_t row_num, const int64_t cell_num_per_row);
  int build_scan_param(ObScanParam& scan_param, PageArena<char>& allocer,
                       const int64_t table_start_version, const bool using_id, int64_t& table_pos, int64_t& prefix);
  RowkeyBuilder& get_rowkey_builder(const int64_t schema_pos);
  CellinfoBuilder& get_cellinfo_builder(const int64_t schema_pos);
  const ObSchema& get_schema(const int64_t schema_pos) const;
  int64_t get_schema_num() const;
  int64_t get_schema_pos(const uint64_t table_id) const;
  int64_t get_schema_pos(const ObString& table_name) const;
 private:
  int get_cur_seed_(const seed_map_t& seed_map, const ObSchema& schema, const ObString& row_key, int64_t& cur_seed, const bool using_id);
  int64_t get_cur_rowkey_info_(const ObSchema& schema, const bool using_id);
 public:
  CellinfoBuilder* cb_array_[OB_MAX_TABLE_NUMBER];
  RowkeyBuilder* rb_array_[OB_MAX_TABLE_NUMBER];
  int32_t prefix_end_array_[OB_MAX_TABLE_NUMBER];
  bool need_update_prefix_end_;
  ObSchemaManager schema_mgr_;
  int64_t max_rownum_per_mutator_;
  int64_t table_num_;
  struct drand48_data table_rand_seed_;
  MockClient client_;
  int64_t table_start_version_;
  seed_map_t seed_map_;
  PageArena<char> allocer_;
};

#endif // _MIXED_TEST_MUTATOR_BUILDER_


