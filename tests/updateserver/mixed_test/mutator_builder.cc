/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * mutator_builder.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include <bitset>
#include "utils.h"
#include "mutator_builder.h"

using namespace sb;
using namespace sb::common;
using namespace sb::common::hash;
using namespace sb::updateserver;

MutatorBuilder::MutatorBuilder() {
  memset(cb_array_, 0, sizeof(CellinfoBuilder*) * OB_MAX_TABLE_NUMBER);
  memset(rb_array_, 0, sizeof(RowkeyBuilder*) * OB_MAX_TABLE_NUMBER);
  memset(prefix_end_array_, 0, sizeof(int32_t) * OB_MAX_TABLE_NUMBER);
  need_update_prefix_end_ = true;
  max_rownum_per_mutator_ = 0;
  table_num_ = 0;
  table_start_version_ = 0;
}

MutatorBuilder::~MutatorBuilder() {
  destroy();
}

void MutatorBuilder::destroy() {
  for (int64_t i = 0; i < table_num_; i++) {
    if (NULL != cb_array_[i]) {
      delete cb_array_[i];
      cb_array_[i] = NULL;
    }
    if (NULL != rb_array_[i]) {
      delete rb_array_[i];
      rb_array_[i] = NULL;
    }
  }
  table_num_ = 0;
  client_.destroy();
}

RowkeyBuilder& MutatorBuilder::get_rowkey_builder(const int64_t schema_pos) {
  return *(rb_array_[schema_pos]);
}

CellinfoBuilder& MutatorBuilder::get_cellinfo_builder(const int64_t schema_pos) {
  return *(cb_array_[schema_pos]);
}

const ObSchema& MutatorBuilder::get_schema(const int64_t schema_pos) const {
  return schema_mgr_.begin()[schema_pos];
}

int64_t MutatorBuilder::get_schema_num() const {
  return table_num_;
}

int MutatorBuilder::init(const ObSchemaManager& schema_mgr,
                         const int64_t prefix_start,
                         const int64_t suffix_length,
                         const char* serv_addr,
                         const int serv_port,
                         const int64_t table_start_version,
                         const int64_t max_op_num,
                         const int64_t max_rownum_per_mutator,
                         const int64_t max_suffix_per_prefix) {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = CellinfoBuilder::check_schema_mgr(schema_mgr))) {
    TBSYS_LOG(WARN, "schema cannot pass check");
  } else {
    table_num_ = schema_mgr.end() - schema_mgr.begin();
    srand48_r(time(NULL), &table_rand_seed_);
    schema_mgr_ = schema_mgr;
    max_rownum_per_mutator_ = max_rownum_per_mutator;
    for (int64_t i = 0; i < table_num_; i++) {
      if (NULL == (cb_array_[i] = new(std::nothrow) CellinfoBuilder(max_op_num))
          || NULL == (rb_array_[i] = new(std::nothrow) RowkeyBuilder(prefix_start, max_suffix_per_prefix, suffix_length))) {
        ret = OB_ERROR;
        break;
      }
    }
  }
  if (OB_SUCCESS != ret) {
    destroy();
  } else {
    ObServer dst_host;
    dst_host.set_ipv4_addr(serv_addr, serv_port);
    client_.init(dst_host);
    table_start_version_ = table_start_version;
    seed_map_.create(2000000);
  }
  return ret;
}

int MutatorBuilder::build_scan_param(ObScanParam& scan_param, PageArena<char>& allocer,
                                     const int64_t table_start_version, const bool using_id, int64_t& table_pos, int64_t& prefix) {
  int ret = OB_SUCCESS;
  scan_param.reset();

  static int64_t row_info_array[OB_MAX_TABLE_NUMBER];
  static int64_t counter = 0;
  //static const int64_t ROW_INFO_CACHE_FLUSH_PERIOD = 10000;
  static const int64_t ROW_INFO_CACHE_FLUSH_PERIOD = 1000000000;
  if (0 == (counter++ % ROW_INFO_CACHE_FLUSH_PERIOD)) {
    memset(row_info_array, 0, sizeof(row_info_array));
  }

  int64_t rand = 0;
  lrand48_r(&table_rand_seed_, &rand);
  table_pos = range_rand(0, table_num_ - 1, rand);
  const ObSchema& schema = schema_mgr_.begin()[table_pos];

  if (0 == row_info_array[table_pos]
      && 0 == (row_info_array[table_pos] = get_cur_rowkey_info_(schema, using_id))) {
    ret = OB_ERROR;
  } else {
    ObRange range;
    std::pair<ObString, ObString> key_range = rb_array_[table_pos]->get_rowkey4scan(row_info_array[table_pos], allocer, prefix);
    range.start_key_ = key_range.first;
    range.end_key_ = key_range.second;
    const ObColumnSchema* iter = NULL;
    for (iter = schema.column_begin(); iter != schema.column_end(); iter++) {
      if (using_id) {
        scan_param.add_column(iter->get_id());
      } else {
        ObString column_name;
        column_name.assign_ptr(const_cast<char*>(iter->get_name()), strlen(iter->get_name()));
        scan_param.add_column(column_name);
      }
    }

    if (using_id) {
      scan_param.set(schema.get_table_id(), ObString(), range);
    } else {
      ObString table_name;
      table_name.assign(const_cast<char*>(schema.get_table_name()), strlen(schema.get_table_name()));
      scan_param.set(OB_INVALID_ID, table_name, range);
    }

    ObVersionRange version_range;
    version_range.start_version_ = table_start_version;
    version_range.border_flag_.set_max_value();
    version_range.border_flag_.set_inclusive_start();
    scan_param.set_version_range(version_range);
  }
  return ret;
}

int MutatorBuilder::build_get_param(ObGetParam& get_param, PageArena<char>& allocer,
                                    const int64_t table_start_version, const bool using_id,
                                    const int64_t row_num, const int64_t cell_num_per_row) {
  int ret = OB_SUCCESS;
  get_param.reset();

  static int64_t row_info_array[OB_MAX_TABLE_NUMBER];
  static int64_t counter = 0;
  //static const int64_t ROW_INFO_CACHE_FLUSH_PERIOD = 10000;
  static const int64_t ROW_INFO_CACHE_FLUSH_PERIOD = 1000000000;
  if (0 == (counter++ % ROW_INFO_CACHE_FLUSH_PERIOD)) {
    memset(row_info_array, 0, sizeof(row_info_array));
  }
  for (int64_t i = 0; i < row_num; i++) {
    int64_t rand = 0;
    lrand48_r(&table_rand_seed_, &rand);
    int64_t table_pos = range_rand(0, table_num_ - 1, rand);
    const ObSchema& cur_schema = schema_mgr_.begin()[table_pos];

    if (0 == row_info_array[table_pos]
        && 0 == (row_info_array[table_pos] = get_cur_rowkey_info_(cur_schema, using_id))) {
      break;
    }
    ObCellInfo ci;
    ci.row_key_ = rb_array_[table_pos]->get_random_rowkey(row_info_array[table_pos], allocer);
    if (using_id) {
      ci.table_id_ = cur_schema.get_table_id();
    } else {
      ci.table_name_.assign_ptr(const_cast<char*>(cur_schema.get_table_name()), strlen(cur_schema.get_table_name()));
    }
    if (using_id) {
      ci.column_id_ = SEED_COLUMN_ID;
      ci.column_name_.assign_ptr(NULL, 0);
    } else {
      ci.column_id_ = OB_INVALID_ID;
      ci.column_name_.assign_ptr(SEED_COLUMN_NAME, strlen(SEED_COLUMN_NAME));
    }
    get_param.add_cell(ci);

    lrand48_r(&table_rand_seed_, &rand);
    int64_t cell_num = range_rand(1, cell_num_per_row, rand);
    int64_t column_num = cur_schema.column_end() - cur_schema.column_begin();
    for (int64_t j = 0; j < cell_num; j++) {
      lrand48_r(&table_rand_seed_, &rand);
      //int64_t column_pos = range_rand(0, column_num - 1, rand);
      int64_t column_pos = 0;
      const ObColumnSchema& column_schema = cur_schema.column_begin()[column_pos];
      if (using_id) {
        ci.column_id_ = column_schema.get_id();
        ci.column_name_.assign_ptr(NULL, 0);
      } else {
        ci.column_id_ = OB_INVALID_ID;
        ci.column_name_.assign_ptr(const_cast<char*>(column_schema.get_name()), strlen(column_schema.get_name()));
      }
      get_param.add_cell(ci);
    }
  }
  ObVersionRange version_range;
  version_range.start_version_ = table_start_version_;
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  get_param.set_version_range(version_range);

  return ret;
}

int64_t MutatorBuilder::get_cur_rowkey_info_(const ObSchema& schema, const bool using_id) {
  int64_t ret = 0;

  ObGetParam get_param;
  ObScanner scanner;
  ObCellInfo cell_info;
  if (using_id) {
    cell_info.table_id_ = schema.get_table_id();
    cell_info.column_id_ = ROWKEY_INFO_COLUMN_ID;
  } else {
    cell_info.table_name_.assign_ptr(const_cast<char*>(schema.get_table_name()), strlen(schema.get_table_name()));
    cell_info.column_name_.assign_ptr(ROWKEY_INFO_COLUMN_NAME, strlen(ROWKEY_INFO_COLUMN_NAME));
  }
  cell_info.row_key_.assign_ptr(ROWKEY_INFO_ROWKEY, strlen(ROWKEY_INFO_ROWKEY));
  get_param.add_cell(cell_info);
  ObVersionRange version_range;
  version_range.start_version_ = table_start_version_;
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  get_param.set_version_range(version_range);

  if (OB_SUCCESS == client_.ups_get(get_param, scanner, TIMEOUT_MS)) {
    while (OB_SUCCESS == scanner.next_cell()) {
      ObCellInfo* ci = NULL;
      if (OB_SUCCESS != scanner.get_cell(&ci)) {
        break;
      }
      if (OB_SUCCESS != ci->value_.get_int(ret)) {
        break;
      }
    }
  }

  return ret;
}

int MutatorBuilder::init_prefix_end(const int32_t prefix_end) {
  int ret = OB_SUCCESS;
  const ObSchema* schema_array = schema_mgr_.begin();
  for (int64_t i = 0; i < table_num_; ++i) {
    if (0 == prefix_end) {
      bool using_id = false;
      prefix_end_array_[i] = get_cur_rowkey_info_(schema_array[i], using_id);
      need_update_prefix_end_ = false;
    } else {
      prefix_end_array_[i] = prefix_end;
    }
    if (0 == prefix_end_array_[i]) {
      ret = OB_ERROR;
      break;
    }
  }
  return ret;
}

int MutatorBuilder::build_mutator(ObMutator& mutator, PageArena<char>& allocer, const bool using_id) {
  int ret = OB_SUCCESS;
  mutator.reset();
  const ObSchema* schema_array = schema_mgr_.begin();
  int64_t cur_prefix_end = 0;

  seed_map_t& seed_map = seed_map_;
  //seed_map_t seed_map;
  int64_t rand = 0;
  lrand48_r(&table_rand_seed_, &rand);
  int64_t row_num = range_rand(1, max_rownum_per_mutator_, rand);
  //seed_map.create(row_num);
  std::bitset<OB_MAX_TABLE_NUMBER> table_set;
  for (int64_t i = 0; i < table_num_; i++) {
    table_set.set(i);
  }
  for (int64_t i = 0; i < row_num; i++) {
    lrand48_r(&table_rand_seed_, &rand);
    int64_t table_pos = range_rand(0, table_num_ - 1, rand);

    ObString row_key = rb_array_[table_pos]->get_rowkey4apply(allocer_, &cur_prefix_end);
    if (prefix_end_array_[table_pos] < cur_prefix_end) {
      table_set.reset(table_pos);
      i--;
      if (table_set.any()) {
        continue;
      } else {
        break;
      }
    }
    int64_t cur_seed = 0;
    if (OB_SUCCESS != (ret = get_cur_seed_(seed_map, schema_array[table_pos], row_key, cur_seed, using_id))) {
      break;
    }
    if (OB_SUCCESS != (ret = cb_array_[table_pos]->get_mutator(row_key, schema_array[table_pos], cur_seed, mutator, allocer))) {
      break;
    }
    TEKey te_key;
    te_key.table_id = schema_array[table_pos].get_table_id();
    te_key.row_key = row_key;
    seed_map.set(te_key, cur_seed, 1);

    if (need_update_prefix_end_) {
      ObString table_name;
      table_name.assign_ptr(const_cast<char*>(schema_array[table_pos].get_table_name()), strlen(schema_array[table_pos].get_table_name()));
      ObString rowkey_info_column_name;
      rowkey_info_column_name.assign(ROWKEY_INFO_COLUMN_NAME, strlen(ROWKEY_INFO_COLUMN_NAME));
      ObString rowkey_info_rowkey;
      rowkey_info_rowkey.assign(ROWKEY_INFO_ROWKEY, strlen(ROWKEY_INFO_ROWKEY));
      ObObj rowkey_info_obj;
      rowkey_info_obj.set_int(cur_prefix_end);
      if (OB_SUCCESS != (ret = mutator.update(table_name, rowkey_info_rowkey, rowkey_info_column_name, rowkey_info_obj))) {
        break;
      }
    }
  }
  return ret;
}

int MutatorBuilder::get_cur_seed_(const seed_map_t& seed_map, const ObSchema& schema, const ObString& row_key, int64_t& cur_seed, const bool using_id) {
  int ret = OB_SUCCESS;

  TEKey te_key;
  te_key.table_id = schema.get_table_id();
  te_key.row_key = row_key;

  int hash_ret = 0;
  if (HASH_EXIST != (hash_ret = seed_map.get(te_key, cur_seed))) {
    //ObGetParam get_param;
    //ObScanner scanner;
    //ObCellInfo cell_info;
    //cell_info.row_key_ = row_key;
    //if (using_id)
    //{
    //  cell_info.table_id_ = schema.get_table_id();
    //  cell_info.column_id_ = SEED_COLUMN_ID;
    //}
    //else
    //{
    //  cell_info.table_name_.assign_ptr(const_cast<char*>(schema.get_table_name()), strlen(schema.get_table_name()));
    //  cell_info.column_name_.assign_ptr(SEED_COLUMN_NAME, strlen(SEED_COLUMN_NAME));
    //}
    //get_param.add_cell(cell_info);
    //ObVersionRange version_range;
    //version_range.start_version_ = table_start_version_;
    //version_range.border_flag_.set_max_value();
    //version_range.border_flag_.set_inclusive_start();
    //get_param.set_version_range(version_range);
    //if (OB_SUCCESS == (ret = client_.ups_get(get_param, scanner, TIMEOUT_MS)))
    //{
    //  ret = OB_ERROR;
    //  while (OB_SUCCESS == scanner.next_cell())
    //  {
    //    ObCellInfo *ci = NULL;
    //    if (OB_SUCCESS == (ret = scanner.get_cell(&ci)))
    //    {
    //      ret = ci->value_.get_int(cur_seed);
    //    }
    //  }
    //  if (OB_SUCCESS != ret)
    //  {
    ret = OB_SUCCESS;
    cur_seed = SEED_START;
    TBSYS_LOG(DEBUG, "first seed from cur_seed=%ld row_key=[%.*s]", cur_seed + 1, row_key.length(), row_key.ptr());
    //  }
    //}
  }
  TBSYS_LOG(DEBUG, "get cur_seed=%ld hash_ret=%d row_key=[%.*s]", cur_seed, hash_ret, row_key.length(), row_key.ptr());
  return ret;
}

int64_t MutatorBuilder::get_schema_pos(const uint64_t table_id) const {
  int64_t ret = -1;
  for (int64_t i = 0; i < table_num_; i++) {
    if (schema_mgr_.begin()[i].get_table_id() == table_id) {
      ret = i;
      break;
    }
  }
  return ret;
}

int64_t MutatorBuilder::get_schema_pos(const ObString& table_name) const {
  int64_t ret = -1;
  for (int64_t i = 0; i < table_num_; i++) {
    if (0 == memcmp(schema_mgr_.begin()[i].get_table_name(), table_name.ptr(), table_name.length())) {
      ret = i;
      break;
    }
  }
  return ret;
}


