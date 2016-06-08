/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_read_param_decoder.cc for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_groupby.h"
#include "mergeserver/ob_groupby_operator.h"
#include "mergeserver/ob_read_param_decoder.h"
using namespace sb;
using namespace sb::common;
using namespace sb::mergeserver;
using namespace testing;
using namespace std;

TEST(ob_read_param_decoder, get_param) {
  ObGetParam org_param;
  ObGetParam decoded_param;
  ObSchemaManagerV2* mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini", config));
  ObString str;
  ObString table_name;
  ObString column_name;
  ObStringBuf buffer;
  ObString row_key;
  ObCellInfo cell;
  ObCellInfo id_cell;
  vector<ObCellInfo> id_vec;
  str.assign(const_cast<char*>("rowkey"), strlen("rowkey"));
  EXPECT_EQ(buffer.write_string(str, &row_key), OB_SUCCESS);
  cell.row_key_ = row_key;

  str.assign(const_cast<char*>("collect_info"), strlen("collect_info"));
  EXPECT_EQ(buffer.write_string(str, &table_name), OB_SUCCESS);
  str.assign(const_cast<char*>("info_user_nick"), strlen("info_user_nick"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_EQ(org_param.add_cell(cell), OB_SUCCESS);
  id_cell.table_id_ = 1001;
  id_cell.column_id_ = 2;
  id_vec.push_back(id_cell);

  str.assign(const_cast<char*>("collect_info"), strlen("collect_info"));
  EXPECT_EQ(buffer.write_string(str, &table_name), OB_SUCCESS);
  str.assign(const_cast<char*>("info_is_shared"), strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_EQ(org_param.add_cell(cell), OB_SUCCESS);
  id_cell.table_id_ = 1001;
  id_cell.column_id_ = 3;
  id_vec.push_back(id_cell);

  str.assign(const_cast<char*>("collect_item"), strlen("collect_item"));
  EXPECT_EQ(buffer.write_string(str, &table_name), OB_SUCCESS);
  str.assign(const_cast<char*>("item_picurl"), strlen("item_picurl"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_EQ(org_param.add_cell(cell), OB_SUCCESS);
  id_cell.table_id_ = 1002;
  id_cell.column_id_ = 11;
  id_vec.push_back(id_cell);


  str.assign(const_cast<char*>("collect_info"), strlen("collect_info"));
  EXPECT_EQ(buffer.write_string(str, &table_name), OB_SUCCESS);
  str.assign(const_cast<char*>("info_tag"), strlen("info_tag"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_EQ(org_param.add_cell(cell), OB_SUCCESS);
  id_cell.table_id_ = 1001;
  id_cell.column_id_ = 8;
  id_vec.push_back(id_cell);

  EXPECT_EQ(ob_decode_get_param(org_param, *mgr, decoded_param), OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_cell_size(), static_cast<int64_t>(id_vec.size()));
  for (int64_t i = 0; i < static_cast<int64_t>(id_vec.size()); i++) {
    EXPECT_EQ(decoded_param[i]->table_id_, id_vec[i].table_id_);
    EXPECT_EQ(decoded_param[i]->column_id_, id_vec[i].column_id_);
  }
}


TEST(ob_read_param_decoder, scan_param_without_group) {
  ObScanParam org_param;
  ObScanParam decoded_param;
  ObSchemaManagerV2* mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini", config));
  ObString str;
  ObString table_name;
  ObString column_name;
  ObStringBuf buffer;
  ObString row_key;
  ObCellInfo cell;
  ObRange range;
  vector<uint64_t> id_vec;

  str.assign(const_cast<char*>("collect_info"), strlen("collect_info"));
  EXPECT_EQ(buffer.write_string(str, &table_name), OB_SUCCESS);
  range.border_flag_.set_min_value();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_min_value();
  range.border_flag_.unset_inclusive_start();
  org_param.set(OB_INVALID_ID, table_name, range);

  /// add columns
  str.assign(const_cast<char*>("info_user_nick"), strlen("info_user_nick"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(2);


  str.assign(const_cast<char*>("item_title"), strlen("item_title"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(10);

  str.assign(const_cast<char*>("info_is_shared"), strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(3);

  str.assign(const_cast<char*>("item_picurl"), strlen("item_picurl"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(11);

  str.assign(const_cast<char*>("item_price"), strlen("item_price"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(14);

  /// add filter
  ObObj obj;
  std::vector<int64_t> cond_ids;

  str.assign(const_cast<char*>("info_is_shared"), strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(org_param.add_cond(column_name, LT, obj), OB_SUCCESS);
  cond_ids.push_back(2);

  str.assign(const_cast<char*>("item_price"), strlen("item_price"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(org_param.add_cond(column_name, GT, obj), OB_SUCCESS);
  cond_ids.push_back(4);

  /// add order by
  vector<int64_t> order_idx;
  str.assign(const_cast<char*>("info_is_shared"), strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name, ObScanParam::ASC), OB_SUCCESS);
  order_idx.push_back(2);

  str.assign(const_cast<char*>("item_price"), strlen("item_price"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name, ObScanParam::ASC), OB_SUCCESS);
  order_idx.push_back(4);

  /// add limit
  const int64_t org_limit_offset = 10;
  const int64_t org_limit_count = 15;
  EXPECT_EQ(org_param.set_limit_info(org_limit_offset, org_limit_count), OB_SUCCESS);

  /// decode
  EXPECT_EQ(ob_decode_scan_param(org_param, *mgr, decoded_param), OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_table_id(), 1001ull);

  EXPECT_EQ(decoded_param.get_column_id_size(), static_cast<int64_t>(id_vec.size()));
  for (uint32_t i = 0; i < id_vec.size(); i++) {
    EXPECT_EQ(decoded_param.get_column_id()[i], id_vec[i]);
  }

  EXPECT_EQ(decoded_param.get_filter_info().get_count(), static_cast<int64_t>(cond_ids.size()));
  for (uint32_t i = 0; i < cond_ids.size(); i++) {
    EXPECT_EQ(cond_ids[i], decoded_param.get_filter_info()[i]->get_column_index());
  }

  int64_t orderby_size = -1;
  int64_t const* order_columns = NULL;
  uint8_t const* orders = NULL;
  decoded_param.get_orderby_column(order_columns, orders, orderby_size);
  EXPECT_EQ(orderby_size, static_cast<int64_t>(order_idx.size()));
  for (uint32_t i = 0; i < order_idx.size(); i ++) {
    EXPECT_EQ(order_idx[i], order_columns[i]);
  }

  int64_t limit_offset = -1;
  int64_t limit_count = -1;
  decoded_param.get_limit_info(limit_offset, limit_count);
  EXPECT_EQ(org_limit_count, limit_count);
  EXPECT_EQ(org_limit_offset, limit_offset);
}

TEST(ob_read_param_decoder, scan_param_with_group) {
  ObScanParam org_param;
  ObScanParam decoded_param;
  ObSchemaManagerV2* mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini", config));
  ObString str;
  ObString table_name;
  ObString column_name;
  ObStringBuf buffer;
  ObString row_key;
  ObCellInfo cell;
  ObRange range;
  vector<uint64_t> id_vec;

  str.assign(const_cast<char*>("collect_info"), strlen("collect_info"));
  EXPECT_EQ(buffer.write_string(str, &table_name), OB_SUCCESS);
  range.border_flag_.set_min_value();
  range.border_flag_.unset_inclusive_start();
  org_param.set(OB_INVALID_ID, table_name, range);

  /// add columns
  str.assign(const_cast<char*>("info_user_nick"), strlen("info_user_nick"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(2);

  str.assign(const_cast<char*>("item_title"), strlen("item_title"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(10);

  str.assign(const_cast<char*>("info_is_shared"), strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(3);

  str.assign(const_cast<char*>("item_picurl"), strlen("item_picurl"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(11);

  str.assign(const_cast<char*>("item_price"), strlen("item_price"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  id_vec.push_back(14);

  /// add filter
  ObObj obj;
  std::vector<int64_t> cond_ids;

  str.assign(const_cast<char*>("info_is_shared"), strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(org_param.add_cond(column_name, LT, obj), OB_SUCCESS);
  cond_ids.push_back(2);

  str.assign(const_cast<char*>("item_price"), strlen("item_price"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(org_param.add_cond(column_name, GT, obj), OB_SUCCESS);
  cond_ids.push_back(4);

  /// add group by
  vector<int64_t> groupby_idx;
  str.assign(const_cast<char*>("info_user_nick"), strlen("info_user_nick"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_groupby_column(column_name), OB_SUCCESS);
  groupby_idx.push_back(0);

  vector<int64_t> return_idx;
  str.assign(const_cast<char*>("info_is_shared"), strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_return_column(column_name), OB_SUCCESS);
  return_idx.push_back(2);

  vector<int64_t> agg_idx;
  ObString as_column_name;
  str.assign(const_cast<char*>("item_price"), strlen("item_price"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  str.assign(const_cast<char*>("sum_item_price"), strlen("sum_item_price"));
  EXPECT_EQ(buffer.write_string(str, &as_column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_aggregate_column(column_name, as_column_name, SUM), OB_SUCCESS);
  agg_idx.push_back(4);

  /// add order by
  vector<int64_t> order_idx;
  str.assign(const_cast<char*>("info_is_shared"), strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name, ObScanParam::ASC), OB_SUCCESS);
  order_idx.push_back(1);

  str.assign(const_cast<char*>("sum_item_price"), strlen("sum_item_price"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name, ObScanParam::ASC), OB_SUCCESS);
  order_idx.push_back(2);

  /// add limit
  const int64_t org_limit_offset = 10;
  const int64_t org_limit_count = 15;
  EXPECT_EQ(org_param.set_limit_info(org_limit_offset, org_limit_count), OB_SUCCESS);

  /// decode
  EXPECT_EQ(ob_decode_scan_param(org_param, *mgr, decoded_param), OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_table_id(), 1001ull);

  /// basic columns
  EXPECT_EQ(decoded_param.get_column_id_size(), static_cast<int64_t>(id_vec.size()));
  for (uint32_t i = 0; i < id_vec.size(); i++) {
    EXPECT_EQ(decoded_param.get_column_id()[i], id_vec[i]);
  }

  /// filter
  EXPECT_EQ(decoded_param.get_filter_info().get_count(), static_cast<int64_t>(cond_ids.size()));
  for (uint32_t i = 0; i < cond_ids.size(); i++) {
    EXPECT_EQ(cond_ids[i], decoded_param.get_filter_info()[i]->get_column_index());
  }

  /// group by
  EXPECT_EQ(decoded_param.get_group_by_param().get_aggregate_row_width(),
            static_cast<int64_t>(groupby_idx.size() + return_idx.size() + agg_idx.size()));
  EXPECT_EQ(decoded_param.get_group_by_param().get_groupby_columns().get_array_index(),
            static_cast<int64_t>(groupby_idx.size()));
  for (uint32_t i = 0; i < groupby_idx.size(); i++) {
    EXPECT_EQ(groupby_idx[i], decoded_param.get_group_by_param().get_groupby_columns().at(i)->org_column_idx_);
  }

  EXPECT_EQ(decoded_param.get_group_by_param().get_return_columns().get_array_index(),
            static_cast<int64_t>(return_idx.size()));
  for (uint32_t i = 0; i < return_idx.size(); i++) {
    EXPECT_EQ(return_idx[i], decoded_param.get_group_by_param().get_return_columns().at(i)->org_column_idx_);
  }

  EXPECT_EQ(decoded_param.get_group_by_param().get_aggregate_columns().get_array_index(),
            static_cast<int64_t>(agg_idx.size()));
  for (uint32_t i = 0; i < agg_idx.size(); i++) {
    EXPECT_EQ(agg_idx[i], decoded_param.get_group_by_param().get_aggregate_columns().at(i)->get_org_column_idx());
  }


  /// orders
  int64_t orderby_size = -1;
  int64_t const* order_columns = NULL;
  uint8_t const* orders = NULL;
  decoded_param.get_orderby_column(order_columns, orders, orderby_size);
  EXPECT_EQ(orderby_size, static_cast<int64_t>(order_idx.size()));
  for (uint32_t i = 0; i < order_idx.size(); i ++) {
    EXPECT_EQ(order_idx[i], order_columns[i]);
  }

  /// limit
  int64_t limit_offset = -1;
  int64_t limit_count = -1;
  decoded_param.get_limit_info(limit_offset, limit_count);
  EXPECT_EQ(org_limit_count, limit_count);
  EXPECT_EQ(org_limit_offset, limit_offset);

  /// orderby not exist column
  /// add columns
  str.assign(const_cast<char*>("wrong_column"), strlen("wrong_column"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name), OB_SUCCESS);

  str.assign(const_cast<char*>("info_user_nick"), strlen("info_user_nick"));
  EXPECT_EQ(buffer.write_string(str, &column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name), OB_SUCCESS);
  EXPECT_NE(ob_decode_scan_param(org_param, *mgr, decoded_param), OB_SUCCESS);
}

TEST(ob_read_param_decoder, scan_param_select_all) {
  ObScanParam org_param;
  ObScanParam decoded_param;
  ObSchemaManagerV2* mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini", config));
  ObString str;
  ObString table_name;
  ObString column_name;
  ObStringBuf buffer;
  ObString row_key;
  ObCellInfo cell;
  ObRange range;
  vector<uint64_t> id_vec;

  str.assign(const_cast<char*>("collect_info"), strlen("collect_info"));
  EXPECT_EQ(buffer.write_string(str, &table_name), OB_SUCCESS);
  range.border_flag_.set_min_value();
  range.border_flag_.unset_inclusive_start();
  org_param.set(OB_INVALID_ID, table_name, range);

  EXPECT_EQ(ob_decode_scan_param(org_param, *mgr, decoded_param), OB_SUCCESS);

  uint64_t table_id = OB_INVALID_ID;
  const ObTableSchema* table_schema = NULL;
  table_schema = mgr->get_table_schema(table_name);
  EXPECT_NE(table_schema, reinterpret_cast<void*>(0));
  EXPECT_EQ(table_schema->get_table_id(), 1001ull);
  table_id = table_schema->get_table_id();
  table_schema = mgr->get_table_schema(table_id);
  EXPECT_NE(table_schema, reinterpret_cast<void*>(0));
  const ObColumnSchemaV2* column_info = NULL;
  int32_t column_size = 0;
  column_info = mgr->get_table_schema(table_schema->get_table_id(), column_size);
  EXPECT_NE(column_info, reinterpret_cast<void*>(0));
  EXPECT_GE(column_size, 0);
  for (int32_t i = 0; i < column_size; i++) {
    EXPECT_EQ(column_info[i].get_id(), decoded_param.get_column_id()[i]);
  }
}

int main(int argc, char** argv) {
  srandom(time(NULL));
  ob_init_memory_pool(64 * 1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


