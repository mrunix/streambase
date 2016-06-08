/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_memtable.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <list>
#include <fstream>
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "updateserver/ob_memtable.h"
#include "updateserver/ob_update_server_main.h"
#include "test_utils.h"
#include "gtest/gtest.h"

#define _US_ if (true) {
#define _UE_ }

using namespace sb;
using namespace updateserver;
using namespace common;
using namespace common::hash;

TEST(TestMemTable, init_and_destroy) {
  MemTable mt;

  EXPECT_EQ(OB_SUCCESS, mt.init(1024, 128));
  EXPECT_EQ(OB_ERROR, mt.init());

  mt.clear();
  EXPECT_EQ(OB_ERROR, mt.init());

  mt.destroy();
  EXPECT_EQ(OB_SUCCESS, mt.init(1024, 128));

  mt.destroy();
}

TEST(TestMemTable, set) {
  MemTable mt;
  PageArena<char> allocer;
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos("test_cases/test_mt_set.ci.ini", "MT_SET_CI", allocer, mutator, result);
  ObScanParam scan_param;
  read_scan_param("test_cases/test_mt_set.scan.ini", "MT_SET_SCAN", allocer, scan_param);
  ups_mutator.set_mutate_timestamp(tbsys::CTimeUtil::getTime());

  EXPECT_EQ(OB_ERROR, mt.set(ups_mutator, false));

  mt.init(1024, 128);
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  ObUpsTableMgr& tm = main->get_update_server().get_table_mgr();
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;
  schema_mgr.parse_from_file("test_cases/test_mt_schema.ini", config);
  tm.set_schemas(schema_mgr);

#ifdef _BTREE_ENGINE_
  EXPECT_EQ(OB_SUCCESS, mt.set(ups_mutator, false));
#else
  EXPECT_EQ(OB_ERROR, mt.set(ups_mutator, false));

  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  ObUpsTableMgr& tm = main->get_update_server().get_table_mgr();
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;
  schema_mgr.parse_from_file("test_cases/test_mt_schema.ini", config);
  tm.set_schemas(schema_mgr);

  EXPECT_EQ(OB_SUCCESS, mt.set(ups_mutator, false));
#endif

  MemTableTransHandle handle;
  MemTableIterator iter;
  mt.start_transaction(READ_TRANSACTION, handle);
  mt.scan(handle, *(scan_param.get_range()), false, iter);
  while (OB_SUCCESS == result.next_cell()) {
    EXPECT_EQ(OB_SUCCESS, iter.next_cell());
    ObMutatorCellInfo* ci_orig = NULL;
    ObCellInfo* ci_scan = NULL;
    result.get_cell(&ci_orig);
    EXPECT_EQ(OB_SUCCESS, iter.get_cell(&ci_scan));
    EXPECT_EQ(true, ci_orig->cell_info == *ci_scan);
  }
  mt.end_transaction(handle);

#ifndef _BTREE_ENGINE_
  // btree不可以在read handle析构之前destroy
  mt.destroy();
#endif
}

TEST(TestMemTable, trans_set) {
  MemTable mt;
  PageArena<char> allocer;
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos("test_cases/test_mt_set.ci.ini", "MT_SET_CI", allocer, mutator, result);
  ObScanParam scan_param;
  read_scan_param("test_cases/test_mt_set.scan.ini", "MT_SET_SCAN", allocer, scan_param);
  ups_mutator.set_mutate_timestamp(tbsys::CTimeUtil::getTime());

  MemTableTransHandle handle;
  EXPECT_EQ(OB_ERROR, mt.set(handle, ups_mutator));

  mt.init(1024, 128);
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  ObUpsTableMgr& tm = main->get_update_server().get_table_mgr();
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;
  schema_mgr.parse_from_file("test_cases/test_mt_schema.ini", config);
  tm.set_schemas(schema_mgr);

  EXPECT_EQ(OB_ERROR, mt.set(handle, ups_mutator));

  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(WRITE_TRANSACTION, handle));
  EXPECT_NE(OB_SUCCESS, mt.start_transaction(WRITE_TRANSACTION, handle));
  _US_
  // case begin
  // 对于hash engine，测试在已经有rowkey A 的情况下，开一个事务按A B A的顺序写入，在结束事务的时候是否正常
  // 避免出现结束事务时引用的rowkey是mutator临时申请的
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos("test_cases/test_mt_set_unsorted.ci.ini", "MT_SET_UNSORTED_CI", allocer, mutator, result);
  ups_mutator.set_mutate_timestamp(tbsys::CTimeUtil::getTime());
  EXPECT_EQ(OB_SUCCESS, mt.set(handle, ups_mutator));
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(WRITE_TRANSACTION, handle));
  EXPECT_EQ(OB_SUCCESS, mt.set(handle, ups_mutator));
  // 此处模拟mutator被析构或重置
  while (OB_SUCCESS == mutator.next_cell()) {
    ObMutatorCellInfo* mutator_ci = NULL;
    if (OB_SUCCESS == mutator.get_cell(&mutator_ci)) {
      ObString rk = mutator_ci->cell_info.row_key_;
      memset(rk.ptr(), 0, rk.length());
    }
  }
  mutator.reset_iter();
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));

  mutator.reset();
  result.reset();
  read_cell_infos("test_cases/test_mt_ret_unsorted.ci.ini", "MT_RET_UNSORTED_CI", allocer, mutator, result);
  MemTableTransHandle handle;
  MemTableIterator iter;
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(READ_TRANSACTION, handle));
  EXPECT_EQ(OB_SUCCESS, mt.scan(handle, *(scan_param.get_range()), false, iter));
  while (OB_SUCCESS == result.next_cell()) {
    EXPECT_EQ(OB_SUCCESS, iter.next_cell());
    ObMutatorCellInfo* ci_orig = NULL;
    ObCellInfo* ci_scan = NULL;
    result.get_cell(&ci_orig);
    EXPECT_EQ(OB_SUCCESS, iter.get_cell(&ci_scan));
    EXPECT_EQ(true, ci_orig->cell_info == *ci_scan);
  }
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));
  mt.clear();
  _UE_
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(WRITE_TRANSACTION, handle));
  // case end
  EXPECT_EQ(OB_SUCCESS, mt.set(handle, ups_mutator));

  // 事务没有提交，scan结果应该为空
  _US_
  MemTableTransHandle handle;
  MemTableIterator iter;
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(READ_TRANSACTION, handle));
  _US_
  MemTableTransHandle handle;
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(READ_TRANSACTION, handle));
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));
  _UE_
  EXPECT_EQ(OB_SUCCESS, mt.scan(handle, *(scan_param.get_range()), false, iter));
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));
  EXPECT_EQ(OB_ITER_END, iter.next_cell());
  _UE_

  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));
  // memtable 陷阱: 注意为了性能和内存使用考虑，rowkey内存都是外部提供的
  ObUpsMutator ups_mutator2;
  ObMutator& mutator2 = ups_mutator2.get_mutator();
  ObMutator result2;
  read_cell_infos("test_cases/test_mt_trans_set.ci.ini", "MT_TRANS_SET_CI", allocer, mutator2, result2);
  ups_mutator2.set_mutate_timestamp(tbsys::CTimeUtil::getTime());
  EXPECT_EQ(OB_ERROR, mt.set(handle, ups_mutator2));
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(WRITE_TRANSACTION, handle));
  EXPECT_EQ(OB_SUCCESS, mt.set(handle, ups_mutator2));

  // 只能scan到第一个事务的结果
  _US_
  MemTableTransHandle handle;
  MemTableIterator iter;
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(READ_TRANSACTION, handle));
  EXPECT_EQ(OB_SUCCESS, mt.scan(handle, *(scan_param.get_range()), false, iter));
  while (OB_SUCCESS == result.next_cell()) {
    EXPECT_EQ(OB_SUCCESS, iter.next_cell());
    ObMutatorCellInfo* ci_orig = NULL;
    ObCellInfo* ci_scan = NULL;
    result.get_cell(&ci_orig);
    EXPECT_EQ(OB_SUCCESS, iter.get_cell(&ci_scan));
    EXPECT_EQ(true, ci_orig->cell_info == *ci_scan);
  }
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));
  _UE_

  // 第二个事务提交后可以看到全部结果
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));
  _US_
  MemTableTransHandle handle;
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos("test_cases/test_mt_trans_set.ret.ini", "MT_TRANS_SET_RET", allocer, mutator, result);
  MemTableIterator iter;
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(READ_TRANSACTION, handle));
  EXPECT_EQ(OB_SUCCESS, mt.scan(handle, *(scan_param.get_range()), false, iter));
  while (OB_SUCCESS == result.next_cell()) {
    EXPECT_EQ(OB_SUCCESS, iter.next_cell());
    ObMutatorCellInfo* ci_orig = NULL;
    ObCellInfo* ci_scan = NULL;
    result.get_cell(&ci_orig);
    EXPECT_EQ(OB_SUCCESS, iter.get_cell(&ci_scan));
    EXPECT_EQ(true, ci_orig->cell_info == *ci_scan);
  }
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));
  _UE_

  // 第三个事务被会滚 看到的结果与第二个一样
  ObUpsMutator ups_mutator3;
  ObUpsMutator ups_result3;
  ObMutator& mutator3 = ups_mutator3.get_mutator();
  ObMutator& result3 = ups_result3.get_mutator();
  read_cell_infos("test_cases/test_mt_trans_set.ci.ini", "MT_TRANS_SET_CI", allocer, mutator3, result3);
  ups_mutator3.set_mutate_timestamp(tbsys::CTimeUtil::getTime());
  EXPECT_EQ(OB_ERROR, mt.set(handle, ups_mutator3));
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(WRITE_TRANSACTION, handle));
  EXPECT_EQ(OB_SUCCESS, mt.set(handle, ups_mutator3));
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle, true));
  _US_
  MemTableTransHandle handle;
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos("test_cases/test_mt_trans_set.ret.ini", "MT_TRANS_SET_RET", allocer, mutator, result);
  MemTableIterator iter;
  EXPECT_EQ(OB_SUCCESS, mt.start_transaction(READ_TRANSACTION, handle));
  EXPECT_EQ(OB_SUCCESS, mt.scan(handle, *(scan_param.get_range()), false, iter));
  while (OB_SUCCESS == result.next_cell()) {
    EXPECT_EQ(OB_SUCCESS, iter.next_cell());
    ObMutatorCellInfo* ci_orig = NULL;
    ObCellInfo* ci_scan = NULL;
    result.get_cell(&ci_orig);
    EXPECT_EQ(OB_SUCCESS, iter.get_cell(&ci_scan));
    EXPECT_EQ(true, ci_orig->cell_info == *ci_scan);
  }
  EXPECT_EQ(OB_SUCCESS, mt.end_transaction(handle));
  _UE_

#ifndef _BTREE_ENGINE_
  // btree不可以在read handle析构之前destroy
  mt.destroy();
#endif
}

void test_normal_scan(MemTable& mt, const char* scan_fname, const char* scan_section,
                      const char* ret_fname, const char* ret_section) {
  PageArena<char> allocer;
  MemTableTransHandle handle;
  mt.start_transaction(READ_TRANSACTION, handle);
  ObScanParam scan_param;
  read_scan_param(scan_fname, scan_section, allocer, scan_param);
  MemTableIterator iter;
  EXPECT_EQ(OB_SUCCESS, mt.scan(handle, *(scan_param.get_range()), false, iter));
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos(ret_fname, ret_section, allocer, mutator, result);
  while (OB_SUCCESS == result.next_cell()) {
    EXPECT_EQ(OB_SUCCESS, iter.next_cell());
    ObMutatorCellInfo* ci_orig = NULL;
    ObCellInfo* ci_scan = NULL;
    result.get_cell(&ci_orig);
    EXPECT_EQ(OB_SUCCESS, iter.get_cell(&ci_scan));
    EXPECT_EQ(true, ci_orig->cell_info == *ci_scan);
  }
  EXPECT_EQ(OB_ITER_END, iter.next_cell());
  EXPECT_EQ(OB_ITER_END, result.next_cell());
  mt.end_transaction(handle);
}

void test_abnormal_scan_error(MemTable& mt, const char* scan_fname, const char* scan_section) {
  PageArena<char> allocer;
  MemTableTransHandle handle;
  mt.start_transaction(READ_TRANSACTION, handle);
  ObScanParam scan_param;
  read_scan_param(scan_fname, scan_section, allocer, scan_param);
  MemTableIterator iter;
  EXPECT_NE(OB_SUCCESS, mt.scan(handle, *(scan_param.get_range()), false, iter));
  EXPECT_EQ(OB_ITER_END, iter.next_cell());
  mt.end_transaction(handle);
}

void test_abnormal_scan_empty(MemTable& mt, const char* scan_fname, const char* scan_section) {
  PageArena<char> allocer;
  MemTableTransHandle handle;
  mt.start_transaction(READ_TRANSACTION, handle);
  ObScanParam scan_param;
  read_scan_param(scan_fname, scan_section, allocer, scan_param);
  MemTableIterator iter;
  EXPECT_EQ(OB_SUCCESS, mt.scan(handle, *(scan_param.get_range()), false, iter));
  EXPECT_EQ(OB_ITER_END, iter.next_cell());
  mt.end_transaction(handle);
}

TEST(TestMemTable, scan) {
  MemTable mt;
  PageArena<char> allocer;
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos("test_cases/test_mt_scan.ci.ini", "MT_SCAN_CI", allocer, mutator, result);
  ups_mutator.set_mutate_timestamp(tbsys::CTimeUtil::getTime());

  mt.init(1024, 128);
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  ObUpsTableMgr& tm = main->get_update_server().get_table_mgr();
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;
  schema_mgr.parse_from_file("test_cases/test_mt_schema.ini", config);
  tm.set_schemas(schema_mgr);

  EXPECT_EQ(OB_SUCCESS, mt.set(ups_mutator, false));

  // 有效数据范围前后闭区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1.ini", "MT_SCAN_SCAN1",
                   "test_cases/test_mt_scan.ret1.ini", "MT_SCAN_RET1");
  // 有效数据范围内闭区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_1.ini", "MT_SCAN_SCAN1_1",
                   "test_cases/test_mt_scan.ret1_1.ini", "MT_SCAN_RET1_1");
  // 有效数据范围内开区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_2.ini", "MT_SCAN_SCAN1_2",
                   "test_cases/test_mt_scan.ret1_2.ini", "MT_SCAN_RET1_2");
  // 单一key闭区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_3.ini", "MT_SCAN_SCAN1_3",
                   "test_cases/test_mt_scan.ret1_3.ini", "MT_SCAN_RET1_3");
  // 查询end=数据start 查询start在有效数据范围外
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_4.ini", "MT_SCAN_SCAN1_4",
                   "test_cases/test_mt_scan.ret1_4.ini", "MT_SCAN_RET1_4");
  // 查询start=数据end 查询end在有效数据范围外
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_5.ini", "MT_SCAN_SCAN1_5",
                   "test_cases/test_mt_scan.ret1_5.ini", "MT_SCAN_RET1_5");
  // 有效数据范围前后开区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_6.ini", "MT_SCAN_SCAN1_6",
                   "test_cases/test_mt_scan.ret1.ini", "MT_SCAN_RET1");
  // tableid不同
  test_normal_scan(mt, "test_cases/test_mt_scan.scan2.ini", "MT_SCAN_SCAN2",
                   "test_cases/test_mt_scan.ret2.ini", "MT_SCAN_RET2");
  // key prefix不同
  test_normal_scan(mt, "test_cases/test_mt_scan.scan3.ini", "MT_SCAN_SCAN3",
                   "test_cases/test_mt_scan.ret3.ini", "MT_SCAN_RET3");

  // 区间合法 但是开区间中没有数据
  test_abnormal_scan_empty(mt, "test_cases/test_mt_scan.scan1_ab1.ini", "MT_SCAN_SCAN1_AB1");
  // 没有这个区间
  test_abnormal_scan_empty(mt, "test_cases/test_mt_scan.scan1_ab2.ini", "MT_SCAN_SCAN1_AB2");
#ifndef _BTREE_ENGINE_
  // end > start不合法
  test_abnormal_scan_error(mt, "test_cases/test_mt_scan.scan1_ab3.ini", "MT_SCAN_SCAN1_AB3");
  // 前缀不相等
  test_abnormal_scan_error(mt, "test_cases/test_mt_scan.scan1_ab4.ini", "MT_SCAN_SCAN1_AB4");
  // 使用min和max
  test_abnormal_scan_error(mt, "test_cases/test_mt_scan.scan1_ab5.ini", "MT_SCAN_SCAN1_AB5");
#endif
  // 查找区间在有效区间前面
  test_abnormal_scan_empty(mt, "test_cases/test_mt_scan.scan1_ab6.ini", "MT_SCAN_SCAN1_AB6");
  // 查找区间在有效区间后面
  test_abnormal_scan_empty(mt, "test_cases/test_mt_scan.scan1_ab7.ini", "MT_SCAN_SCAN1_AB7");

  mt.destroy();
}

TEST(TestMemTable, create_index) {
  MemTable mt;
  PageArena<char> allocer;
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos("test_cases/test_mt_scan.ci.ini", "MT_SCAN_CI", allocer, mutator, result);
  ups_mutator.set_mutate_timestamp(tbsys::CTimeUtil::getTime());

  mt.init(1024, 128);
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  ObUpsTableMgr& tm = main->get_update_server().get_table_mgr();
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;
  schema_mgr.parse_from_file("test_cases/test_mt_schema.ini", config);
  tm.set_schemas(schema_mgr);

  EXPECT_EQ(OB_SUCCESS, mt.set(ups_mutator, false));

  EXPECT_EQ(OB_SUCCESS, mt.create_index());

  // 有效数据范围前后闭区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1.ini", "MT_SCAN_SCAN1",
                   "test_cases/test_mt_scan.ret1.ini", "MT_SCAN_RET1");
  // 有效数据范围内闭区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_1.ini", "MT_SCAN_SCAN1_1",
                   "test_cases/test_mt_scan.ret1_1.ini", "MT_SCAN_RET1_1");
  // 有效数据范围内开区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_2.ini", "MT_SCAN_SCAN1_2",
                   "test_cases/test_mt_scan.ret1_2.ini", "MT_SCAN_RET1_2");
  // 单一key闭区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_3.ini", "MT_SCAN_SCAN1_3",
                   "test_cases/test_mt_scan.ret1_3.ini", "MT_SCAN_RET1_3");
  // 查询end=数据start 查询start在有效数据范围外
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_4.ini", "MT_SCAN_SCAN1_4",
                   "test_cases/test_mt_scan_index.ret1_4.ini", "MT_SCAN_RET1_4");
  // 查询start=数据end 查询end在有效数据范围外
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_5.ini", "MT_SCAN_SCAN1_5",
                   "test_cases/test_mt_scan_index.ret1_5.ini", "MT_SCAN_RET1_5");
  // 有效数据范围前后开区间
  test_normal_scan(mt, "test_cases/test_mt_scan.scan1_6.ini", "MT_SCAN_SCAN1_6",
                   "test_cases/test_mt_scan.ret1.ini", "MT_SCAN_RET1");
  // tableid不同
  test_normal_scan(mt, "test_cases/test_mt_scan.scan2.ini", "MT_SCAN_SCAN2",
                   "test_cases/test_mt_scan.ret2.ini", "MT_SCAN_RET2");
  // key prefix不同
  test_normal_scan(mt, "test_cases/test_mt_scan.scan3.ini", "MT_SCAN_SCAN3",
                   "test_cases/test_mt_scan.ret3.ini", "MT_SCAN_RET3");

  // 区间合法 但是开区间中没有数据
  test_abnormal_scan_empty(mt, "test_cases/test_mt_scan.scan1_ab1.ini", "MT_SCAN_SCAN1_AB1");
#ifndef _BTREE_ENGINE_
  // end > start不合法
  test_abnormal_scan_error(mt, "test_cases/test_mt_scan.scan1_ab3.ini", "MT_SCAN_SCAN1_AB3");
#endif
  // 查找区间在有效区间前面
  test_abnormal_scan_empty(mt, "test_cases/test_mt_scan.scan1_ab6.ini", "MT_SCAN_SCAN1_AB6");
  // 查找区间在有效区间后面
  test_abnormal_scan_empty(mt, "test_cases/test_mt_scan_index.scan1_ab1.ini", "MT_SCAN_INDEX_SCAN1_AB1");

  mt.destroy();
}

TEST(TestMemTable, min_max_key_1row) {
  MemTable mt;
  PageArena<char> allocer;
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos("test_cases/test_mt_sorted_1row_scan.ci.ini", "MT_SORTED_1ROW_SCAN_CI", allocer, mutator, result);
  ups_mutator.set_mutate_timestamp(tbsys::CTimeUtil::getTime());

  mt.init(1024, 128);
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  ObUpsTableMgr& tm = main->get_update_server().get_table_mgr();
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;
  schema_mgr.parse_from_file("test_cases/test_mt_schema.ini", config);
  tm.set_schemas(schema_mgr);

  EXPECT_EQ(OB_SUCCESS, mt.set(ups_mutator, false));

  EXPECT_EQ(OB_SUCCESS, mt.create_index());

  // (MIN_KEY, MAX_KEY)
  test_normal_scan(mt, "test_cases/test_mt_sorted_1row_scan.scan1.ini", "MT_SORTED_1ROW_SCAN_SCAN1",
                   "test_cases/test_mt_sorted_1row_scan.ret1.ini", "MT_SORTED_1ROW_SCAN_RET1");
  // (MIN_KEY, MAX_KEY]
  test_normal_scan(mt, "test_cases/test_mt_sorted_1row_scan.scan2.ini", "MT_SORTED_1ROW_SCAN_SCAN2",
                   "test_cases/test_mt_sorted_1row_scan.ret1.ini", "MT_SORTED_1ROW_SCAN_RET1");
  // [MIN_KEY, MAX_KEY)
  test_normal_scan(mt, "test_cases/test_mt_sorted_1row_scan.scan3.ini", "MT_SORTED_1ROW_SCAN_SCAN3",
                   "test_cases/test_mt_sorted_1row_scan.ret1.ini", "MT_SORTED_1ROW_SCAN_RET1");
  // [MIN_KEY, MAX_KEY]
  test_normal_scan(mt, "test_cases/test_mt_sorted_1row_scan.scan4.ini", "MT_SORTED_1ROW_SCAN_SCAN4",
                   "test_cases/test_mt_sorted_1row_scan.ret1.ini", "MT_SORTED_1ROW_SCAN_RET1");

  // (MIN_KEY, rowkey)
  test_abnormal_scan_empty(mt, "test_cases/test_mt_sorted_1row_scan.scan5.ini", "MT_SORTED_1ROW_SCAN_SCAN5");
  // (MIN_KEY, rowkey]
  test_normal_scan(mt, "test_cases/test_mt_sorted_1row_scan.scan6.ini", "MT_SORTED_1ROW_SCAN_SCAN6",
                   "test_cases/test_mt_sorted_1row_scan.ret1.ini", "MT_SORTED_1ROW_SCAN_RET1");
  // [MIN_KEY, rowkey)
  test_abnormal_scan_empty(mt, "test_cases/test_mt_sorted_1row_scan.scan7.ini", "MT_SORTED_1ROW_SCAN_SCAN7");
  // [MIN_KEY, rowkwy]
  test_normal_scan(mt, "test_cases/test_mt_sorted_1row_scan.scan8.ini", "MT_SORTED_1ROW_SCAN_SCAN8",
                   "test_cases/test_mt_sorted_1row_scan.ret1.ini", "MT_SORTED_1ROW_SCAN_RET1");

  // (rowkey, MAX_KEY)
  test_abnormal_scan_empty(mt, "test_cases/test_mt_sorted_1row_scan.scan9.ini", "MT_SORTED_1ROW_SCAN_SCAN9");
  // (rowkey, MAX_KEY]
  test_abnormal_scan_empty(mt, "test_cases/test_mt_sorted_1row_scan.scan10.ini", "MT_SORTED_1ROW_SCAN_SCAN10");
  // [rowkey, MAX_KEY)
  test_normal_scan(mt, "test_cases/test_mt_sorted_1row_scan.scan11.ini", "MT_SORTED_1ROW_SCAN_SCAN11",
                   "test_cases/test_mt_sorted_1row_scan.ret1.ini", "MT_SORTED_1ROW_SCAN_RET1");
  // [rowkey, MAX_KEY]
  test_normal_scan(mt, "test_cases/test_mt_sorted_1row_scan.scan12.ini", "MT_SORTED_1ROW_SCAN_SCAN12",
                   "test_cases/test_mt_sorted_1row_scan.ret1.ini", "MT_SORTED_1ROW_SCAN_RET1");

  mt.destroy();
}

TEST(TestMemTable, min_max_key_2row) {
  MemTable mt;
  PageArena<char> allocer;
  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator result;
  read_cell_infos("test_cases/test_mt_sorted_2row_scan.ci.ini", "MT_SORTED_2ROW_SCAN_CI", allocer, mutator, result);
  ups_mutator.set_mutate_timestamp(tbsys::CTimeUtil::getTime());

  mt.init(1024, 128);
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  ObUpsTableMgr& tm = main->get_update_server().get_table_mgr();
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;
  schema_mgr.parse_from_file("test_cases/test_mt_schema.ini", config);
  tm.set_schemas(schema_mgr);

  EXPECT_EQ(OB_SUCCESS, mt.set(ups_mutator, false));

  EXPECT_EQ(OB_SUCCESS, mt.create_index());

  // (MIN_KEY, MAX_KEY)
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan1.ini", "MT_SORTED_2ROW_SCAN_SCAN1",
                   "test_cases/test_mt_sorted_2row_scan.ret2.ini", "MT_SORTED_2ROW_SCAN_RET2");
  // (MIN_KEY, MAX_KEY]
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan2.ini", "MT_SORTED_2ROW_SCAN_SCAN2",
                   "test_cases/test_mt_sorted_2row_scan.ret2.ini", "MT_SORTED_2ROW_SCAN_RET2");
  // [MIN_KEY, MAX_KEY)
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan3.ini", "MT_SORTED_2ROW_SCAN_SCAN3",
                   "test_cases/test_mt_sorted_2row_scan.ret2.ini", "MT_SORTED_2ROW_SCAN_RET2");
  // [MIN_KEY, MAX_KEY]
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan4.ini", "MT_SORTED_2ROW_SCAN_SCAN4",
                   "test_cases/test_mt_sorted_2row_scan.ret2.ini", "MT_SORTED_2ROW_SCAN_RET2");

  // (MIN_KEY, rowkey)
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan5.ini", "MT_SORTED_2ROW_SCAN_SCAN5",
                   "test_cases/test_mt_sorted_2row_scan.ret1_1.ini", "MT_SORTED_2ROW_SCAN_RET1_1");
  // (MIN_KEY, rowkey]
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan6.ini", "MT_SORTED_2ROW_SCAN_SCAN6",
                   "test_cases/test_mt_sorted_2row_scan.ret2.ini", "MT_SORTED_2ROW_SCAN_RET2");
  // [MIN_KEY, rowkey)
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan7.ini", "MT_SORTED_2ROW_SCAN_SCAN7",
                   "test_cases/test_mt_sorted_2row_scan.ret1_1.ini", "MT_SORTED_2ROW_SCAN_RET1_1");
  // [MIN_KEY, rowkwy]
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan8.ini", "MT_SORTED_2ROW_SCAN_SCAN8",
                   "test_cases/test_mt_sorted_2row_scan.ret2.ini", "MT_SORTED_2ROW_SCAN_RET2");

  // (rowkey, MAX_KEY)
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan9.ini", "MT_SORTED_2ROW_SCAN_SCAN9",
                   "test_cases/test_mt_sorted_2row_scan.ret1_2.ini", "MT_SORTED_2ROW_SCAN_RET1_2");
  // (rowkey, MAX_KEY]
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan10.ini", "MT_SORTED_2ROW_SCAN_SCAN10",
                   "test_cases/test_mt_sorted_2row_scan.ret1_2.ini", "MT_SORTED_2ROW_SCAN_RET1_2");
  // [rowkey, MAX_KEY)
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan11.ini", "MT_SORTED_2ROW_SCAN_SCAN11",
                   "test_cases/test_mt_sorted_2row_scan.ret2.ini", "MT_SORTED_2ROW_SCAN_RET2");
  // [rowkey, MAX_KEY]
  test_normal_scan(mt, "test_cases/test_mt_sorted_2row_scan.scan12.ini", "MT_SORTED_2ROW_SCAN_SCAN12",
                   "test_cases/test_mt_sorted_2row_scan.ret2.ini", "MT_SORTED_2ROW_SCAN_RET2");

  mt.destroy();
}

int main(int argc, char** argv) {
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



