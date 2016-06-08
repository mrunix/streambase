/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_root_table2.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */


#include <gtest/gtest.h>
#include <unistd.h>
#include "common/ob_malloc.h"
#include "common/ob_vector.h"
#include "nameserver/ob_tablet_info_manager.h"
#include "nameserver/ob_root_meta2.h"
#include "nameserver/ob_root_table2.h"
using namespace sb::common;
using namespace sb::nameserver;
namespace {
void build_range(ObRange& r, int64_t tid, int8_t flag, const char* sk, const char* ek) {

  ObString start_key(strlen(sk), strlen(sk), (char*)sk);
  ObString end_key(strlen(ek), strlen(ek), (char*)ek);

  r.table_id_ = tid;
  r.border_flag_.set_data(flag);
  r.start_key_ = start_key;
  r.end_key_ = end_key;

}
}

TEST(RootTable2Test, test_sort) {
  ObRange r1, r2, r3, r4;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;
  uint64_t table2 = 40;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table2, ObBorderFlag::INCLUSIVE_START | ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key1, key4);

  ASSERT_TRUE(r1.compare_with_endkey(r2) < 0);
  ASSERT_TRUE(r2.compare_with_endkey(r3) < 0);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);


  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 1, 0);
  root_table->add(t3, 2, 0);
  root_table->add(t1, 3, 0);
  root_table->add(t1, 4, 0);

  root_table->sort();
  NameMeta* it = root_table->begin();

  const ObTabletInfo* tablet_info = ((const NameTable*)root_table)->get_tablet_info(it);

  EXPECT_TRUE(tablet_info != NULL);
  EXPECT_TRUE(tablet_info->range_.equal(r1));

  it++;
  it++;
  it++;
  tablet_info = ((const NameTable*)root_table)->get_tablet_info(it);

  EXPECT_TRUE(tablet_info != NULL);
  EXPECT_TRUE(tablet_info->range_.equal(r3));
  delete root_table;
  delete info_manager;
}
TEST(RootTable2Test, test_shrink_to) {
  ObRange r1, r2, r3, r4;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;
  uint64_t table2 = 40;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table2, ObBorderFlag::INCLUSIVE_START | ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key1, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);


  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 0, 0);
  root_table->add(t3, 0, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();
  NameMeta* it = root_table->begin();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);
  delete root_table;
  delete info_manager;

  it = shrink_table->begin();
  const ObTabletInfo* tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(it);

  EXPECT_TRUE(tablet_info != NULL);
  EXPECT_TRUE(tablet_info->range_.equal(r1));

  it++;
  it++;
  tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(it);

  EXPECT_TRUE(tablet_info != NULL);
  EXPECT_TRUE(tablet_info->range_.equal(r3));
  //shrink_table->dump();

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_find_key) {
  ObRange r1, r2, r3, r4, r5, r6;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";
  const char* key5 = "xoo5";
  const char* key6 = "yoo6";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END, key3, key4);
  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key4, key5);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END, key5, key6);
  build_range(r6, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key6, key6);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);
  ObTabletInfo t5(r5, 0, 0);
  ObTabletInfo t6(r6, 0, 0);


  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->add(t5, 1, 0);
  root_table->add(t6, 1, 0);
  root_table->add(t4, 1, 0);
  root_table->sort();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  char fk[] = "key3";
  NameTable::const_iterator start;
  NameTable::const_iterator end;
  NameTable::const_iterator ptr;
  ObString obfk(strlen(fk), strlen(fk), fk);
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_key(table1, obfk, 10, start, end, ptr));
  const ObTabletInfo* tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(ptr);
  printf(" count = %ld %ld\n", end - start, ptr - start);

  EXPECT_TRUE(tablet_info->range_.equal(r2));
  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_find_range) {
  ObRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;
  uint64_t table2 = 40;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key3, key4);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key4, key4);
  build_range(r4, table2, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE | ObBorderFlag::MIN_VALUE, key1, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);


  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->add(t4, 1, 0);
  root_table->sort();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  NameTable::const_iterator first;
  NameTable::const_iterator last;
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r5, first, last));
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(first);
  tablet_info->range_.hex_dump();
  EXPECT_TRUE(tablet_info->range_.equal(r2));

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(first);
  tablet_info->range_.hex_dump();
  EXPECT_TRUE(tablet_info->range_.equal(r4));
  delete shrink_table;
  delete info_manager2;
}

TEST(RootTable2Test, test_range_pos_type) {
  ObRange r1, r2, r3, r4, r5, r6, r7, r8;
  const char* key1 = "foo1";
  const char* key1_2 = "foo2";
  const char* key1_3 = "foo3";
  const char* key2 = "key2";
  const char* key2_3 = "key3";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key1_2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2, key2_3);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key2, key4);

  build_range(r6, table1, ObBorderFlag::INCLUSIVE_END, key1_2, key1_3);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  NameTable::const_iterator first;
  NameTable::const_iterator last;
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r2, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type = shrink_table->get_range_pos_type(r2, first, last);
  ASSERT_EQ(NameTable::POS_TYPE_SAME_RANGE, range_pos_type);

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  range_pos_type = shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(NameTable::POS_TYPE_SPLIT_RANGE, range_pos_type);

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r5, first, last));
  range_pos_type = shrink_table->get_range_pos_type(r5, first, last);
  ASSERT_EQ(NameTable::POS_TYPE_MERGE_RANGE, range_pos_type);

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r6, first, last));
  range_pos_type = shrink_table->get_range_pos_type(r6, first, last);
  ASSERT_EQ(NameTable::POS_TYPE_ADD_RANGE, range_pos_type);

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_split_range_top) {
  ObRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key2_3 = "key3";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2, key2_3);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key2, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  NameTable::const_iterator first;
  NameTable::const_iterator last;

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type = shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(NameTable::POS_TYPE_SPLIT_RANGE, range_pos_type);

  ObTabletInfo t4(r4, 0, 0);

  shrink_table->split_range(t4, first, 1, 5);
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE(tablet_info->range_.equal(r4));

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_split_range_middle) {
  ObRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key2_3 = "key3";
  const char* key2_4 = "key4";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2_3, key2_4);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key2, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  NameTable::const_iterator first;
  NameTable::const_iterator last;

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type = shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(NameTable::POS_TYPE_SPLIT_RANGE, range_pos_type);

  ObTabletInfo t4(r4, 0, 0);

  shrink_table->split_range(t4, first, 1, 5);
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE(tablet_info->range_.equal(r4));

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_add_range) {
  ObRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key2_3 = "key3";
  const char* key2_4 = "key4";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2_4, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key3, key4);

  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2, key2_3);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END, key2_3, key2_4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  NameTable::const_iterator first;
  NameTable::const_iterator last;

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r5, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type = shrink_table->get_range_pos_type(r5, first, last);
  ASSERT_EQ(NameTable::POS_TYPE_ADD_RANGE, range_pos_type);
  ObTabletInfo t5(r5, 0, 0);
  shrink_table->add_range(t5, first, 1, 5);
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r5, first, last));
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE(tablet_info->range_.equal(r5));

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  range_pos_type = shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(NameTable::POS_TYPE_ADD_RANGE, range_pos_type);
  ObTabletInfo t4(r4, 0, 0);
  shrink_table->add_range(t4, first, 1, 5);
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE(tablet_info->range_.equal(r4));

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_add_lost_range) {
  ObRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key2_4 = "key4";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2_4, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key3, key4);

  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2, key2_4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);

  shrink_table->sort();
  shrink_table->add_lost_range();

  delete root_table;
  delete info_manager;

  NameTable::const_iterator first;
  NameTable::const_iterator last;

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE(tablet_info->range_.equal(r4));

  shrink_table->dump();

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_create_table) {
  ObRange r1, r2, r3, r4, r5, r6;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, 30, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE | ObBorderFlag::MIN_VALUE, key1, key4);
  build_range(r5, 25, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE | ObBorderFlag::MIN_VALUE, key1, key4);
  build_range(r6, 35, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MAX_VALUE | ObBorderFlag::MIN_VALUE, key1, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);
  ObTabletInfo t5(r5, 0, 0);
  ObTabletInfo t6(r6, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->add(t4, 1, 0);
  root_table->sort();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  int32_t server_indexes[3];
  server_indexes[0] = 0;
  server_indexes[1] = 0;
  server_indexes[2] = 0;
  ASSERT_EQ(OB_SUCCESS, shrink_table->create_table(t5, server_indexes, 3, 0));
  shrink_table->dump();
  NameTable::const_iterator it = shrink_table->end();
  it--;
  const ObTabletInfo* tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(it);
  ASSERT_TRUE(tablet_info->range_.equal(r4));
  it--;
  tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(it);
  ASSERT_TRUE(tablet_info->range_.equal(r5));

  ASSERT_EQ(OB_SUCCESS, shrink_table->create_table(t6, server_indexes, 3, 0));
  shrink_table->dump();
  it = shrink_table->end();
  it--;
  tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(it);
  ASSERT_TRUE(tablet_info->range_.equal(r6));

  delete shrink_table;
  delete info_manager2;
}


TEST(RootTable2Test, test_split_range_top_max) {
  ObRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key3_4 = "too4";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END | ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::MAX_VALUE, key3, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  NameTable* root_table = new NameTable(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();


  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  NameTable* shrink_table = new NameTable(info_manager2);
  root_table->shrink_to(shrink_table);

  shrink_table->sort();
  shrink_table->dump();

  delete root_table;
  delete info_manager;

  NameTable::const_iterator first;
  NameTable::const_iterator last;

  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key3, key3_4);

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type = shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(NameTable::POS_TYPE_SPLIT_RANGE, range_pos_type);

  ObTabletInfo t4(r4, 0, 0);

  shrink_table->split_range(t4, first, 1, 5);
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  TBSYS_LOG(WARN, "================================================================");
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const NameTable*)shrink_table)->get_tablet_info(first);
  tablet_info->range_.dump();
  r4.dump();
  EXPECT_TRUE(tablet_info->range_.equal(r4));
  EXPECT_TRUE(tablet_info->range_.border_flag_.get_data() == r4.border_flag_.get_data());

  delete shrink_table;
  delete info_manager2;
}

int main(int argc, char** argv) {
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



