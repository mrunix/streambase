/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cc is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */

#include <gtest/gtest.h>
#include "common/ob_malloc.h"
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "sstable/ob_sstable_block_reader.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_trailer.h"
#include "sstable/ob_sstable_block_builder.h"
using namespace sb;
using namespace sb::common;
using namespace sb::sstable;


void create_row_key(
  CharArena& allocator,
  ObString& rowkey,
  const char* sk) {
  int64_t sz = strlen(sk);
  char* msk = allocator.alloc(sz);
  memcpy(msk, sk, sz);
  rowkey.assign_ptr(msk, sz);
}

void create_row(
  ObSSTableRow& row,
  CharArena& allocator,
  const char* key,
  int64_t f1,
  double f2
) {
  ObString row_key;
  create_row_key(allocator, row_key, key);
  row.set_row_key(row_key);
  row.set_table_id(100);
  row.set_column_group_id(0);
  ObObj obj1;
  obj1.set_int(f1);
  ObObj obj2;
  obj2.set_double(f2);
  row.add_obj(obj1);
  row.add_obj(obj2);
}



TEST(ObTestObSSTableBlockReader, add) {

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  int ret = builder.init();
  ASSERT_EQ(0, ret);

  ObSSTableRow row1, row2, row3;
  char* key[] = { "aoo", "foo", "koo" };
  create_row(row1, allocator, key[0], 1, 1.1);
  create_row(row2, allocator, key[1], 2, 2.1);
  create_row(row3, allocator, key[2], 3, 3.1);

  ret = builder.add_row(row1);
  ASSERT_EQ(0, ret);
  ret = builder.add_row(row2);
  ASSERT_EQ(0, ret);
  ret = builder.add_row(row3);
  ASSERT_EQ(0, ret);
  ret = builder.build_block();
  ASSERT_EQ(0, ret);

  ObSSTableBlockReader image;
  int64_t pos = 0;
  char buf[1024];
  ret = image.deserialize(buf, 1024, builder.block_buf(), builder.get_block_data_size(), pos);
  ASSERT_EQ(0, ret);
  EXPECT_EQ(3, image.get_row_count());

  typedef ObSSTableBlockReader::const_iterator const_iterator;

  ObString rowkey;
  ObString query_rowkey;
  ObObj ids[OB_MAX_COLUMN_NUMBER];
  ObObj query_columns[OB_MAX_COLUMN_NUMBER];
  int64_t column_size = OB_MAX_COLUMN_NUMBER;

  rowkey.assign_ptr(key[0], 3);
  const_iterator index = image.lower_bound(rowkey);
  EXPECT_LT(index, image.end());
  ret = image.get_row_key(index, query_rowkey);
  EXPECT_EQ(0, ret);
  EXPECT_EQ(0, memcmp(key[0], query_rowkey.ptr(), 3));
  ret = image.get_row_columns(OB_SSTABLE_STORE_DENSE, index, ids, query_columns, column_size);
  EXPECT_EQ(0, ret);
  EXPECT_EQ(2, column_size);
  int64_t f1 = 0;
  query_columns[0].get_int(f1);
  double f2 = 0;
  query_columns[1].get_double(f2);
  EXPECT_EQ(1.1, f2);

  rowkey.assign_ptr(key[1], 3);
  index = image.lower_bound(rowkey);
  EXPECT_LT(index, image.end());
  ret = image.get_row_key(index, query_rowkey);
  EXPECT_EQ(0, memcmp(key[1], query_rowkey.ptr(), 3));

  rowkey.assign_ptr(key[2], 3);
  index = image.lower_bound(rowkey);
  EXPECT_LT(index, image.end());
  ret = image.get_row_key(index, query_rowkey);
  EXPECT_EQ(0, memcmp(key[2], query_rowkey.ptr(), 3));

}

int main(int argc, char** argv) {
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

