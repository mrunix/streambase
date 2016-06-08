/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ./test/btree_read_handle_test.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include <limits.h>
#include <btree_read_handle_new.h>
#include <btree_base.h>
#include <gtest/gtest.h>

/**
 * 测试BtreeReadHandle
 */
namespace sb {
namespace common {
TEST(BtreeReadHandleTest, construct) {
  BtreeBaseHandle handle;
  BtreeReadHandle handle1;
  handle1.set_from_key_range(const_cast<char*>(BtreeBase::MIN_KEY), 0, 0);
  handle1.set_to_key_range(const_cast<char*>(BtreeBase::MAX_KEY), 0, 0);
  int32_t size = CONST_KEY_MAX_LENGTH + 1;
  char buffer[size];
  handle1.set_from_key_range(buffer, size, 0);
  handle1.set_to_key_range(buffer, size, 0);
}
} // end namespace common
} // end namespace sb

