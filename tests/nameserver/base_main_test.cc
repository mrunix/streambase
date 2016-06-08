/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * base_main_test.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include <gtest/gtest.h>
#include "test_main.h"
using namespace sb::common;
TEST(BaseMainTest, basic) {
  BaseMain* pmain = BaseMainTest::get_instance();
  int argc = 3;
  char* argv[] = {"ddd", "-f", "base_main_test.conf"};

  pmain->start(argc, argv, "test");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


