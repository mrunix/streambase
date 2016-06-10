/*
 * src/nameserver/base_main_test.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
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


