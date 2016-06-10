/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include <gtest/gtest.h>
#include "common/ob_obi_config.h"

using namespace sb::common;

TEST(ObiConfigTest, test_serialization) {
  ObiConfig conf;
  ASSERT_EQ(0, conf.get_read_percentage());
  conf.set_read_percentage(70);
  ASSERT_EQ(70, conf.get_read_percentage());
  const int buff_size = 128;
  char buff[buff_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, conf.serialize(buff, buff_size, pos));
  int64_t len = conf.get_serialize_size();

  ObiConfig conf2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, conf2.deserialize(buff, len, pos));
  ASSERT_EQ(conf.get_read_percentage(), conf2.get_read_percentage());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
