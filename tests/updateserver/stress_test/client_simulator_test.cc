/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * client_simulator_test.cc for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "client_simulator.h"

void RandomTest() {
  Random random;
  random.init();
  printf("%ld\n", random.rand64(10, 20));
  printf("%ld\n", random.rand64(2));
  for (int i = 0; i < 10000; i++) {
    random.rand64(2);
  }
}

int main() {
  common::ob_init_memory_pool();
  RandomTest();
  return 0;
}

