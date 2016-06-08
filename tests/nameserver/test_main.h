/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_main.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_TESTS_ROOTSERVER_TESTMAIN_H_
#define OCEANBASE_TESTS_ROOTSERVER_TESTMAIN_H_
#include "base_main.h"
class BaseMainTest : public sb::common::BaseMain {
 public:
  static BaseMain* get_instance();
  int do_work();
 public:
  int test_flag;
 private:
  BaseMainTest();
};
#endif

