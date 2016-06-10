/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_main.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include <tbsys.h>

#include "test_main.h"

BaseMainTest::BaseMainTest() {
  test_flag = 0;
}

sb::common::BaseMain* BaseMainTest::get_instance() {
  if (instance_ == NULL) {
    instance_ = new BaseMainTest();
  }
  return instance_;

}

int BaseMainTest::do_work() {
  TBSYS_LOG(DEBUG, "debug do_work");
  TBSYS_LOG(INFO, "info do_work");
  TBSYS_LOG(WARN, "warn do_work");
  return 0;
}


