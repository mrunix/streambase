/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_monitor_task.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "ob_ms_monitor_task.h"
#include "common/ob_malloc.h"

using namespace sb::common;
using namespace sb::mergeserver;

ObMergerMonitorTask::ObMergerMonitorTask() {
}

ObMergerMonitorTask::~ObMergerMonitorTask() {
}

void ObMergerMonitorTask::runTimerTask(void) {
  if (true != check_inner_stat()) {
    TBSYS_LOG(ERROR, "%s", "check monitor timer task inner stat failed");
  } else {
    ob_print_mod_memory_usage();
  }
}





