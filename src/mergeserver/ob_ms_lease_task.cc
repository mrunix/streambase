/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_lease_task.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "ob_ms_lease_task.h"
#include "ob_merge_server_service.h"
#include "common/ob_malloc.h"

using namespace sb::common;
using namespace sb::mergeserver;

ObMergerLeaseTask::ObMergerLeaseTask() {
  service_ = NULL;
}

ObMergerLeaseTask::~ObMergerLeaseTask() {
}

// WARN: if not registered succ then other task can not executed
void ObMergerLeaseTask::runTimerTask(void) {
  int ret = OB_SUCCESS;
  if (true != check_inner_stat()) {
    TBSYS_LOG(ERROR, "%s", "check lease timer task inner stat failed");
  } else if (false == service_->check_lease()) {
    // register lease
    TBSYS_LOG(WARN, "%s", "check lease expired register again");
    // if not registered will be blocked until stop or registered
    ret = service_->register_root_server();
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "register root server failed:ret[%d]", ret);
    } else {
      TBSYS_LOG(INFO, "%s", "register to root server succ");
    }
  } else {
    TBSYS_LOG(DEBUG, "check merge server lease ok");
  }
}




