/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#include "nameserver/name_server_timer_task.h"
#include "nameserver/name_server_worker.h"
#include  "common/ob_define.h"
namespace sb {
namespace nameserver {
int NameServerOperationDuty::init(NameServerWorker* worker) {
  int ret = OB_SUCCESS;
  if (NULL == worker) {
    TBSYS_LOG(WARN, "invalid argument. worker=NULL");
    ret = OB_INVALID_ARGUMENT;
  } else {
    worker_ = worker;
  }
  return ret;
}
void NameServerOperationDuty::runTimerTask(void) {
  worker_->submit_check_task_process();
}
}
}

