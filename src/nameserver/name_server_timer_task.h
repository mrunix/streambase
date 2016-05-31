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

#ifndef __OB_ROOTSERVER_OB_ROOT_TIMER_TASK_H__
#define __OB_ROOTSERVER_OB_ROOT_TIMER_TASK_H__

#include "common/ob_timer.h"

namespace sb {
namespace nameserver {
class NameServerWorker;
class NameServerOperationDuty : public common::ObTimerTask {
 public:
  NameServerOperationDuty(): worker_(NULL)
  {}
  virtual ~NameServerOperationDuty() {}
  int init(NameServerWorker* worker);
  virtual void runTimerTask(void);
 private:
  NameServerWorker* worker_;
};
}
}

#endif

