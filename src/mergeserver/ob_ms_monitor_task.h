/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_monitor_task.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef OB_MERGER_MONITOR_TIMER_TASK_H_
#define OB_MERGER_MONITOR_TIMER_TASK_H_


#include "common/ob_server.h"
#include "common/ob_timer.h"

namespace sb {
namespace mergeserver {
class ObMergerMonitorTask : public common::ObTimerTask {
 public:
  ObMergerMonitorTask();
  ~ObMergerMonitorTask();

 public:
  void runTimerTask(void);

 private:
  bool check_inner_stat(void) const;
};

inline bool ObMergerMonitorTask::check_inner_stat(void) const {
  return true;
}
}
}


#endif //OB_MERGER_MONITOR_TIMER_TASK_H_



