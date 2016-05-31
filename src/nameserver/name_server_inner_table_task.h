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
  *   zhidong <xielun.szd@taobao.com>
  *     - some work details if you want
  */

#ifndef OB_ROOT_INNER_TABLE_TASK_H_
#define OB_ROOT_INNER_TABLE_TASK_H_

#include "common/ob_timer.h"
#include "name_server_async_task_queue.h"

namespace sb {
namespace nameserver {
class NameServerSQLProxy;
class NameServerInnerTableTask: public common::ObTimerTask {
 public:
  NameServerInnerTableTask();
  virtual ~NameServerInnerTableTask();
 public:
  int init(const int cluster_id, NameServerSQLProxy& proxy, common::ObTimer& timer, NameServerAsyncTaskQueue& queue);
  void runTimerTask(void);
 private:
  // check inner stat
  bool check_inner_stat(void) const;
  // process head task
  int process_head_task(void);
  // update all server table
  int modify_all_server_table(const NameServerAsyncTaskQueue::ObSeqTask& task);
  // update all cluster table
  int modify_all_cluster_table(const NameServerAsyncTaskQueue::ObSeqTask& task);
 private:
  // every run process task timeout
  const static int64_t MAX_TIMEOUT = 2000000; // 2s
  const static int64_t TIMEOUT = 1000000; // 1s
  const static int64_t RETRY_TIMES = 1;
  int cluster_id_;
  common::ObTimer* timer_;
  NameServerAsyncTaskQueue* queue_;
  NameServerSQLProxy* proxy_;
};
inline bool NameServerInnerTableTask::check_inner_stat(void) const {
  return ((timer_ != NULL) && (queue_ != NULL) && (NULL != proxy_));
}
}
}

#endif //OB_ROOT_INNER_TABLE_TASK_H_
