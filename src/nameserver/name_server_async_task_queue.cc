/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#include "common/ob_atomic.h"
#include "name_server_async_task_queue.h"

using namespace sb::common;
using namespace sb::nameserver;

uint64_t NameServerAsyncTaskQueue::ObSeqTask::get_task_id(void) const {
  return task_id_;
}

int64_t NameServerAsyncTaskQueue::ObSeqTask::get_task_timestamp(void) const {
  return timestamp_;
}

bool NameServerAsyncTaskQueue::ObSeqTask::operator == (const ObSeqTask& other) const {
  return (memcmp(this, &other, sizeof(other)) == 0);
}

bool NameServerAsyncTaskQueue::ObSeqTask::operator != (const ObSeqTask& other) const {
  return !(*this == other);
}

void NameServerAsyncTaskQueue::ObSeqTask::print_info(void) const {
  TBSYS_LOG(INFO, "seq task info:timestamp[%ld], timeout[%ld], remain_times[%ld], task_type[%d], server[%s], cluster_role[%d] status[%d]",
            timestamp_, max_timeout_, remain_times_, type_, server_.to_cstring(), cluster_role_, server_status_);
}

NameServerAsyncTaskQueue::NameServerAsyncTaskQueue() {
  id_allocator_ = 0;
}

NameServerAsyncTaskQueue::~NameServerAsyncTaskQueue() {
}

int NameServerAsyncTaskQueue::init(const int64_t max_count) {
  return queue_.init(max_count);
}

int NameServerAsyncTaskQueue::push(const ObSeqTask& task) {
  const_cast<ObSeqTask&>(task).timestamp_ = tbsys::CTimeUtil::getTime();
  const_cast<ObSeqTask&>(task).task_id_ = atomic_inc(&id_allocator_);
  return queue_.push(task);
}

int NameServerAsyncTaskQueue::head(ObSeqTask& task) const {
  return queue_.head(task);
}

int NameServerAsyncTaskQueue::pop(ObSeqTask& task) {
  return queue_.pop(task);
}

int64_t NameServerAsyncTaskQueue::size(void) const {
  return queue_.size();
}

void NameServerAsyncTaskQueue::print_info(void) const {
  if (size() > 0) {
    ObSeqTask task;
    int ret = head(task);
    TBSYS_LOG(INFO, "print queue info:id[%lu], size[%ld], first_timestamp[%ld], cur_timestamp[%ld]",
              id_allocator_, size(), task.timestamp_, tbsys::CTimeUtil::getTime());
    if (OB_SUCCESS == ret) {
      task.print_info();
    }
  }
}

