/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_server_main.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "ob_merge_server_main.h"

namespace sb {
namespace mergeserver {
ObMergeServerMain::ObMergeServerMain() {
}

ObMergeServerMain* ObMergeServerMain::get_instance() {
  if (instance_ == NULL) {
    instance_ = new(std::nothrow) ObMergeServerMain();
    if (NULL == instance_) {
      TBSYS_LOG(ERROR, "check alloc ObMergeServerMain failed");
    }
  }
  return dynamic_cast<ObMergeServerMain*>(instance_);
}

int ObMergeServerMain::do_work() {
  server_.start();
  // server_.wait_for_queue();
  return common::OB_SUCCESS;
}

void ObMergeServerMain::do_signal(const int sig) {
  switch (sig) {
  case SIGTERM:
  case SIGINT:
    server_.stop();
    break;
  default:
    break;
  }
}
} /* mergeserver */
} /* sb */


