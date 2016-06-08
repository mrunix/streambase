/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_chunk_server_main.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     ruohai <ruohai@taobao.com>
 *     rizhao <rizhao.ych@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *
 */

#include <new>
#include "ob_chunk_server_main.h"
#include "common/ob_malloc.h"

namespace sb {
namespace chunkserver {

// ----------------------------------------------------------
// class ObChunkServerMain
// ----------------------------------------------------------
ObChunkServerMain::ObChunkServerMain() {
}


ObChunkServerMain* ObChunkServerMain::get_instance() {
  if (NULL == instance_) {
    instance_ = new(std::nothrow) ObChunkServerMain();
  }
  return dynamic_cast<ObChunkServerMain*>(instance_);
}

int ObChunkServerMain::do_work() {
  server_.start();
  // server_.wait_for_queue();
  return common::OB_SUCCESS;
}

void ObChunkServerMain::do_signal(const int sig) {
  switch (sig) {
  case SIGTERM:
  case SIGINT:
    server_.stop();
    break;
  default:
    break;
  }
}

} // end namespace chunkserver
} // end namespace sb

