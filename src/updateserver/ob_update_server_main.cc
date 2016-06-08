/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_update_server_main.cc for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#include "ob_update_server_main.h"

using namespace sb::common;

namespace sb {
namespace updateserver {
ObUpdateServerMain::ObUpdateServerMain()
  : server_(param_), shadow_server_(&server_) {
  shadow_server_.set_priority(LOW_PRI);
}

ObUpdateServerMain* ObUpdateServerMain::get_instance() {
  if (NULL == instance_) {
    instance_ = new(std::nothrow) ObUpdateServerMain();
  }

  return dynamic_cast<ObUpdateServerMain*>(instance_);
}

int ObUpdateServerMain::do_work() {
  add_signal_catched(SIG_RESET_MEMORY_LIMIT);
  int err = param_.load_from_config();
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "failed to load from conf, err=%d", err);
  } else {
    common::ObPacketFactory packet_factory;
    shadow_server_.set_packet_factory(&packet_factory);
    shadow_server_.set_listen_port(param_.get_ups_inner_port());

    shadow_server_.start(false);
    server_.start(false);

    shadow_server_.stop();
    shadow_server_.wait();
    server_.stop();
    server_.wait();
  }
  return common::OB_SUCCESS;
}

void ObUpdateServerMain::do_signal(const int sig) {
  switch (sig) {
  case SIGTERM:
  case SIGINT:
    server_.stop();
    shadow_server_.stop();
    TBSYS_LOG(INFO, "KILLED by signal, sig=%d", sig);
    break;
  case SIG_RESET_MEMORY_LIMIT:
    common::ob_set_memory_size_limit(INT64_MAX);
    break;
  default:
    break;
  }
}

void ObUpdateServerMain::print_version() {
  fprintf(stderr, "%s\n", __VERSION_ID__);
  fprintf(stderr, "Build time: %s %s\n", __DATE__, __TIME__);
#ifdef _BTREE_ENGINE_
  fprintf(stderr, "Using Btree Key-Value Engine.\n");
#else
  fprintf(stderr, "Using Hash Key-Value Engine.\n");
#endif
}
}
}


