/*
 * src/nameserver/name_server_reload_config.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include "name_server_reload_config.h"
#include "name_server.h"

using namespace sb;
using namespace sb::nameserver;
using namespace sb::common;

NameServerReloadConfig::NameServerReloadConfig(const NameServerConfig& config)
  : config_(config), name_server_(NULL) {
}

NameServerReloadConfig::~NameServerReloadConfig() {
}

int NameServerReloadConfig::operator()() {
  int ret = OB_SUCCESS;

  if (NULL == name_server_) {
    TBSYS_LOG(WARN, "NULL name server.");
    ret = OB_NOT_INIT;
  } else {
    const NameServerConfig& config = name_server_->get_config();
    //config.print();

    if (OB_SUCCESS == ret && NULL != name_server_->ups_manager_) {
      name_server_->ups_manager_->set_ups_config((int32_t)config.read_master_master_ups_percent,
                                                 (int32_t)config.read_slave_master_ups_percent);
    }

    TBSYS_LOG(INFO, "after reload config, ret=%d", ret);
  }
  return ret;
}

void NameServerReloadConfig::set_name_server(NameServer& name_server) {
  name_server_ = &name_server;
}
