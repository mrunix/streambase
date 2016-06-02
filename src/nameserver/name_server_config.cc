/*
 * src/nameserver/name_server_config.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */


#include "name_server_config.h"

using namespace sb;
using namespace sb::common;
using namespace sb::nameserver;

int NameServerConfig::get_name_server(ObServer& server) const {
  int ret = OB_SUCCESS;
  if (!server.set_ipv4_addr(name_server_ip, (int32_t)port)) {
    TBSYS_LOG(WARN, "Bad nameserver address! ip: [%s], port: [%s]",
              name_server_ip.str(), port.str());
    ret = OB_BAD_ADDRESS;
  }
  return ret;
}

int NameServerConfig::get_master_name_server(ObServer& server) const {
  int ret = OB_SUCCESS;
  if (!server.set_ipv4_addr(master_name_server_ip, (int32_t)master_name_server_port)) {
    TBSYS_LOG(WARN, "Bad nameserver address! ip: [%s], port: [%s]",
              master_name_server_ip.str(), master_name_server_port.str());
    ret = OB_BAD_ADDRESS;
  }
  return ret;
}
