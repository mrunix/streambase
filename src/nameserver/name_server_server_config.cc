/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 12:43:01 fufeng.syd>
 * Version: $Id$
 * Filename: name_server_server_config.cc
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#include "name_server_server_config.h"

using namespace sb;
using namespace sb::common;
using namespace sb::nameserver;

int NameServerServerConfig::get_name_server(ObServer& server) const {
  int ret = OB_SUCCESS;
  if (!server.set_ipv4_addr(name_server_ip, (int32_t)port)) {
    TBSYS_LOG(WARN, "Bad nameserver address! ip: [%s], port: [%s]",
              name_server_ip.str(), port.str());
    ret = OB_BAD_ADDRESS;
  }
  return ret;
}

int NameServerServerConfig::get_master_name_server(ObServer& server) const {
  int ret = OB_SUCCESS;
  if (!server.set_ipv4_addr(master_name_server_ip, (int32_t)master_name_server_port)) {
    TBSYS_LOG(WARN, "Bad nameserver address! ip: [%s], port: [%s]",
              master_name_server_ip.str(), master_name_server_port.str());
    ret = OB_BAD_ADDRESS;
  }
  return ret;
}
