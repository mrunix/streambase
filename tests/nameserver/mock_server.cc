/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * mock_server.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */

#include "mock_server.h"

namespace sb {
namespace nameserver {
int MockServer::set_self(const char* dev_name, const int32_t port) {
  int ret = OB_SUCCESS;
  int32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
  if (0 == ip) {
    TBSYS_LOG(ERROR, "cannot get valid local addr on dev:%s.", dev_name);
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret) {
    bool res = self_.set_ipv4_addr(ip, port);
    if (!res) {
      TBSYS_LOG(ERROR, "chunk server dev:%s, port:%s is invalid.",
                dev_name, port);
      ret = OB_ERROR;
    }
  }
  return ret;
}

}
}

