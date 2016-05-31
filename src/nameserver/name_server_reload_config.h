/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 14:46:37 fufeng.syd>
 * Version: $Id$
 * Filename: name_server_reload_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_ROOT_RELOAD_CONFIG_H_
#define _OB_ROOT_RELOAD_CONFIG_H_

#include "common/ob_reload_config.h"
#include "name_server_server_config.h"

namespace sb {
namespace nameserver {
/* forward declearation */
class NameServer;

class NameServerReloadConfig
  : public common::ObReloadConfig {
 public:
  NameServerReloadConfig(const NameServerServerConfig& config);
  virtual ~NameServerReloadConfig();

  int operator()();

  void set_name_server(NameServer& name_server);
 private:
  const NameServerServerConfig& config_;
  NameServer* name_server_;
};
} // end of namespace nameserver
} // end of namespace sb

#endif /* _OB_ROOT_RELOAD_CONFIG_H_ */
