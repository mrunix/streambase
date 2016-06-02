/*
 * src/nameserver/name_server_reload_config.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for NameServerReloadConfig.
 *
 * Library: nameserver
 * Package: nameserver
 * Module : NameServerReloadConfig
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */


#ifndef SRC_NAMESERVER_NAME_SERVER_RELOAD_CONFIG_H_
#define SRC_NAMESERVER_NAME_SERVER_RELOAD_CONFIG_H_

#include "common/ob_reload_config.h"
#include "name_server_server_config.h"

namespace sb {
namespace nameserver {

/* forward declearation */
class NameServer;

class NameServerReloadConfig
  : public common::ObReloadConfig {
 public:
  NameServerReloadConfig(const NameServerConfig& config);
  virtual ~NameServerReloadConfig();

  int operator()();

  void set_name_server(NameServer& name_server);
 private:
  const NameServerConfig& config_;
  NameServer* name_server_;
};

} // end of namespace nameserver
} // end of namespace sb

#endif /* SRC_NAMESERVER_NAME_SERVER_RELOAD_CONFIG_H_ */
