/*
 * src/nameserver/name_server_main.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for NameServerMain.
 *
 * Library: nameserver
 * Package: nameserver
 * Module : NameServerMain
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef SRC_NAMESERVER_NAME_SERVER_MAIN_H
#define SRC_NAMESERVER_NAME_SERVER_MAIN_H

#include "common/ob_packet_factory.h"
#include "common/base_main.h"
#include "common/ob_define.h"
#include "common/ob_config_manager.h"
#include "name_server_worker.h"
#include "name_server_reload_config.h"
#include "name_server_config.h"

namespace sb {
namespace nameserver {

class NameServerMain : public common::BaseMain {
 public:
  static common::BaseMain* get_instance();
  int do_work();
  void do_signal(const int sig);
 private:
  virtual void print_version();
  NameServerMain();
  NameServerConfig ns_config_;
  NameServerReloadConfig ns_reload_config_;
  common::ObConfigManager config_mgr_;
  NameServerWorker worker;
};

}
}

#endif

