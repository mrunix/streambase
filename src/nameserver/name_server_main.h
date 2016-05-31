/*
 * src/nameserver/name_server_main.h
 *
 * Copyright (C) 2016 Michael. All rights reserved.
 */

/*
 * The definition for NameServerMain.
 *
 * Library: nameserver
 * Package: nameserver
 * Module : NameServerMain
 * Author : Michael(Yang Lifeng)
 */

#ifndef SRC_NAMESERVER_NAME_SERVER_MAIN_H
#define SRC_NAMESERVER_NAME_SERVER_MAIN_H

#include "common/ob_packet_factory.h"
#include "common/base_main.h"
#include "common/ob_define.h"
#include "common/ob_config_manager.h"
#include "ob_root_worker.h"
#include "ob_root_reload_config.h"
#include "ob_root_server_config.h"

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
  ObRootServerConfig rs_config_;
  ObRootReloadConfig rs_reload_config_;
  common::ObConfigManager config_mgr_;
  ObRootWorker worker;
};

}
}

#endif

