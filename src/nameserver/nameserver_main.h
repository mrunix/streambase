/*
 * src/nameserver/.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for .
 *
 * Library: nameserver
 * Package: nameserver
 * Module :
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_MAIN_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_MAIN_H_
#include "common/ob_packet_factory.h"
#include "common/base_main.h"
#include "common/ob_define.h"
#include "nameserver/nameserver_worker.h"
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
  NameWorker worker;
  common::ObPacketFactory packet_factory_;
};
}
}
#endif

