/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include "nameserver/nameserver_main.h"

namespace {
const char* STR_NAME_SECTION = "name_server";
const char* STR_DEV_NAME = "dev_name";
const char* STR_LISTEN_PORT = "port";
}

const char* build_date();
const char* build_time();

namespace sb {
using common::OB_SUCCESS;
using common::OB_ERROR;
namespace nameserver {
NameServerMain::NameServerMain() {
}
common::BaseMain* NameServerMain::get_instance() {
  if (instance_ == NULL) {
    instance_ = new(std::nothrow)NameServerMain();
  }
  return instance_;
}

void NameServerMain::print_version() {
  fprintf(stderr, "nameserver (%s %s)\n", PACKAGE_STRING, RELEASEID);
  fprintf(stderr, "BUILD_TIME: %s %s\n\n", build_date(), build_time());
  fprintf(stderr, "Copyright (c) 2007-2011 Taobao Inc.\n");
}

static const int START_REPORT_SIG = 49;
static const int START_MERGE_SIG = 50;
static const int DUMP_ROOT_TABLE_TO_LOG = 51;
static const int DUMP_AVAILABLE_SEVER_TO_LOG = 52;
static const int SWITCH_SCHEMA = 53;
static const int RELOAD_CONFIG = 54;
static const int DO_CHECK_POINT = 55;
static const int DROP_CURRENT_MERGE = 56;
static const int CREATE_NEW_TABLE = 57;

int NameServerMain::do_work() {
  //add signal I want to catch
  // we don't process the following signals any more, but receive them for backward compatibility
  add_signal_catched(START_REPORT_SIG);
  add_signal_catched(START_MERGE_SIG);
  add_signal_catched(DUMP_ROOT_TABLE_TO_LOG);
  add_signal_catched(DUMP_AVAILABLE_SEVER_TO_LOG);
  add_signal_catched(SWITCH_SCHEMA);
  add_signal_catched(RELOAD_CONFIG);
  add_signal_catched(DO_CHECK_POINT);
  add_signal_catched(DROP_CURRENT_MERGE);
  add_signal_catched(CREATE_NEW_TABLE);

  int ret = OB_SUCCESS;
  int port = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_LISTEN_PORT, 0);
  ret = worker.set_listen_port(port);
  if (ret == OB_SUCCESS) {
    const char* dev_name = TBSYS_CONFIG.getString(STR_NAME_SECTION, STR_DEV_NAME, NULL);
    ret = worker.set_dev_name(dev_name);
  }
  if (ret == OB_SUCCESS) {
    ret = worker.set_config_file_name(config_file_name_);
  }
  if (ret == OB_SUCCESS) {
    worker.set_packet_factory(&packet_factory_);
  }
  if (ret == OB_SUCCESS) {
    ret = worker.start();
  }
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to start nameserver, see the log for details, err=%d\n", ret);
  }
  return ret;
}
void NameServerMain::do_signal(const int sig) {
  switch (sig) {
  case SIGTERM:
  case SIGINT:
    TBSYS_LOG(INFO, "stop signal received");
    worker.stop();
    break;
  default:
    TBSYS_LOG(WARN, "unknown signal ignored, sig=%d", sig);
    break;
  }
}
}
}

