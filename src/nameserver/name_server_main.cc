/*
 * src/nameserver/name_server_main.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include "nameserver/name_server_main.h"
#include "common/ob_version.h"

namespace sb {

using common::OB_SUCCESS;
using common::OB_ERROR;

namespace nameserver {

NameServerMain::NameServerMain()
  : ns_reload_config_(ns_config_),
    config_mgr_(ns_config_, ns_reload_config_), worker(config_mgr_, ns_config_) {
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
  fprintf(stderr, "Copyright (c) 2016 Michael(311155@qq.com)\n");
}

static const int START_REPORT_SIG            = 49;
static const int START_MERGE_SIG             = 50;
static const int DUMP_ROOT_TABLE_TO_LOG      = 51;
static const int DUMP_AVAILABLE_SEVER_TO_LOG = 52;
static const int SWITCH_SCHEMA               = 53;
static const int RELOAD_CONFIG               = 54;
static const int DO_CHECK_POINT              = 55;
static const int DROP_CURRENT_MERGE          = 56;
static const int CREATE_NEW_TABLE            = 57;

int NameServerMain::do_work() {
  int ret = OB_SUCCESS;
  char dump_config_path[OB_MAX_FILE_NAME_LENGTH];

  TBSYS_LOG(INFO, "oceanbase-root "
            "build_date=[%s] build_time=[%s]", build_date(), build_time());

  ns_reload_config_.set_name_server(worker.get_name_server());

  /* read config from binary config file if it existed. */
  snprintf(dump_config_path,
           sizeof(dump_config_path), "etc/%s.config.bin", server_name_);
  config_mgr_.set_dump_path(dump_config_path);
  if (OB_SUCCESS != (ret = config_mgr_.base_init())) {
    TBSYS_LOG(ERROR, "init config manager error, ret: [%d]", ret);
  } else if (OB_SUCCESS != (ret = config_mgr_.load_config(config_))) {
    TBSYS_LOG(ERROR, "load config error, path: [%s], ret: [%d]",
              config_, ret);
  } else {
    TBSYS_LOG(INFO, "load config file successfully. path: [%s]",
              strlen(config_) > 0 ? config_ : dump_config_path);
  }

  /* set configuration pass from command line */
  if (0 != cmd_cluster_id_) {
    ns_config_.cluster_id = static_cast<int64_t>(cmd_cluster_id_);
  }
  if (strlen(cmd_ns_ip_) > 0) {
    ns_config_.name_server_ip.set_value(cmd_ns_ip_); /* rs vip */
  }
  if (cmd_ns_port_ > 0) {
    ns_config_.port = cmd_ns_port_; /* listen port */
  }
  if (strlen(cmd_master_ns_ip_) > 0) {
    ns_config_.master_name_server_ip.set_value(cmd_master_ns_ip_);
  }
  if (cmd_master_ns_port_ > 0) {
    ns_config_.master_name_server_port = cmd_master_ns_port_;
  }
  if (strlen(cmd_devname_) > 0) {
    ns_config_.devname.set_value(cmd_devname_);
  }
  if (strlen(cmd_extra_config_) > 0
      && OB_SUCCESS != (ret = ns_config_.add_extra_config(cmd_extra_config_))) {
    TBSYS_LOG(ERROR, "Parse extra config error! string: [%s], ret: [%d]",
              cmd_extra_config_, ret);
  }
  ns_config_.print();

  if (OB_SUCCESS != ret) {
  } else if (OB_SUCCESS != (ret = ns_config_.check_all())) {
    TBSYS_LOG(ERROR, "check config failed, ret: [%d]", ret);
  } else {
    // add signal I want to catch
    // we don't process the following
    // signals any more, but receive them for backward
    // compatibility
    add_signal_catched(START_REPORT_SIG);
    add_signal_catched(START_MERGE_SIG);
    add_signal_catched(DUMP_ROOT_TABLE_TO_LOG);
    add_signal_catched(DUMP_AVAILABLE_SEVER_TO_LOG);
    add_signal_catched(SWITCH_SCHEMA);
    add_signal_catched(RELOAD_CONFIG);
    add_signal_catched(DO_CHECK_POINT);
    add_signal_catched(DROP_CURRENT_MERGE);
    add_signal_catched(CREATE_NEW_TABLE);
  }

  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "start name server failed, ret: [%d]", ret);
  } else {
    worker.set_io_thread_count((int32_t)ns_config_.io_thread_count);
    ret = worker.start(false);
  }

  return ret;
}

void NameServerMain::do_signal(const int sig) {
  switch (sig) {
  case SIGTERM:
  case SIGINT:
    signal(SIGINT, SIG_IGN);
    signal(SIGTERM, SIG_IGN);
    TBSYS_LOG(INFO, "stop signal received");
    worker.stop();
    break;
  default:
    TBSYS_LOG(INFO, "signal processed by base_main, sig=%d", sig);
    break;
  }
}
}
}
