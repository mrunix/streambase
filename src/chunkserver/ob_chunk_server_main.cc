/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *
 */

#include <new>
#include "ob_chunk_server_main.h"
#include "common/ob_malloc.h"

namespace sb {
namespace chunkserver {

// ----------------------------------------------------------
// class ObChunkServerMain
// ----------------------------------------------------------
ObChunkServerMain::ObChunkServerMain()
  : server_(cs_config_, config_mgr_), config_mgr_(cs_config_, cs_reload_config_) {
}


ObChunkServerMain* ObChunkServerMain::get_instance() {
  if (NULL == instance_) {
    instance_ = new(std::nothrow) ObChunkServerMain();
  }
  return dynamic_cast<ObChunkServerMain*>(instance_);
}

int ObChunkServerMain::do_work() {
  int ret = OB_SUCCESS;
  char dump_config_path[OB_MAX_FILE_NAME_LENGTH];
  TBSYS_LOG(INFO, "oceanbase-chunk "
            "build_data=[%s] build_time=[%s]", build_date(), build_time());

  cs_reload_config_.set_chunk_server(server_);

  snprintf(dump_config_path,
           sizeof(dump_config_path), "etc/%s.config.bin", server_name_);
  config_mgr_.set_dump_path(dump_config_path);
  if (OB_SUCCESS != (ret = config_mgr_.base_init())) {
    TBSYS_LOG(ERROR, "init config manager error, ret: [%d]", ret);
  } else if (OB_SUCCESS != (ret = config_mgr_.load_config(config_))) {
    TBSYS_LOG(ERROR, "load config error, path: [%s], ret: [%d]",
              config_, ret);
  }

  /* set rs address if command line has past in */
  if (strlen(cmd_rs_ip_) > 0 && cmd_rs_port_ != 0) {
    cs_config_.root_server_ip.set_value(cmd_rs_ip_);
    cs_config_.root_server_port = cmd_rs_port_;
  }
  if (cmd_port_ != 0) {
    cs_config_.port = cmd_port_;
  }
  if (strlen(cmd_data_dir_) > 0) {
    cs_config_.datadir.set_value(cmd_data_dir_);
  }
  if (strlen(cmd_appname_) > 0) {
    cs_config_.appname.set_value(cmd_appname_);
  }
  if (strlen(cmd_devname_) > 0) {
    cs_config_.devname.set_value(cmd_devname_);
  }
  if (strlen(config_)) {
    TBSYS_LOG(INFO, "using config file path: [%s]", config_);
  }
  if (strlen(cmd_extra_config_) > 0
      && OB_SUCCESS != (ret = cs_config_.add_extra_config(cmd_extra_config_))) {
    TBSYS_LOG(ERROR, "Parse extra config error! string: [%s], ret: [%d]",
              cmd_extra_config_, ret);
  }
  cs_config_.print();

  if (OB_SUCCESS == ret && OB_SUCCESS != (ret = cs_config_.check_all())) {
    TBSYS_LOG(ERROR, "check config failed, ret: [%d]", ret);
  }

  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "Start chunk server failed, ret: [%d]", ret);
  } else {
    server_.set_io_thread_count((int)cs_config_.io_thread_count);
    server_.start();
  }

  return ret;
}

void ObChunkServerMain::do_signal(const int sig) {
  switch (sig) {
  case SIGTERM:
  case SIGINT:
    signal(SIGINT, SIG_IGN);
    signal(SIGTERM, SIG_IGN);
    TBSYS_LOG(INFO, "KILLED by signal, sig is %d", sig);
    server_.stop_eio();
    break;
  default:
    break;
  }
}

void ObChunkServerMain::print_version() {
  fprintf(stderr, "chunkserver (%s %s)\n", PACKAGE_STRING, RELEASEID);
  fprintf(stderr, "BUILD_TIME: %s %s\n\n", build_date(), build_time());
  fprintf(stderr, "Copyright (c) 2007-2011 Taobao Inc.\n");
}

} // end namespace chunkserver
} // end namespace sb
