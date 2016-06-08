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

#ifndef _OB_ROOT_ADMIN2_H
#define _OB_ROOT_ADMIN2_H 1

#include "common/ob_base_client.h"
#include "common/ob_obi_role.h"
#include "nameserver/nameserver_stat_key.h"

namespace sb {
namespace nameserver {
using sb::common::ObBaseClient;

struct Arguments;
typedef int (*CmdHandler)(ObBaseClient& client, Arguments& args);

// declare all the handlers here
int do_get_obi_role(ObBaseClient& client, Arguments& args);
int do_set_obi_role(ObBaseClient& client, Arguments& args);
int do_rs_admin(ObBaseClient& client, Arguments& args);
int do_change_log_level(ObBaseClient& client, Arguments& args);
int do_rs_stat(ObBaseClient& client, Arguments& args);
int do_get_obi_config(ObBaseClient& client, Arguments& args);
int do_set_obi_config(ObBaseClient& client, Arguments& args);
int do_set_ups_config(ObBaseClient& client, Arguments& args);

struct Command {
  const char* cmdstr;
  int pcode;
  CmdHandler handler;
};

struct Arguments {
  Command command;
  const char* rs_host;
  int rs_port;
  int64_t request_timeout_us;
  sb::common::ObiRole obi_role;
  int log_level;
  int stat_key;
  int32_t obi_read_percentage;
  const char* ups_ip;           // ups or rs ip
  int ups_port;                 // ups or rs port
  int32_t ms_read_percentage;
  int32_t cs_read_percentage;
  Arguments() {
    command.cmdstr = NULL;
    command.pcode = -1;
    command.handler = NULL;
    rs_host = DEFAULT_RS_HOST;
    rs_port = DEFAULT_RS_PORT;
    request_timeout_us = DEFAULT_REQUEST_TIMEOUT_US;
    log_level = INVALID_LOG_LEVEL;
    stat_key = OB_RS_STAT_COMMON;
    obi_read_percentage = -1;
    ups_ip = NULL;
    ups_port = 0;
    cs_read_percentage = -1;
    ms_read_percentage = -1;
  }
  void print();
 public:
  static const int INVALID_LOG_LEVEL = -1;
 private:
  static const char* DEFAULT_RS_HOST;
  static const int DEFAULT_RS_PORT = 2500;
  static const int64_t DEFAULT_REQUEST_TIMEOUT_US = 2000000; // 2s
};

void usage();
void version();
int parse_cmd_line(int argc, char* argv[], Arguments& args);

} // end namespace nameserver
} // end namespace sb

#endif /* _OB_ROOT_ADMIN2_H */

