/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * base_main.h for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */
#ifndef OCEANBASE_ROOTSERVER_BASE_MAIN_H_
#define OCEANBASE_ROOTSERVER_BASE_MAIN_H_
#include <tbsys.h>

#include "ob_define.h"
namespace sb {
namespace common {
class BaseMain {
 public:
  //static BaseMain* instance() {
  //  return instance_;
  //}
  virtual ~BaseMain();
  virtual int start(const int argc, char* argv[], const char* section_name);
  virtual void destroy();
 protected:
  BaseMain();
  virtual void do_signal(const int sig);
  void add_signal_catched(const int sig);
  static BaseMain* instance_;
  virtual void print_usage(const char* prog_name);
  virtual void print_version();
  virtual const char* parse_cmd_line(const int argc, char* const argv[]);
  virtual int do_work() = 0;
  char config_file_name_[OB_MAX_FILE_NAME_LENGTH];
 private:
  static void sign_handler(const int sig);
  bool use_deamon_;
};
}
}
#endif

