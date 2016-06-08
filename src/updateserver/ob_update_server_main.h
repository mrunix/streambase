/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_update_server_main.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_MAIN_H__
#define __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_MAIN_H__

#include "common/base_main.h"
#include "common/ob_shadow_server.h"
#include "ob_update_server.h"

namespace sb {
namespace updateserver {
class ObUpdateServerMain : public common::BaseMain {
  static const int SIG_RESET_MEMORY_LIMIT = 34;
 protected:
  ObUpdateServerMain();

 protected:
  virtual int do_work();
  virtual void do_signal(const int sig);
  virtual void print_version();

 public:
  static ObUpdateServerMain* get_instance();
 public:
  const ObUpdateServer& get_update_server() const {
    return server_;
  }

  ObUpdateServer& get_update_server() {
    return server_;
  }

 private:
  ObUpdateServerParam param_;
  ObUpdateServer server_;
  common::ObShadowServer shadow_server_;
};
}
}

#endif //__OB_UPDATE_SERVER_MAIN_H__



