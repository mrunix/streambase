/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_update_server_main.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include "mock_update_server.h"

using namespace sb::nameserver;

int main(int argc, char** argv) {
  ob_init_memory_pool();
  MockUpdateServer server;
  MockServerRunner update_server(server);
  tbsys::CThread update_server_thread;
  update_server_thread.start(&update_server, NULL);
  update_server_thread.join();
  return 0;
}


