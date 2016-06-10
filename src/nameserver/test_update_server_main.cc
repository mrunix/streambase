/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
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

