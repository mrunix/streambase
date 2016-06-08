/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_chunk_server_main.cc for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */
#include "mock_chunk_server.h"

using namespace sb::nameserver;

int main(int argc, char** argv) {
  ob_init_memory_pool();
  MockChunkServer server;
  if (argc != 3) {
    printf("arg error\n"
           "%s total_coont thisnumber\n", argv[0]);
    return 0;
  }
  int total = atoi(argv[1]);
  int number = atoi(argv[2]);
  if (total < 3 || number >= total || number < 0 || total > 900) {
    printf("total and number error\n"
           "%s total_coont thisnumber\n", argv[0]);
    return 0;
  }
  server.set_args(total, number);
  MockServerRunner update_server(server);
  tbsys::CThread update_server_thread;
  update_server_thread.start(&update_server, NULL);
  update_server_thread.join();
  return 0;
}


