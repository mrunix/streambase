/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include "nameserver_admin.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
using namespace sb::nameserver;
using namespace sb::common;

int main(int argc, char* argv[]) {
  int ret = OB_SUCCESS;
  ob_init_memory_pool();
  TBSYS_LOGGER.setFileName("rs_admin.log", true);
  Arguments args;

  if (OB_SUCCESS != (ret = parse_cmd_line(argc, argv, args))) {
    printf("parse cmd line error, err=%d\n", ret);
  } else {
    // init
    ObServer server(ObServer::IPV4, args.rs_host, args.rs_port);
    ObBaseClient client;
    if (OB_SUCCESS != (ret = client.initialize(server))) {
      printf("failed to init client, err=%d\n", ret);
    } else {
      ret = args.command.handler(client, args);
    }
    // destroy
    client.destroy();
  }
  return ret;
}

