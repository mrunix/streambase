/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * rt_stats_reader.cc for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */

#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "nameserver/ob_root_stat.h"
using namespace sb;
using namespace sb::common;
using namespace sb::nameserver;

ObClientManager client;
ObServer name_server;
char* str[4] = {"ok_get", "ok_scan", "err_get", "err_scan"};
int main(int argc, char** argv) {
  if (argc != 3) {
    printf("%s root_ip root_port \n", argv[0]);
    return 0;
  }
  ob_init_memory_pool();
  tbnet::Transport transport_;
  common::ObPacketFactory packet_factory_;
  tbnet::DefaultPacketStreamer streamer_;
  streamer_.setPacketFactory(&packet_factory_);
  transport_.start();
  client.initialize(&transport_, &streamer_);
  name_server.set_ipv4_addr(argv[1], atoi(argv[2]));
  char* p_data = (char*)malloc(1024 * 1024 * 2);
  ObDataBuffer data_buff(p_data, 1024 * 1024 * 2);
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret) {
    ret = client.send_request(name_server, OB_FETCH_STATS, 1,
                              100000, data_buff);
  }
  int64_t pos = 0;

  ObResultCode result_code;
  ObStatManager stat(0);
  if (OB_SUCCESS == ret) {
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
  }
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
  } else {
    ret = result_code.result_code_;
  }
  if (OB_SUCCESS == ret) {
    ret = stat.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
  }
  if (OB_SUCCESS == ret) {
    ObStatManager::const_iterator it = stat.begin();
    for (; it != stat.end(); ++it) {
      for (int i = 0; i < 4; i++) {
        TBSYS_LOG(INFO, "%s %lld", str[i], it->get_value(i));
      }
    }
  } else {
    TBSYS_LOG(INFO, "read err %d", ret);
  }
  return 0;
}

