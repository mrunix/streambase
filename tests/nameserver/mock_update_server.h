/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * mock_update_server.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef MOCK_UPDATE_SERVER_H_
#define MOCK_UPDATE_SERVER_H_
#include "mock_server.h"

namespace sb {
namespace nameserver {
class MockUpdateServer : public MockServer {
 public:
  static const int32_t UPDATE_SERVER_PORT = 12343;
  int initialize();
  int do_request(ObPacket* base_packet);

 private:
  int handle_mock_freeze(ObPacket* ob_packet);
  int handle_drop_tablets(ObPacket* ob_packet);
  ObServer root_server_;

};
}
}


#endif //MOCK_UPDATE_SERVER_H_


