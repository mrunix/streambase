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
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef MOCK_UPDATE_SERVER_H_
#define MOCK_UPDATE_SERVER_H_

#include "mock_server.h"


namespace sb {
namespace mergeserver {
namespace test {
class MockUpdateServer : public MockServer {
 public:
  static const int32_t UPDATE_SERVER_PORT = 12343;
  int initialize();
  int do_request(ObPacket* base_packet);

 private:
  // get table cell
  int handle_get_table(ObPacket* ob_packet);

  // scan table row
  int handle_scan_table(ObPacket* ob_packet);

  int handle_mock_get(ObPacket* ob_packet);
  int handle_mock_scan(ObPacket* ob_packet);


};
}
}
}


#endif //MOCK_UPDATE_SERVER_H_



