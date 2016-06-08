/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * mock_chunk_server2.h for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#ifndef MOCK_CHUNK_SERVER_H_
#define MOCK_CHUNK_SERVER_H_

#include "mock_server.h"


namespace sb {
namespace updateserver {
namespace test {
class MockChunkServer : public MockServer {
 public:
  static const int32_t CHUNK_SERVER_PORT = 13111;
  int initialize();
  int do_request(ObPacket* base_packet);

 private:
  // get table cell
  int handle_get_table(ObPacket* ob_packet);

  // scan table row
  int handle_scan_table(ObPacket* ob_packet);

  // do mock request
  int handle_mock_get(ObPacket* ob_packet);
  int handle_mock_scan(ObPacket* ob_packet);
};
}
}
}


#endif //MOCK_CHUNK_SERVER_H_



