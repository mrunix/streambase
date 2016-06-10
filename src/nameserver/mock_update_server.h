/*
 * src/nameserver/.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for ...
 *
 * Library: nameserver
 * Package: nameserver
 * Module : MockUpdateServer
 * Author : Michael(Yang Lifeng), 311155@qq.com
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
  ObServer name_server_;

};

}
}


#endif //MOCK_UPDATE_SERVER_H_


