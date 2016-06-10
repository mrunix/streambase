/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * mock_chunk_server.h for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */
#ifndef MOCK_CHUNK_SERVER_H_
#define MOCK_CHUNK_SERVER_H_
#include "mock_server.h"
#include "common/ob_tablet_info.h"
#include <map>
#include <string>
using namespace std;

namespace sb {
namespace nameserver {
class MockChunkServer : public MockServer {
 public:
  MockChunkServer();
  ~MockChunkServer();
  int initialize();
  int do_request(ObPacket* base_packet);
  int set_args(int total,  int number);

 private:
  int handle_hb(ObPacket* ob_packet);
  int handle_start_merge(ObPacket* ob_packet);
  int handle_drop(ObPacket* ob_packet);
  int regist_self();
  int generate_root_table();
  int report_tablets();
  int report_tablets(const ObTabletReportInfoList& tablets, int64_t time_stamp, bool has_more);
  int split_table();
 private:
  tbsys::CThreadMutex mutex_;
  ObServer name_server_;
  int total_;
  int number_;
  int64_t root_table_size_;
  int64_t seed_;
  int64_t version_;
  map<string, int> root_table_; //rowkey status
 private:
  class controlThread : public tbsys::CDefaultRunnable {
   public:
    explicit controlThread(MockChunkServer* server);
    void run(tbsys::CThread* thread, void* arg);
   private:
    MockChunkServer* server_;
  };
  controlThread control_thread_;
};
}
}


#endif //MOCK_UPDATE_SERVER_H_


