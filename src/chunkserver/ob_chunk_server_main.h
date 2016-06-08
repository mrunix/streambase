/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_chunk_server_main.h
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_CHUNKSERVERMAIN_H_
#define OCEANBASE_CHUNKSERVER_CHUNKSERVERMAIN_H_

#include "common/base_main.h"
#include "ob_chunk_server.h"

namespace sb {
namespace chunkserver {


class ObChunkServerMain : public common::BaseMain {
 protected:
  ObChunkServerMain();
 protected:
  virtual int do_work();
  virtual void do_signal(const int sig);
 public:
  static ObChunkServerMain* get_instance();
 public:
  const ObChunkServer& get_chunk_server() const { return server_ ; }
  ObChunkServer& get_chunk_server() { return server_ ; }
 private:
  ObChunkServer server_;
};


#define THE_CHUNK_SERVER ObChunkServerMain::get_instance()->get_chunk_server()

} // end namespace chunkserver
} // end namespace sb


#endif //OCEANBASE_CHUNKSERVER_CHUNKSERVERMAIN_H_

