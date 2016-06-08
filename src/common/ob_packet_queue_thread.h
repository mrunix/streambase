/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_packet_queue_thread.h for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_PACKET_QUEUE_THREAD_H_
#define OCEANBASE_COMMON_OB_PACKET_QUEUE_THREAD_H_

#include "tbnet.h"
#include "ob_define.h"
#include "ob_packet.h"
#include "ob_packet_queue.h"

namespace sb {
namespace common {
class ObPacketQueueThread : public tbsys::CDefaultRunnable {
 public:
  ObPacketQueueThread();

  virtual ~ObPacketQueueThread();

  void setThreadParameter(int threadCount, tbnet::IPacketQueueHandler* handler, void* args);

  void stop(bool waitFinish = false);

  bool push(ObPacket* packet, int maxQueueLen = 0, bool block = true);

  void pushQueue(ObPacketQueue& packetQueue, int maxQueueLen = 0);

  void run(tbsys::CThread* thread, void* arg);

  ObPacket* head() {
    return queue_.head();
  }
  ObPacket* tail() {
    return queue_.tail();
  }
  size_t size() {
    return queue_.size();
  }

  void clear();

 protected:
  bool wait_finish_;
  bool waiting_;
  ObPacketQueue queue_;
  tbnet::IPacketQueueHandler* handler_;
  tbsys::CThreadCond cond_;
  tbsys::CThreadCond pushcond_;
  void* args_;
};

} // end namespace common
} // end namespace sb

#endif // OCEANBASE_COMMON_OB_PACKET_QUEUE_THREAD_H_

