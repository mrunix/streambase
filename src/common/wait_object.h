/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * wait_object.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_WAIT_OBJECT_H_
#define OCEANBASE_COMMON_WAIT_OBJECT_H_
#include <tbsys.h>
#include <tbnet.h>

#include "ob_packet.h"
#include "ob_array_helper.h"
#include "hash/ob_hashmap.h"

namespace sb {
namespace common {
class WaitObject {
  friend class WaitObjectManager;
 public:
  WaitObject();
  virtual ~WaitObject();

  int64_t get_id() const;
  /** wait for response, timeout (us) */
  bool wait(const int64_t timeout_in_us = 0);
  void wakeup();
  ObPacket* get_response();

 private:
  int64_t seq_id_;
  int32_t done_count_;
  tbsys::CThreadCond cond_;
  char* response_buffer_;
  ObPacket* response_;
};
class WaitObjectManager {
 public:
  static const int64_t THREAD_BUFFER_SIZE = sizeof(ObPacket) + OB_MAX_PACKET_LENGTH;
  static const int64_t THREAD_BUFFER_ADVANCE_SIZE = sizeof(ObPacket);
 public:
  static const int64_t WAIT_OBJECT_MAP_SIZE = 1023;
  WaitObjectManager();
  virtual ~WaitObjectManager();

  WaitObject* create_wait_object();
  void destroy_wait_object(WaitObject*& wait_object);
  /** when return false, the response is not managed
    * it's the caller's duty to make sure it will be freed.
    * WARNING: make sure the response is unique
    */
  bool wakeup_wait_object(const int64_t id, tbnet::Packet* response);

 private:
  bool insert_wait_object(WaitObject* wait_object);

 private:
  int64_t seq_id_;
  tbsys::CThreadMutex mutex_;
  hash::ObHashMap<int64_t, WaitObject*> wait_objects_map_;
  ThreadSpecificBuffer* thread_buffer_;
};
} /* common */
} /* sb */

#endif /* end of include guard: OCEANBASE_COMMON_WAIT_OBJECT_H_ */

