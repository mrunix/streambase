/*
 * src/nameserver/.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for .
 *
 * Library: nameserver
 * Package: nameserver
 * Module :
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef OCEANBASE_ROOTSERVER_FETCH_THREAD_H_
#define OCEANBASE_ROOTSERVER_FETCH_THREAD_H_

#include "common/ob_fetch_runnable.h"
#include "nameserver/nameserver_log_manager.h"

namespace sb {
namespace nameserver {
class ObRootFetchThread : public common::ObFetchRunnable {
  const static int64_t WAIT_INTERVAL;

 public:
  ObRootFetchThread();

  int wait_recover_done();

  int got_ckpt(uint64_t ckpt_id);

  void set_log_manager(NameServerLogManager* log_manager);

 private:
  int recover_ret_;
  bool is_recover_done_;
  NameServerLogManager* log_manager_;
};
} /* nameserver */
} /* sb */

#endif /* end of include guard: OCEANBASE_ROOTSERVER_FETCH_THREAD_H_ */
#include "common/ob_fetch_runnable.h"


