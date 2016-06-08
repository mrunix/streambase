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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_LOG_REPLAY
#define OCEANBASE_ROOTSERVER_OB_ROOT_LOG_REPLAY

#include "common/ob_log_replay_runnable.h"
#include "common/ob_log_entry.h"
#include "nameserver/nameserver_log_manager.h"

namespace sb {
namespace nameserver {
class ObRootLogManager;
class ObRootLogReplay : public common::ObLogReplayRunnable {
 public:
  ObRootLogReplay();
  ~ObRootLogReplay();

 public:
  void set_log_manager(ObRootLogManager* log_manage);
  int replay(common::LogCommand cmd, uint64_t seq, const char* log_data, const int64_t data_len);

 private:
  ObRootLogManager* log_manager_;
};
} /* nameserver */
} /* sb */

#endif /* end of include guard: OCEANBASE_ROOTSERVER_OB_ROOT_LOG_REPLAY */

