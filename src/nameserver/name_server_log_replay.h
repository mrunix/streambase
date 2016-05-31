/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_LOG_REPLAY
#define OCEANBASE_ROOTSERVER_OB_ROOT_LOG_REPLAY

#include "common/ob_log_replay_runnable.h"
#include "common/ob_log_entry.h"
#include "nameserver/name_server_log_manager.h"

namespace sb {
namespace nameserver {
class NameServerLogManager;
class NameServerLogReplay : public common::ObLogReplayRunnable {
 public:
  NameServerLogReplay();
  ~NameServerLogReplay();

 public:
  void wait_replay(ObLogCursor& end_cursor);
  void set_log_manager(NameServerLogManager* log_manage);
  int replay(common::LogCommand cmd, uint64_t seq, const char* log_data, const int64_t data_len);
  void run(tbsys::CThread* thread, void* arg);

 private:
  NameServerLogManager* log_manager_;
  ObLogCursor master_end_cursor_;
};
} /* nameserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOTSERVER_OB_ROOT_LOG_REPLAY */
