/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_replay_runnable.h for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#ifndef OCEANBASE_UPDATESERVER_OB_UPS_REPLAY_RUNNABLE_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_REPLAY_RUNNABLE_H_

#include "common/ob_log_replay_runnable.h"
#include "common/ob_log_entry.h"

namespace sb {
namespace tests {
namespace updateserver {
//forward decleration
class TestObUpsReplayRunnable_test_init_Test;
}
}
namespace updateserver {
class ObUpsReplayRunnable : public common::ObLogReplayRunnable {
  friend class tests::updateserver::TestObUpsReplayRunnable_test_init_Test;
 public:
  ObUpsReplayRunnable();
  virtual ~ObUpsReplayRunnable();
 public:
  virtual int replay(common::LogCommand cmd, uint64_t seq, const char* log_data, const int64_t data_len);
};
} // end namespace updateserver
} // end namespace sb


#endif // OCEANBASE_UPDATESERVER_OB_UPS_REPLAY_RUNNABLE_H_



