/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_executor.h for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#ifndef OCEANBASE_TEST_OB_EXECUTOR_H_
#define OCEANBASE_TEST_OB_EXECUTOR_H_

#include "ob_test_bomb.h"
#include "mock_client.h"

namespace sb {
namespace test {
class ObExecutor {
 public:
  ObExecutor();
  virtual ~ObExecutor();
  int init(common::ObServer& ms, common::ObServer& ups);
  virtual int exec(const ObTestBomb& bomb);

 protected:
  virtual int exec_update(const ObTestBomb& bomb);
  virtual int exec_scan(const ObTestBomb& bomb);

 private:
  MockClient scan_client_;
  MockClient apply_client_;
};
} // end namespace test
} // end namespace sb

#endif // OCEANBASE_TEST_OB_EXECUTOR_H_

