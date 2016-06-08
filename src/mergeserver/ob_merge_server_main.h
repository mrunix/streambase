/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_server_main.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_MERGESERVERMAIN_H_
#define OCEANBASE_MERGESERVER_MERGESERVERMAIN_H_

#include "common/base_main.h"
#include "ob_merge_server.h"

namespace sb {
namespace mergeserver {
class ObMergeServerMain : public common::BaseMain {
 public:
  static ObMergeServerMain* get_instance();
  int do_work();
  void do_signal(const int sig);

 private:
  ObMergeServerMain();

 private:
  ObMergeServer server_;
};
} /* mergeserver */
} /* sb */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVERMAIN_H_ */


