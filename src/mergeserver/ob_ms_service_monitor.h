/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_service_monitor.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef OB_MS_SERVICE_MONITOR_H_
#define OB_MS_SERVICE_MONITOR_H_

#include "common/ob_statistics.h"

namespace sb {
namespace mergeserver {
class ObMergerServiceMonitor : public common::ObStatManager {
 public:
  ObMergerServiceMonitor(const int64_t timestamp);
  virtual ~ObMergerServiceMonitor();

 public:
  enum {
    // get
    SUCC_GET_OP_COUNT = 0,
    SUCC_GET_OP_TIME,
    FAIL_GET_OP_COUNT,
    FAIL_GET_OP_TIME,

    // scan
    SUCC_SCAN_OP_COUNT,
    SUCC_SCAN_OP_TIME,
    FAIL_SCAN_OP_COUNT,
    FAIL_SCAN_OP_TIME,

    // cache hit
    HIT_CS_CACHE_COUNT,
    MISS_CS_CACHE_COUNT,

    // cs version error
    FAIL_CS_VERSION_COUNT,

    // local query
    LOCAL_CS_QUERY_COUNT,
    REMOTE_CS_QUERY_COUNT,
  };

 private:
  int64_t startup_timestamp_;
};
}
}


#endif //OB_MS_SERVICE_MONITOR_H_




