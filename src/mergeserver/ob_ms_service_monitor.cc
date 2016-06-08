/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_service_monitor.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "ob_ms_service_monitor.h"

using namespace sb::common;
using namespace sb::mergeserver;

ObMergerServiceMonitor::ObMergerServiceMonitor(const int64_t timestamp)
  : ObStatManager(SERVER_TYPE_MERGE) {
  startup_timestamp_ = timestamp;
}

ObMergerServiceMonitor::~ObMergerServiceMonitor() {

}



