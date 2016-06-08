/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_stat.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "ob_ups_stat.h"
#include "ob_update_server_main.h"

#ifndef NO_STAT
namespace sb {
namespace sstable {
using namespace updateserver;
void set_stat(const uint64_t table_id, const int32_t index, const int64_t value) {
  UNUSED(table_id);
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  if (NULL == main) {
    TBSYS_LOG(ERROR, "get updateserver main null pointer");
  } else {
    UpsStatMgr& stat_mgr = main->get_update_server().get_stat_mgr();
    stat_mgr.set_value(UpsStatMgr::SSTABLE_STAT_TOTAL, index, value);
  }
}

void inc_stat(const uint64_t table_id, const int32_t index, const int64_t inc_value) {
  UNUSED(table_id);
  ObUpdateServerMain* main = ObUpdateServerMain::get_instance();
  if (NULL == main) {
    TBSYS_LOG(ERROR, "get updateserver main null pointer");
  } else {
    UpsStatMgr& stat_mgr = main->get_update_server().get_stat_mgr();
    stat_mgr.inc(UpsStatMgr::SSTABLE_STAT_TOTAL, index, inc_value);
  }
}
}
}
#endif


