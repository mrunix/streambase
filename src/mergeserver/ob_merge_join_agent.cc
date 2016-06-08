/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_join_agent.cc for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_merge_join_agent.h"
#include "common/ob_malloc.h"
#include "common/ob_cache.h"
#include "common/ob_scanner.h"
#include "common/ob_action_flag.h"
//#include <iostream>

using namespace sb;
using namespace sb::common;
using namespace sb::mergeserver;

sb::mergeserver::ObMergeJoinAgent::ObMergeJoinAgent(ObMergerRpcProxy& proxy)
  : scan_agent_(proxy), get_agent_(proxy) {
  req_agent_ = NULL;
}

sb::mergeserver::ObMergeJoinAgent::~ObMergeJoinAgent() {
  clear();
  req_agent_ = NULL;
}

int sb::mergeserver::ObMergeJoinAgent::set_request_param(const ObScanParam& scan_param,
                                                         ObUPSCellStream& ups_stream,
                                                         ObUPSCellStream& ups_join_stream,
                                                         const ObSchemaManagerV2& schema_mgr,
                                                         const int64_t max_memory_size) {
  int err = OB_SUCCESS;
  clear();
  err = scan_agent_.set_request_param(scan_param, ups_stream,
                                      ups_join_stream, schema_mgr, max_memory_size);
  if (OB_SUCCESS == err) {
    req_agent_ = & scan_agent_;
  }
  return err;
}
int sb::mergeserver::ObMergeJoinAgent::set_request_param(const ObGetParam& get_param,
                                                         ObUPSCellStream& ups_stream,
                                                         ObUPSCellStream& ups_join_stream,
                                                         const ObSchemaManagerV2& schema_mgr,
                                                         const int64_t max_memory_size) {
  int err = OB_SUCCESS;
  clear();
  err = get_agent_.set_request_param(get_param, ups_stream,
                                     ups_join_stream, schema_mgr, max_memory_size);
  if (OB_SUCCESS == err) {
    req_agent_ = & get_agent_;
  }
  return err;
}


int sb::mergeserver::ObMergeJoinAgent::get_cell(sb::common::ObCellInfo * *cell) {
  int err = OB_INNER_STAT_ERROR;
  if (NULL != req_agent_) {
    err = req_agent_->get_cell(cell);
  }
  return err;
}

int sb::mergeserver::ObMergeJoinAgent::get_cell(sb::common::ObCellInfo * *cell,
                                                bool* is_row_changed) {
  int err = OB_INNER_STAT_ERROR;
  if (NULL != req_agent_) {
    err = req_agent_->get_cell(cell, is_row_changed);
  }
  return err;
}

int sb::mergeserver::ObMergeJoinAgent::next_cell() {
  int err = OB_INNER_STAT_ERROR;
  if (NULL != req_agent_) {
    err = req_agent_->next_cell();
  }
  return err;
}

void sb::mergeserver::ObMergeJoinAgent::clear() {
  if (NULL != req_agent_) {
    req_agent_->clear();
    req_agent_ = NULL;
  }
}


bool sb::mergeserver::ObMergeJoinAgent::is_request_fullfilled() {
  bool result = false;
  if (NULL != req_agent_) {
    result = req_agent_->is_request_fullfilled();
  }
  return result;
}


