/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cs_get_cell_stream_wrapper.cc for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */

#include "tbsys.h"
#include "common/ob_define.h"
#include "ob_cs_get_cell_stream_wrapper.h"

using namespace sb::common;
using namespace sb::mergeserver;

ObMSUpsStreamWrapper::ObMSUpsStreamWrapper(const int64_t retry_times,
                                           const int64_t timeout, const ObServer& root_server)
  : rpc_proxy_(retry_times, timeout, root_server, update_server_,
               merge_server_, common::CHUNK_SERVER) {
  schema_mgr_ = NULL;
}

ObMSUpsStreamWrapper::~ObMSUpsStreamWrapper() {
  if (schema_mgr_ != NULL) {
    ob_free(schema_mgr_);
    schema_mgr_ = NULL;
  }
}

void ObMSUpsStreamWrapper::set_update_server(const ObServer& server) {
  update_server_ = server;
  rpc_proxy_.set_update_server(update_server_);
}

int ObMSUpsStreamWrapper::init(const ThreadSpecificBuffer* rpc_buffer,
                               const ObClientManager* rpc_frame) {
  int err  = OB_SUCCESS;
  if (NULL == rpc_buffer || NULL == rpc_frame) {
    TBSYS_LOG(WARN, "param error [rpc_buffer:%p,rpc_frame:%p]", rpc_buffer, rpc_frame);
    err = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == err) {
    char* schema_mgr_buffer = reinterpret_cast<char*>(ob_malloc(sizeof(ObMergerSchemaManager)));
    if (NULL == schema_mgr_buffer) {
      TBSYS_LOG(WARN, "%s", "fail to allocate memory for schema manager");
      err  = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      schema_mgr_ = new(schema_mgr_buffer)ObMergerSchemaManager;
    }
  }

  if (OB_SUCCESS == err) {
    err = rpc_stub_.init(rpc_buffer, rpc_frame);
  }

  if (OB_SUCCESS == err) {
    err = rpc_proxy_.init(&rpc_stub_, schema_mgr_, &location_cache_);
  }
  return err;
}

ObMergerRpcProxy* ObMSUpsStreamWrapper::get_ups_rpc_proxy() {
  return &rpc_proxy_;
}

ObMSGetCellStreamWrapper::ObMSGetCellStreamWrapper(ObMSUpsStreamWrapper& ups_stream)
  : get_cell_stream_(ups_stream.get_ups_rpc_proxy()),
    scan_cell_stream_(ups_stream.get_ups_rpc_proxy()) {
}

ObMSGetCellStreamWrapper::~ObMSGetCellStreamWrapper() {
}

ObMSGetCellStream* ObMSGetCellStreamWrapper::get_ups_get_cell_stream() {
  return &get_cell_stream_;
}

ObMSScanCellStream* ObMSGetCellStreamWrapper::get_ups_scan_cell_stream() {
  return &scan_cell_stream_;
}

