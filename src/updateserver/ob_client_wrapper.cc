/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_client_wrapper.cc for ...
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *
 */
#include "ob_client_wrapper.h"
#include "mergeserver/ob_ms_get_cell_stream.h"
#include "common/ob_define.h"

namespace sb {
namespace updateserver {
using namespace common;
using namespace mergeserver;
ObClientWrapper::ObClientWrapper(const int64_t rpc_retry_times,
                                 const int64_t rpc_timeout,
                                 const ObServer& root_server,
                                 const ObServer& update_server,
                                 const ObServer& merge_server): rpc_proxy_(rpc_retry_times, rpc_timeout, root_server, update_server, merge_server), ups_rpc_agent_(rpc_proxy_) {
  init_ = false;
}

ObClientWrapper::~ObClientWrapper() {
  if (NULL != ups_stream_) {
    delete ups_stream_;
  }
  if (NULL != ups_join_stream_) {
    delete ups_join_stream_;
  }
}

int ObClientWrapper::init(ObMergerRpcStub* rpc_stub,
                          ObMergerSchemaManager* schema,
                          ObMergerTabletLocationCache* cache,
                          ObMergerServiceMonitor* monitor) {
  int ret = OB_SUCCESS;
  if (init_) {
    TBSYS_LOG(WARN, "have already inited");
    ret = OB_ERROR;
  } else {
    if (NULL == rpc_stub || NULL == schema || NULL == cache) {
      TBSYS_LOG(WARN, "input param error, rpc=%p, schema=%p, cache=%p, monitor=%p",
                rpc_stub, schema, cache, monitor);
      ret = OB_ERROR;
    } else {
      if ((OB_SUCCESS == rpc_proxy_.init(rpc_stub, schema, cache, monitor))) {
        ups_stream_ = new(std::nothrow) ObMSGetCellStream(&rpc_proxy_);
        if (NULL == ups_stream_) {
          TBSYS_LOG(WARN, "new ups_stream failed");
          ret = OB_ERROR;
        } else {
          ups_join_stream_ = new(std::nothrow) ObMSGetCellStream(&rpc_proxy_);
          if (NULL == ups_join_stream_) {
            TBSYS_LOG(WARN, "new ups_join_stream failed");
            ret = OB_ERROR;
          } else {
            init_ = true;
          }
        }
      } else {
        ret = OB_ERROR;
      }
    }
  }
  return ret;
}


int ObClientWrapper::get_cell(sb::common::ObCellInfo * *cell) {
  return ups_rpc_agent_.get_cell(cell);
}

int ObClientWrapper::next_cell() {
  return ups_rpc_agent_.next_cell();
}

void ObClientWrapper::clear() {
  ups_rpc_agent_.clear();
}

int ObClientWrapper::get(const ObGetParam& get_param, const ObSchemaManagerV2& schema_mgr) {
  int ret = OB_SUCCESS;
  if (!init_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret) {
    const ObCellInfo* cell = get_param[0];
    if ((0 == get_param.get_cell_size()) || (NULL == cell)) {
      TBSYS_LOG(ERROR, "check get param failed:size[%ld], cell[%p]",
                get_param.get_cell_size(), cell);
      ret = OB_ERROR;
    } else {
      //to modify
      int64_t max_memory_size = 2 * 1024L * 1024L;
      ret = ups_rpc_agent_.set_request_param(get_param, *ups_stream_, *ups_join_stream_, schema_mgr, max_memory_size);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ups rpc agent set request param failed,ret=%d", ret);
      }
    }
  }
  return ret;
}
}//end of namespace updateserver
}//end of namespace sb


