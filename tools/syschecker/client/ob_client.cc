/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_client.cc for define sb client API.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include "ob_client.h"

namespace sb {
namespace client {
using namespace common;

ObClient::ObClient(ObServerManager& servers_mgr)
  : servers_mgr_(servers_mgr), timeout_(DEFAULT_TIME_OUT) {

}

ObClient::~ObClient() {

}

int ObClient::init(const int64_t timeout) {
  int ret = OB_SUCCESS;

  if (timeout <= 0) {
    TBSYS_LOG(WARN, "invalid param, timeout=%ld", timeout);
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret) {
    ret = ObBaseClient::init();
    if (OB_SUCCESS == ret) {
      timeout_ = timeout;
      ret = rpc_stub_.init(&get_client_manager());
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "failed to init server rpc stub");
      }
    }
  }

  if (OB_SUCCESS == ret) {
    ret = start();
  }

  return ret;
}

int ObClient::fetch_schema(const int64_t timestap,
                           ObSchemaManagerV2& schema_mgr) {
  return (rpc_stub_.fetch_schema(servers_mgr_.get_root_server(), timestap,
                                 schema_mgr, timeout_));
}

int ObClient::fetch_update_server(ObServer& update_server) {
  return (rpc_stub_.fetch_update_server(servers_mgr_.get_root_server(),
                                        update_server, timeout_));
}

int ObClient::ms_scan(const ObScanParam& scan_param,
                      ObScanner& scanner) {
  return (rpc_stub_.scan(servers_mgr_.get_random_merge_server(),
                         scan_param, scanner, timeout_));
}

int ObClient::ms_get(const ObGetParam& get_param,
                     ObScanner& scanner) {
  return (rpc_stub_.get(servers_mgr_.get_random_merge_server(),
                        get_param, scanner, timeout_));
}

int ObClient::ups_apply(const ObMutator& mutator) {
  return (rpc_stub_.ups_apply(servers_mgr_.get_update_server(),
                              mutator, timeout_));
}

int ObClient::ups_scan(const ObScanParam& scan_param,
                       ObScanner& scanner) {
  return (rpc_stub_.scan(servers_mgr_.get_update_server(),
                         scan_param, scanner, timeout_));
}

int ObClient::ups_get(const ObGetParam& get_param,
                      ObScanner& scanner) {
  return (rpc_stub_.get(servers_mgr_.get_update_server(),
                        get_param, scanner, timeout_));
}

int ObClient::cs_scan(const ObScanParam& scan_param,
                      ObScanner& scanner) {
  return OB_SUCCESS;
  //    return (rpc_stub_.scan(servers_mgr_.get_chunk_server(),
  //                           scan_param, scanner, timeout_));
}

int ObClient::cs_get(const ObGetParam& get_param,
                     ObScanner& scanner) {
  return OB_SUCCESS;
  //    return (rpc_stub_.get(servers_mgr_.get_chunk_server(),
  //                          get_param, scanner, timeout_));
}
} // end namespace client
} // end namespace sb
