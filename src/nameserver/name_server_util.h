/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * name_server_util.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_UTIL_H
#define _OB_ROOT_UTIL_H 1
#include "ob_chunk_server_manager.h"
#include "name_server_rpc_stub.h"
#include "common/ob_tablet_info.h"
#include <stdint.h>

namespace sb {
namespace nameserver {
class NameServerUtil {
 public:
  static int delete_tablets(NameServerRpcStub& rpc_stub, const ObChunkServerManager& server_manager, common::ObTabletReportInfoList& delete_list, const int64_t timeout);
};
} // end namespace nameserver
} // end namespace sb

#endif /* _OB_ROOT_UTIL_H */

