/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_chunk_server_merger_proxy.h
 *
 * Authors:
 *     maoqi <maoqi@taobao.com>
 * Changes:
 *     qushan <qushan@taobao.com>
 *     yubai <yubai.lk@taobao.com>
 *     xielun <xielun.szd@taobao.com>
 *
 */

#ifndef OCEANBASE_CHUNKSERVER_OB_CHUNK_SERVER_MERGER_PROXY_H_
#define OCEANBASE_CHUNKSERVER_OB_CHUNK_SERVER_MERGER_PROXY_H_

#include "mergeserver/ob_ms_rpc_proxy.h"
#include "ob_merge_reader.h"
#include "common/ob_cell_array.h"

namespace sb {
namespace chunkserver {
class ObTabletManager;

class ObChunkServerMergerProxy : public mergeserver::ObMergerRpcProxy {
 public:
  ObChunkServerMergerProxy(ObTabletManager& manager);
  virtual ~ObChunkServerMergerProxy();

 public:

  virtual int cs_scan(const common::ObScanParam& scan_param,
                      mergeserver::ObMergerTabletLocation& addr,
                      common::ObScanner& scanner,
                      common::ObIterator*& it_out);

  void reset();

 private:
  ObMergeReader cs_reader_;
  common::ObCellArray cell_array_;
  int64_t max_memory_size_;
};

} /* chunkserver */

} /* sb */
#endif
