/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_transfer_sstable_query.h for ...
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_UPDATESERVER_OB_TRANSFER_SSTABLE_QUERY_H_
#define OCEANBASE_UPDATESERVER_OB_TRANSFER_SSTABLE_QUERY_H_

#include "common/thread_buffer.h"
#include "common/ob_fileinfo_manager.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_sstable_mgr.h"

namespace sb {
namespace updateserver {
class common::ObGetParam;
class common::ObScanParam;
class common::ObIterator;
class sstable::ObSSTableReader;

class ObTransferSSTableQuery {
 public:
  ObTransferSSTableQuery(common::IFileInfoMgr& fileinfo_mgr);
  ~ObTransferSSTableQuery();

  int init(const sstable::ObBlockCacheConf& bc_conf,
           const sstable::ObBlockIndexCacheConf& bic_conf);

  int get(const common::ObGetParam& get_param, sstable::ObSSTableReader& reader,
          common::ObIterator* iterator);

  int scan(const common::ObScanParam& scan_param, sstable::ObSSTableReader& reader,
           common::ObIterator* iterator);

  sstable::ObBlockIndexCache& get_block_index_cache() {
    return block_index_cache_;
  };

 private:
  DISALLOW_COPY_AND_ASSIGN(ObTransferSSTableQuery);

  bool inited_;
  sstable::ObBlockCache block_cache_;
  sstable::ObBlockIndexCache block_index_cache_;
};
}//end namespace updateserver
}//end namespace sb

#endif  // OCEANBASE_UPDATESERVER_OB_TRANSFER_SSTABLE_QUERY_H_


