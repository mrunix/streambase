/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_transfer_sstable_query.cc for ...
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tbsys.h>
#include "common/ob_get_param.h"
#include "common/ob_scan_param.h"
#include "sstable/ob_seq_sstable_scanner.h"
#include "sstable/ob_sstable_getter.h"
#include "ob_transfer_sstable_query.h"

namespace sb {
namespace updateserver {
using namespace tbsys;
using namespace common;
using namespace sstable;

ObTransferSSTableQuery::ObTransferSSTableQuery(common::IFileInfoMgr& fileinfo_mgr)
  : inited_(false), block_cache_(fileinfo_mgr),
    block_index_cache_(fileinfo_mgr) {

}

ObTransferSSTableQuery::~ObTransferSSTableQuery() {

}

int ObTransferSSTableQuery::init(const ObBlockCacheConf& bc_conf,
                                 const ObBlockIndexCacheConf& bic_conf) {
  int ret = OB_SUCCESS;

  //ret = fileinfo_cache_.init(bc_conf.ficache_max_num);
  if (OB_SUCCESS == ret) {
    ret = block_cache_.init(bc_conf);
  }

  if (OB_SUCCESS == ret) {
    ret = block_index_cache_.init(bic_conf);
  }

  if (OB_SUCCESS == ret) {
    inited_ = true;
  }

  return ret;
}

int ObTransferSSTableQuery::get(const ObGetParam& get_param,
                                ObSSTableReader& reader,
                                ObIterator* iterator) {
  int ret                         = OB_SUCCESS;
  int64_t cell_size               = get_param.get_cell_size();
  int64_t row_size                = get_param.get_row_size();
  ObSSTableGetter* sstable_getter = dynamic_cast<ObSSTableGetter*>(iterator);
  const ObGetParam::ObRowIndex* row_index = get_param.get_row_index();
  static __thread const ObSSTableReader* sstable_reader = NULL;
  sstable_reader = &reader;

  if (!inited_) {
    TBSYS_LOG(WARN, "transfer sstable query has not inited");
    ret = OB_ERROR;
  } else if (cell_size <= 0 || NULL == row_index || row_size <= 0
             || row_size > OB_MAX_GET_ROW_NUMBER) {
    TBSYS_LOG(WARN, "invalid get param, cell_size=%ld, row_index=%p, row_size=%ld",
              cell_size, row_index, row_size);
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == sstable_getter) {
    TBSYS_LOG(WARN, "get thread local instance of sstable getter failed");
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret) {
    ret = sstable_getter->init(block_cache_, block_index_cache_,
                               get_param, &sstable_reader, 1);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to init sstable getter, ret=%d", ret);
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObTransferSSTableQuery::scan(const ObScanParam& scan_param, ObSSTableReader& reader,
                                 ObIterator* iterator) {
  int ret                           = OB_SUCCESS;
  ObSeqSSTableScanner* seq_scanner  = dynamic_cast<ObSeqSSTableScanner*>(iterator);

  if (NULL == seq_scanner) {
    TBSYS_LOG(WARN, "failed to get thread local sequence scaner, seq_scanner=%p",
              seq_scanner);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ret = seq_scanner->set_scan_param(scan_param, block_cache_, block_index_cache_);
  }

  if (OB_SUCCESS == ret) {
    ret = seq_scanner->add_sstable_reader(&reader);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "add sstable reader object to seq scanner failed, "
                "seq_scanner=%p, reader=%p.",
                seq_scanner, &reader);
    } else {
      // do nothing
    }
  }

  return ret;
}
}//end namespace updateserver
}//end namespace sb


