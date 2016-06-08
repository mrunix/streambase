/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_memtable_rowiter.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_MEMTABLE_ROWITER_H_
#define  OCEANBASE_UPDATESERVER_MEMTABLE_ROWITER_H_
#include <sys/types.h>
#include <dirent.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "ob_atomic.h"
#include "common/ob_define.h"
#include "common/ob_vector.h"
#include "common/page_arena.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_list.h"
#include "common/ob_regex.h"
#include "common/ob_fileinfo_manager.h"
#include "common/ob_tsi_factory.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_block_builder.h"
#include "sstable/ob_sstable_trailer.h"
#include "ob_ups_utils.h"
#include "ob_sstable_mgr.h"
#include "ob_column_filter.h"
#include "ob_memtable.h"
#include "ob_schema_mgrv2.h"

#define DEFAULT_COMPRESSOR_NAME "lzo_1.0"

namespace sb {
namespace updateserver {
typedef MemTableTransHandle TableTransHandle;

class CellInfoAllocator {
  typedef common::ObList<ObCellInfoNode*> ClearList;
  typedef common::hash::SimpleAllocer<ObCellInfoNode> Allocator;
 public:
  ObCellInfoNode* alloc(const int32_t nbyte) {
    ObCellInfoNode* ret = NULL;
    if (nbyte >= (int32_t)sizeof(ObCellInfoNode)) {
      if (NULL != (ret = allocator_.allocate())) {
        clear_list_.push_back(ret);
      }
    }
    return ret;
  };
  void reset() {
    ClearList::iterator iter;
    for (iter = clear_list_.begin(); iter != clear_list_.end(); iter++) {
      allocator_.deallocate(*iter);
    }
    clear_list_.clear();
  };
  void clear() {
    reset();
    clear_list_.destroy();
    allocator_.clear();
  };
 private:
  ClearList clear_list_;
  Allocator allocator_;
};

class MemTableRowIterator : public IRowIterator {
 public:
  MemTableRowIterator();
  virtual ~MemTableRowIterator();
 public:
  int init(MemTable* memtable,
           const char* compressor_name = DEFAULT_COMPRESSOR_NAME,
           const int64_t block_size = sstable::ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE,
           const int store_type = sstable::OB_SSTABLE_STORE_SPARSE);
  void destroy();
 public:
  virtual int next_row();
  virtual int get_row(sstable::ObSSTableRow& sstable_row);
  virtual int reset_iter();
  virtual bool get_compressor_name(common::ObString& compressor_str);
  virtual bool get_sstable_schema(sstable::ObSSTableSchema& sstable_schema);
  virtual bool get_store_type(int& store_type);
  virtual bool get_block_size(int64_t& block_size);
 private:
  void reset_();
  void revert_schema_handle_();
  bool get_schema_handle_();
 private:
  bool first_next_;
  MemTable* memtable_;
  TableEngineIterator memtable_iter_;
  TableEngineTransHandle memtable_trans_handle_;
  UpsSchemaMgr::SchemaHandle schema_handle_;
  int64_t block_size_;
  int store_type_;
  char compressor_name_[common::OB_MAX_COMPRESSOR_NAME_LENGTH];
  CellInfoAllocator allocator_;
};
}
}

#endif //OCEANBASE_UPDATESERVER_MEMTABLE_ROWITER_H_



