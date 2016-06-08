/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_mod_define.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include "ob_memory_pool.h"
#ifndef OCEANBASE_COMMON_OB_MOD_DEFINE_H_
#define OCEANBASE_COMMON_OB_MOD_DEFINE_H_
namespace sb {
namespace common {
struct ObModInfo {
  ObModInfo() {
    mod_id_  = 0;
    mod_name_ = NULL;
  }
  int32_t mod_id_;
  const char* mod_name_;
};


static const int32_t G_MAX_MOD_NUM = 2048;
extern ObModInfo OB_MOD_SET[G_MAX_MOD_NUM];


#define DEFINE_MOD(mod_name) mod_name

#define ADD_MOD(mod_name)\
  do \
  {\
    if (ObModIds::mod_name <= ObModIds::OB_MOD_END\
    && ObModIds::mod_name >= ObModIds::OB_MOD_DEFAULT \
    && ObModIds::mod_name < G_MAX_MOD_NUM) \
    {\
      OB_MOD_SET[ObModIds::mod_name].mod_id_ = ObModIds::mod_name; \
      OB_MOD_SET[ObModIds::mod_name].mod_name_ = # mod_name;\
    }\
  }while (0)

/// 使用方法：
/// 1. 在ObModIds中定义自己的模块名称, e.x. OB_MS_CELL_ARRAY
/// 2. 在init_ob_mod_set中添加之前定义的模块名称，e.x. ADD_MOD(OB_MS_CELL_ARRAY)
/// 3. 在调用ob_malloc的时候，使用定义的模块名称作为第二个参数，e.x. ob_malloc(512, ObModIds::OB_MS_CELL_ARRAY)
/// 4. 发现内存泄露，调用ob_print_memory_usage()打印每个模块的内存使用量，以发现内存泄露模块
/// 5. 也可以通过调用ob_get_mod_memory_usage(ObModIds::OB_MS_CELL_ARRAY)获取单个模块的内存使用量
class ObModIds {
 public:
  enum {
    OB_MOD_DEFAULT,
    /// define modules here
    // common modules
    OB_COMMON_NETWORK,
    OB_THREAD_BUFFER,
    OB_VARCACHE,
    OB_KVSTORE_CACHE,
    OB_TSI_FACTORY,
    OB_THREAD_OBJPOOL,
    OB_ROW_COMPACTION,

    // mergeserver modules
    OB_MS_CELL_ARRAY,
    OB_MS_LOCATION_CACHE,
    OB_MS_BTREE,
    OB_MS_RPC,
    OB_MS_JOIN_CACHE,

    // updateserver modules
    OB_UPS_ENGINE,
    OB_UPS_MEMTABLE,
    OB_UPS_LOG,
    OB_UPS_SCHEMA,

    // chunkserver modules
    OB_CS_SSTABLE_READER,
    OB_CS_TABLET_IMAGE,

    // sstable modules
    OB_SSTABLE_AIO,
    OB_SSTABLE_EGT_SCAN,
    OB_SSTABLE_WRITER,
    OB_SSTABLE_READER,

    OB_MOD_END
  };
};

inline void init_ob_mod_set() {
  ADD_MOD(OB_MOD_DEFAULT);
  /// add modules here, modules must be first defined
  ADD_MOD(OB_COMMON_NETWORK);
  ADD_MOD(OB_THREAD_BUFFER);
  ADD_MOD(OB_VARCACHE);
  ADD_MOD(OB_KVSTORE_CACHE);
  ADD_MOD(OB_TSI_FACTORY);
  ADD_MOD(OB_THREAD_OBJPOOL);
  ADD_MOD(OB_ROW_COMPACTION);

  ADD_MOD(OB_MS_CELL_ARRAY);
  ADD_MOD(OB_MS_LOCATION_CACHE);
  ADD_MOD(OB_MS_BTREE);
  ADD_MOD(OB_MS_RPC);
  ADD_MOD(OB_MS_JOIN_CACHE);

  ADD_MOD(OB_UPS_ENGINE);
  ADD_MOD(OB_UPS_MEMTABLE);
  ADD_MOD(OB_UPS_LOG);
  ADD_MOD(OB_UPS_SCHEMA);

  ADD_MOD(OB_CS_SSTABLE_READER);
  ADD_MOD(OB_CS_TABLET_IMAGE);

  ADD_MOD(OB_MOD_END);
}
class ObModSet : public sb::common::ObMemPoolModSet {
 public:
  ObModSet();
  virtual ~ObModSet() {
  }
  virtual int32_t get_mod_id(const char* mod_name)const;
  virtual const char* get_mod_name(const int32_t mod_id) const;
  virtual int32_t get_max_mod_num()const;
 private:
  int32_t mod_num_;
};
}
}

#endif /* OCEANBASE_COMMON_OB_MOD_DEFINE_H_ */

