/*
 * src/nameserver/.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for .
 *
 * Library: nameserver
 * Package: nameserver
 * Module :
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef OCEANBASE_ROOTSERVER_OB_BATCH_MIGRATE_INFO_H_
#define OCEANBASE_ROOTSERVER_OB_BATCH_MIGRATE_INFO_H_
#include "common/ob_range.h"
#include "common/ob_array_helper.h"
namespace sb {
namespace nameserver {
class ObChunkServerManager;
//we will find some tablet to migrate parallel
class ObBatchMigrateInfo {
 public:
  ObBatchMigrateInfo(const common::ObRange* range, const int32_t src_server_index, const int32_t dest_server_index, const bool keep_src);
  ObBatchMigrateInfo();
  const common::ObRange* get_range() const;
  int32_t get_src_server_index() const;
  int32_t get_dest_server_index() const;
  bool get_keep_src() const;
 private:
  const common::ObRange* range_;
  int32_t src_server_index_;
  int32_t dest_server_index_;
  bool keep_src_;
};
class ObBatchMigrateInfoManager {
 public:
  enum {
    MAX_BATCH_MIGRATE = 20,
    MAX_SRC_THREAD = 2,
    MAX_DEST_THREAD = 2,
  };
  enum {
    ADD_OK = 0,
    ADD_REACH_MAX = 1,
    ADD_TOO_MANY_SRC = 2,
    ADD_TOO_MANY_DEST = 3,
    BE_BUSY = 4,
  };

  ObBatchMigrateInfoManager();

  int add(const ObBatchMigrateInfo& bmi, const int64_t monotonic_now,
          ObChunkServerManager& chunk_server_manager);
  void reset();
  const ObBatchMigrateInfo* begin() const;
  const ObBatchMigrateInfo* end() const;

 private:
  ObBatchMigrateInfo data_holder_[MAX_BATCH_MIGRATE];
  common::ObArrayHelper<ObBatchMigrateInfo> batch_migrate_infos_;
};
}
}

#endif

