/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include "nameserver/batch_migrate_info.h"
#include "nameserver/chunk_server_manager.h"
namespace sb {
namespace nameserver {
using namespace common;
ObBatchMigrateInfo::ObBatchMigrateInfo(const common::ObRange* range,
                                       const int32_t src_server_index, const int32_t dest_server_index, const bool keep_src):
  range_(range), src_server_index_(src_server_index), dest_server_index_(dest_server_index), keep_src_(keep_src) {
}
ObBatchMigrateInfo::ObBatchMigrateInfo():
  range_(NULL), src_server_index_(OB_INVALID_INDEX), dest_server_index_(OB_INVALID_INDEX), keep_src_(true) {
}
const common::ObRange* ObBatchMigrateInfo::get_range() const {
  return range_;
}
int32_t ObBatchMigrateInfo::get_src_server_index() const {
  return src_server_index_;
}
int32_t ObBatchMigrateInfo::get_dest_server_index() const {
  return dest_server_index_;
}
bool ObBatchMigrateInfo::get_keep_src() const {
  return keep_src_;
}
ObBatchMigrateInfoManager::ObBatchMigrateInfoManager() {
  batch_migrate_infos_.init(MAX_BATCH_MIGRATE, data_holder_);
}
/*
 * add migrate info
 * @return 0 add ok, 1 reache MAX_BATCH_MIGRATE, 4 BE_BUSY
 */
int ObBatchMigrateInfoManager::add(const ObBatchMigrateInfo& bmi, const int64_t monotonic_now,
                                   ObChunkServerManager& chunk_server_manager) {
  int ret = ADD_OK;
  if (batch_migrate_infos_.get_array_index() < MAX_BATCH_MIGRATE) {
    //int src_thread = 0;
    //int dest_thread = 0;
    //for (int32_t i = 0; i < batch_migrate_infos_.get_array_index(); ++i)
    //{
    //  if (data_holder_[i].get_src_server_index() == bmi.get_src_server_index())
    //  {
    //    src_thread++;
    //  }
    //  if (data_holder_[i].get_dest_server_index() == bmi.get_dest_server_index())
    //  {
    //    dest_thread++;
    //  }
    //}
    //if (src_thread > MAX_SRC_THREAD)
    //{
    //  ret = ADD_TOO_MANY_SRC;
    //}else if (dest_thread > MAX_DEST_THREAD)
    //{
    //  ret = ADD_TOO_MANY_DEST;
    //}
    if (ADD_OK == ret) {
      ObServerStatus* src_status = chunk_server_manager.get_server_status(bmi.get_src_server_index());
      ObServerStatus* dest_status = chunk_server_manager.get_server_status(bmi.get_dest_server_index());
      if (src_status == NULL || dest_status == NULL) {
        TBSYS_LOG(ERROR, "you should not get this bugss!");
        ret = BE_BUSY;
      }
      if (src_status->migrate_out_finish_time_ < monotonic_now &&
          dest_status->migrate_in_finish_time_ < monotonic_now) {
        batch_migrate_infos_.push_back(bmi);
        //we will reset this when we real send migrate command to cs,
        //here we just make sure this src serve and dest serve will not too busy
        src_status->migrate_out_finish_time_ = monotonic_now;
        dest_status->migrate_in_finish_time_ = monotonic_now;
      } else {
        TBSYS_LOG(DEBUG, "serve is busy monotonic_now = %ld "
                  "src_index = %d, src_out_finish_time =%ld, "
                  "dest_index = %d dest_in_finish_time = %ld",
                  monotonic_now,
                  bmi.get_src_server_index(), src_status->migrate_out_finish_time_,
                  bmi.get_dest_server_index(), dest_status->migrate_in_finish_time_);
        ret = BE_BUSY;
      }
    }
  } else {
    ret = ADD_REACH_MAX;
  }
  return ret;
}
void ObBatchMigrateInfoManager::reset() {
  batch_migrate_infos_.init(MAX_BATCH_MIGRATE, data_holder_);
}
const ObBatchMigrateInfo* ObBatchMigrateInfoManager::begin() const {
  return data_holder_;
}
const ObBatchMigrateInfo* ObBatchMigrateInfoManager::end() const {
  return data_holder_ + batch_migrate_infos_.get_array_index();
}

}
}

