/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_log_mgr.h for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#ifndef OCEANBASE_UPDATESERVER_OB_UPS_LOG_MGR_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_LOG_MGR_H_

#include "common/ob_define.h"
#include "common/ob_log_writer.h"
#include "common/ob_role_mgr.h"
#include "ob_ups_table_mgr.h"

namespace sb {
namespace tests {
namespace updateserver {
// forward decleration
class TestObUpsLogMgr_test_init_Test;
}
}
namespace updateserver {
/// scan log directory & load log replay point
/// serialize log replay point to file
class ObUpsLogMgr : public common::ObLogWriter {
  friend class tests::updateserver::TestObUpsLogMgr_test_init_Test;
 public:
  static const char* UPS_LOG_REPLAY_POINT_FILE;
  static const int UINT64_MAX_LEN;
 public:
  ObUpsLogMgr();
  virtual ~ObUpsLogMgr();
  int init(const char* log_dir, const int64_t log_file_max_size,
           common::ObSlaveMgr* slave_mgr, common::ObRoleMgr* role_mgr, int64_t log_sync_type);

  /// @brief replay all commit log from replay point
  /// after initialization, invoke this method to replay all commit log
  /// @param [in] table_mgr replay log to this UpsTableMgr
  int replay_log(ObUpsTableMgr& table_mgr);

  /// @brief set new replay point
  /// this method will write replay point to replay_point_file
  int write_replay_point(uint64_t replay_point);

  void set_replay_point(uint64_t replay_point) {replay_point_ = replay_point;}
  inline uint64_t get_replay_point() {return replay_point_;}

  int add_slave(const common::ObServer& server, uint64_t& new_log_file_id);

  inline uint64_t get_max_log_id() {return max_log_id_;}
  void get_cur_log_point(int64_t& log_file_id, int64_t& log_seq_id, int64_t& log_offset) {
    log_file_id = get_cur_log_file_id();
    log_seq_id = get_cur_log_seq();
    log_offset = get_cur_log_offset();
  }

 private:
  int load_replay_point_();

  inline int check_inner_stat() const {
    int ret = common::OB_SUCCESS;
    if (!is_initialized_) {
      TBSYS_LOG(ERROR, "ObUpsLogMgr has not been initialized");
      ret = common::OB_NOT_INIT;
    }
    return ret;
  }

 private:
  common::ObRoleMgr* role_mgr_;
  uint64_t replay_point_;
  uint64_t max_log_id_;
  bool is_initialized_;
  bool is_log_dir_empty_;
  char replay_point_fn_[common::OB_MAX_FILE_NAME_LENGTH];
  char log_dir_[common::OB_MAX_FILE_NAME_LENGTH];
};
} // end namespace updateserver
} // end namespace sb

#endif // OCEANBASE_UPDATESERVER_OB_UPS_LOG_MGR_H_


