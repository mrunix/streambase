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

#ifndef OCEANBASE_ROOT_SERVER_LOG_MANAGER_H_
#define OCEANBASE_ROOT_SERVER_LOG_MANAGER_H_

#include "common/ob_define.h"
#include "common/ob_log_writer.h"
#include "nameserver/nameserver_log_worker.h"
#include "nameserver/nameserver.h"

namespace sb {
namespace nameserver {
class ObRootLogManager : public common::ObLogWriter {
 public:
  static const int UINT64_MAX_LEN;
  ObRootLogManager();
  ~ObRootLogManager();

 public:
  int init(NameServer* name_server, common::ObSlaveMgr* slave_mgr);

  /// @brief replay all commit log from replay point
  /// after initialization, invoke this method to replay all commit log
  int replay_log();
  int do_after_recover_check_point();

  int recover_checkpoint(uint64_t ckpt_id);

  int load_server_status();

  int do_check_point(const uint64_t checkpoint_id = 0);

  int add_slave(const common::ObServer& server, uint64_t& new_log_file_id);

  const char* get_log_dir_path() const { return log_dir_; }
  inline uint64_t get_replay_point() {return replay_point_;}
  inline uint64_t get_check_point() {return ckpt_id_;}

  ObRootLogWorker* get_log_worker() { return &log_worker_; }
  tbsys::CThreadMutex* get_log_sync_mutex() { return &log_sync_mutex_; }

 private:
  uint64_t ckpt_id_;
  uint64_t replay_point_;
  uint64_t max_log_id_;
  int rt_server_status_;
  bool is_initialized_;
  bool is_log_dir_empty_;
  char log_dir_[common::OB_MAX_FILE_NAME_LENGTH];

  NameServer* name_server_;
  ObRootLogWorker log_worker_;
  tbsys::CThreadMutex log_sync_mutex_;
};
} /* nameserver */
} /* sb */

#endif /* end of include guard: OCEANBASE_ROOT_SERVER_LOG_MANAGER_H_ */

