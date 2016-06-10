/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_update_server.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "common/ob_trace_log.h"
#include "common/serialization.h"
#include "common/utility.h"
#include "common/ob_log_dir_scanner.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_log_cursor.h"
#include "sstable/ob_aio_buffer_mgr.h"
#include "ob_update_server.h"
#include "ob_ups_utils.h"
#include "ob_update_server_main.h"
using namespace sb::common;

#define RPC_CALL_WITH_RETRY(function, retry_times, timeout, args...) \
  ({ \
    int err = OB_RESPONSE_TIME_OUT; \
    for (int64_t i = 0; ObRoleMgr::STOP != role_mgr_.get_state() \
        && ObRoleMgr::ERROR != role_mgr_.get_state() \
        && OB_SUCCESS != err && i < retry_times; ++i) \
    { \
      int64_t timeu = tbsys::CTimeUtil::getMonotonicTime(); \
      err = ups_rpc_stub_.function(args, timeout); \
      TBSYS_LOG(INFO, "%s, retry_times=%ld, err=%d", #function, i, err); \
      timeu = tbsys::CTimeUtil::getMonotonicTime() - timeu; \
      if (OB_SUCCESS != err \
          && timeu < timeout) \
      { \
        TBSYS_LOG(INFO, "timeu=%ld not match timeout=%ld, will sleep %ld", timeu, timeout, timeout - timeu); \
        int sleep_ret = precise_sleep(timeout - timeu); \
        if (OB_SUCCESS != sleep_ret) \
        { \
          TBSYS_LOG(ERROR, "precise_sleep ret=%d"); \
        } \
      } \
    } \
    err; \
  })

namespace sb {
namespace updateserver {
static const int32_t ADDR_BUF_LEN = 64;

ObUpdateServer::ObUpdateServer(ObUpdateServerParam& param)
  : param_(param),
    rpc_buffer_(RPC_BUFFER_SIZE),
    read_task_queue_size_(DEFAULT_TASK_READ_QUEUE_SIZE),
    write_task_queue_size_(DEFAULT_TASK_WRITE_QUEUE_SIZE),
    lease_task_queue_size_(DEFAULT_TASK_LEASE_QUEUE_SIZE),
    log_task_queue_size_(DEFAULT_TASK_LOG_QUEUE_SIZE),
    store_thread_queue_size_(DEFAULT_STORE_THREAD_QUEUE_SIZE),
    sstable_query_(sstable_mgr_) {
}

ObUpdateServer::~ObUpdateServer() {
}

int ObUpdateServer::initialize() {
  int err = OB_SUCCESS;

  // do not handle batch packet.
  // process packet one by one.
  set_batch_process(false);

  uint32_t vip = 0;

  vip = tbsys::CNetUtil::getAddr(param_.get_ups_vip());
  if (tbsys::CNetUtil::isLocalAddr(vip)) {
    TBSYS_LOG(INFO, "automatic detection: role=MASTER");
    role_mgr_.set_role(ObRoleMgr::MASTER);
  } else {
    TBSYS_LOG(INFO, "automatic detection: role=SLAVE");
    role_mgr_.set_role(ObRoleMgr::SLAVE);
  }
  ups_master_.set_ipv4_addr(param_.get_ups_vip(), param_.get_ups_port());

  name_server_.set_ipv4_addr(param_.get_root_server_ip(), param_.get_root_server_port());

  if (strlen(param_.get_ups_inst_master_ip()) > 0) {
    ups_inst_master_.set_ipv4_addr(param_.get_ups_inst_master_ip(), param_.get_ups_inst_master_port());
    TBSYS_LOG(INFO, "UPS Instance Master is %s:%p", param_.get_ups_inst_master_ip(), param_.get_ups_inst_master_port());
    obi_slave_stat_ = FOLLOWED_SLAVE;

    char addr_buf[ADDR_BUF_LEN];
    ups_inst_master_.to_string(addr_buf, sizeof(addr_buf));
    addr_buf[ADDR_BUF_LEN - 1] = '\0';
    TBSYS_LOG(INFO, "Slave stat set to FOLLOWED_SLAVE: ups_inst_master_=%s", addr_buf);
  } else if (strlen(param_.get_lsync_ip()) > 0) {
    lsync_server_.set_ipv4_addr(param_.get_lsync_ip(), param_.get_lsync_port());
    obi_slave_stat_ = STANDALONE_SLAVE;

    char addr_buf[ADDR_BUF_LEN];
    lsync_server_.to_string(addr_buf, sizeof(addr_buf));
    addr_buf[ADDR_BUF_LEN - 1] = '\0';
    TBSYS_LOG(INFO, "Slave stat set to STANDALONE_SLAVE, lsync_server=%s", addr_buf);
  } else {
    obi_slave_stat_ = UNKNOWN_SLAVE;
  }

  if (OB_SUCCESS == err) {
    read_task_queue_size_ = param_.get_read_task_queue_size();
    write_task_queue_size_ = param_.get_write_task_queue_size();
    lease_task_queue_size_ = param_.get_lease_task_queue_size();
    log_task_queue_size_ = param_.get_log_task_queue_size();
    store_thread_queue_size_ = param_.get_store_thread_queue_size();
  }

  if (OB_SUCCESS == err) {
    err = client_manager_.initialize(get_transport(), get_packet_streamer());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to init client manager, err=%d", err);
    }
  }

  if (OB_SUCCESS == err) {
    err = ups_rpc_stub_.init(&client_manager_);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to init common rpc stub, err=%d", err);
    }
  }

  if (OB_SUCCESS == err) {
    err = set_listen_port(param_.get_ups_port());
  }

  if (OB_SUCCESS == err) {
    err = set_self_(param_.get_dev_name(), param_.get_ups_port());
  }

  if (OB_SUCCESS == err) {
    err = check_thread_.init(&role_mgr_, vip, param_.get_state_check_period_us(),
                             &ups_rpc_stub_, &ups_master_, &self_addr_);
  }

  // set packet factory object
  if (OB_SUCCESS == err) {
    err = set_packet_factory(&packet_factory_);
  }

  // init slave_mgr
  if (OB_SUCCESS == err) {
    err = slave_mgr_.init(
            vip,
            &ups_rpc_stub_,
            param_.get_log_sync_timeout_us(),
            param_.get_lease_interval_us(),
            param_.get_lease_reserved_time_us(),
            param_.get_log_sync_retry_times(),
            param_.get_slave_fail_wait_lease_on() != 0);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to init slave mgr, err=%d", err);
    }
  }

  // init mgr
  if (OB_SUCCESS == err) {
    err = table_mgr_.init();
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to init table mgr, err=%d", err);
    } else {
      table_mgr_.set_replay_checksum_flag(param_.get_replay_checksum_flag());
      ob_set_memory_size_limit(param_.get_total_memory_limit());
      MemTableAttr memtable_attr;
      if (OB_SUCCESS == table_mgr_.get_memtable_attr(memtable_attr)) {
        memtable_attr.total_memlimit = param_.get_table_memory_limit();
        table_mgr_.set_memtable_attr(memtable_attr);
      }
    }
  }

  if (OB_SUCCESS == err) {
    const char* store_root = param_.get_store_root();
    const char* raid_regex = param_.get_raid_regex();
    const char* dir_regex = param_.get_dir_regex();
    err = sstable_mgr_.init(store_root, raid_regex, dir_regex);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to init sstable mgr, err=%d", err);
    } else {
      table_mgr_.reg_table_mgr(sstable_mgr_);
      if (!sstable_mgr_.load_new()) {
        TBSYS_LOG(WARN, "sstable mgr load new fail");
        err = OB_ERROR;
      } else if (OB_SUCCESS != (err = table_mgr_.check_sstable_id())) {
        TBSYS_LOG(WARN, "check sstable id fail err=%d", err);
      } else {
        table_mgr_.log_table_info();
      }
    }
  }

  if (OB_SUCCESS == err) {
    sstable::ObBlockCacheConf bc_conf;
    sstable::ObBlockIndexCacheConf bic_conf;
    bc_conf.block_cache_memsize_mb = param_.get_blockcache_size_mb();
    bic_conf.cache_mem_size = param_.get_blockindex_cache_size_mb() * 1024L * 1024L;
    err = sstable_query_.init(bc_conf, bic_conf);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to init sstable query, err=%d", err);
    }
  }

  if (OB_SUCCESS == err) {
    int64_t read_thread_count = param_.get_read_thread_count();
    log_thread_queue_.setThreadParameter(1, this, NULL);
    read_thread_queue_.setThreadParameter(read_thread_count, this, NULL);
    //read_thread_queue_.setWaitTime(param_.get_high_priv_wait_time_us() / 1000L);
    write_thread_queue_.setThreadParameter(1, this, NULL);
    lease_thread_queue_.setThreadParameter(1, this, NULL);
    store_thread_.setThreadParameter(1, this, NULL);

    is_first_log_ = true;
  }

  if (OB_SUCCESS == err) {
    err = timer_.init();
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "ObTimer init error, err=%d", err);
    }
  }

  return err;
}

void ObUpdateServer::wait_for_queue() {
  /*
  log_thread_queue_.wait();
  read_thread_queue_.wait();
  write_thread_queue_.wait();
  */
}

void ObUpdateServer::destroy() {
  role_mgr_.set_state(ObRoleMgr::STOP);

}

void ObUpdateServer::cleanup() {
  /// 写线程
  write_thread_queue_.stop();

  /// 读线程
  read_thread_queue_.stop();

  /// Lease线程
  lease_thread_queue_.stop();

  /// Check线程
  check_thread_.stop();

  /// 转储线程
  store_thread_.stop();


  /// 写线程
  write_thread_queue_.wait();

  /// 读线程
  read_thread_queue_.wait();

  /// Lease线程
  lease_thread_queue_.wait();

  /// Check线程
  check_thread_.wait();

  /// 转储线程
  store_thread_.wait();


  if (ObiRole::SLAVE == obi_role_.get_role() || ObRoleMgr::SLAVE == role_mgr_.get_role()) {
    /// 抓取日志线程
    fetch_thread_.stop();

    /// 日志回放线程
    log_replay_thread_.stop();

    /// 抓取日志线程
    fetch_thread_.wait();

    /// 日志回放线程
    log_replay_thread_.wait();
  }

  timer_.destroy();
  transport_.stop();
  transport_.wait();
}

int ObUpdateServer::start_service() {
  int err = OB_SUCCESS;

  err = start_threads();

  if (OB_SUCCESS == err) {
    RPC_CALL_WITH_RETRY(get_obi_role, INT64_MAX, DEFAULT_NETWORK_TIMEOUT, name_server_, obi_role_);

    if (obi_role_.get_role() == ObiRole::INIT) {
      TBSYS_LOG(INFO, "get_obi_role=INIT, waiting for new obi role");
      while (OB_SUCCESS == err
             && obi_role_.get_role() == ObiRole::INIT
             && ObRoleMgr::STOP != role_mgr_.get_state()
             && ObRoleMgr::ERROR != role_mgr_.get_state()) {
        usleep(DEFAULT_GET_OBI_ROLE_INTERVAL);
        RPC_CALL_WITH_RETRY(get_obi_role, INT64_MAX, DEFAULT_NETWORK_TIMEOUT, name_server_, obi_role_);
      }
    }

    if (ObRoleMgr::STOP == role_mgr_.get_state()
        || ObRoleMgr::ERROR == role_mgr_.get_state()) {
      err = OB_ERROR;
    } else {
      TBSYS_LOG(INFO, "obi_role=%s", obi_role_.get_role() == ObiRole::MASTER ? "MASTER" : "SLAVE");
    }
  }

  if (OB_SUCCESS == err) {
    ObRoleMgr::Role role = role_mgr_.get_role();

    if (ObiRole::MASTER == obi_role_.get_role()) {
      if (ObRoleMgr::MASTER == role) {
        err = start_master_master_();
      } else if (ObRoleMgr::SLAVE == role) {
        err = start_master_slave_();
      } else if (ObRoleMgr::STANDALONE == role) {
        err = start_standalone_();
      } else {
        TBSYS_LOG(WARN, "invalid ObRoleMgr role, role=%d", role);
        err = OB_ERROR;
      }
    } else if (ObiRole::SLAVE == obi_role_.get_role()) {
      if (ObRoleMgr::MASTER == role) {
        err = start_slave_master_();
      } else if (ObRoleMgr::SLAVE == role) {
        err = start_slave_slave_();
      } else if (ObRoleMgr::STANDALONE == role) {
        err = start_standalone_();
      } else {
        TBSYS_LOG(WARN, "invalid ObRoleMgr role, role=%d", role);
        err = OB_ERROR;
      }
    } else {
      TBSYS_LOG(WARN, "invalid ObiRole role, role=%d", obi_role_.get_role());
      err = OB_ERROR;
    }
  }

  cleanup();
  TBSYS_LOG(INFO, "server stoped.");

  return err;
}

void ObUpdateServer::stop() {
  ObRoleMgr::State server_stat = role_mgr_.get_state();
  if (ObRoleMgr::HOLD == server_stat) {
    TBSYS_LOG(WARN, "server stat will switch to HOLD");
    while (true) {
      usleep(10 * 1000); // sleep 10ms
    }
  }

  if (!stoped_) {
    stoped_ = true;
    destroy();
  }
}

int ObUpdateServer::start_threads() {
  int ret = OB_SUCCESS;

  /// 写线程
  write_thread_queue_.start();

  /// 读线程
  read_thread_queue_.start();

  /// Lease线程
  lease_thread_queue_.start();

  /// Check线程
  check_thread_.start();

  /// 转储线程
  store_thread_.start();

  return ret;
}

int ObUpdateServer::start_master_master_() {
  int err = OB_SUCCESS;

  if (OB_SUCCESS == err) {
    err = set_schema();
  }

  if (OB_SUCCESS == err) {
    int64_t log_file_max_size = param_.get_log_size_mb() * 1000L * 1000L;
    err = log_mgr_.init(param_.get_log_dir_path(), log_file_max_size,
                        &slave_mgr_, &role_mgr_, param_.get_log_sync_type());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to init log mgr, path=%s, log_file_size=%ld, err=%d",
                param_.get_log_dir_path(), log_file_max_size, err);
    } else {
      uint64_t log_id = sstable_mgr_.get_max_clog_id();
      TBSYS_LOG(INFO, "get max clog id=%lu", log_id);
      if (OB_INVALID_ID != log_id) {
        log_mgr_.set_replay_point(log_id);
      }

      if (OB_SUCCESS == err) {
        if (OB_SUCCESS != (err = log_mgr_.replay_log(table_mgr_))) {
          TBSYS_LOG(WARN, "failed to replay log, err=%d", err);
        } else if (OB_SUCCESS != (err = log_mgr_.start_log(log_mgr_.get_max_log_id(), log_mgr_.get_cur_log_seq()))) {
          TBSYS_LOG(ERROR, "ObUpsLogMgr start_log[get_max_log_id()=%lu get_cur_log_seq()=%lu",
                    log_mgr_.get_max_log_id(), log_mgr_.get_cur_log_seq());
        } else if (OB_SUCCESS != (err = table_mgr_.write_start_log())) {
          TBSYS_LOG(ERROR, "write start flag to commitlog fail err=%d", err);
        } else if (OB_SUCCESS != (err = table_mgr_.sstable_scan_finished(param_.get_minor_num_limit()))) {
          TBSYS_LOG(ERROR, "sstable_scan_finished error, err=%d", err);
        } else {
          table_mgr_.log_table_info();
        }
      }
    }
  }

  if (OB_SUCCESS == err) {
    // fetch schema
    bool write_log = true;
    err = update_schema(true, write_log);
  }

  // start read and write thread
  if (OB_SUCCESS == err) {
    err = set_timer_major_freeze();
    if (OB_SUCCESS == err) {
      err = set_timer_handle_fronzen();
    }
  }

  // modify master status
  if (OB_SUCCESS == err) {
    role_mgr_.set_state(ObRoleMgr::ACTIVE);
    err = submit_report_freeze();
  }

  if (OB_SUCCESS == err) {
    TBSYS_LOG(INFO, "start service, role=MASTER");
  } else {
    TBSYS_LOG(ERROR, "start service fail, role=MASTER, err=%d", err);
  }

  // wait finish
  while (ObiRole::MASTER == obi_role_.get_role()
         && ObRoleMgr::ACTIVE == role_mgr_.get_state()) {
    usleep(10 * 1000);
  }

  return err;
}

int ObUpdateServer::start_master_slave_() {
  int err = OB_SUCCESS;

  if (OB_SUCCESS == err) {
    //log_thread_queue_.start();
  }

  if (OB_SUCCESS == err) {
    err = set_schema();
  }

  if (OB_SUCCESS == err) {
    err = init_slave_log_mgr();
  }

  uint64_t replay_point;
  if (OB_SUCCESS == err) {
    err = register_and_start_fetch(ups_master_, replay_point);
  }

  if (OB_SUCCESS == err) {
    err = init_replay_thread(replay_point, 0);
  }

  // modify slave status
  if (OB_SUCCESS == err) {
    role_mgr_.set_state(ObRoleMgr::ACTIVE);
  }

  if (OB_SUCCESS == err) {
    err = timer_.init();
    if (OB_SUCCESS == err) {
      err = set_timer_handle_fronzen();
    }
  }

  if (OB_SUCCESS == err) {
    TBSYS_LOG(INFO, "start service, role=SLAVE");
  } else {
    TBSYS_LOG(ERROR, "start service fail, role=SLAVE, err=%d", err);
  }

  while (OB_SUCCESS == err
         && ObiRole::MASTER == obi_role_.get_role()
         && ObRoleMgr::STOP != role_mgr_.get_state()
         && ObRoleMgr::ERROR != role_mgr_.get_state()
         && ObRoleMgr::HOLD != role_mgr_.get_state()) {
    // waiting slave switching
    while (ObiRole::MASTER == obi_role_.get_role()
           && ObRoleMgr::ACTIVE == role_mgr_.get_state()) {
      // check status
      usleep(10 * 1000); // sleep 10ms
    }

    if (ObiRole::MASTER != obi_role_.get_role()) {
      TBSYS_LOG(WARN, "ObiRole is not MASTER");
    } else if (ObRoleMgr::ERROR == role_mgr_.get_state()) {
      TBSYS_LOG(WARN, "ERROR occurs");
    } else if (ObRoleMgr::STOP == role_mgr_.get_state()) {
      TBSYS_LOG(INFO, "STOP slave");
    } else if (ObRoleMgr::SWITCHING == role_mgr_.get_state()) {
      // switching to master
      TBSYS_LOG(INFO, "SWITCHING state happen");
      role_mgr_.set_role(ObRoleMgr::MASTER);

      //log_thread_queue_.stop();
      //log_thread_queue_.wait();
      write_thread_queue_.stop();
      write_thread_queue_.wait();
      write_thread_queue_.clear();
      log_replay_thread_.wait();
      fetch_thread_.wait();

      write_thread_queue_.start();

      err = table_mgr_.sstable_scan_finished(param_.get_minor_num_limit());
      table_mgr_.log_table_info();

      if (OB_SUCCESS == err) {
        bool repeat = true;
        err = timer_.schedule(major_freeze_duty_, MajorFreezeDuty::SCHEDULE_PERIOD, repeat);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "schedule major_freeze_duty fail err=%d", err);
        }
      }

      if (OB_SUCCESS == err) {
        err = submit_report_freeze();
      }

      role_mgr_.set_state(ObRoleMgr::ACTIVE);

      TBSYS_LOG(INFO, "switch SLAVE ==> MASTER succ");
      // wait finish
      while (OB_SUCCESS == err) {
        if (ObRoleMgr::STOP == role_mgr_.get_state()
            || ObRoleMgr::ERROR == role_mgr_.get_state()) {
          break;
        }
        usleep(10 * 1000); // sleep 10ms
      }
    } else if (ObRoleMgr::INIT == role_mgr_.get_state()) {
      // re-register
      TBSYS_LOG(INFO, "REREGISTER slave");
      // waiting fetch thread complete
      fetch_thread_.wait();

      //log_thread_queue_.stop();
      //log_thread_queue_.wait();
      //log_thread_queue_.clear();
      write_thread_queue_.stop();
      write_thread_queue_.wait();
      write_thread_queue_.clear();

      is_first_log_ = true;
      is_registered_ = false;

      log_mgr_.reset_log();

      //log_thread_queue_.start();
      write_thread_queue_.start();

      ObUpsFetchParam fetch_param;
      // slave register
      if (OB_SUCCESS == err) {
        err = slave_register_followed(ups_master_, fetch_param);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "failed to register, err=%d", err);
        }
      }

      // restart fetch thread
      if (OB_SUCCESS == err) {
        //fetch_param.min_log_id_ = last_max_log_id;
        TBSYS_LOG(INFO, "restart fetch thread, min_log_id=%lu, max_log_id=%lu",
                  fetch_param.min_log_id_, fetch_param.max_log_id_);

        fetch_thread_.clear();
        fetch_thread_.set_fetch_param(fetch_param);
        fetch_thread_.start();
      }

      if (OB_SUCCESS != err) {
        role_mgr_.set_state(ObRoleMgr::ERROR);
        TBSYS_LOG(INFO, "REREGISTER slave err");
      } else {
        role_mgr_.set_state(ObRoleMgr::ACTIVE);
        TBSYS_LOG(INFO, "REREGISTER slave succ");
      }
    }
  }

  if (ObRoleMgr::SLAVE == role_mgr_.get_role() && is_registered_) {
    ups_rpc_stub_.slave_quit(ups_master_, get_self(), DEFAULT_SLAVE_QUIT_TIMEOUT);
    is_registered_ = false;
  }

  cleanup();
  transport_.stop();
  transport_.wait();
  TBSYS_LOG(INFO, "server stoped.");

  return err;
}

int ObUpdateServer::start_slave_master_() {
  int err = OB_SUCCESS;

  if (OB_SUCCESS == err) {
    err = set_schema();
  }

  if (OB_SUCCESS == err) {
    err = init_slave_log_mgr();
  }

  uint64_t log_id_start = 0, log_seq_start = 0;
  if (OB_SUCCESS == err) {
    if (FOLLOWED_SLAVE == obi_slave_stat_) {
      err = register_and_start_fetch(ups_inst_master_, log_id_start);
    } else if (STANDALONE_SLAVE == obi_slave_stat_) {
      err = slave_standalone_prepare(log_id_start, log_seq_start);
    } else {
      TBSYS_LOG(ERROR, "Not FOLLOWED_SLAVE nor STANDALONE_SLAVE");
      err = OB_ERROR;
    }
  }

  if (OB_SUCCESS == err) {
    err = init_replay_thread(log_id_start, log_seq_start);
  }

  if (OB_SUCCESS == err) {
    err = set_timer_handle_fronzen();
  }

  if (OB_SUCCESS == err) {
    return enter_slave_master();
  } else {
    return err;
  }
}

int ObUpdateServer::start_slave_slave_() {
  int ret = OB_SUCCESS;

  return ret;
}

int ObUpdateServer::enter_master_master() {
  int ret = OB_SUCCESS;

  role_mgr_.set_state(ObRoleMgr::ACTIVE);

  while (ObiRole::MASTER == obi_role_.get_role()
         && ObRoleMgr::ACTIVE == role_mgr_.get_state()) {
    usleep(10 * 1000);
  }

  return ret;
}

int ObUpdateServer::enter_slave_master() {
  int ret = OB_SUCCESS;

  role_mgr_.set_state(ObRoleMgr::ACTIVE);
  while (ObiRole::SLAVE == obi_role_.get_role()
         && ObRoleMgr::ACTIVE == role_mgr_.get_state()) {
    usleep(10 * 1000);
  }

  if (ObiRole::SLAVE == obi_role_.get_role()) {
    if (ObRoleMgr::ERROR == role_mgr_.get_state()) {
      TBSYS_LOG(WARN, "ERROR occurs");
      ret = OB_ERROR;
    } else if (ObRoleMgr::STOP == role_mgr_.get_state()) {
      TBSYS_LOG(INFO, "STOP slave");
    } else if (ObRoleMgr::INIT == role_mgr_.get_state()) {
      if (FOLLOWED_SLAVE == obi_slave_stat_) {
        ret = reregister_followed(ups_inst_master_);
      } else if (STANDALONE_SLAVE == obi_slave_stat_) {
        ret = reregister_standalone();
      } else {
        TBSYS_LOG(WARN, "unknown ObiSlaveStat=%d", obi_slave_stat_);
        ret = OB_ERROR;
      }
      if (OB_SUCCESS != ret) {
        role_mgr_.set_state(ObRoleMgr::ERROR);
      } else {
        return enter_slave_master();
      }
    }
  } else if (ObiRole::MASTER == obi_role_.get_role()) {
    ret = switch_to_master_master();
    if (OB_SUCCESS != ret) {
      role_mgr_.set_state(ObRoleMgr::ERROR);
      TBSYS_LOG(ERROR, "switch SLAVE ==> MASTER failed");
    } else {
      TBSYS_LOG(INFO, "switch SLAVE ==> MASTER succ");
      return enter_master_master();
    }
  } else {
    TBSYS_LOG(ERROR, "Unknown ObiRole: %d", obi_role_.get_role());
    ret = OB_ERROR;
  }

  return ret;
}

int ObUpdateServer::switch_to_master_master() {
  int err = OB_SUCCESS;

  // switching to master
  TBSYS_LOG(INFO, "SWITCHING state happen");
  role_mgr_.set_role(ObRoleMgr::MASTER);

  //log_thread_queue_.stop();
  //log_thread_queue_.wait();
  write_thread_queue_.stop();
  write_thread_queue_.wait();
  write_thread_queue_.clear();
  if (STANDALONE_SLAVE == obi_slave_stat_) {
    fetch_lsync_.stop();
    fetch_lsync_.wait();
  }
  log_replay_thread_.wait();
  fetch_thread_.wait();

  write_thread_queue_.start();
  err = table_mgr_.sstable_scan_finished(param_.get_minor_num_limit());
  table_mgr_.log_table_info();

  if (OB_SUCCESS == err) {
    err = set_timer_major_freeze();
  }

  return err;
}

int ObUpdateServer::reregister_followed(const ObServer& master) {
  int err = OB_SUCCESS;

  TBSYS_LOG(INFO, "reregister FOLLOWED SLAVE");
  // waiting fetch thread complete
  fetch_thread_.wait();

  //log_thread_queue_.stop();
  //log_thread_queue_.wait();
  //log_thread_queue_.clear();
  write_thread_queue_.stop();
  write_thread_queue_.wait();
  write_thread_queue_.clear();

  is_first_log_ = true;
  is_registered_ = false;

  log_mgr_.reset_log();

  //log_thread_queue_.start();
  write_thread_queue_.start();

  ObUpsFetchParam fetch_param;
  // slave register
  if (OB_SUCCESS == err) {
    err = slave_register_followed(master, fetch_param);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to register, err=%d", err);
    }
  }

  // restart fetch thread
  if (OB_SUCCESS == err) {
    //fetch_param.min_log_id_ = last_max_log_id;
    TBSYS_LOG(INFO, "restart fetch thread, min_log_id=%lu, max_log_id=%lu",
              fetch_param.min_log_id_, fetch_param.max_log_id_);

    fetch_thread_.clear();
    fetch_thread_.set_fetch_param(fetch_param);
    fetch_thread_.start();
  }

  if (OB_SUCCESS != err) {
    TBSYS_LOG(INFO, "REREGISTER err");
  } else {
    TBSYS_LOG(INFO, "REREGISTER succ");
  }
  return err;
}

int ObUpdateServer::reregister_standalone() {
  int err = OB_SUCCESS;

  TBSYS_LOG(INFO, "reregister STANDALONE SLAVE");
  is_registered_ = false;

  // slave register
  if (OB_SUCCESS == err) {
    uint64_t log_id_start = 0;
    uint64_t log_seq_start = 0;
    err = slave_register_standalone(log_id_start, log_seq_start);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to register, err=%d", err);
    }
  }

  if (OB_SUCCESS != err) {
    TBSYS_LOG(INFO, "REREGISTER err");
  } else {
    TBSYS_LOG(INFO, "REREGISTER succ");
  }
  return err;
}

int ObUpdateServer::set_schema() {
  int err = OB_SUCCESS;

  if (!table_mgr_.get_schema_mgr().has_schema()) {
    bool write_log = false;
    err = update_schema(true, write_log);
  }

  return err;
}

int ObUpdateServer::set_timer_major_freeze() {
  int err = OB_SUCCESS;

  bool repeat = true;
  err = timer_.schedule(major_freeze_duty_, MajorFreezeDuty::SCHEDULE_PERIOD, repeat);
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "schedule major_freeze_duty fail err=%d", err);
  }

  return err;
}

int ObUpdateServer::set_timer_handle_fronzen() {
  int err = OB_SUCCESS;

  bool repeat = true;
  err = timer_.schedule(handle_frozen_duty_, HandleFrozenDuty::SCHEDULE_PERIOD, repeat);
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "schedule handle_frozen_duty fail err=%d", err);
  }

  return err;
}

int ObUpdateServer::init_slave_log_mgr() {
  int err = OB_SUCCESS;

  int64_t log_file_max_size = param_.get_log_size_mb() * 1000L * 1000L;
  err = log_mgr_.init(param_.get_log_dir_path(), log_file_max_size,
                      &slave_mgr_, &role_mgr_, param_.get_log_sync_type());
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "failed to init log mgr, path=%s, log_file_size=%ld, err=%d",
              param_.get_log_dir_path(), log_file_max_size, err);
  } else {
    err = clog_receiver_.init(&log_mgr_, this, param_.get_trans_proc_time_warn_us());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "ObCommitLogReceiver init error, err=%d", err);
    }
  }

  return err;
}

int ObUpdateServer::register_and_start_fetch(const ObServer& master, uint64_t& replay_point) {
  int err = OB_SUCCESS;

  ObUpsFetchParam fetch_param;
  if (OB_SUCCESS == err) {
    err = slave_register_followed(master, fetch_param);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to register");
    }
  }

  // start fetch thread
  if (OB_SUCCESS == err) {
    err = fetch_thread_.init(master, param_.get_log_dir_path(), fetch_param, &role_mgr_, &log_replay_thread_, &sstable_mgr_);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to init fetch thread, log_dir=%s, min_log_id=%ld, "
                "max_log_id=%ld, err=%d", param_.get_log_dir_path(), fetch_param.min_log_id_,
                fetch_param.max_log_id_, err);
    } else {
      err = fetch_thread_.set_usr_opt(param_.get_log_fetch_option());
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "fetch_thread_ set_usr_opt error, err=%d opt=%s", err, param_.get_log_fetch_option());
      } else {
        fetch_thread_.set_limit_rate(param_.get_log_sync_limit_kb());
        fetch_thread_.start();
      }
    }
  }

  if (OB_SUCCESS == err) {
    replay_point = fetch_param.min_log_id_;
    log_mgr_.set_replay_point(fetch_param.min_log_id_);
    TBSYS_LOG(INFO, "set log replay point to %lu", fetch_param.min_log_id_);
  }

  return err;
}

int ObUpdateServer::slave_standalone_prepare(uint64_t& log_id_start, uint64_t& log_seq_start) {
  int err = OB_SUCCESS;

  if (OB_SUCCESS == err) {
    uint64_t log_id = sstable_mgr_.get_max_clog_id();
    if (OB_INVALID_ID != log_id) {
      log_mgr_.set_replay_point(log_id);
      TBSYS_LOG(INFO, "get max clog id=%lu", log_id);
    }

    if (OB_SUCCESS == err) {
      // replay log
      err = log_mgr_.replay_log(table_mgr_);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to replay log, err=%d", err);
      } else {
        TBSYS_LOG(INFO, "replay log succ, log_id=%lu log_seq=%lu",
                  log_mgr_.get_max_log_id(), log_mgr_.get_cur_log_seq());
      }
    }
  }

  if (OB_SUCCESS == err) {
    if (OB_SUCCESS != (err = slave_register_standalone(log_id_start, log_seq_start))) {
      TBSYS_LOG(ERROR, "failed to register to lsync");
    } else if (0 == log_id_start) {
      TBSYS_LOG(ERROR, "failed to register to lsync, lsync returned log_id_start=0");
      err = OB_ERROR;
    } else {
      TBSYS_LOG(INFO, "set log_id_start=%lu log_seq_start=%lu", log_id_start, log_seq_start);
      err = log_mgr_.start_log(log_id_start, log_seq_start);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(ERROR, "start log error, err=%d", err);
      }
    }
  }

  if (OB_SUCCESS == err) {
    err = fetch_lsync_.init(get_lsync_server(), log_id_start, log_seq_start,
                            &ups_rpc_stub_, &clog_receiver_, param_.get_lsync_fetch_timeout(), &role_mgr_);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "ObUpsFetchLsync init error, err=%d", err);
    } else {
      fetch_lsync_.start();
      TBSYS_LOG(INFO, "Lsync fetch thread start");
    }
  }

  return err;
}

int ObUpdateServer::init_replay_thread(const uint64_t log_id_start, const uint64_t log_seq_start) {
  int err = OB_SUCCESS;

  err = log_replay_thread_.init(param_.get_log_dir_path(), log_id_start, log_seq_start, &role_mgr_, &obi_role_, param_.get_replay_wait_time_us());
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "failed to start log replay thread, log_dir=%s, log_id_start=%ld, log_seq_start=%ld, err=%d",
              param_.get_log_dir_path(), log_id_start, log_seq_start, err);
  } else {
    log_replay_thread_.start();
  }

  return err;
}


int ObUpdateServer::start_standalone_() {
  int err = OB_SUCCESS;
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;

  // load schema from schema ini file
  bool parse_ret = schema_mgr.parse_from_file(param_.get_standalone_schema(), config);
  if (!parse_ret) {
    TBSYS_LOG(WARN, "failed to load schema");
    err = OB_ERROR;
  } else {
    err = table_mgr_.set_schemas(schema_mgr);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to set schema, err=%d", err);
    }
  }

  // start read and write thread
  if (OB_SUCCESS == err) {
    read_thread_queue_.start();
    write_thread_queue_.start();
  }

  // modify standalone status
  if (OB_SUCCESS == err) {
    role_mgr_.set_state(ObRoleMgr::ACTIVE);
  }

  TBSYS_LOG(INFO, "start service, role=standalone, err=%d", err);
  // wait finish
  if (OB_SUCCESS == err) {
    read_thread_queue_.wait();
    write_thread_queue_.wait();
  }

  return err;
}

int ObUpdateServer::set_self_(const char* dev_name, const int32_t port) {
  int ret = OB_SUCCESS;
  int32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
  if (0 == ip) {
    TBSYS_LOG(ERROR, "cannot get valid local addr on dev:%s.", dev_name);
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret) {
    bool res = self_addr_.set_ipv4_addr(ip, port);
    if (!res) {
      TBSYS_LOG(ERROR, "chunk server dev:%s, port:%d is invalid.",
                dev_name, port);
      ret = OB_ERROR;
    }
  }

  return ret;
}

int ObUpdateServer::report_frozen_version_() {
  int ret = OB_SUCCESS;
  int64_t num_times = param_.get_resp_root_times();
  int64_t timeout = param_.get_resp_root_timeout_us();
  uint64_t last_frozen_version = 0;
  ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_version);
  if (OB_SUCCESS == ret) {
    ret = RPC_CALL_WITH_RETRY(report_freeze, num_times, timeout, name_server_, ups_master_, last_frozen_version);
  }
  if (OB_RESPONSE_TIME_OUT == ret) {
    TBSYS_LOG(ERROR, "report fronzen version timeout, num_times=%d, timeout=%ldus", num_times, timeout);
  } else if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "error occurs when report frozen version, ret=%d", ret);
  } else {
    TBSYS_LOG(INFO, "report succ frozen version=%ld", last_frozen_version);
  }
  return ret;
}

int ObUpdateServer::update_schema(const bool always_try, const bool write_log) {
  int err = OB_SUCCESS;
  int64_t num_times = always_try ? INT64_MAX : param_.get_fetch_schema_times();
  int64_t timeout = param_.get_fetch_schema_timeout_us();

  CommonSchemaManagerWrapper schema_mgr;
  err = RPC_CALL_WITH_RETRY(fetch_schema, num_times, timeout, name_server_, 0, schema_mgr);

  if (OB_RESPONSE_TIME_OUT == err) {
    TBSYS_LOG(ERROR, "fetch schema timeout, num_times=%d, timeout=%ldus",
              num_times, timeout);
    err = OB_RESPONSE_TIME_OUT;
  } else if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "Error occurs when fetching schema, err=%d", err);
  } else {
    TBSYS_LOG(INFO, "Fetching schema succeed version=%ld", schema_mgr.get_version());
  }

  if (OB_SUCCESS == err) {
    if (write_log) {
      err = table_mgr_.switch_schemas(schema_mgr);
    } else {
      err = table_mgr_.set_schemas(schema_mgr);
    }
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "failed to set schema, err=%d", err);
    }
  }

  return err;
}

int ObUpdateServer::slave_register_followed(const ObServer& master, ObUpsFetchParam& fetch_param) {
  int err = OB_SUCCESS;
  int64_t num_times = INT64_MAX;
  int64_t timeout = param_.get_register_timeout_us();
  const ObServer& self_addr = get_self();

  ObSlaveInfo slave_info;
  slave_info.self = self_addr;
  slave_info.min_sstable_id = sstable_mgr_.get_min_sstable_id();
  slave_info.max_sstable_id = sstable_mgr_.get_max_sstable_id();

  err = RPC_CALL_WITH_RETRY(slave_register_followed, num_times, timeout, master, slave_info, fetch_param);

  if (ObRoleMgr::STOP == role_mgr_.get_state()) {
    TBSYS_LOG(INFO, "The Update Server Slave is stopped manually.");
    err = OB_ERROR;
  } else if (OB_RESPONSE_TIME_OUT == err) {
    TBSYS_LOG(WARN, "slave register timeout, num_times=%d, timeout=%ldus",
              num_times, timeout);
    err = OB_RESPONSE_TIME_OUT;
  } else if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "Error occurs when slave register, err=%d", err);
  }

  // start lease
  if (OB_SUCCESS == err) {
    int64_t renew_lease_network_timeout = 1000 * 1000L; // 1000ms
    //ObLease lease;
    //err = check_thread_.renew_lease(lease);
    //        if (OB_SUCCESS != err)
    //        {
    //          TBSYS_LOG(WARN, "failed to start lease, err=%d", err);
    //        }
    //        else
    {
      check_thread_.set_renew_lease_timeout(renew_lease_network_timeout);
    }
  }

  char addr_buf[ADDR_BUF_LEN];
  self_addr.to_string(addr_buf, sizeof(addr_buf));
  addr_buf[ADDR_BUF_LEN - 1] = '\0';
  TBSYS_LOG(INFO, "slave register, self=[%s], min_log_id=%ld, max_log_id=%ld, err=%d",
            addr_buf, fetch_param.min_log_id_, fetch_param.max_log_id_, err);

  if (OB_SUCCESS == err) {
    is_registered_ = true;
  }

  return err;
}

int ObUpdateServer::slave_register_standalone(uint64_t& log_id_start, uint64_t& log_seq_start) {
  int err = OB_SUCCESS;
  int64_t num_times = INT64_MAX;
  int64_t timeout = param_.get_register_timeout_us();

  log_seq_start = log_mgr_.get_cur_log_seq() == 0 ? 0 : log_mgr_.get_cur_log_seq();
  log_id_start = log_seq_start == 0 ? 0 : log_mgr_.get_max_log_id();

  err = RPC_CALL_WITH_RETRY(slave_register_standalone, num_times, timeout, get_lsync_server(),
                            log_id_start, log_seq_start, log_id_start, log_seq_start);

  if (ObRoleMgr::STOP == role_mgr_.get_state()) {
    TBSYS_LOG(INFO, "The Update Server Slave is stopped manually.");
    err = OB_ERROR;
  } else if (OB_RESPONSE_TIME_OUT == err) {
    TBSYS_LOG(WARN, "slave register timeout, num_times=%d, timeout=%ldus",
              num_times, timeout);
    err = OB_RESPONSE_TIME_OUT;
  } else if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "Error occurs when slave register, err=%d", err);
  }

  char addr_buf[ADDR_BUF_LEN];
  get_lsync_server().to_string(addr_buf, sizeof(addr_buf));
  addr_buf[ADDR_BUF_LEN - 1] = '\0';
  TBSYS_LOG(INFO, "slave register, lsync_server=[%s], log_id_start=%ld, log_seq_start=%ld, err=%d",
            addr_buf, log_id_start, log_seq_start, err);

  if (OB_SUCCESS == err) {
    is_registered_ = true;
  }

  return err;
}

tbnet::IPacketHandler::HPRetCode ObUpdateServer::handlePacket(tbnet::Connection* connection, tbnet::Packet* packet) {
  tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::FREE_CHANNEL;
  if (!packet->isRegularPacket()) {
    TBSYS_LOG(WARN, "control packet, packet code: %d", ((tbnet::ControlPacket*)packet)->getCommand());
  } else {
    ObPacket* req = (ObPacket*) packet;
    req->set_connection(connection);
    bool ps = true;
    int packet_code = req->get_packet_code();
    int32_t priority = req->get_packet_priority();
    TBSYS_LOG(DEBUG, "get packet code is %d, priority=%d", packet_code, priority);
    switch (packet_code) {
    case OB_SET_OBI_ROLE:
      ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                                   (NORMAL_PRI == priority)
                                   ? PriorityPacketQueueThread::NORMAL_PRIV
                                   : PriorityPacketQueueThread::LOW_PRIV);
      break;
    case OB_SEND_LOG:
      if (ObRoleMgr::SLAVE == role_mgr_.get_role()
          || ObiRole::SLAVE == obi_role_.get_role()) {
        ps = write_thread_queue_.push(req, log_task_queue_size_, false);
      } else {
        ps = false;
      }
      break;
    case OB_WRITE:
    case OB_SWITCH_SCHEMA:
    case OB_UPS_FORCE_FETCH_SCHEMA:
    case OB_UPS_SWITCH_COMMIT_LOG:
    case OB_SLAVE_REG:
    case OB_FREEZE_MEM_TABLE:
    case OB_UPS_MINOR_FREEZE_MEMTABLE:
    case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
      if (ObiRole::MASTER != obi_role_.get_role()
          || ObRoleMgr::MASTER != role_mgr_.get_role()) {
        response_result_(OB_NOT_MASTER, OB_WRITE_RES, 1, connection, packet->getChannelId());
        ps = false;
      } else if (ObRoleMgr::ACTIVE != role_mgr_.get_state()) {
        response_result_(OB_NOT_MASTER, OB_WRITE_RES, 1, connection, packet->getChannelId());
        ps = false;
      } else {
        ps = write_thread_queue_.push(req, write_task_queue_size_, false);
      }
      break;
    case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
      if (ObRoleMgr::MASTER == role_mgr_.get_role()) {
        ps = write_thread_queue_.push(req, write_task_queue_size_, false);
      } else {
        ps = false;
      }
      break;
    case OB_GET_CLOG_CURSOR:
    case OB_GET_CLOG_MASTER:
    case OB_GET_REQUEST:
    case OB_SCAN_REQUEST:
    case OB_UPS_GET_BLOOM_FILTER:
    case OB_UPS_DUMP_TEXT_MEMTABLE:
    case OB_UPS_DUMP_TEXT_SCHEMAS:
    case OB_UPS_MEMORY_WATCH:
    case OB_UPS_MEMORY_LIMIT_SET:
    case OB_UPS_PRIV_QUEUE_CONF_SET:
    case OB_UPS_RELOAD_CONF:
    case OB_UPS_GET_LAST_FROZEN_VERSION:
    case OB_UPS_GET_TABLE_TIME_STAMP:
    case OB_UPS_ENABLE_MEMTABLE_CHECKSUM:
    case OB_UPS_DISABLE_MEMTABLE_CHECKSUM:
    case OB_FETCH_STATS:
    case OB_UPS_STORE_MEM_TABLE:
    case OB_UPS_DROP_MEM_TABLE:
    case OB_UPS_DELAY_DROP_MEMTABLE:
    case OB_UPS_IMMEDIATELY_DROP_MEMTABLE:
    case OB_UPS_ASYNC_FORCE_DROP_MEMTABLE:
    case OB_UPS_ERASE_SSTABLE:
    case OB_UPS_LOAD_NEW_STORE:
    case OB_UPS_RELOAD_ALL_STORE:
    case OB_UPS_RELOAD_STORE:
    case OB_UPS_UMOUNT_STORE:
    case OB_UPS_FORCE_REPORT_FROZEN_VERSION:
      ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                                   (NORMAL_PRI == priority)
                                   ? PriorityPacketQueueThread::NORMAL_PRIV
                                   : PriorityPacketQueueThread::LOW_PRIV);
      break;
    case OB_SLAVE_QUIT:
    case OB_RENEW_LEASE_REQUEST:
    case OB_GRANT_LEASE_REQUEST:
      //            if (ObRoleMgr::MASTER == role_mgr_.get_role())
      //            {
      ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
      //            }
      //            else
      //            {
      //              ps = false;
      //            }
      break;
    case OB_SET_SYNC_LIMIT_REQUEST:
    case OB_PING_REQUEST:
    case OB_UPS_CHANGE_VIP_REQUEST:
      if (ObRoleMgr::MASTER == role_mgr_.get_role()) {
        ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
      } else if (ObRoleMgr::SLAVE == role_mgr_.get_role()) {
        ps = lease_thread_queue_.push(req, log_task_queue_size_, false);
      } else {
        ps = false;
      }
      break;
    default:
      TBSYS_LOG(ERROR, "UNKNOWN packet %d, ignore this", packet_code);
      ps = false;
      break;
    }

    if (!ps) {
      TBSYS_LOG(WARN, "packet %d can not be distribute to queue", packet_code);
      rc = tbnet::IPacketHandler::KEEP_CHANNEL;
      INC_STAT_INFO(UPS_STAT_TBSYS_DROP_COUNT, 1);
    }
  }
  return rc;
}

bool ObUpdateServer::handleBatchPacket(tbnet::Connection* connection, tbnet::PacketQueue& packetQueue) {
  UNUSED(connection);
  UNUSED(packetQueue);
  TBSYS_LOG(ERROR, "you should not reach this, not supported");
  return true;
}

bool ObUpdateServer::handlePacketQueue(tbnet::Packet* packet, void* args) {
  UNUSED(args);
  bool ret = true;
  int return_code = OB_SUCCESS;

  ObPacket* ob_packet = reinterpret_cast<ObPacket*>(packet);
  int packet_code = ob_packet->get_packet_code();
  int version = ob_packet->get_api_version();
  int32_t priority = ob_packet->get_packet_priority();
  return_code = ob_packet->deserialize();
  uint32_t channel_id = ob_packet->getChannelId();//tbnet need this
  if (OB_SUCCESS != return_code) {
    TBSYS_LOG(ERROR, "packet deserialize error packet code is %d", packet_code);
  } else {
    int64_t packet_timewait = (0 == ob_packet->get_source_timeout()) ?
                              param_.get_packet_max_timewait() : ob_packet->get_source_timeout();
    ObDataBuffer* in_buf = ob_packet->get_buffer();
    in_buf->get_limit() = in_buf->get_position() + ob_packet->get_data_length();
    if ((ob_packet->get_receive_ts() + packet_timewait) < tbsys::CTimeUtil::getTime()) {
      INC_STAT_INFO(UPS_STAT_PACKET_LONG_WAIT_COUNT, 1);
      TBSYS_LOG(WARN, "packet wait too long time, receive_time=%ld cur_time=%ld packet_max_timewait=%ld packet_code=%d "
                "priority=%d last_log_network_elapse=%ld last_log_disk_elapse=%ld "
                "read_task_queue_size=%d write_task_queue_size=%d lease_task_queue_size=%d log_task_queue_size=%d",
                ob_packet->get_receive_ts(), tbsys::CTimeUtil::getTime(), packet_timewait, packet_code, priority,
                log_mgr_.get_last_net_elapse(), log_mgr_.get_last_disk_elapse(),
                read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size(), log_thread_queue_.size());
    } else if (in_buf == NULL) {
      TBSYS_LOG(ERROR, "in_buff is NUll should not reach this");
    } else {
      packet_timewait -= DEFAULT_REQUEST_TIMEOUT_RESERVE;
      tbnet::Connection* conn = ob_packet->get_connection();
      ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
      if (my_buffer != NULL) {
        my_buffer->reset();
        ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
        TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
        switch (packet_code) {
        case OB_SET_OBI_ROLE:
          return_code = set_obi_role(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_SEND_LOG:
          return_code = ups_slave_write_log(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_SET_SYNC_LIMIT_REQUEST:
          return_code = ups_set_sync_limit(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_PING_REQUEST:
          return_code = ups_ping(version, conn, channel_id);
          break;
        case OB_GET_CLOG_CURSOR:
          return_code = ups_get_clog_cursor(version, conn, channel_id, thread_buff);
          break;
        case OB_GET_CLOG_MASTER:
          return_code = ups_get_clog_master(version, conn, channel_id, thread_buff);
          break;
        case OB_GET_REQUEST:
          CLEAR_TRACE_LOG();
          FILL_TRACE_LOG("start handle get, packet wait=%ld start_time=%ld timeout=%ld src=%s priority=%d",
                         tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts(),
                         ob_packet->get_receive_ts(), packet_timewait, inet_ntoa_r(conn->getPeerId()), priority);
          return_code = ups_get(version, *in_buf, conn, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait, priority);
          break;
        case OB_SCAN_REQUEST:
          CLEAR_TRACE_LOG();
          FILL_TRACE_LOG("start handle scan, packet wait=%ld start_time=%ld timeout=%ld src=%s priority=%d",
                         tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts(),
                         ob_packet->get_receive_ts(), packet_timewait, inet_ntoa_r(conn->getPeerId()), priority);
          return_code = ups_scan(version, *in_buf, conn, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait, priority);
          break;
        case OB_UPS_GET_BLOOM_FILTER:
          return_code = ups_get_bloomfilter(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_CREATE_MEMTABLE_INDEX:
          return_code = ups_create_memtable_index();
          break;
        case OB_UPS_DUMP_TEXT_MEMTABLE:
          return_code = ups_dump_text_memtable(version, *in_buf, conn, channel_id);
          break;
        case OB_UPS_DUMP_TEXT_SCHEMAS:
          return_code = ups_dump_text_schemas(version, conn, channel_id);
          break;
        case OB_UPS_MEMORY_WATCH:
          return_code = ups_memory_watch(version, conn, channel_id, thread_buff);
          break;
        case OB_UPS_MEMORY_LIMIT_SET:
          return_code = ups_memory_limit_set(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_UPS_PRIV_QUEUE_CONF_SET:
          return_code = ups_priv_queue_conf_set(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_UPS_RELOAD_CONF:
          return_code = ups_reload_conf(version, *in_buf, conn, channel_id);
          break;
        case OB_SLAVE_QUIT:
          return_code = ups_slave_quit(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_RENEW_LEASE_REQUEST:
          return_code = ups_renew_lease(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_GRANT_LEASE_REQUEST:
          return_code = ups_grant_lease(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_UPS_GET_LAST_FROZEN_VERSION:
          return_code = ups_get_last_frozen_version(version, conn, channel_id, thread_buff);
          break;
        case OB_UPS_GET_TABLE_TIME_STAMP:
          return_code = ups_get_table_time_stamp(version, *in_buf, conn, channel_id, thread_buff);
          break;
        case OB_UPS_ENABLE_MEMTABLE_CHECKSUM:
          return_code = ups_enable_memtable_checksum(version, conn, channel_id);
          break;
        case OB_UPS_DISABLE_MEMTABLE_CHECKSUM:
          return_code = ups_disable_memtable_checksum(version, conn, channel_id);
          break;
        case OB_FETCH_STATS:
          return_code = ups_fetch_stat_info(version, conn, channel_id, thread_buff);
          break;
        case OB_UPS_CHANGE_VIP_REQUEST:
          return_code = ups_change_vip(version, *in_buf, conn, channel_id);
          break;
        case OB_UPS_STORE_MEM_TABLE:
          return_code = ups_store_memtable(version, *in_buf, conn, channel_id);
          break;
        case OB_UPS_DROP_MEM_TABLE:
          return_code = ups_drop_memtable(version, conn, channel_id);
          break;
        case OB_UPS_DELAY_DROP_MEMTABLE:
          return_code = ups_delay_drop_memtable(version, conn, channel_id);
          break;
        case OB_UPS_IMMEDIATELY_DROP_MEMTABLE:
          return_code = ups_immediately_drop_memtable(version, conn, channel_id);
          break;
        case OB_UPS_ASYNC_FORCE_DROP_MEMTABLE:
          return_code = ups_drop_memtable();
          break;
        case OB_UPS_ERASE_SSTABLE:
          return_code = ups_erase_sstable(version, conn, channel_id);
          break;
        case OB_UPS_ASYNC_HANDLE_FROZEN:
          return_code = ups_handle_frozen();
          break;
        case OB_UPS_ASYNC_REPORT_FREEZE:
          return_code = report_frozen_version_();
          break;
        case OB_UPS_LOAD_NEW_STORE:
          return_code = ups_load_new_store(version, conn, channel_id);
          break;
        case OB_UPS_RELOAD_ALL_STORE:
          return_code = ups_reload_all_store(version, conn, channel_id);
          break;
        case OB_UPS_RELOAD_STORE:
          return_code = ups_reload_store(version, *in_buf, conn, channel_id);
          break;
        case OB_UPS_UMOUNT_STORE:
          return_code = ups_umount_store(version, *in_buf, conn, channel_id);
          break;
        case OB_UPS_FORCE_REPORT_FROZEN_VERSION:
          return_code = ups_froce_report_frozen_version(version, conn, channel_id);
          break;
        default:
          return_code = OB_ERROR;
          break;
        }

        if (OB_SUCCESS != return_code) {
          TBSYS_LOG(ERROR, "call func error packet_code is %d return code is %d", packet_code, return_code);
        }
      } else {
        TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
      }
    }
  }
  return ret;//if return true packet will be deleted.
}

bool ObUpdateServer::handleBatchPacketQueue(const int64_t batch_num, tbnet::Packet** packets, void* args) {
  UNUSED(args);
  enum __trans_status__ {
    TRANS_NOT_START = 0,
    TRANS_STARTED = 1,
  };
  bool ret = true;
  int return_code = OB_SUCCESS;
  int64_t trans_status = TRANS_NOT_START;
  int64_t first_trans_idx = 0;
  UpsTableMgrTransHandle handle;

  ObPacket packet_repl;
  ObPacket* ob_packet = &packet_repl;
  for (int64_t i = 0; OB_SUCCESS == return_code && i < batch_num; ++i) {
    ObPacket* packet_orig = reinterpret_cast<ObPacket*>(packets[i]);
    packet_repl = *packet_orig;
    int packet_code = ob_packet->get_packet_code();
    int version = ob_packet->get_api_version();
    return_code = ob_packet->deserialize();
    uint32_t channel_id = ob_packet->getChannelId();//tbnet need this
    //TBSYS_LOG(DEBUG, "packet i=%ld batch_num=%ld %s", i, batch_num, ob_packet->print_self());
    if (OB_SUCCESS != return_code) {
      TBSYS_LOG(ERROR, "packet deserialize error packet code is %d", packet_code);
    } else {
      int64_t packet_timewait = (0 == ob_packet->get_source_timeout()) ?
                                param_.get_packet_max_timewait() : ob_packet->get_source_timeout();
      ObDataBuffer* in_buf = ob_packet->get_buffer();
      in_buf->get_limit() = in_buf->get_position() + ob_packet->get_data_length();
      if ((ob_packet->get_receive_ts() + packet_timewait) < tbsys::CTimeUtil::getTime()) {
        INC_STAT_INFO(UPS_STAT_PACKET_LONG_WAIT_COUNT, 1);
        TBSYS_LOG(WARN, "packet wait too long time, receive_time=%ld cur_time=%ld packet_max_timewait=%ld packet_code=%d "
                  "read_task_queue_size=%d write_task_queue_size=%d lease_task_queue_size=%d log_task_queue_size=%d",
                  ob_packet->get_receive_ts(), tbsys::CTimeUtil::getTime(), packet_timewait, packet_code,
                  read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size(), log_thread_queue_.size());
        return_code = OB_RESPONSE_TIME_OUT;
      } else if (in_buf == NULL) {
        TBSYS_LOG(ERROR, "in_buff is NUll should not reach this");
        return_code = OB_ERROR;
      } else {
        tbnet::Connection* conn = ob_packet->get_connection();
        ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
        if (my_buffer == NULL) {
          TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
          return_code = OB_ERROR;
        } else {
          my_buffer->reset();
          ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
          TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
          // preprocess
          switch (packet_code) {
          case OB_FREEZE_MEM_TABLE:
          case OB_UPS_MINOR_FREEZE_MEMTABLE:
          case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
          case OB_SLAVE_REG:
          case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
          case OB_SWITCH_SCHEMA:
          case OB_UPS_FORCE_FETCH_SCHEMA:
          case OB_UPS_SWITCH_COMMIT_LOG:
            if (TRANS_STARTED == trans_status) {
              return_code = ups_end_transaction(packets, first_trans_idx, i - 1, handle, OB_SUCCESS);
              trans_status = TRANS_NOT_START;
              if (OB_SUCCESS != return_code) {
                TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                break;
              }
            }
            break;
          case OB_SEND_LOG:
          case OB_WRITE:
            break;
          default:
            TBSYS_LOG(WARN, "unexpected packet_code %d", packet_code);
            return_code = OB_ERR_UNEXPECTED;
            break;
          }

          if (OB_SUCCESS == return_code) {
            switch (packet_code) {
            case OB_FREEZE_MEM_TABLE:
            case OB_UPS_MINOR_FREEZE_MEMTABLE:
            case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
              return_code = ups_freeze_memtable(version, packet_orig, thread_buff, packet_code);
              break;
            case OB_SWITCH_SCHEMA:
              return_code = ups_switch_schema(version, packet_orig, *in_buf);
              break;
            case OB_UPS_FORCE_FETCH_SCHEMA:
              return_code = ups_force_fetch_schema(version, conn, channel_id);
              break;
            case OB_UPS_SWITCH_COMMIT_LOG:
              return_code = ups_switch_commit_log(version, conn, channel_id, thread_buff);
              break;
            case OB_SLAVE_REG:
              return_code = ups_slave_register(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
              return_code = ups_clear_active_memtable(version, conn, channel_id);
              break;
            case OB_WRITE:
              if (TRANS_NOT_START == trans_status) {
                return_code = ups_start_transaction(WRITE_TRANSACTION, handle);
                if (OB_SUCCESS != return_code) {
                  TBSYS_LOG(ERROR, "failed to start transaction, err=%d", return_code);
                } else {
                  trans_status = TRANS_STARTED;
                  first_trans_idx = i;
                }
              }

              if (OB_SUCCESS == return_code) {
                return_code = ups_apply(handle, *in_buf);
                if (OB_EAGAIN == return_code) {
                  if (first_trans_idx < i) {
                    TBSYS_LOG(INFO, "exceeds memory limit, should retry");
                  } else {
                    TBSYS_LOG(WARN, "mutator too large more than log_buffer, cannot apply");
                    return_code = OB_BUF_NOT_ENOUGH;
                  }
                } else if (OB_SUCCESS != return_code) {
                  TBSYS_LOG(WARN, "failed to apply mutation, err=%d", return_code);
                }
                FILL_TRACE_LOG("ups_apply src=%s ret=%d batch_num=%ld cur_trans_idx=%ld last_trans_idx=%ld",
                               inet_ntoa_r(conn->getPeerId()), return_code, batch_num, first_trans_idx, i);
              }

              if (OB_EAGAIN == return_code) {
                return_code = ups_end_transaction(packets, first_trans_idx, i - 1, handle, OB_SUCCESS);
                --i; // re-execute the last mutation
                trans_status = TRANS_NOT_START;
                if (OB_SUCCESS != return_code) {
                  TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                  break;
                }
              } else if (OB_SUCCESS != return_code) {
                return_code = ups_end_transaction(packets, first_trans_idx, i, handle, return_code);
                trans_status = TRANS_NOT_START;
                if (OB_SUCCESS != return_code) {
                  TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                  break;
                }
              } else if (i == batch_num - 1) {
                return_code = ups_end_transaction(packets, first_trans_idx, i, handle, OB_SUCCESS);
                trans_status = TRANS_NOT_START;
                if (OB_SUCCESS != return_code) {
                  TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                  break;
                }
              }
              break;

            case OB_SEND_LOG:
              return_code = ups_slave_write_log(version, *in_buf, conn, channel_id, thread_buff);
              break;
            default:
              TBSYS_LOG(WARN, "unexpected packet_code %d", packet_code);
              return_code = OB_ERR_UNEXPECTED;
              break;
            }
          }

          if (OB_SUCCESS != return_code) {
            TBSYS_LOG(ERROR, "call func error packet_code is %d return code is %d", packet_code, return_code);
          }
        }
      }
    }
    // do something after every loop
    if (OB_SUCCESS != return_code) {
      if (TRANS_STARTED == trans_status) {
        return_code = ups_end_transaction(packets, first_trans_idx, i, handle, return_code);
        trans_status = TRANS_NOT_START;
        if (OB_SUCCESS != return_code) {
          TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
          return_code = OB_SUCCESS;
        }
      } else {
        return_code = OB_SUCCESS;
      }
    }
  }
  packet_repl.get_buffer()->reset();

  if (ObiRole::MASTER == obi_role_.get_role()
      && ObRoleMgr::MASTER == role_mgr_.get_role()
      && ObRoleMgr::ACTIVE == role_mgr_.get_state()) {
    TableMgr::FreezeType freeze_type = TableMgr::AUTO_TRIG;
    uint64_t frozen_version = 0;
    bool report_version_changed = false;
    if (OB_SUCCESS == table_mgr_.freeze_memtable(freeze_type, frozen_version, report_version_changed)) {
      if (report_version_changed) {
        submit_report_freeze();
      }
      submit_handle_frozen();
    }
  }

  return ret;//if return true packet will be deleted.
}

common::ThreadSpecificBuffer::Buffer* ObUpdateServer::get_rpc_buffer() const {
  return rpc_buffer_.get_buffer();
}

ObUpsRpcStub& ObUpdateServer::get_ups_rpc_stub() {
  return ups_rpc_stub_;
}

int ObUpdateServer::set_obi_role(const int32_t version, common::ObDataBuffer& in_buff,
                                 tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(out_buff);

  int err = OB_SUCCESS;

  if (version != MY_VERSION) {
    err = OB_ERROR_FUNC_VERSION;
  }

  ObiRole obi_role;
  err = obi_role.deserialize(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position());
  if (OB_SUCCESS != err) {
    TBSYS_LOG(ERROR, "ObiRole deserialize error, err=%d", err);
  } else {
    if (ObiRole::MASTER == obi_role.get_role()) {
      if (ObiRole::MASTER == obi_role_.get_role()) {
        TBSYS_LOG(INFO, "ObiRole is already MASTER");
      } else if (ObiRole::SLAVE == obi_role_.get_role()) {
        role_mgr_.set_state(ObRoleMgr::SWITCHING);
        obi_role_.set_role(ObiRole::MASTER);
        TBSYS_LOG(INFO, "ObiRole is set to MASTER");
      } else if (ObiRole::INIT == obi_role_.get_role()) {
        obi_role_.set_role(ObiRole::MASTER);
        TBSYS_LOG(INFO, "ObiRole is set to MASTER");
      }
    } else if (ObiRole::SLAVE == obi_role.get_role()) {
      if (ObiRole::MASTER == obi_role_.get_role()) {
        role_mgr_.set_state(ObRoleMgr::INIT);
        obi_role_.set_role(ObiRole::SLAVE);
        TBSYS_LOG(INFO, "ObiRole is set to SLAVE");
      } else if (ObiRole::SLAVE == obi_role_.get_role()) {
        TBSYS_LOG(INFO, "ObiRole is already SLAVE");
      } else if (ObiRole::INIT == obi_role_.get_role()) {
        obi_role_.set_role(ObiRole::SLAVE);
        TBSYS_LOG(INFO, "ObiRole is set to SLAVE");
      }
    } else {
      TBSYS_LOG(ERROR, "Unknown ObiRole: %d", obi_role.get_role());
      err = OB_ERROR;
    }
  }

  // send response to MASTER before writing to disk
  err = response_result_(err, OB_SET_OBI_ROLE_RESPONSE, MY_VERSION, conn, channel_id);

  return err;
}

int ObUpdateServer::ups_slave_write_log(const int32_t version, common::ObDataBuffer& in_buff,
                                        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(out_buff);

  int err = OB_SUCCESS;

  if (version != MY_VERSION) {
    err = OB_ERROR_FUNC_VERSION;
  }

  int64_t in_buff_begin = in_buff.get_position();
  bool switch_log_flag = false;
  uint64_t log_id;

  // send response to MASTER before writing to disk
  err = response_result_(err, OB_SEND_LOG_RES, MY_VERSION, conn, channel_id);

  if (OB_SUCCESS == err) {
    if (is_first_log_) {
      is_first_log_ = false;

      ObLogEntry log_ent;
      err = log_ent.deserialize(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position());
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "ObLogEntry deserialize error, err=%d", err);
        hex_dump(in_buff.get_data(), in_buff.get_limit(), TBSYS_LOG_LEVEL_WARN);
      } else {
        if (OB_LOG_SWITCH_LOG != log_ent.cmd_) {
          TBSYS_LOG(WARN, "the first log of slave should be switch_log, cmd_=%d", log_ent.cmd_);
          err = OB_ERROR;
        } else {
          err = serialization::decode_i64(in_buff.get_data(), in_buff.get_limit(),
                                          in_buff.get_position(), (int64_t*)&log_id);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "decode_i64 log_id error, err=%d", err);
          } else {
            err = log_mgr_.start_log(log_id);
            if (OB_SUCCESS != err) {
              TBSYS_LOG(WARN, "start_log error, log_id=%lu err=%d", log_id, err);
            } else {
              in_buff.get_position() = in_buff.get_limit();
            }
          }
        }
      }
    }
  }

  if (OB_SUCCESS == err) {
    int64_t data_begin = in_buff.get_position();
    while (OB_SUCCESS == err && in_buff.get_position() < in_buff.get_limit()) {
      ObLogEntry log_ent;
      err = log_ent.deserialize(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position());
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "ObLogEntry deserialize error, err=%d", err);
        err = OB_ERROR;
      } else {
        log_mgr_.set_cur_log_seq(log_ent.seq_);

        // check switch_log
        if (OB_LOG_SWITCH_LOG == log_ent.cmd_) {
          if (data_begin != in_buff_begin
              || ((in_buff.get_position() + log_ent.get_log_data_len()) != in_buff.get_limit())) {
            TBSYS_LOG(ERROR, "swith_log is not single, this should not happen, "
                      "in_buff.pos=%ld log_data_len=%d in_buff.limit=%ld",
                      in_buff.get_position(), log_ent.get_log_data_len(), in_buff.get_limit());
            hex_dump(in_buff.get_data(), in_buff.get_limit(), TBSYS_LOG_LEVEL_WARN);
            err = OB_ERROR;
          } else {
            err = serialization::decode_i64(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position(), (int64_t*)&log_id);
            if (OB_SUCCESS != err) {
              TBSYS_LOG(WARN, "decode_i64 log_id error, err=%d", err);
            } else {
              switch_log_flag = true;
            }
          }
        }
        in_buff.get_position() += log_ent.get_log_data_len();
      }
    }

    if (OB_SUCCESS == err && in_buff.get_limit() - data_begin > 0) {
      int64_t store_start_time = tbsys::CTimeUtil::getTime();
      err = log_mgr_.store_log(in_buff.get_data() + data_begin, in_buff.get_limit() - data_begin);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(ERROR, "ObUpsLogMgr store_log error, err=%d", err);
      }

      int64_t store_elapse = tbsys::CTimeUtil::getTime() - store_start_time;
      if (store_elapse > param_.get_trans_proc_time_warn_us()) {
        TBSYS_LOG(WARN, "store log time is too long, store_time=%ld cur_time=%ld log_size=%ld log_task_queue_size=%d",
                  store_elapse, tbsys::CTimeUtil::getTime(), in_buff.get_limit() - data_begin, log_thread_queue_.size());
      }
    }

    if (switch_log_flag) {
      err = log_mgr_.switch_to_log_file(log_id + 1);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "switch_to_log_file error, log_id=%lu err=%d", log_id, err);
      }
    }
  }

  return err;
}

int ObUpdateServer::ups_set_sync_limit(const int32_t version, common::ObDataBuffer& in_buff,
                                       tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(out_buff);
  int ret = OB_SUCCESS;

  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  int64_t new_limit = 0;
  ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position(), &new_limit);
  if (OB_SUCCESS == ret) {
    fetch_thread_.set_limit_rate(new_limit);
    TBSYS_LOG(INFO, "update sync limit=%ld", fetch_thread_.get_limit_rate());
  }

  ret = response_result_(ret, OB_SET_SYNC_LIMIT_RESPONSE, MY_VERSION, conn, channel_id);

  return ret;
}

int ObUpdateServer::ups_ping(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;

  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  ret = response_result_(ret, OB_PING_RESPONSE, MY_VERSION, conn, channel_id);

  return ret;
}

int ObUpdateServer::ups_get_clog_master(const int32_t version, tbnet::Connection* conn,
                                        const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int err = OB_SUCCESS;
  common::ObServer* master = NULL;
  if (MY_VERSION != version) {
    err = OB_ERROR_FUNC_VERSION;
    TBSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
  } else if (ObiRole::MASTER == obi_role_.get_role()) {
    if (ObRoleMgr::MASTER == role_mgr_.get_role()) {
      master = &self_addr_;
    } else if (ObRoleMgr::SLAVE == role_mgr_.get_role()) {
      master = &ups_master_;
    } else {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "ob_role != MASTER && obi_role != SLAVE");
    }
  } else if (ObiRole::SLAVE == obi_role_.get_role()) {
    if (ObRoleMgr::MASTER == role_mgr_.get_role()) {
      if (STANDALONE_SLAVE == obi_slave_stat_) {
        master = &lsync_server_;
      } else if (FOLLOWED_SLAVE == obi_slave_stat_) {
        master = &ups_inst_master_;
      }
    } else if (ObRoleMgr::SLAVE == role_mgr_.get_role()) {
      if (STANDALONE_SLAVE == obi_slave_stat_) {
        //master = &lsync_server_;
        err = OB_NOT_SUPPORTED;
        TBSYS_LOG(ERROR, "slave slave ups not allowed to connect lsyncserver");
      } else if (FOLLOWED_SLAVE == obi_slave_stat_) {
        master = &ups_master_;
      }
    } else {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "ob_role != MASTER && obi_role != SLAVE");
    }
  } else {
    err = OB_ERROR;
    TBSYS_LOG(ERROR, "obi_role != MASTER && obi_role != SLAVE");
  }

  if (OB_SUCCESS != err)
  {}
  else if (NULL == master) {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "NULL == master");
  } else if (OB_SUCCESS != (err = response_data_(err, *master, OB_GET_CLOG_MASTER_RESPONSE, MY_VERSION, conn, channel_id, out_buff))) {
    TBSYS_LOG(ERROR, "response_data()=>%d", err);
  }

  return err;
}

int ObUpdateServer::ups_get_clog_cursor(const int32_t version, tbnet::Connection* conn,
                                        const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int err = OB_SUCCESS;
  ObLogCursor log_cursor;
  if (MY_VERSION != version) {
    err = OB_ERROR_FUNC_VERSION;
    TBSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
  } else if (ObRoleMgr::SLAVE == role_mgr_.get_role()
             || ObiRole::SLAVE == obi_role_.get_role()) {
    log_replay_thread_.get_cur_replay_point(log_cursor.file_id_, log_cursor.log_id_, log_cursor.offset_);
    TBSYS_LOG(INFO, "slave: file_id=%lu, log_id=%lu, offset=%lu", log_cursor.file_id_, log_cursor.log_id_, log_cursor.offset_);
  } else {
    log_mgr_.get_cur_log_point(log_cursor.file_id_, log_cursor.log_id_, log_cursor.offset_);
    TBSYS_LOG(INFO, "master: file_id=%lu, log_id=%lu, offset=%lu", log_cursor.file_id_, log_cursor.log_id_, log_cursor.offset_);
  }

  if (OB_SUCCESS != err)
  {}
  else if (OB_SUCCESS != (err = response_data_(err, log_cursor, OB_GET_CLOG_CURSOR_RESPONSE, MY_VERSION, conn, channel_id, out_buff))) {
    TBSYS_LOG(ERROR, "response_data()=>%d", err);
  }

  return err;
}

int ObUpdateServer::ups_get(const int32_t version, common::ObDataBuffer& in_buff,
                            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
                            const int64_t start_time, const int64_t timeout, const int32_t priority) {
  int ret = OB_SUCCESS;
  ObGetParam get_param_stack;
  ObScanner scanner_stack;
  ObGetParam* get_param_ptr = GET_TSI(ObGetParam);
  ObScanner* scanner_ptr = GET_TSI(ObScanner);
  ObGetParam& get_param = (NULL == get_param_ptr) ? get_param_stack : *get_param_ptr;
  ObScanner& scanner = (NULL == scanner_ptr) ? scanner_stack : *scanner_ptr;

  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  //CLEAR_TRACE_LOG();
  if (OB_SUCCESS == ret) {
    ret = get_param.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "deserialize get param error, ret=%d", ret);
    }
    FILL_TRACE_LOG("get param deserialize ret=%d", ret);
  }

  if (OB_SUCCESS == ret) {
    if (get_param.get_is_read_consistency()) {
      if (!(ObiRole::MASTER == obi_role_.get_role() && ObRoleMgr::MASTER == role_mgr_.get_role())) {
        TBSYS_LOG(INFO, "The Get Request require consistency, ObiRole:%s RoleMgr:%s",
                  obi_role_.get_role_str(), role_mgr_.get_role_str());
        ret = OB_NOT_MASTER;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    thread_read_prepare();
    scanner.reset();
    ret = table_mgr_.get(get_param, scanner, start_time, timeout);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to get, err=%d", ret);
    }
    FILL_TRACE_LOG("get from table mgr ret=%d", ret);
  }

  ret = response_data_(ret, scanner, OB_GET_RESPONSE, MY_VERSION, conn, channel_id, out_buff, &priority);
  INC_STAT_INFO(UPS_STAT_GET_COUNT, 1);
  INC_STAT_INFO(UPS_STAT_GET_TIMEU, GET_TRACE_TIMEU());
  FILL_TRACE_LOG("response scanner ret=%d", ret);
  PRINT_TRACE_LOG();

  thread_read_complete();

  return ret;
}

template <class T>
int ObUpdateServer::response_data_(int32_t ret_code, const T& data,
                                   int32_t cmd_type, int32_t func_version,
                                   tbnet::Connection* conn, const uint32_t channel_id,
                                   common::ObDataBuffer& out_buff, const int32_t* priority) {
  int ret = OB_SUCCESS;
  common::ObResultCode result_msg;
  result_msg.result_code_ = ret_code;
  ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
    ret = OB_ERROR;
  } else {
    common::ObDataBuffer tmp_buffer = out_buff;
    ret = ups_serialize(data, out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "serialize data error, ret=%d", ret);
      ret = OB_ERROR;
    } else {
      ret = send_response(cmd_type, func_version, out_buff, conn, channel_id);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && NULL != priority && PriorityPacketQueueThread::LOW_PRIV == *priority) {
        low_priv_speed_control_(out_buff.get_position());
      }
    }
  }

  return ret;
}

int ObUpdateServer::response_fetch_param_(int32_t ret_code, const ObUpsFetchParam& fetch_param,
                                          int32_t cmd_type, int32_t func_version,
                                          tbnet::Connection* conn, const uint32_t channel_id,
                                          common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  common::ObResultCode result_msg;
  result_msg.result_code_ = ret_code;
  ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
    ret = OB_ERROR;
  } else {
    ret = fetch_param.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "serialize fetch param error, ret=%d", ret);
      ret = OB_ERROR;
    } else {
      ret = send_response(cmd_type, func_version, out_buff, conn, channel_id);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
        ret = OB_ERROR;
      }
    }
  }
  return ret;
}

int ObUpdateServer::response_lease_(int32_t ret_code, const ObLease& lease,
                                    int32_t cmd_type, int32_t func_version,
                                    tbnet::Connection* conn, const uint32_t channel_id,
                                    common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  common::ObResultCode result_msg;
  result_msg.result_code_ = ret_code;
  ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
    ret = OB_ERROR;
  } else {
    ret = lease.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "serialize lease error, ret=%d", ret);
      ret = OB_ERROR;
    } else {
      ret = send_response(cmd_type, func_version, out_buff, conn, channel_id);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
        ret = OB_ERROR;
      }
    }
  }
  return ret;
}

int ObUpdateServer::ups_scan(const int32_t version, common::ObDataBuffer& in_buff,
                             tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
                             const int64_t start_time, const int64_t timeout, const int32_t priority) {
  int err = OB_SUCCESS;
  ObScanParam scan_param_stack;
  ObScanner scanner_stack;
  ObScanParam* scan_param_ptr = GET_TSI(ObScanParam);
  ObScanner* scanner_ptr = GET_TSI(ObScanner);
  ObScanParam& scan_param = (NULL == scan_param_ptr) ? scan_param_stack : *scan_param_ptr;
  ObScanner& scanner = (NULL == scanner_ptr) ? scanner_stack : *scanner_ptr;
  common::ObResultCode result_msg;

  if (version != MY_VERSION) {
    err = OB_ERROR_FUNC_VERSION;
  }

  //CLEAR_TRACE_LOG();
  if (OB_SUCCESS == err) {
    err = scan_param.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "deserialize scan param error, err=%d", err);
    }
    FILL_TRACE_LOG("scan param deserialize ret=%d", err);
  }

  if (OB_SUCCESS == err) {
    if (scan_param.get_is_read_consistency()) {
      if (!(ObiRole::MASTER == obi_role_.get_role() && ObRoleMgr::MASTER == role_mgr_.get_role())) {
        TBSYS_LOG(INFO, "The Scan Request require consistency, ObiRole:%s RoleMgr:%s",
                  obi_role_.get_role_str(), role_mgr_.get_role_str());
        err = OB_NOT_MASTER;
      }
    }
  }

  if (OB_SUCCESS == err) {
    thread_read_prepare();
    scanner.reset();
    err = table_mgr_.scan(scan_param, scanner, start_time, timeout);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to scan, err=%d", err);
    }
    FILL_TRACE_LOG("scan from table mgr ret=%d", err);
  }

  err = response_data_(err, scanner, OB_SCAN_RESPONSE, MY_VERSION, conn, channel_id, out_buff, &priority);
  INC_STAT_INFO(UPS_STAT_SCAN_COUNT, 1);
  INC_STAT_INFO(UPS_STAT_SCAN_TIMEU, GET_TRACE_TIMEU());
  FILL_TRACE_LOG("response scanner ret=%d", err);
  PRINT_TRACE_LOG();

  thread_read_complete();

  return err;
}

int ObUpdateServer::ups_get_bloomfilter(const int32_t version, common::ObDataBuffer& in_buff,
                                        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  int64_t frozen_version = 0;
  TableBloomFilter table_bf;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  } else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &frozen_version))) {
    TBSYS_LOG(WARN, "decode cur version fail ret=%d", ret);
  } else {
    ret = table_mgr_.get_frozen_bloomfilter(frozen_version, table_bf);
  }
  ret = response_data_(ret, table_bf, OB_UPS_GET_BLOOM_FILTER_RESPONSE, MY_VERSION, conn, channel_id, out_buff);
  return ret;
}

int ObUpdateServer::ups_freeze_memtable(const int32_t version, ObPacket* packet_orig, common::ObDataBuffer& out_buff, const int pcode) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  TableMgr::FreezeType freeze_type = TableMgr::AUTO_TRIG;
  if (OB_UPS_MINOR_FREEZE_MEMTABLE == pcode) {
    freeze_type = TableMgr::FORCE_MINOR;
  } else if (OB_FREEZE_MEM_TABLE == pcode
             || OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE == pcode) {
    freeze_type = TableMgr::FORCE_MAJOR;
  }
  uint64_t frozen_version = 0;
  bool report_version_changed = false;
  if (OB_SUCCESS != (ret = table_mgr_.freeze_memtable(freeze_type, frozen_version, report_version_changed))) {
    TBSYS_LOG(WARN, "freeze memtable fail ret=%d", ret);
  } else {
    if (report_version_changed) {
      submit_report_freeze();
    }
    submit_handle_frozen();
  }
  if (OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE != pcode) {
    // 本地异步任务不需要应答
    ret = response_data_(ret, frozen_version, OB_FREEZE_MEM_TABLE_RESPONSE, MY_VERSION,
                         packet_orig->get_connection(), packet_orig->getChannelId(), out_buff);
  } else {
    ret = (OB_EAGAIN == ret) ? OB_SUCCESS : ret;
  }
  return ret;
}

int ObUpdateServer::ups_store_memtable(const int32_t version, common::ObDataBuffer& in_buf,
                                       tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  int64_t store_all = 0;
  if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position(), &store_all))) {
    TBSYS_LOG(WARN, "decode store_all flag fail ret=%d", ret);
  } else {
    TBSYS_LOG(INFO, "store memtable store_all=%ld", store_all);
    table_mgr_.store_memtable(0 != store_all);
  }
  response_result_(ret, OB_UPS_STORE_MEM_TABLE_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_handle_frozen() {
  int ret = OB_SUCCESS;
  bool force = false;
  table_mgr_.drop_memtable(force);
  table_mgr_.erase_sstable(force);
  bool store_all = false;
  table_mgr_.store_memtable(store_all);
  table_mgr_.log_table_info();
  return ret;
}

int ObUpdateServer::submit_handle_frozen() {
  return submit_async_task_(OB_UPS_ASYNC_HANDLE_FROZEN, store_thread_, store_thread_queue_size_);
}

int ObUpdateServer::submit_report_freeze() {
  return submit_async_task_(OB_UPS_ASYNC_REPORT_FREEZE, store_thread_, store_thread_queue_size_);
}

int ObUpdateServer::submit_major_freeze() {
  return submit_async_task_(OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE, write_thread_queue_, write_task_queue_size_);
}

int ObUpdateServer::submit_force_drop() {
  return submit_async_task_(OB_UPS_ASYNC_FORCE_DROP_MEMTABLE, read_thread_queue_, read_task_queue_size_);
}

int ObUpdateServer::submit_immediately_drop() {
  int ret = OB_SUCCESS;
  submit_delay_drop();
  warm_up_duty_.finish_immediately();
  return ret;
}

int ObUpdateServer::submit_delay_drop() {
  int ret = OB_SUCCESS;
  if (warm_up_duty_.drop_start()) {
    schedule_warm_up_duty();
  } else {
    TBSYS_LOG(INFO, "there is still a warm up duty running, will not schedule another");
  }
  return ret;
}

void ObUpdateServer::schedule_warm_up_duty() {
  int ret = OB_SUCCESS;
  bool repeat = false;
  if (OB_SUCCESS != (ret = timer_.schedule(warm_up_duty_, get_warm_up_conf().warm_up_step_interval_us, repeat))) {
    TBSYS_LOG(WARN, "schedule warm_up_duty fail ret=%d, will force drop", ret);
    submit_force_drop();
  } else {
    TBSYS_LOG(INFO, "warm up start");
  }
}

template <class Queue>
int ObUpdateServer::submit_async_task_(const PacketCode pcode, Queue& qthread, int32_t& task_queue_size) {
  int ret = OB_SUCCESS;
  ObPacket* ob_packet = NULL;
  if (NULL == (ob_packet = dynamic_cast<ObPacket*>(packet_factory_.createPacket(pcode)))) {
    TBSYS_LOG(WARN, "create packet fail");
    ret = OB_ERROR;
  } else {
    ob_packet->set_packet_code(pcode);
    ob_packet->set_target_id(OB_SELF_FLAG);
    ob_packet->set_receive_ts(tbsys::CTimeUtil::getTime());
    ob_packet->set_source_timeout(INT32_MAX);
    if (OB_SUCCESS != (ret = ob_packet->serialize())) {
      TBSYS_LOG(WARN, "ob_packet serialize fail ret=%d", ret);
    } else if (!qthread.push(ob_packet, task_queue_size, false)) {
      TBSYS_LOG(WARN, "add create index task to thread queue fail task_queue_size=%d", task_queue_size);
      ret = OB_ERROR;
    } else {
      TBSYS_LOG(INFO, "submit async task succ pcode=%d", pcode);
    }
    if (OB_SUCCESS != ret) {
      packet_factory_.destroyPacket(ob_packet);
      ob_packet = NULL;
    }
  }
  return ret;
}

int ObUpdateServer::ups_switch_schema(const int32_t version, ObPacket* packet_orig, common::ObDataBuffer& in_buf) {
  int ret = OB_SUCCESS;
  CommonSchemaManagerWrapper new_schema;

  if (version != MY_VERSION) {
    TBSYS_LOG(ERROR, "Version do not match, MY_VERSION=%d version= %d",
              MY_VERSION, version);
    ret = OB_ERROR_FUNC_VERSION;
  }

  if (OB_SUCCESS == ret) {
    ret = new_schema.deserialize(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "deserialize schema from packet error, ret=%d buf=%p pos=%ld cap=%ld", ret, in_buf.get_data(), in_buf.get_position(), in_buf.get_capacity());
    }
  }

  if (OB_SUCCESS == ret) {
    ret = table_mgr_.switch_schemas(new_schema);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "set_schemas failed, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret) {
    TBSYS_LOG(INFO, "switch schema succ");
  } else {
    TBSYS_LOG(ERROR, "switch schema err, ret=%d schema_version=%ld", ret, new_schema.get_version());
    hex_dump(in_buf.get_data(), in_buf.get_capacity(), false, TBSYS_LOG_LEVEL_ERROR);
  }

  ret = response_result_(ret, OB_SWITCH_SCHEMA_RESPONSE, MY_VERSION,
                         packet_orig->get_connection(), packet_orig->getChannelId());
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "response_result_ err, ret=%d version=%d conn=%p channel_id=%u",
              ret, MY_VERSION, packet_orig->get_connection(), packet_orig->getChannelId());
  }
  return ret;
}

int ObUpdateServer::ups_create_memtable_index() {
  int ret = OB_SUCCESS;

  // memtable建索引 重复调用直接返回OB_SUCCESS
  ret = table_mgr_.create_index();
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "create index fail ret=%d", ret);
  }

  // 反序列化出timestamp
  uint64_t new_version = 0;
  if (OB_SUCCESS == ret) {
    ret = table_mgr_.get_active_memtable_version(new_version);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "decode new version fail ret=%d", ret);
    }
  }

  // 发送返回消息给nameserver
  int64_t retry_times = param_.get_resp_root_times();
  int64_t timeout = param_.get_resp_root_timeout_us();
  ret = RPC_CALL_WITH_RETRY(send_freeze_memtable_resp, retry_times, timeout, name_server_, ups_master_, new_version);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "send freeze memtable resp fail ret_code=%d schema_version=%ld", ret, new_version);
  }
  return ret;
}

int ObUpdateServer::ups_drop_memtable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  bool force = true;
  table_mgr_.drop_memtable(force);
  ret = response_result_(ret, OB_DROP_OLD_TABLETS_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_delay_drop_memtable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  submit_delay_drop();
  ret = response_result_(ret, OB_UPS_DELAY_DROP_MEMTABLE_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_immediately_drop_memtable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  submit_immediately_drop();
  ret = response_result_(ret, OB_UPS_IMMEDIATELY_DROP_MEMTABLE_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_drop_memtable() {
  bool force = true;
  table_mgr_.drop_memtable(force);
  warm_up_duty_.drop_end();
  return OB_SUCCESS;
}

int ObUpdateServer::ups_erase_sstable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  bool force = true;
  table_mgr_.erase_sstable(force);
  ret = response_result_(ret, OB_DROP_OLD_TABLETS_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_load_new_store(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  ret = sstable_mgr_.load_new() ? OB_SUCCESS : OB_ERROR;
  ret = response_result_(ret, OB_UPS_LOAD_NEW_STORE_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_reload_all_store(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  sstable_mgr_.reload_all();
  ret = response_result_(ret, OB_UPS_RELOAD_ALL_STORE_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_froce_report_frozen_version(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  submit_report_freeze();
  ret = response_result_(ret, OB_UPS_FORCE_REPORT_FROZEN_VERSION_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_reload_store(const int32_t version, common::ObDataBuffer& in_buf,
                                     tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  StoreMgr::Handle store_handle = StoreMgr::INVALID_HANDLE;
  if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position(), (int64_t*)&store_handle))) {
    TBSYS_LOG(WARN, "decode store_handle fail ret=%d", ret);
  } else {
    TBSYS_LOG(INFO, "reload store handle=%lu", store_handle);
    sstable_mgr_.reload(store_handle);
  }
  response_result_(ret, OB_UPS_RELOAD_STORE_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_umount_store(const int32_t version, common::ObDataBuffer& in_buff,
                                     tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  ObString umount_dir;
  if (OB_SUCCESS == ret) {
    ret = umount_dir.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "deserialize umount dir error, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret) {
    TBSYS_LOG(INFO, "umount store=[%s]", umount_dir.ptr());
    sstable_mgr_.umount_store(umount_dir.ptr());
    sstable_mgr_.check_broken();
  }
  ret = response_result_(ret, OB_UPS_UMOUNT_STORE_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_slave_register(const int32_t version, common::ObDataBuffer& in_buff,
                                       tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int err = OB_SUCCESS;

  if (version != MY_VERSION) {
    err = OB_ERROR_FUNC_VERSION;
  }

  uint64_t new_log_file_id = 0;
  // deserialize ups_slave
  ObSlaveInfo slave_info;
  err = slave_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "deserialize ObSlaveInfo failed, err=%d", err);
  }

  if (OB_SUCCESS == err) {
    err = log_mgr_.add_slave(slave_info.self, new_log_file_id);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "ObUpsLogMgr add_slave error, err=%d", err);
    }

    char addr_buf[ADDR_BUF_LEN];
    slave_info.self.to_string(addr_buf, sizeof(addr_buf));
    addr_buf[ADDR_BUF_LEN - 1] = '\0';
    TBSYS_LOG(INFO, "add slave, slave_addr=%s, new_log_file_id=%ld, err=%d",
              addr_buf, new_log_file_id, err);
  }

  if (OB_SUCCESS == err) {
    if (LEASE_ON == param_.get_lease_on()) {
      ObLease lease;
      err = slave_mgr_.extend_lease(slave_info.self, lease);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to extend lease, err=%d", err);
      }
    }
  }

  // reply ups slave with related info
  if (OB_SUCCESS == err) {
    ObUpsFetchParam fetch_param;
    fetch_param.fetch_log_ = true;
    fetch_param.fetch_ckpt_ = false;
    fetch_param.min_log_id_ = log_mgr_.get_replay_point();
    fetch_param.max_log_id_ = new_log_file_id - 1;
    err = sstable_mgr_.fill_fetch_param(slave_info.min_sstable_id,
                                        slave_info.max_sstable_id, param_.get_slave_sync_sstable_num(), fetch_param);

    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "ObSSTableMgr fill_fetch_param error, err=%d", err);
    } else {
      err = response_fetch_param_(err, fetch_param, OB_SLAVE_REG_RES, MY_VERSION,
                                  conn, channel_id, out_buff);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to response fetch param, err=%d", err);
      }
    }
  }

  return err;
}

int ObUpdateServer::ups_slave_quit(const int32_t version, common::ObDataBuffer& in_buff,
                                   tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int err = OB_SUCCESS;

  UNUSED(out_buff);

  if (version != MY_VERSION) {
    err = OB_ERROR_FUNC_VERSION;
  }

  // deserialize ups_slave
  ObServer ups_slave;
  err = ups_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "deserialize ups_slave failed, err=%d", err);
  }

  if (OB_SUCCESS == err) {
    err = slave_mgr_.delete_server(ups_slave);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "ObSlaveMgr delete_slave error, err=%d", err);
    }

    char addr_buf[ADDR_BUF_LEN];
    ups_slave.to_string(addr_buf, sizeof(addr_buf));
    addr_buf[ADDR_BUF_LEN - 1] = '\0';
    TBSYS_LOG(INFO, "slave quit, slave_addr=%s, err=%d", addr_buf, err);
  }

  // reply ups slave
  if (OB_SUCCESS == err) {
    err = response_result_(err, OB_SLAVE_QUIT_RES, MY_VERSION, conn, channel_id);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to response slave quit, err=%d", err);
    }
  }

  return err;
}

int ObUpdateServer::ups_apply(UpsTableMgrTransHandle& handle, common::ObDataBuffer& in_buff) {
  int ret = OB_SUCCESS;
  ObUpsMutator ups_mutator_stack;
  ObUpsMutator* ups_mutator_ptr = GET_TSI(ObUpsMutator);
  ObUpsMutator& ups_mutator = (NULL == ups_mutator_ptr) ? ups_mutator_stack : *ups_mutator_ptr;
  ret = ups_mutator.get_mutator().deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
  FILL_TRACE_LOG("mutator deserialize ret=%d", ret);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "deserialize mutator fail ret=%d", ret);
  } else {
    ret = table_mgr_.apply(handle, ups_mutator);
  }
  INC_STAT_INFO(UPS_STAT_APPLY_COUNT, 1);
  INC_STAT_INFO(UPS_STAT_APPLY_TIMEU, GET_TRACE_TIMEU());
  FILL_TRACE_LOG("ret=%d", ret);
  PRINT_TRACE_LOG();
  if (OB_SUCCESS != ret) {
    INC_STAT_INFO(UPS_STAT_APPLY_FAIL_COUNT, 1);
  }
  return ret;
}

int ObUpdateServer::ups_start_transaction(const MemTableTransType type, UpsTableMgrTransHandle& handle) {
  int ret = OB_SUCCESS;
  start_trans_timestamp_ = tbsys::CTimeUtil::getTime();
  ret = table_mgr_.start_transaction(type, handle);
  CLEAR_TRACE_LOG();
  FILL_TRACE_LOG("ret=%d", ret);
  return ret;
}

int ObUpdateServer::response_result_(int32_t ret_code, int32_t cmd_type, int32_t func_version,
                                     tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  common::ObResultCode result_msg;
  ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
  ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());
  result_msg.result_code_ = ret_code;
  ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
  if (OB_SUCCESS == ret) {
    ret = send_response(cmd_type, func_version, out_buff, conn, channel_id);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "send response fail ret=%d conn=%p channel_id=%u result_msg=%d cmd_type=%d func_version=%d",
                ret, conn, channel_id, ret_code, cmd_type, func_version);
    }
  } else {
    TBSYS_LOG(WARN, "send response fail ret=%d conn=%p channel_id=%u result_msg=%d cmd_type=%d func_version=%d",
              ret, conn, channel_id, ret_code, cmd_type, func_version);
  }
  return ret;
}

int ObUpdateServer::ups_end_transaction(tbnet::Packet** packets, const int64_t start_idx,
                                        const int64_t last_idx, UpsTableMgrTransHandle& handle, int32_t last_err_code) {
  bool rollback = false;
  int proc_ret = table_mgr_.end_transaction(handle, rollback);
  int resp_ret = (OB_SUCCESS == proc_ret) ? proc_ret : OB_RESPONSE_TIME_OUT;
  ObPacket** ob_packets = reinterpret_cast<ObPacket**>(packets);

  if (NULL == ob_packets) {
    TBSYS_LOG(WARN, "ob_packet null pointer start_idx=%ld last_idx=%ld", start_idx, last_idx);
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = start_idx; i < last_idx; i++) {
      if (NULL == ob_packets[i]) {
        TBSYS_LOG(WARN, "ob_packet[%ld] null pointer, start_idx=%ld last_idx=%ld", i, start_idx, last_idx);
      } else {
        tmp_ret = response_result_(resp_ret, OB_WRITE_RES, MY_VERSION,
                                   ob_packets[i]->get_connection(), ob_packets[i]->getChannelId());
        TBSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", i, tmp_ret, resp_ret, proc_ret);
      }
    }

    if (NULL == ob_packets[last_idx]) {
      TBSYS_LOG(WARN, "last ob_packet[%ld] null pointer, start_idx=%ld", last_idx, start_idx);
    } else {
      resp_ret = (OB_SUCCESS == last_err_code) ? resp_ret : last_err_code;
      // 最后一个如果成功则返回提交的结果 如果失败则返回apply的结果
      tmp_ret = response_result_(resp_ret, OB_WRITE_RES, MY_VERSION,
                                 ob_packets[last_idx]->get_connection(), ob_packets[last_idx]->getChannelId());
      TBSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", last_idx, tmp_ret, resp_ret, proc_ret);
    }
  }

  int64_t trans_proc_time = tbsys::CTimeUtil::getTime() - start_trans_timestamp_;
  if (trans_proc_time > param_.get_trans_proc_time_warn_us()) {
    TBSYS_LOG(WARN, "transcation process time is too long, process_time=%ld cur_time=%ld response_num=%ld "
              "last_log_network_elapse=%ld last_log_disk_elapse=%ld "
              "read_task_queue_size=%d write_task_queue_size=%d lease_task_queue_size=%d log_task_queue_size=%d",
              trans_proc_time, tbsys::CTimeUtil::getTime(), last_idx - start_idx + 1,
              log_mgr_.get_last_net_elapse(), log_mgr_.get_last_disk_elapse(),
              read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size(), log_thread_queue_.size());
  }

  INC_STAT_INFO(UPS_STAT_BATCH_COUNT, 1);
  INC_STAT_INFO(UPS_STAT_BATCH_TIMEU, GET_TRACE_TIMEU());
  FILL_TRACE_LOG("resp_ret=%d proc_ret=%d", resp_ret, proc_ret);
  PRINT_TRACE_LOG();
  return proc_ret;
}

int ObUpdateServer::ups_reload_conf(const int32_t version, common::ObDataBuffer& in_buff,
                                    tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  ObString conf_file;

  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  } else {
    ret = conf_file.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "deserialize conf file error, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret) {
    ret = param_.reload_from_config(conf_file);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to reload config, ret=%d", ret);
    } else {
      int32_t vip = tbsys::CNetUtil::getAddr(param_.get_ups_vip());
      check_thread_.reset_vip(vip);
      slave_mgr_.reset_vip(vip);
      ob_set_memory_size_limit(param_.get_total_memory_limit());
      MemTableAttr memtable_attr;
      if (OB_SUCCESS == table_mgr_.get_memtable_attr(memtable_attr)) {
        memtable_attr.total_memlimit = param_.get_table_memory_limit();
        table_mgr_.set_memtable_attr(memtable_attr);
      }
      if (param_.get_low_priv_cur_percent() >= 0) {
        read_thread_queue_.set_low_priv_cur_percent(param_.get_low_priv_cur_percent());
      }
    }
  }
  ret = response_result_(ret, OB_UPS_RELOAD_CONF_RESPONSE, MY_VERSION, conn, channel_id);

  return ret;
}

int ObUpdateServer::ups_renew_lease(const int32_t version, common::ObDataBuffer& in_buff,
                                    tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;

  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  ObServer ups_slave;
  ObLease lease;
  if (OB_SUCCESS == ret) {
    ret = ups_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to deserialize ups slave, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret) {
    ret = slave_mgr_.extend_lease(ups_slave, lease);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to extend lease, ret=%d", ret);
    }
  }

  ret = response_lease_(ret, lease, OB_RENEW_LEASE_RESPONSE, MY_VERSION,
                        conn, channel_id, out_buff);

  return ret;
}

int ObUpdateServer::ups_grant_lease(const int32_t version, common::ObDataBuffer& in_buff,
                                    tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;

  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  ObLease lease;
  if (OB_SUCCESS == ret) {
    ret = lease.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to deserialize ups slave, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret) {
    ret = check_thread_.renew_lease(lease);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to renew lease, ret=%d", ret);
    }
  }

  ret = response_lease_(ret, lease, OB_GRANT_LEASE_RESPONSE, MY_VERSION,
                        conn, channel_id, out_buff);

  return ret;
}

int ObUpdateServer::ups_change_vip(const int32_t version, common::ObDataBuffer& in_buff,
                                   tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;

  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  int32_t new_vip = 0;
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_i32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &new_vip);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to decode vip, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret) {
    check_thread_.reset_vip(new_vip);
    slave_mgr_.reset_vip(new_vip);
  }

  ret = response_result_(ret, OB_UPS_CHANGE_VIP_RESPONSE, MY_VERSION, conn, channel_id);

  return ret;
}

int ObUpdateServer::ups_dump_text_memtable(const int32_t version, common::ObDataBuffer& in_buff,
                                           tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  ObString dump_dir;
  if (OB_SUCCESS == ret) {
    ret = dump_dir.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "deserialize dump dir error, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret) {
    ret = response_result_(ret, OB_UPS_DUMP_TEXT_MEMTABLE_RESPONSE, MY_VERSION, conn, channel_id);
    table_mgr_.dump_memtable(dump_dir);
  }
  return ret;
}

int ObUpdateServer::ups_dump_text_schemas(const int32_t version,
                                          tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  if (OB_SUCCESS == ret) {
    table_mgr_.dump_schemas();
  }

  if (OB_SUCCESS == ret) {
    ob_print_mod_memory_usage();
  }

  ret = response_result_(ret, OB_UPS_DUMP_TEXT_SCHEMAS_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_force_fetch_schema(const int32_t version,
                                           tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  if (OB_SUCCESS == ret) {
    bool write_log = true;
    ret = update_schema(false, write_log);
  }
  ret = response_result_(ret, OB_UPS_FORCE_FETCH_SCHEMA_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_memory_watch(const int32_t version,
                                     tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  UpsMemoryInfo memory_info;
  if (OB_SUCCESS == ret) {
    memory_info.total_size = ob_get_memory_size_handled();
    memory_info.cur_limit_size = ob_get_memory_size_limit();
    table_mgr_.get_memtable_memory_info(memory_info.table_mem_info);
    table_mgr_.log_table_info();
    ob_print_mod_memory_usage();
  }
  ret = response_data_(ret, memory_info, OB_UPS_MEMORY_WATCH_RESPONSE, MY_VERSION,
                       conn, channel_id, out_buff);
  return ret;
}

int ObUpdateServer::ups_memory_limit_set(const int32_t version, common::ObDataBuffer& in_buff,
                                         tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  UpsMemoryInfo memory_info;
  if (OB_SUCCESS == ret) {
    ret = memory_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS == ret) {
      ob_set_memory_size_limit(memory_info.cur_limit_size);
      MemTableAttr memtable_attr;
      if (OB_SUCCESS == table_mgr_.get_memtable_attr(memtable_attr)) {
        memtable_attr.total_memlimit = memory_info.table_mem_info.memtable_limit;
        table_mgr_.set_memtable_attr(memtable_attr);
      }
    }
    memory_info.total_size = ob_get_memory_size_handled();
    memory_info.cur_limit_size = ob_get_memory_size_limit();
    table_mgr_.get_memtable_memory_info(memory_info.table_mem_info);
    table_mgr_.log_table_info();
  }
  ret = response_data_(ret, memory_info, OB_UPS_MEMORY_LIMIT_SET_RESPONSE, MY_VERSION,
                       conn, channel_id, out_buff);
  return ret;
}

int ObUpdateServer::ups_priv_queue_conf_set(const int32_t version, common::ObDataBuffer& in_buff,
                                            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  UpsPrivQueueConf priv_queue_conf;
  if (OB_SUCCESS == ret) {
    ret = priv_queue_conf.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS == ret) {
      param_.set_priv_queue_conf(priv_queue_conf);
      if (param_.get_low_priv_cur_percent() >= 0) {
        read_thread_queue_.set_low_priv_cur_percent(param_.get_low_priv_cur_percent());
      }
    }
    priv_queue_conf.low_priv_network_lower_limit = param_.get_low_priv_network_lower_limit();
    priv_queue_conf.low_priv_network_upper_limit = param_.get_low_priv_network_upper_limit();
    priv_queue_conf.low_priv_adjust_flag = param_.get_low_priv_adjust_flag();
    priv_queue_conf.low_priv_cur_percent = param_.get_low_priv_cur_percent();
  }

  ret = response_data_(ret, priv_queue_conf, OB_UPS_PRIV_QUEUE_CONF_SET_RESPONSE, MY_VERSION,
                       conn, channel_id, out_buff);
  return ret;
}

int ObUpdateServer::ups_clear_active_memtable(const int32_t version,
                                              tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  if (OB_SUCCESS == ret) {
    ret = table_mgr_.clear_active_memtable();
  }
  ret = response_result_(ret, OB_UPS_CLEAR_ACTIVE_MEMTABLE_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_switch_commit_log(const int32_t version,
                                          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  uint64_t new_log_file_id = 0;
  int proc_ret = OB_SUCCESS;
  if (OB_SUCCESS == ret) {
    proc_ret = log_mgr_.switch_log_file(new_log_file_id);
    TBSYS_LOG(INFO, "switch log file id ret=%d new_log_file_id=%lu", ret, new_log_file_id);
  }
  ret = response_data_(proc_ret, new_log_file_id, OB_UPS_SWITCH_COMMIT_LOG_RESPONSE, MY_VERSION,
                       conn, channel_id, out_buff);
  return ret;
}

int ObUpdateServer::ups_get_last_frozen_version(const int32_t version,
                                                tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  uint64_t last_frozen_memtable_version = 0;
  if (OB_SUCCESS == ret) {
    ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_memtable_version);
  }
  ret = response_data_(ret, last_frozen_memtable_version, OB_UPS_MEMORY_LIMIT_SET_RESPONSE, MY_VERSION,
                       conn, channel_id, out_buff);
  return ret;
}

int ObUpdateServer::ups_get_table_time_stamp(const int32_t version, common::ObDataBuffer& in_buff,
                                             tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  int proc_ret = OB_SUCCESS;
  uint64_t major_version = 0;
  int64_t time_stamp = 0;
  if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&major_version))) {
    TBSYS_LOG(WARN, "decode major_version fail ret=%d", ret);
  } else {
    proc_ret = table_mgr_.get_table_time_stamp(major_version, time_stamp);
    TBSYS_LOG(INFO, "get_table_time_stamp ret=%d major_version=%lu time_stamp=%ld", proc_ret, major_version, time_stamp);
  }
  ret = response_data_(proc_ret, time_stamp, OB_UPS_GET_TABLE_TIME_STAMP_RESPONSE, MY_VERSION,
                       conn, channel_id, out_buff);
  return ret;
}

int ObUpdateServer::ups_enable_memtable_checksum(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  if (OB_SUCCESS == ret) {
    table_mgr_.set_replay_checksum_flag(true);
  }
  ret = response_result_(ret, OB_UPS_ENABLE_MEMTABLE_CHECKSUM_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_disable_memtable_checksum(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  if (OB_SUCCESS == ret) {
    table_mgr_.set_replay_checksum_flag(false);
  }
  ret = response_result_(ret, OB_UPS_ENABLE_MEMTABLE_CHECKSUM_RESPONSE, MY_VERSION, conn, channel_id);
  return ret;
}

int ObUpdateServer::ups_fetch_stat_info(const int32_t version,
                                        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }
  if (OB_SUCCESS == ret) {
    SET_STAT_INFO(UPS_STAT_MEMORY_TOTAL, ob_get_memory_size_handled());
    SET_STAT_INFO(UPS_STAT_MEMORY_LIMIT, ob_get_memory_size_limit());
    table_mgr_.update_memtable_stat_info();
  }
  ret = response_data_(ret, stat_mgr_, OB_FETCH_STATS_RESPONSE, MY_VERSION,
                       conn, channel_id, out_buff);
  return ret;
}

int ObUpdateServer::low_priv_speed_control_(const int64_t scanner_size) {
  int ret = OB_SUCCESS;
  static volatile int64_t s_stat_times = 0;
  static volatile int64_t s_stat_size = 0;
  static volatile int64_t s_last_stat_time_ms = tbsys::CTimeUtil::getTime() / 1000L;
  static volatile int32_t flag = 0;

  if (scanner_size < 0) {
    TBSYS_LOG(WARN, "invalid param, scanner_size=%d", scanner_size);
    ret = OB_ERROR;
  } else {
    atomic_inc((volatile uint64_t*) &s_stat_times);
    atomic_add((volatile uint64_t*) &s_stat_size, scanner_size);

    if (s_stat_times >= SEG_STAT_TIMES || s_stat_size >= SEG_STAT_SIZE * 1024L * 1024L) {
      if (atomic_compare_exchange((volatile uint32_t*) &flag, 1, 0) == 0) {
        // only one thread is allowed to adjust network limit
        int64_t cur_time_ms = tbsys::CTimeUtil::getTime() / 1000L;

        TBSYS_LOG(DEBUG, "stat_size=%ld cur_time_ms=%ld last_stat_time_ms=%ld", s_stat_size,
                  cur_time_ms, s_last_stat_time_ms);

        int64_t adjust_flag = param_.get_low_priv_adjust_flag();

        if (1 == adjust_flag) { // auto adjust low priv percent
          int64_t lower_limit = param_.get_low_priv_network_lower_limit() * 1024L * 1024L;
          int64_t upper_limit = param_.get_low_priv_network_upper_limit() * 1024L * 1024L;

          int64_t low_priv_percent = read_thread_queue_.get_low_priv_cur_percent();

          if (s_stat_size * 1000L < lower_limit * (cur_time_ms - s_last_stat_time_ms)) {
            if (low_priv_percent < PriorityPacketQueueThread::LOW_PRIV_MAX_PERCENT) {
              ++low_priv_percent;
              read_thread_queue_.set_low_priv_cur_percent(low_priv_percent);

              TBSYS_LOG(INFO, "network lower limit, lower_limit=%ld, low_priv_percent=%ld",
                        lower_limit, low_priv_percent);
            }
          } else if (s_stat_size * 1000L > upper_limit * (cur_time_ms - s_last_stat_time_ms)) {
            if (low_priv_percent > PriorityPacketQueueThread::LOW_PRIV_MIN_PERCENT) {
              --low_priv_percent;
              read_thread_queue_.set_low_priv_cur_percent(low_priv_percent);

              TBSYS_LOG(INFO, "network upper limit, upper_limit=%ld, low_priv_percent=%ld",
                        upper_limit, low_priv_percent);
            }
          }
        }

        // reset stat_times, stat_size and last_stat_time
        s_stat_times = 0;
        s_stat_size = 0;
        s_last_stat_time_ms = cur_time_ms;

        flag = 0;
      }
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

UpsWarmUpDuty::UpsWarmUpDuty() : duty_start_time_(0),
  cur_warm_up_percent_(0),
  duty_waiting_(0) {
}

UpsWarmUpDuty::~UpsWarmUpDuty() {
}

bool UpsWarmUpDuty::drop_start() {
  bool bret = false;
  if (0 == atomic_compare_exchange(&duty_waiting_, 1, 0)) {
    bret = true;
  } else {
    if (0 != duty_start_time_
        && tbsys::CTimeUtil::getTime() > (MAX_DUTY_IDLE_TIME + duty_start_time_)) {
      TBSYS_LOG(WARN, "duty has run too long and will be rescheduled, duty_start_time=%ld",
                duty_start_time_);
      bret = true;
    }
  }
  if (bret) {
    duty_start_time_ = tbsys::CTimeUtil::getTime();
    cur_warm_up_percent_ = 0;
  }
  return bret;
}

void UpsWarmUpDuty::drop_end() {
  atomic_exchange(&duty_waiting_, 0);
  duty_start_time_ = 0;
  cur_warm_up_percent_ = 0;
}

void UpsWarmUpDuty::finish_immediately() {
  duty_start_time_ = 0;
}

void UpsWarmUpDuty::runTimerTask() {
  if (tbsys::CTimeUtil::getTime() > (duty_start_time_ + get_warm_up_conf().warm_up_time_s * 1000L * 1000L)) {
    submit_force_drop();
    TBSYS_LOG(INFO, "warm up finished, will drop memtable, cur_warm_up_percent=%ld cur_time=%ldus warm_time=%lds",
              cur_warm_up_percent_, tbsys::CTimeUtil::getTime(), get_warm_up_conf().warm_up_time_s);
  } else {
    if (CacheWarmUpConf::STOP_PERCENT > cur_warm_up_percent_) {
      cur_warm_up_percent_++;
      TBSYS_LOG(INFO, "warm up percent update to %ld \%", cur_warm_up_percent_);
      set_warm_up_percent(cur_warm_up_percent_);
    }
    schedule_warm_up_duty();
  }
}

}
}


