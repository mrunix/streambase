/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include <tbsys.h>

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_packet.h"
#include "common/ob_result.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/utility.h"
#include "common/ob_atomic.h"
#include "nameserver/nameserver_worker.h"
#include "nameserver/nameserver_admin_cmd.h"
#include "nameserver/nameserver_stat_key.h"
#include <sys/types.h>
#include <unistd.h>
//#define PRESS_TEST

namespace {
const int WRITE_THREAD_FLAG = 1;
const int LOG_THREAD_FLAG = 2;
const int DEFAULT_TASK_READ_QUEUE_SIZE = 500;
const int DEFAULT_TASK_WRITE_QUEUE_SIZE = 50;
const int DEFAULT_TASK_LOG_QUEUE_SIZE = 50;
const int DEFAULT_VIP_CHECK_PERIOD_US = 500 * 1000; // 500ms
const int64_t DEFAULT_LOG_SYNC_LIMIT_KB = 1024 * 40;
const int64_t DEFAULT_LOG_SYNC_TIMEOUT_US = 500 * 1000;
const int64_t DEFAULT_REPLAY_WAIT_TIME_US = 100L * 1000; // 100ms
const int64_t DEFAULT_REGISTER_TIMEOUT_US = 3L * 1000 * 1000; // 3s
const int64_t DEFAULT_LEASE_ON = 1; // default to on
const int64_t DEFAULT_LEASE_INTERVAL_US = 8 * 1000 * 1000; // 5 seconds
const int64_t DEFAULT_LEASE_RESERVED_TIME_US = 5 * 1000 * 1000;
const int64_t LEASE_ON = 1;
const int64_t DEFAULT_SLAVE_QUIT_TIMEOUT = 1000 * 1000; // 1s

const char* STR_NAME_SECTION = "name_server";
const char* STR_THREAD_COUNT = "thread_count";
const char* STR_READ_QUEUE_SIZE = "read_queue_size";
const char* STR_WRITE_QUEUE_SIZE = "write_queue_size";
const char* STR_LOG_QUEUE_SIZE = "log_queue_size";
const char* STR_VIP = "vip";
const char* STR_NETWORK_TIMEOUT = "network_timeout";

const char* STR_LOG_SYNC_LIMIT_KB = "log_sync_limit_kb";
const char* STR_LOG_SYNC_TIMEOUT_US = "log_sync_timeout_us";
const char* STR_REPLAY_WAIT_TIME_US = "replay_wait_time_us";
const char* STR_REGISTER_TIMEOUT_US = "register_timeout_us";
const char* STR_VIP_CHECK_PERIOD_US = "vip_check_period_us";

const char* STR_LEASE_ON = "lease_on";
const char* STR_LEASE_INTERVAL_US = "lease_interval_us";
const char* STR_LEASE_RESERVED_TIME_US = "lease_reserved_time_us";
const char* STR_EXPECTED_PROCESS_US = "expected_process_us";
const int64_t PACKET_TIME_OUT = 100 * 1000;  //100 ms
const int32_t ADDR_BUF_LEN = 64;
}
namespace sb {
using namespace common;
namespace nameserver {
using namespace sb::common;

//inet_ntoa_r(conn->getPeerId())
const char* inet_ntoa_r(const uint64_t ipport) {
  static const int64_t BUFFER_SIZE = 32;
  static __thread char buffers[2][BUFFER_SIZE];
  static __thread int64_t i = 0;
  char* buffer = buffers[i++ % 2];
  buffer[0] = '\0';

  uint32_t ip = (uint32_t)(ipport & 0xffffffff);
  int port = (int)((ipport >> 32) & 0xffff);
  unsigned char* bytes = (unsigned char*) &ip;
  if (port > 0) {
    snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
  } else {
    snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:-1", bytes[0], bytes[1], bytes[2], bytes[3]);
  }

  return buffer;
}

NameServerWorker::NameServerWorker()
  : is_registered_(false),
    task_read_queue_size_(DEFAULT_TASK_READ_QUEUE_SIZE),
    task_write_queue_size_(DEFAULT_TASK_WRITE_QUEUE_SIZE),
    task_log_queue_size_(DEFAULT_TASK_LOG_QUEUE_SIZE),
    network_timeout_(PACKET_TIME_OUT),
    stat_manager_() {
  config_file_name_[0] = '\0';
}

NameServerWorker::~NameServerWorker() {
}

int NameServerWorker::set_config_file_name(const char* conf_file_name) {
  strncpy(config_file_name_, conf_file_name, OB_MAX_FILE_NAME_LENGTH);
  config_file_name_[OB_MAX_FILE_NAME_LENGTH - 1] = '\0';
  return OB_SUCCESS;
}

int NameServerWorker::initialize() {
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == ret) {
    ret = client_manager.initialize(get_transport(), get_packet_streamer());
  }

  if (OB_SUCCESS == ret) {
    ret = ns_rpc_stub_.init(&client_manager, &my_thread_buffer);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "init rpc stub failed, err=%d", ret);
    }
  }

  const char* vip_str = NULL;
  if (ret == OB_SUCCESS) {
    vip_str =  TBSYS_CONFIG.getString(STR_NAME_SECTION, STR_VIP, NULL);
    if (vip_str == NULL) {
      TBSYS_LOG(ERROR, "vip of nameserver is not set, section: %s, config item: %s", STR_NAME_SECTION, STR_VIP);
      ret = OB_INVALID_ARGUMENT;
    }
  }
  uint32_t vip = tbsys::CNetUtil::getAddr(vip_str);

  if (ret == OB_SUCCESS) {
    int32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name_);
    if (!self_addr_.set_ipv4_addr(local_ip, port_)) {
      TBSYS_LOG(ERROR, "nameserver address invalid, ip:%d, port:%d", local_ip, port_);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret) {
    expected_process_us_ = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_EXPECTED_PROCESS_US, 10000);
    if (0 >= expected_process_us_) {
      TBSYS_LOG(ERROR, "invalid expected_process_us_=%ld", expected_process_us_);
      ret = OB_INVALID_ARGUMENT;
    } else {
      TBSYS_LOG(INFO, "expected_process_us=%ld", expected_process_us_);
    }
  }

  if (OB_SUCCESS == ret) {
    int64_t log_sync_timeout = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_LOG_SYNC_TIMEOUT_US, DEFAULT_LOG_SYNC_TIMEOUT_US);
    int64_t lease_interval = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_LEASE_INTERVAL_US, DEFAULT_LEASE_INTERVAL_US);
    int64_t lease_reserv = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_LEASE_RESERVED_TIME_US, DEFAULT_LEASE_RESERVED_TIME_US);
    ret = slave_mgr_.init(vip, &ns_rpc_stub_, log_sync_timeout, lease_interval, lease_reserv);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "failed to init slave manager, err=%d", ret);
    }
  }

  if (OB_SUCCESS == ret) {
    int thread_count = 0;
    thread_count = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_THREAD_COUNT, 20);
    read_thread_queue_.setThreadParameter(thread_count, this, NULL);
    void* args = reinterpret_cast<void*>(WRITE_THREAD_FLAG);
    write_thread_queue_.setThreadParameter(1, this, args);

    args = reinterpret_cast<void*>(LOG_THREAD_FLAG);
    log_thread_queue_.setThreadParameter(1, this, args);

    task_read_queue_size_ = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_READ_QUEUE_SIZE, DEFAULT_TASK_READ_QUEUE_SIZE);
    task_write_queue_size_ = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_WRITE_QUEUE_SIZE, DEFAULT_TASK_WRITE_QUEUE_SIZE);
    task_log_queue_size_ = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_LOG_QUEUE_SIZE, DEFAULT_TASK_LOG_QUEUE_SIZE);
    // read_thread_queue_.start();
    // write_thread_queue_.start();
  }

  if (ret == OB_SUCCESS) {
    if (tbsys::CNetUtil::isLocalAddr(vip)) {
      TBSYS_LOG(INFO, "I am holding the VIP, set role to MASTER");
      role_mgr_.set_role(ObRoleMgr::MASTER);
    } else {
      TBSYS_LOG(INFO, "I am not holding the VIP, set role to SLAVE");
      role_mgr_.set_role(ObRoleMgr::SLAVE);
    }
    rt_master_.set_ipv4_addr(vip, port_);

    int64_t vip_check_period_us = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_VIP_CHECK_PERIOD_US, DEFAULT_VIP_CHECK_PERIOD_US);
    ret = check_thread_.init(&role_mgr_, vip, vip_check_period_us, &ns_rpc_stub_, &rt_master_, &self_addr_);
  }

  if (ret == OB_SUCCESS) {
    int64_t now = 0;
    now = tbsys::CTimeUtil::getTime();
    if (!name_server_.init(config_file_name_, now, this)) {
      ret = OB_ERROR;
    }
  }
  TBSYS_LOG(INFO, "root worker init, ret=%d", ret);
  return ret;
}

int NameServerWorker::start_service() {
  int ret = OB_ERROR;

  ObRoleMgr::Role role = role_mgr_.get_role();
  if (role == ObRoleMgr::MASTER) {
    ret = start_as_master();
  } else if (role == ObRoleMgr::SLAVE) {
    ret = start_as_slave();
  } else {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "unknow role: %d, nameserver start failed", role);
  }

  return ret;
}

int NameServerWorker::start_as_master() {
  int ret = OB_ERROR;
  TBSYS_LOG(INFO, "[NOTICE] master start step1");
  ret = log_manager_.init(&name_server_, &slave_mgr_);
  if (ret == OB_SUCCESS) {
    // try to replay log
    TBSYS_LOG(INFO, "[NOTICE] master start step2");
    ret = log_manager_.replay_log();
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "[NOTICE] master replay log failed, err=%d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    TBSYS_LOG(INFO, "[NOTICE] master start step3");
    log_manager_.get_log_worker()->reset_cs_hb_time();
  }

  if (ret == OB_SUCCESS) {
    TBSYS_LOG(INFO, "[NOTICE] master start step4");
    network_timeout_ = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_NETWORK_TIMEOUT, PACKET_TIME_OUT);
    role_mgr_.set_state(ObRoleMgr::ACTIVE);

    read_thread_queue_.start();
    write_thread_queue_.start();
    check_thread_.start();
    TBSYS_LOG(INFO, "[NOTICE] master start-up finished");

    // wait finish
    for (;;) {
      if (ObRoleMgr::STOP == role_mgr_.get_state()
          || ObRoleMgr::ERROR == role_mgr_.get_state()) {
        TBSYS_LOG(INFO, "role manager change state, stat=%d", role_mgr_.get_state());
        break;
      }
      usleep(10 * 1000); // 10 ms
    }
  }

  TBSYS_LOG(INFO, "[NOTICE] going to quit");
  stop();

  return ret;
}

int NameServerWorker::start_as_slave() {
  int err = OB_SUCCESS;

  // get obi role from the master
  if (err == OB_SUCCESS) {
    err = get_obi_role_from_master();
  }

  log_thread_queue_.start();
  err = log_manager_.init(&name_server_, &slave_mgr_);

  ObFetchParam fetch_param;
  if (err == OB_SUCCESS) {
    err = slave_register_(fetch_param);
  }

  if (err == OB_SUCCESS) {
    int64_t replay_wait_time = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_REPLAY_WAIT_TIME_US, DEFAULT_REPLAY_WAIT_TIME_US);
    err = log_replay_thread_.init(log_manager_.get_log_dir_path(), fetch_param.min_log_id_, 0, &role_mgr_, NULL, replay_wait_time);
    log_replay_thread_.set_log_manager(&log_manager_);
  }

  if (err == OB_SUCCESS) {
    err = fetch_thread_.init(rt_master_, log_manager_.get_log_dir_path(), fetch_param, &role_mgr_, &log_replay_thread_);
    if (err == OB_SUCCESS) {
      int64_t log_limit = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_LOG_SYNC_LIMIT_KB, DEFAULT_LOG_SYNC_LIMIT_KB);
      fetch_thread_.set_limit_rate(log_limit);
      fetch_thread_.add_ckpt_ext(NameServer::ROOT_TABLE_EXT); // add root table file
      fetch_thread_.add_ckpt_ext(NameServer::CHUNKSERVER_LIST_EXT); // add chunkserver list file
      fetch_thread_.set_log_manager(&log_manager_);
      fetch_thread_.start();
      TBSYS_LOG(INFO, "slave fetch_thread started");

      if (fetch_param.fetch_ckpt_) {
        err = fetch_thread_.wait_recover_done();
      }
    } else {
      TBSYS_LOG(ERROR, "failed to init fetch log thread");
    }
  }

  if (err == OB_SUCCESS) {
    role_mgr_.set_state(ObRoleMgr::ACTIVE);
    // we SHOULD start root_modifier after recover checkpoint
    name_server_.start_threads();
    name_server_.wait_init_finished();
    TBSYS_LOG(INFO, "slave root_table_modifier, balance_worker, heartbeat_checker threads started");
  }

  // we SHOULD start replay thread after wait_init_finished
  if (err == OB_SUCCESS) {
    log_replay_thread_.start();
    TBSYS_LOG(INFO, "slave log_replay_thread started");
  } else {
    TBSYS_LOG(ERROR, "failed to start log replay thread");
  }

  if (err == OB_SUCCESS) {
    check_thread_.start();
    TBSYS_LOG(INFO, "slave check_thread started");

    while (ObRoleMgr::SWITCHING != role_mgr_.get_state() // lease is valid and vip is mine
           && ObRoleMgr::INIT != role_mgr_.get_state() //  lease is invalid, should reregister to master
           // but now just let it exit.
           && ObRoleMgr::STOP != role_mgr_.get_state() // stop normally
           && ObRoleMgr::ERROR != role_mgr_.get_state()) {
      usleep(10 * 1000); // 10 ms
    }

    if (ObRoleMgr::SWITCHING == role_mgr_.get_state()) {
      TBSYS_LOG(WARN, "nameserver slave begin switch to master");

      role_mgr_.set_role(ObRoleMgr::MASTER);
      TBSYS_LOG(INFO, "[NOTICE] set role to master");
      log_manager_.get_log_worker()->reset_cs_hb_time();

      log_thread_queue_.stop();
      log_thread_queue_.wait();
      // log replay thread will stop itself when
      // role switched to MASTER and nothing
      // more to replay
      log_replay_thread_.wait();
      fetch_thread_.wait();

      // update role after log replay thread done
      //role_mgr_.set_role(ObRoleMgr::MASTER);

      read_thread_queue_.start();
      write_thread_queue_.start();

      //get last frozen mem table version from updateserver
      int64_t frozen_version = 1;
      if (OB_SUCCESS == ns_rpc_stub_.get_last_frozen_version(name_server_.get_update_server_info(),
                                                             network_timeout_, frozen_version)) {
        name_server_.report_frozen_memtable(frozen_version, false);
      } else {
        TBSYS_LOG(WARN, "get frozen version failed");
      }

      role_mgr_.set_state(ObRoleMgr::ACTIVE);
      TBSYS_LOG(INFO, "set stat to ACTIVE");

      is_registered_ = false;
      TBSYS_LOG(WARN, "nameserver slave switched to master");

      for (;;) {
        if (ObRoleMgr::STOP == role_mgr_.get_state()
            || ObRoleMgr::ERROR == role_mgr_.get_state()) {
          TBSYS_LOG(INFO, "role manager change state, stat=%d", role_mgr_.get_state());
          break;
        }
        usleep(10 * 1000);
      }
    }
  }

  TBSYS_LOG(INFO, "[NOTICE] going to quit");
  stop();
  TBSYS_LOG(INFO, "[NOTICE] server terminated");
  return err;
}

int NameServerWorker::get_obi_role_from_master() {
  int ret = OB_SUCCESS;
  ObiRole role;
  const static int SLEEP_US_WHEN_INIT = 2000 * 1000; // 2s
  while (true) {
    ret = ns_rpc_stub_.get_obi_role(rt_master_, network_timeout_, role);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "failed to get obi_role from the master, err=%d", ret);
      break;
    } else if (ObiRole::INIT == role.get_role()) {
      TBSYS_LOG(INFO, "we should wait when obi_role=INIT");
      usleep(SLEEP_US_WHEN_INIT);
    } else {
      ret = name_server_.set_obi_role(role);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "failed to set_obi_role, err=%d", ret);
      }
      break;
    }
    if (ObRoleMgr::STOP == role_mgr_.get_state()) {
      TBSYS_LOG(INFO, "server stopped, break");
      ret = OB_ERROR;
      break;
    }
  } // end while
  return ret;
}

void NameServerWorker::destroy() {
  role_mgr_.set_state(ObRoleMgr::STOP);

  if (ObRoleMgr::SLAVE == role_mgr_.get_role()) {
    if (is_registered_) {
      ns_rpc_stub_.slave_quit(rt_master_, self_addr_, DEFAULT_SLAVE_QUIT_TIMEOUT);
      is_registered_ = false;
    }
    log_thread_queue_.stop();
    fetch_thread_.stop();
    log_replay_thread_.stop();
    check_thread_.stop();
  } else {
    read_thread_queue_.stop();
    write_thread_queue_.stop();
    check_thread_.stop();
  }
  TBSYS_LOG(INFO, "stop flag set");
  wait_for_queue();
  name_server_.stop_threads();
}

void NameServerWorker::wait_for_queue() {
  if (ObRoleMgr::SLAVE == role_mgr_.get_role()) {
    log_thread_queue_.wait();
    TBSYS_LOG(INFO, "log thread stopped");
    fetch_thread_.wait();
    TBSYS_LOG(INFO, "fetch thread stopped");
    log_replay_thread_.wait();
    TBSYS_LOG(INFO, "replay thread stopped");
    check_thread_.wait();
    TBSYS_LOG(INFO, "check thread stopped");
  } else {
    read_thread_queue_.wait();
    write_thread_queue_.wait();
    check_thread_.wait();
  }
}

tbnet::IPacketHandler::HPRetCode NameServerWorker::handlePacket(tbnet::Connection* connection, tbnet::Packet* packet) {
  tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::FREE_CHANNEL;
  if (!packet->isRegularPacket()) {
    TBSYS_LOG(WARN, "control packet, packet code: %d", ((tbnet::ControlPacket*)packet)->getCommand());
  } else {
    ObPacket* req = (ObPacket*) packet;
    req->set_connection(connection);
    bool ps = true;
    int packet_code = req->get_packet_code();
    TBSYS_LOG(DEBUG, "get packet code is %d", packet_code);
    switch (packet_code) {
    case OB_SEND_LOG:
    case OB_GRANT_LEASE_REQUEST:
      if (ObRoleMgr::SLAVE == role_mgr_.get_role()) {
        ps = log_thread_queue_.push(req, task_log_queue_size_, false);
      } else {
        ps = false;
      }
      break;
    case OB_RENEW_LEASE_REQUEST:
    case OB_SLAVE_QUIT:
      if (ObRoleMgr::MASTER == role_mgr_.get_role()) {
        ps = read_thread_queue_.push(req, task_read_queue_size_, false);
      } else {
        ps = false;
      }
      break;
    case OB_REPORT_TABLETS:
    case OB_SERVER_REGISTER:
    case OB_MIGRATE_OVER:
    case OB_REPORT_CAPACITY_INFO:
    case OB_SLAVE_REG:
    case OB_WAITING_JOB_DONE:
      //the packet will cause write to b+ tree
      if (ObRoleMgr::MASTER == role_mgr_.get_role()) {
        ps = write_thread_queue_.push(req, task_write_queue_size_, false);
      } else {
        ps = false;
      }
      break;
    case OB_FETCH_SCHEMA:
    case OB_FETCH_SCHEMA_VERSION:
    case OB_GET_REQUEST:
    case OB_SCAN_REQUEST:
    case OB_HEARTBEAT:
    case OB_DUMP_CS_INFO:
    case OB_FETCH_STATS:
    case OB_GET_UPDATE_SERVER_INFO:
    case OB_UPDATE_SERVER_REPORT_FREEZE:
    case OB_GET_UPDATE_SERVER_INFO_FOR_MERGE:
    case OB_GET_MERGE_DELAY_INTERVAL:
    case OB_GET_OBI_ROLE:
    case OB_SET_OBI_ROLE:
    case OB_CHANGE_LOG_LEVEL:
    case OB_RS_ADMIN:
    case OB_RS_STAT:
    case OB_GET_OBI_CONFIG:
    case OB_SET_OBI_CONFIG:
    case OB_GET_UPS:
    case OB_SET_UPS_CONFIG:
    case OB_GET_CLIENT_CONFIG:
    case OB_GET_CS_LIST:
    case OB_GET_MS_LIST:
      if (ObRoleMgr::MASTER == role_mgr_.get_role()) {
        ps = read_thread_queue_.push(req, task_read_queue_size_, false);
      } else {
        ps = false;
      }
      break;
    case OB_PING_REQUEST: // response PING immediately
      ps = true;
      {
        ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
        ObDataBuffer thread_buffer(my_buffer->current(), my_buffer->remain());
        int return_code = rt_ping(req->get_api_version(), *req->get_buffer(),
                                  connection, req->getChannelId(), thread_buffer);
        if (OB_SUCCESS != return_code) {
          TBSYS_LOG(WARN, "response ping error. return code is %d", return_code);
        }
      }
      break;
    default:
      ps = false; // so this unknown packet will be freed
      TBSYS_LOG(WARN, "UNKNOWN packet %d, ignore this", packet_code);
      break;
    }
    if (!ps) {
      if (OB_FETCH_STATS != packet_code) {
        TBSYS_LOG(ERROR, "packet %d can not be distribute to queue, role=%d rqueue_size=%ld wqueue_size=%ld",
                  packet_code, role_mgr_.get_role(),
                  read_thread_queue_.size(),
                  write_thread_queue_.size());
      }
      rc = tbnet::IPacketHandler::KEEP_CHANNEL;
    }
  }
  return rc;
}

bool NameServerWorker::handleBatchPacket(tbnet::Connection* connection, tbnet::PacketQueue& packetQueue) {
  UNUSED(connection);
  UNUSED(packetQueue);
  TBSYS_LOG(ERROR, "you should not reach this, not supporrted");
  return true;
}

bool NameServerWorker::handlePacketQueue(tbnet::Packet* packet, void* args) {
  bool ret = true;
  int return_code = OB_SUCCESS;
  static __thread int64_t worker_counter = 0;
  static volatile uint64_t total_counter = 0;

  ObPacket* sb_packet = reinterpret_cast<ObPacket*>(packet);
  int packet_code = sb_packet->get_packet_code();
  int version = sb_packet->get_api_version();
  uint32_t channel_id = sb_packet->getChannelId();//tbnet need this

  int64_t source_timeout = sb_packet->get_source_timeout();
  if (source_timeout > 0) {
    int64_t block_us = tbsys::CTimeUtil::getTime() - sb_packet->get_receive_ts();
    int64_t expected_process_us = expected_process_us_;
    if (source_timeout <= expected_process_us_) {
      expected_process_us = 0;
    }
    if (block_us + expected_process_us > source_timeout) {
      TBSYS_LOG(WARN, "packet timeout, pcode=%d timeout=%ld block_us=%ld expected_us=%ld receive_ts=%ld",
                packet_code, source_timeout, block_us, expected_process_us, sb_packet->get_receive_ts());
      return_code = OB_RESPONSE_TIME_OUT;
    }
  }

  if (OB_SUCCESS == return_code) {
    return_code = sb_packet->deserialize();
    if (OB_SUCCESS == return_code) {
      ObDataBuffer* in_buf = sb_packet->get_buffer();
      if (in_buf == NULL) {
        TBSYS_LOG(ERROR, "in_buff is NUll should not reach this");
      } else {
        tbnet::Connection* conn = sb_packet->get_connection();
        ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
        if (my_buffer != NULL) {
          my_buffer->reset();
          ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
          if ((void*)WRITE_THREAD_FLAG == args) {
            //write stuff
            TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
            switch (packet_code) {
            case OB_REPORT_TABLETS:
              return_code = rt_report_tablets(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_SERVER_REGISTER:
              return_code = rt_register(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_MIGRATE_OVER:
              return_code = rt_migrate_over(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_REPORT_CAPACITY_INFO:
              return_code = rt_report_capacity_info(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_SLAVE_REG:
              return_code = rt_slave_register(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_WAITING_JOB_DONE:
              return_code = rt_waiting_jsb_done(version, *in_buf, conn, channel_id, thread_buff);
              break;
            default:
              return_code = OB_ERROR;
              break;
            }
          } else if ((void*)LOG_THREAD_FLAG == args) {
            switch (packet_code) {
            case OB_GRANT_LEASE_REQUEST:
              return_code = rt_grant_lease(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_SEND_LOG:
              in_buf->get_limit() = in_buf->get_position() + sb_packet->get_data_length();
              return_code = rt_slave_write_log(version, *in_buf, conn, channel_id, thread_buff);
              break;
            default:
              return_code = OB_ERROR;
              break;
            }
          } else {
            TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
            switch (packet_code) {
            case OB_FETCH_SCHEMA:
              return_code = rt_fetch_schema(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_FETCH_SCHEMA_VERSION:
              return_code = rt_fetch_schema_version(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_GET_REQUEST:
              return_code = rt_get(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_SCAN_REQUEST:
              return_code = rt_scan(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_HEARTBEAT:
              return_code = rt_heartbeat(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_DUMP_CS_INFO:
              return_code = rt_dump_cs_info(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_FETCH_STATS:
              return_code = rt_fetch_stats(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_GET_UPDATE_SERVER_INFO:
              TBSYS_LOG(INFO, "receive get ups quest");
              return_code = rt_get_update_server_info(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_GET_UPDATE_SERVER_INFO_FOR_MERGE:
              return_code = rt_get_update_server_info(version, *in_buf, conn, channel_id, thread_buff, true);
              break;
            case OB_GET_MERGE_DELAY_INTERVAL:
              return_code = rt_get_merge_delay_interval(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_RENEW_LEASE_REQUEST:
              return_code = rt_renew_lease(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_SLAVE_QUIT:
              return_code = rt_slave_quit(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_UPDATE_SERVER_REPORT_FREEZE:
              return_code = rt_update_server_report_freeze(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_GET_OBI_ROLE:
              return_code = rt_get_obi_role(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_SET_OBI_ROLE:
              return_code = rt_set_obi_role(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_RS_ADMIN:
              return_code = rt_admin(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_RS_STAT:
              return_code = rt_stat(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_CHANGE_LOG_LEVEL:
              return_code = rt_change_log_level(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_GET_OBI_CONFIG:
              return_code = rt_get_obi_config(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_SET_OBI_CONFIG:
              return_code = rt_set_obi_config(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_GET_UPS:
              return_code = rt_get_ups(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_SET_UPS_CONFIG:
              return_code = rt_set_ups_config(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_GET_CLIENT_CONFIG:
              return_code = rt_get_client_config(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_GET_CS_LIST:
              return_code = rt_get_cs_list(version, *in_buf, conn, channel_id, thread_buff);
              break;
            case OB_GET_MS_LIST:
              return_code = rt_get_ms_list(version, *in_buf, conn, channel_id, thread_buff);
              break;
            default:
              TBSYS_LOG(ERROR, "unknow packet code %d in read queue", packet_code);
              return_code = OB_ERROR;
              break;
            }
          }
          if (OB_SUCCESS != return_code) {
            TBSYS_LOG(ERROR, "call func error packet_code is %d return code is %d", packet_code, return_code);
          }

        } else {
          TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
        }
      }
    } else {
      TBSYS_LOG(ERROR, "packet deserialize error packet code is %d from server %s",
                packet_code, tbsys::CNetUtil::addrToString(sb_packet->get_connection()->getPeerId()).c_str());
    }
  }

  worker_counter++;
  atomic_inc(&total_counter);
  if (0 == worker_counter % 500) {
    int64_t now = tbsys::CTimeUtil::getTime();
    int64_t receive_ts = sb_packet->get_receive_ts();
    TBSYS_LOG(INFO, "worker report, tid=%ld my_counter=%ld total_counter=%ld current_elapsed_us=%ld",
              syscall(__NR_gettid), worker_counter, total_counter, now - receive_ts);
  }
  return ret;//if return true packet will be deleted.
}

bool NameServerWorker::start_merge() {
  //int64_t now = tbsys::CTimeUtil::getTime();
  bool ret = false;
  int retry = 0;
  while ((ret = name_server_.start_switch()) != true) {
    retry++;
    if (retry > 1000) break;
    usleep(5000);
  }
  return ret;
}

int NameServerWorker::rt_get_update_server_info(const int32_t version, ObDataBuffer& in_buff,
                                                tbnet::Connection* conn, const uint32_t channel_id, ObDataBuffer& out_buff,
                                                bool use_inner_port /* = false*/) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;

  //next two lines only for exmaples, actually this func did not need this
  char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH];
  result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  UNUSED(in_buff); // rt_get_update_server_info() no input params
  common::ObServer found_server;
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    found_server = name_server_.get_update_server_info();

    if (use_inner_port) {
      found_server.set_port(name_server_.get_update_server_inner_port());
    }
  }

  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    ret = found_server.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "found_server.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    TBSYS_LOG(INFO, "response for get ups request");
    send_response(OB_GET_UPDATE_SERVER_INFO_RES, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_get_merge_delay_interval(const int32_t version, ObDataBuffer& in_buff,
                                                  tbnet::Connection* conn, const uint32_t channel_id, ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;

  //next two lines only for exmaples, actually this func did not need this
  char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH];
  result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  UNUSED(in_buff); // rt_get_update_server_info() no input params

  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }

  if (OB_SUCCESS == ret) {
    int64_t interval = name_server_.get_merge_delay_interval();
    ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), interval);
  }

  if (OB_SUCCESS == ret) {
    send_response(OB_GET_MERGE_DELAY_INTERVAL_RES, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}


int NameServerWorker::rt_scan(const int32_t version, ObDataBuffer& in_buff,
                              tbnet::Connection* conn, const uint32_t channel_id, ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;

  char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH];
  result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  ObScanParam scan_param_in;
  ObScanner scanner_out;

  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = scan_param_in.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "scan_param_in.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    result_msg.result_code_ = name_server_.find_root_table_range(scan_param_in, scanner_out);
  }

  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    ret = scanner_out.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "scanner_out.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    send_response(OB_SCAN_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  if (OB_SUCCESS == ret) {
    stat_manager_.inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_SUCCESS_SCAN_COUNT);
  } else {
    stat_manager_.inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_FAIL_SCAN_COUNT);
  }
  return ret;
}
//ObResultCode rt_get(const ObGetParam& get_param, ObScanner& scanner);

int NameServerWorker::rt_get(const int32_t version, ObDataBuffer& in_buff,
                             tbnet::Connection* conn, const uint32_t channel_id, ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;

  char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH];
  result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  ObGetParam get_param_in;
  ObScanner scanner_out;

  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = get_param_in.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "get_param_in.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    result_msg.result_code_ = name_server_.find_root_table_key(get_param_in, scanner_out);
  }

  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    ret = scanner_out.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "scanner_out.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    send_response(OB_GET_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  if (OB_SUCCESS == ret) {
    stat_manager_.inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_SUCCESS_GET_COUNT);
    ObStat* stat = NULL;
    if (OB_SUCCESS == stat_manager_.get_stat(ObRootStatManager::ROOT_TABLE_ID, stat)) {
      if (NULL != stat) {
        int64_t get_count = stat->get_value(ObRootStatManager::INDEX_SUCCESS_GET_COUNT);
        if (0 == get_count % 500) {
          TBSYS_LOG(INFO, "get request stat, count=%ld from=%s", get_count, inet_ntoa_r(conn->getPeerId()));
        }
      }
    }
  } else {
    stat_manager_.inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_FAIL_GET_COUNT);
  }
  return ret;
}

int NameServerWorker::rt_fetch_schema(const int32_t version, common::ObDataBuffer& in_buff,
                                      tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;

  if (version != MY_VERSION) {
    TBSYS_LOG(ERROR, "version not match,version:%d,MY_VERSION:%d", version, MY_VERSION);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  //TBSYS_LOG(DEBUG, "inbuff %d %d", in_buff.get_capacity(), in_buff.get_position());

  //int64_t time_stamp = 0;
  ObSchemaManagerV2* schema = new(std::nothrow) ObSchemaManagerV2();
  if (schema == NULL) {
    TBSYS_LOG(ERROR, "error can not get mem for schema");
    ret = OB_ERROR;
  }

  //if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
  //{
  //  ret = common::serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &time_stamp);
  //  if (ret != OB_SUCCESS)
  //  {
  //    TBSYS_LOG(ERROR, "time_stamp.deserialize error");
  //  }
  //}
  //TBSYS_LOG(DEBUG, "input time_stamp is %ld", time_stamp);

  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    if (!name_server_.get_schema(*schema)) {
      TBSYS_LOG(ERROR, "get schema failed");
      result_msg.result_code_ = OB_ERROR;
    } else {
      TBSYS_LOG(INFO, "get schema, version=%ld", schema->get_version());
    }
  }
  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }

  if (OB_SUCCESS == ret) {
    ret = schema->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "schema.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    send_response(OB_FETCH_SCHEMA_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  if (NULL != schema) {
    delete schema;
    schema = NULL;
  }
  return ret;
}

int NameServerWorker::rt_fetch_schema_version(const int32_t version, common::ObDataBuffer& in_buff,
                                              tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  int64_t schema_version = 0;

  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    schema_version = name_server_.get_schema_version();
  }
  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), schema_version);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "schema version serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    send_response(OB_FETCH_SCHEMA_VERSION_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_report_tablets(const int32_t version, common::ObDataBuffer& in_buff,
                                        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  ObServer server;
  ObTabletReportInfoList tablet_list;
  int64_t time_stamp = 0;

  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "server.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = tablet_list.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "tablet_list.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &time_stamp);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "time_stamp.deserialize error");
    }
  }

  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    result_msg.result_code_ = name_server_.report_tablets(server, tablet_list, time_stamp);
  }

  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }

  if (OB_SUCCESS == ret) {
    send_response(OB_REPORT_TABLETS_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }

  return ret;
}

int NameServerWorker::rt_waiting_jsb_done(const int32_t version, common::ObDataBuffer& in_buff,
                                          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  ObServer server;
  int64_t frozen_mem_version = 0;
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "server.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &frozen_mem_version);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "frozen_mem_version.deserialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    result_msg.result_code_ = name_server_.waiting_jsb_done(server, frozen_mem_version);
  }
  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }

  if (OB_SUCCESS == ret) {
    send_response(OB_WAITING_JOB_DONE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;

}

int NameServerWorker::rt_register(const int32_t version, common::ObDataBuffer& in_buff,
                                  tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    TBSYS_LOG(ERROR, "version:%d,MY_VERSION:%d", version, MY_VERSION);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  ObServer server;
  bool is_merge_server = false;
  int32_t status = 0;
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "server.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &is_merge_server);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "time_stamp.deserialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    TBSYS_LOG(DEBUG, "receive server register,is_merge_server %d", is_merge_server ? 1 : 0);
    result_msg.result_code_ = name_server_.regist_server(server, is_merge_server, status);
  }
  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), status);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "schema.serialize error");
    }
  }

  if (OB_SUCCESS == ret) {
    send_response(OB_SERVER_REGISTER_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;

}

int NameServerWorker::rt_migrate_over(const int32_t version, common::ObDataBuffer& in_buff,
                                      tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  ObRange range;
  ObServer src_server;
  ObServer dest_server;
  bool keep_src = false;
  int64_t tablet_version = 0;
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = range.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "range.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = src_server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "src_server.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = dest_server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "dest_server.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &keep_src);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "keep_src.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &tablet_version);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "keep_src.deserialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    result_msg.result_code_ = name_server_.migrate_over(range, src_server, dest_server, keep_src, tablet_version);
  }
  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }

  if (OB_SUCCESS == ret) {
    send_response(OB_MIGRATE_OVER_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;

}
int NameServerWorker::rt_report_capacity_info(const int32_t version, common::ObDataBuffer& in_buff,
                                              tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  ObServer server;
  int64_t capacity = 0;
  int64_t used = 0;

  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "server.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &capacity);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "capacity.deserialize error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &used);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "used.deserialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    result_msg.result_code_ = name_server_.update_capacity_info(server, capacity, used);
  }
  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }

  if (OB_SUCCESS == ret) {
    send_response(OB_REPORT_CAPACITY_INFO_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;

}
int NameServerWorker::rt_heartbeat(const int32_t version, common::ObDataBuffer& in_buff,
                                   tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  static const int MY_VERSION = 2;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  UNUSED(conn);
  UNUSED(channel_id);
  UNUSED(out_buff);
  int ret = OB_SUCCESS;
  //if (version != MY_VERSION)
  //{
  //  result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  //}
  ObServer server;
  ObRole role = OB_CHUNKSERVER;
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "server.deserialize error");
    }
  }

  if (version == MY_VERSION) {
    ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), reinterpret_cast<int32_t*>(&role));
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "decoe role error");
    }
  }
  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) {
    result_msg.result_code_ = name_server_.receive_hb(server, role);
  }
  return ret;
}
int NameServerWorker::rt_dump_cs_info(const int32_t version, common::ObDataBuffer& in_buff,
                                      tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  ObChunkServerManager* out_server_manager = new ObChunkServerManager();
  if (out_server_manager == NULL) {
    TBSYS_LOG(ERROR, "can not new ObChunkServerManager");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret) {
    result_msg.result_code_ = name_server_.get_cs_info(out_server_manager);
  }
  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    ret = out_server_manager->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "schema.serialize error");
    }
  }
  if (out_server_manager != NULL) {
    delete out_server_manager;
    out_server_manager = NULL;
  }

  if (OB_SUCCESS == ret) {
    send_response(OB_DUMP_CS_INFO_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;

}
int NameServerWorker::rt_fetch_stats(const int32_t version, common::ObDataBuffer& in_buff,
                                     tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  if (OB_SUCCESS == ret) {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    ret = stat_manager_.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "stat_manager_.serialize error");
    }
  }
  if (OB_SUCCESS == ret) {
    send_response(OB_FETCH_STATS_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;

}

NameServerLogManager* NameServerWorker::get_log_manager() {
  return &log_manager_;
}

ObRoleMgr* NameServerWorker::get_role_manager() {
  return &role_mgr_;
}

common::ThreadSpecificBuffer::Buffer* NameServerWorker::get_rpc_buffer() const {
  return my_thread_buffer.get_buffer();
}

int NameServerWorker::rt_ping(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;

  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  } else {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result message serialize failed, err: %d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    send_response(OB_PING_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }

  return ret;
}

int NameServerWorker::rt_slave_quit(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;

  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  ObServer rt_slave;
  if (ret == OB_SUCCESS) {
    ret = rt_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "slave deserialize failed, err=%d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    ret = slave_mgr_.delete_server(rt_slave);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "ObSlaveMgr delete slave error, ret: %d", ret);
    }

    char addr_buf[ADDR_BUF_LEN];
    rt_slave.to_string(addr_buf, sizeof(addr_buf));
    addr_buf[ADDR_BUF_LEN - 1] = '\0';
    TBSYS_LOG(INFO, "slave quit, slave_addr=%s, err=%d", addr_buf, ret);
  }

  if (ret == OB_SUCCESS) {
    common::ObResultCode result_msg;
    result_msg.result_code_ = ret;

    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result mssage serialize faild, err: %d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    send_response(OB_SLAVE_QUIT_RES, MY_VERSION, out_buff, conn, channel_id);
  }

  return ret;
}

int NameServerWorker::rt_update_server_report_freeze(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;
  int64_t frozen_version = 0;

  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  ObServer update_server;
  if (ret == OB_SUCCESS) {
    ret = update_server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "deserialize failed, err=%d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &frozen_version);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "decode frozen version error, ret: %d", ret);
    } else {
      TBSYS_LOG(INFO, "update report a new froze version : [%ld]", frozen_version);
    }
  }

  if (ret == OB_SUCCESS) {
    name_server_.report_frozen_memtable(frozen_version, false);
  }

  if (ret == OB_SUCCESS) {
    common::ObResultCode result_msg;
    result_msg.result_code_ = ret;

    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "result mssage serialize faild, err: %d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    send_response(OB_RESULT, MY_VERSION, out_buff, conn, channel_id);
  }

  return ret;
}


int NameServerWorker::rt_slave_register(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  uint64_t new_log_file_id = 0;
  ObServer rt_slave;
  if (ret == OB_SUCCESS) {
    ret = rt_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "deserialize rt_slave failed, err=%d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    ret = log_manager_.add_slave(rt_slave, new_log_file_id);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "add_slave error, err=%d", ret);
    } else {
      char addr_buf[ADDR_BUF_LEN];
      rt_slave.to_string(addr_buf, sizeof(addr_buf));
      addr_buf[ADDR_BUF_LEN - 1] = '\0';
      TBSYS_LOG(INFO, "add slave, slave_addr=%s, new_log_file_id=%ld, ckpt_id=%lu, err=%d",
                addr_buf, new_log_file_id, log_manager_.get_check_point(), ret);
    }
  }

  if (ret == OB_SUCCESS) {
    int64_t lease_on = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_LEASE_ON, DEFAULT_LEASE_ON);
    if (LEASE_ON == lease_on) {
      ObLease lease;
      ret = slave_mgr_.extend_lease(rt_slave, lease);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(WARN, "failed to extends lease, ret=%d", ret);
      }
    }
  }

  ObFetchParam fetch_param;
  if (ret == OB_SUCCESS) {
    fetch_param.fetch_log_ = true;
    fetch_param.min_log_id_ = log_manager_.get_replay_point();
    fetch_param.max_log_id_ = new_log_file_id - 1;

    if (log_manager_.get_check_point() > 0) {
      TBSYS_LOG(INFO, "master has check point, tell slave fetch check point files, id: %ld", log_manager_.get_check_point());
      fetch_param.fetch_ckpt_ = true;
      fetch_param.ckpt_id_ = log_manager_.get_check_point();
      fetch_param.min_log_id_ = fetch_param.ckpt_id_ + 1;
    } else {
      fetch_param.fetch_ckpt_ = false;
      fetch_param.ckpt_id_ = 0;
    }
  }

  common::ObResultCode result_msg;
  result_msg.result_code_ = ret;

  ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
  if (ret == OB_SUCCESS) {
    ret = fetch_param.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
  }

  if (ret == OB_SUCCESS) {
    send_response(OB_SLAVE_REG_RES, MY_VERSION, out_buff, conn, channel_id);
  }

  return ret;
}

int NameServerWorker::slave_register_(common::ObFetchParam& fetch_param) {
  int err = OB_SUCCESS;
  int64_t timeout = TBSYS_CONFIG.getInt(STR_NAME_SECTION, STR_REGISTER_TIMEOUT_US, DEFAULT_REGISTER_TIMEOUT_US);
  const ObServer& self_addr = self_addr_;

  err = OB_RESPONSE_TIME_OUT;
  for (int64_t i = 0; ObRoleMgr::STOP != role_mgr_.get_state()
       && OB_RESPONSE_TIME_OUT == err; i++) {
    // slave register
    err = ns_rpc_stub_.slave_register(rt_master_, self_addr_, fetch_param, timeout);
    if (OB_RESPONSE_TIME_OUT == err) {
      TBSYS_LOG(INFO, "slave register timeout, i=%ld, err=%d", i, err);
    }
  }

  if (ObRoleMgr::STOP == role_mgr_.get_state()) {
    TBSYS_LOG(INFO, "the slave is stopped manually.");
    err = OB_ERROR;
  } else if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "Error occurs when slave register, err=%d", err);
  }

  if (err == OB_SUCCESS) {
    int64_t renew_lease_timeout = 1000 * 1000L;
    check_thread_.set_renew_lease_timeout(renew_lease_timeout);
  }

  char addr_buf[ADDR_BUF_LEN];
  self_addr.to_string(addr_buf, sizeof(addr_buf));
  addr_buf[ADDR_BUF_LEN - 1] = '\0';
  TBSYS_LOG(INFO, "slave register, self=[%s], min_log_id=%ld, max_log_id=%ld, ckpt_id=%lu, err=%d",
            addr_buf, fetch_param.min_log_id_, fetch_param.max_log_id_, fetch_param.ckpt_id_, err);

  if (err == OB_SUCCESS) {
    is_registered_ = true;
  }

  return err;
}

int NameServerWorker::rt_renew_lease(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  ObServer rt_slave;
  ObLease lease;
  if (ret == OB_SUCCESS) {
    ret = rt_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "failed to deserialize root slave, ret=%d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    ret = slave_mgr_.extend_lease(rt_slave, lease);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "failed to extend lease, ret=%d", ret);
    }
  }

  common::ObResultCode result_msg;
  result_msg.result_code_ = ret;

  ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
  if (ret == OB_SUCCESS) {
    ret = lease.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "lease serialize failed, ret=%d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    send_response(OB_RENEW_LEASE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }

  return ret;
}

int NameServerWorker::rt_grant_lease(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  static const int MY_VERSION = 1;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    ret = OB_ERROR_FUNC_VERSION;
  }

  ObLease lease;
  if (ret == OB_SUCCESS) {
    ret = lease.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "failed to deserialize lease, ret=%d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    ret = check_thread_.renew_lease(lease);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "failed to extend lease, ret=%d", ret);
    }
  }

  common::ObResultCode result_msg;
  result_msg.result_code_ = ret;

  ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
  if (ret == OB_SUCCESS) {
    ret = lease.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "lease serialize failed, ret=%d", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    send_response(OB_GRANT_LEASE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_slave_write_log(const int32_t version, common::ObDataBuffer& in_buffer, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buffer) {
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (version != MY_VERSION) {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  int64_t in_buff_begin = in_buffer.get_position();
  bool switch_log_flag = false;
  uint64_t log_id = 0;
  static bool is_first_log = true;

  // send response to master ASAP
  ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  if (ret == OB_SUCCESS) {
    ret = send_response(OB_SEND_LOG_RES, MY_VERSION, out_buffer, conn, channel_id);
  }

  if (ret == OB_SUCCESS) {
    if (is_first_log) {
      is_first_log = false;

      ObLogEntry log_ent;
      ret = log_ent.deserialize(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position());
      if (ret != OB_SUCCESS) {
        common::hex_dump(in_buffer.get_data(), in_buffer.get_limit(), TBSYS_LOG_LEVEL_INFO);
        TBSYS_LOG(WARN, "ObLogEntry deserialize error, error code: %d, position: %ld, limit: %ld", ret, in_buffer.get_position(), in_buffer.get_limit());
      } else {
        if (OB_LOG_SWITCH_LOG != log_ent.cmd_) {
          TBSYS_LOG(WARN, "the first log of slave should be switch_log, cmd_=%d", log_ent.cmd_);
          ret = OB_ERROR;
        } else {
          ret = serialization::decode_i64(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position(), (int64_t*)&log_id);
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(WARN, "decode_i64 log_id error, err=%d", ret);
          } else {
            ret = log_manager_.start_log(log_id);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(WARN, "start_log error, log_id=%lu err=%d", log_id, ret);
            } else {
              in_buffer.get_position() = in_buffer.get_limit();
            }
          }
        }
      }
    }
  } // end of first log

  if (ret == OB_SUCCESS) {
    int64_t data_begin = in_buffer.get_position();
    while (OB_SUCCESS == ret && in_buffer.get_position() < in_buffer.get_limit()) {
      ObLogEntry log_ent;
      ret = log_ent.deserialize(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position());
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "ObLogEntry deserialize error, err=%d", ret);
        ret = OB_ERROR;
      } else {
        log_manager_.set_cur_log_seq(log_ent.seq_);

        // check switch_log
        if (OB_LOG_SWITCH_LOG == log_ent.cmd_) {
          if (data_begin != in_buff_begin
              || ((in_buffer.get_position() + log_ent.get_log_data_len()) != in_buffer.get_limit())) {
            TBSYS_LOG(ERROR, "swith_log is not single, this should not happen, "
                      "in_buff.pos=%ld log_data_len=%d in_buff.limit=%ld",
                      in_buffer.get_position(), log_ent.get_log_data_len(), in_buffer.get_limit());
            ret = OB_ERROR;
          } else {
            ret = serialization::decode_i64(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position(), (int64_t*)&log_id);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(WARN, "decode_i64 log_id error, err=%d", ret);
            } else {
              switch_log_flag = true;
            }
          }
        }
        in_buffer.get_position() += log_ent.get_log_data_len();
      }
    }

    if (OB_SUCCESS == ret && in_buffer.get_limit() - data_begin > 0) {
      ret = log_manager_.store_log(in_buffer.get_data() + data_begin, in_buffer.get_limit() - data_begin);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "ObUpsLogMgr store_log error, err=%d", ret);
      }
    }

    if (switch_log_flag) {
      ret = log_manager_.switch_to_log_file(log_id + 1);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "switch_to_log_file error, log_id=%lu err=%d", log_id, ret);
      }
    }
  }

  return ret;
}


int NameServerWorker::rt_get_obi_role(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(version);
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  ObiRole role = name_server_.get_obi_role();

  stat_manager_.inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_GET_OBI_ROLE_COUNT);
  ObStat* stat = NULL;
  if (OB_SUCCESS == stat_manager_.get_stat(ObRootStatManager::ROOT_TABLE_ID, stat)) {
    if (NULL != stat) {
      int64_t count = stat->get_value(ObRootStatManager::INDEX_GET_OBI_ROLE_COUNT);
      if (0 == count % 500) {
        TBSYS_LOG(INFO, "get obi role, count=%ld role=%s from=%s", count, role.get_role_str(), inet_ntoa_r(conn->getPeerId()));
      }
    }
  }
  if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else if (OB_SUCCESS != (ret = role.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else {
    ret = send_response(OB_GET_OBI_ROLE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_set_obi_role(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;
  UNUSED(version);
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  ObiRole role;
  if (OB_SUCCESS != (ret = role.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position()))) {
    TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
  } else {
    ret = name_server_.set_obi_role(role);
  }
  result_msg.result_code_ = ret;
  // send response
  if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else {
    ret = send_response(OB_SET_OBI_ROLE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_admin(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;
  UNUSED(version);
  common::ObResultCode result;
  result.result_code_ = OB_SUCCESS;
  int32_t admin_cmd = -1;
  if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &admin_cmd))) {
    TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
  } else {
    result.result_code_ = do_admin(admin_cmd);
    TBSYS_LOG(INFO, "admin cmd=%d, err=%d", admin_cmd, result.result_code_);
    if (OB_SUCCESS != (ret = result.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
      TBSYS_LOG(WARN, "serialize error, err=%d", ret);
    } else {
      ret = send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
    }
  }
  return ret;
}

int NameServerWorker::do_admin(int admin_cmd) {
  int ret = OB_SUCCESS;
  switch (admin_cmd) {
  case OB_RS_ADMIN_CHECKPOINT:
    if (name_server_.is_master()) {
      ret = log_manager_.do_check_point();
      TBSYS_LOG(INFO, "do checkpoint, ret=%d", ret);
    } else {
      TBSYS_LOG(WARN, "I'm not the master");
    }
    break;
  case OB_RS_ADMIN_RELOAD_CONFIG:
    if (!name_server_.reload_config(true)) {
      ret = OB_ERROR;
    }
    break;
  case OB_RS_ADMIN_SWITCH_SCHEMA:
    name_server_.use_new_schema();
    break;
  case OB_RS_ADMIN_DUMP_ROOT_TABLE:
    name_server_.dump_root_table();
    break;
  case OB_RS_ADMIN_DUMP_UNUSUAL_TABLETS:
    name_server_.dump_unusual_tablets();
    break;
  case OB_RS_ADMIN_DUMP_SERVER_INFO:
    name_server_.print_alive_server();
    break;
  case OB_RS_ADMIN_DUMP_MIGRATE_INFO:
    name_server_.dump_migrate_info();
    break;
  case OB_RS_ADMIN_INC_LOG_LEVEL:
    TBSYS_LOGGER._level++;
    break;
  case OB_RS_ADMIN_DEC_LOG_LEVEL:
    TBSYS_LOGGER._level--;
    break;
  default:
    TBSYS_LOG(WARN, "unknown admin command, cmd=%d\n", admin_cmd);
    ret = OB_ENTRY_NOT_EXIST;
    break;
  }
  return ret;
}

int NameServerWorker::rt_change_log_level(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;
  UNUSED(version);
  common::ObResultCode result;
  result.result_code_ = OB_SUCCESS;
  int32_t log_level = -1;
  if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &log_level))) {
    TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
  } else {
    if (TBSYS_LOG_LEVEL_ERROR <= log_level
        && TBSYS_LOG_LEVEL_DEBUG >= log_level) {
      TBSYS_LOGGER._level = log_level;
      TBSYS_LOG(INFO, "change log level, level=%d", log_level);
    } else {
      TBSYS_LOG(WARN, "invalid log level, level=%d", log_level);
      result.result_code_ = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS != (ret = result.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
      TBSYS_LOG(WARN, "serialize error, err=%d", ret);
    } else {
      ret = send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
    }
  }
  return ret;
}

int NameServerWorker::rt_stat(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;
  UNUSED(version);
  common::ObResultCode result;
  result.result_code_ = OB_SUCCESS;
  int32_t stat_key = -1;
  if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &stat_key))) {
    TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
  } else {
    char str_ptr[] =  "hello world";
    result.message_.assign_ptr(str_ptr, strlen(str_ptr));
    if (OB_SUCCESS != (ret = result.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
      TBSYS_LOG(WARN, "serialize error, err=%d", ret);
    } else {
      TBSYS_LOG(DEBUG, "get stat, stat_key=%d", stat_key);
      do_stat(stat_key, out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      ret = send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
    }
  }
  return ret;
}

using sb::common::databuff_printf;

int NameServerWorker::do_stat(int stat_key, char* buf, const int64_t buf_len, int64_t& pos) {
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "do_stat start, stat_key=%d buf=%p buf_len=%ld pos=%ld",
            stat_key, buf, buf_len, pos);
  switch (stat_key) {
  case OB_RS_STAT_RS_SLAVE_NUM:
    databuff_printf(buf, buf_len, pos, "slave_num: %d", slave_mgr_.get_num());
    ret = OB_SUCCESS;
    break;
  default:
    ret = name_server_.do_stat(stat_key, buf, buf_len, pos);
    break;
  }
  // skip the trailing '\0'
  pos++;
  TBSYS_LOG(DEBUG, "do_stat finish, stat_key=%d buf=%p buf_len=%ld pos=%ld",
            stat_key, buf, buf_len, pos);
  return ret;
}

int NameServerWorker::rt_get_obi_config(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  UNUSED(version);
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  ObiConfig conf;
  result_msg.result_code_ = name_server_.get_obi_config(conf);

  if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else if (OB_SUCCESS == result_msg.result_code_
             && OB_SUCCESS != (ret = conf.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else {
    ret = send_response(OB_GET_OBI_CONFIG_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_set_obi_config(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  static const int OLD_VERSION = 1;
  static const int MY_VERSION = 2;
  UNUSED(version);
  common::ObResultCode res;
  res.result_code_ = OB_SUCCESS;
  ObiConfig conf;
  ObServer rs_addr;
  if (OLD_VERSION == version) {
    if (OB_SUCCESS != (ret = conf.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position()))) {
      TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
    } else {
      ret = name_server_.set_obi_config(conf);
    }
  } else if (MY_VERSION == version) {
    if (OB_SUCCESS != (ret = conf.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position()))) {
      TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
    } else if (OB_SUCCESS != (ret = rs_addr.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position()))) {
      TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
    } else {
      if (0 == rs_addr.get_port()) {
        ret = name_server_.set_obi_config(conf);
      } else {
        ret = name_server_.set_obi_config(rs_addr, conf);
      }
    }
  } else {
    TBSYS_LOG(ERROR, "invalid request version=%d", version);
    ret = OB_ERROR_FUNC_VERSION;
  }
  res.result_code_ = ret;

  // send response
  if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else {
    TBSYS_LOG(INFO, "set obi config, read_percentage=%d ret=%d", conf.get_read_percentage(), res.result_code_);
    ret = send_response(OB_SET_OBI_CONFIG_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_get_ups(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  if (MY_VERSION != version) {
    TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
    ret = OB_ERROR_FUNC_VERSION;
  }
  common::ObResultCode res;
  res.result_code_ = ret;
  if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else if (OB_SUCCESS == res.result_code_ && OB_SUCCESS != (ret = name_server_.get_ups_list().serialize(
      out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else {
    ret = send_response(OB_GET_UPS_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_set_ups_config(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  common::ObServer ups_addr;
  int32_t ms_read_percentage = -1;
  int32_t cs_read_percentage = -1;
  if (MY_VERSION != version) {
    TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
    ret = OB_ERROR_FUNC_VERSION;
  } else if (OB_SUCCESS != (ret = ups_addr.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position()))) {
    printf("failed to serialize ups_addr, err=%d\n", ret);
  } else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &ms_read_percentage))) {
    printf("failed to serialize read_percentage, err=%d\n", ret);
  } else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &cs_read_percentage))) {
    printf("failed to serialize read_percentage, err=%d\n", ret);
  } else {
    common::ObResultCode res;
    res.result_code_ = name_server_.set_ups_config(ups_addr, ms_read_percentage, cs_read_percentage);
    if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
      TBSYS_LOG(WARN, "serialize error, err=%d", ret);
    } else {
      ret = send_response(OB_SET_UPS_CONFIG_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
    }
  }
  return ret;
}

int NameServerWorker::rt_get_client_config(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  if (MY_VERSION != version) {
    TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
    ret = OB_ERROR_FUNC_VERSION;
  }
  common::ObResultCode res;
  res.result_code_ = ret;
  if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else if (OB_SUCCESS == res.result_code_ && OB_SUCCESS != (ret = name_server_.get_client_config().serialize(
      out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else {
    ret = send_response(OB_GET_CLIENT_CONFIG_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_get_cs_list(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  if (MY_VERSION != version) {
    TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
    ret = OB_ERROR_FUNC_VERSION;
  }
  common::ObResultCode res;
  res.result_code_ = ret;
  if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else if (OB_SUCCESS == res.result_code_ && OB_SUCCESS != (ret = name_server_.serialize_cs_list(
      out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else {
    ret = send_response(OB_GET_CS_LIST_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

int NameServerWorker::rt_get_ms_list(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff) {
  int ret = OB_SUCCESS;
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  if (MY_VERSION != version) {
    TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
    ret = OB_ERROR_FUNC_VERSION;
  }
  common::ObResultCode res;
  res.result_code_ = ret;
  if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else if (OB_SUCCESS == res.result_code_ && OB_SUCCESS != (ret = name_server_.serialize_ms_list(
      out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))) {
    TBSYS_LOG(WARN, "serialize error, err=%d", ret);
  } else {
    ret = send_response(OB_GET_MS_LIST_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return ret;
}

}
}

