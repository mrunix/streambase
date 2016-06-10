/*
 * src/nameserver/nameserver.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include <new>
#include <string.h>

#include <tbsys.h>

#include "common/ob_schema.h"
#include "common/ob_range.h"
#include "common/ob_scanner.h"
#include "common/ob_define.h"
#include "common/ob_action_flag.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "nameserver/nameserver.h"
#include "nameserver/nameserver_worker.h"
#include "nameserver/nameserver_stat_key.h"

using sb::common::databuff_printf;

namespace sb {
namespace nameserver {
const char* STR_SECTION_ROOT_SERVER = "name_server";
const char* STR_SECTION_UPDATE_SERVER = "update_server";
const char* STR_SECTION_OB_INSTANCES = "sb_instances";
const char* STR_SECTION_CLIENT = "client";

const char* STR_IP = "vip";
const char* STR_PORT = "port";
const char* STR_UPS_INNER_PORT = "ups_inner_port";

const char* STR_SECTION_SCHEMA_INFO = "schema";
const char* STR_FILE_NAME = "file_name";
const char* STR_SECTION_CHUNK_SERVER = "chunk_server";
const char* STR_LEASE = "lease";
const char* STR_MIGRATE_WAIT_SECONDS = "migrate_wait_seconds";
const char* STR_SAFE_LOST_ONE_DURATION = "safe_lost_one_duration";
const char* STR_SAFE_WAIT_INIT_DURATION = "wait_init_duration";
const char* STR_MAX_MERGE_DURATION = "max_merge_duration";
const char* STR_CS_COMMAND_INTERVAL = "cs_command_interval_us";
const char* STR_SAFE_COPY_COUNT_IN_INIT = "__safe_copy_count_in_init";
const char* STR_SAFE_COPY_COUNT_IN_MERGE = "__safe_copy_count_in_merge";
const char* STR_CREATE_TABLE_IN_INIT = "__create_table_in_init";
const char* STR_TABLET_REPLICAS_NUM = "tablet_replicas_num";
const char* STR_UPS_COUNT = "ups_count";
const char* STR_OBI_COUNT = "obi_count";
const char* STR_BNL_ALPHA = "BNL_alpha";
const char* STR_BNL_ALPHA_DENOMINATOR = "BNL_alpha_denominator";
const char* STR_BNL_THRESHOLD = "BNL_threshold";
const char* STR_BNL_THRESHOLD_DENOMINATOR = "BNL_threshold_denominator";
const char* STR_BALANCE_TOLERANCE = "balance_tolerance";
const char* STR_MAX_CONCURRENT_MIGRATE = "max_concurrent_migrate_per_cs";
const char* STR_MAX_BATCH_MIGRATE_OUT_PER_CS = "max_batch_migrate_out_per_cs";
const char* STR_ENABLE_BALANCE = "__enable_balance";
const char* STR_ENABLE_REREPLICATION = "__enable_rereplication";
const char* STR_MAX_BATCH_MIGRATE_TIMEOUT = "max_batch_migrate_timeout";
const char* STR_BALANCE_WORKER_IDLE_SLEEP_SEC = "balance_worker_idle_sleep_sec";

const int WAIT_SECONDS = 1;
const int PACKAGE_TIME_OUT = 1;
const int SLAVE_SLEEP_TIME = 1 * 1000; // 1 ms
//const int64_t REMAIN_TIME = 1000000 * 500;  //500s
const int RETURN_BACH_COUNT = 8;
const int MAX_RETURN_BACH_ROW_COUNT = 1000;
const int DEFAULT_BALANCE_TOLERANCE = 10;
const int MIN_BALANCE_TOLERANCE = 3;
const int64_t DEFAULT_BALANCE_TIMEOUT_US_DELTA = 10 * 1000 * 1000; // 10s
const int32_t DEFAULT_MAX_CONCURRENT_MIGRATE = 2;
const int32_t DEFAULT_MAX_MIGRATE_OUT_PER_CS = 20;
const int32_t DEFAULT_MAX_MIGRATE_TIMEOUT = 600; // 10min
const int32_t DEFAULT_BALANCE_WORKER_IDLE_SLEEP_SEC = 30; // 30s

const char* ROOT_1_PORT    = "1_port";
const char* ROOT_1_MS_PORT = "1_ms_port";
const char* ROOT_1_IPV6_1  = "1_ipv6_1";
const char* ROOT_1_IPV6_2  = "1_ipv6_2";
const char* ROOT_1_IPV6_3  = "1_ipv6_3";
const char* ROOT_1_IPV6_4  = "1_ipv6_4";
const char* ROOT_1_IPV4    = "1_ipv4";
const char* ROOT_1_TABLET_VERSION = "1_tablet_version";

const char* ROOT_2_PORT    = "2_port";
const char* ROOT_2_MS_PORT = "2_ms_port";
const char* ROOT_2_IPV6_1  = "2_ipv6_1";
const char* ROOT_2_IPV6_2  = "2_ipv6_2";
const char* ROOT_2_IPV6_3  = "2_ipv6_3";
const char* ROOT_2_IPV6_4  = "2_ipv6_4";
const char* ROOT_2_IPV4    = "2_ipv4";
const char* ROOT_2_TABLET_VERSION = "2_tablet_version";

const char* ROOT_3_PORT    = "3_port";
const char* ROOT_3_MS_PORT = "3_ms_port";
const char* ROOT_3_IPV6_1  = "3_ipv6_1";
const char* ROOT_3_IPV6_2  = "3_ipv6_2";
const char* ROOT_3_IPV6_3  = "3_ipv6_3";
const char* ROOT_3_IPV6_4  = "3_ipv6_4";
const char* ROOT_3_IPV4    = "3_ipv4";
const char* ROOT_3_TABLET_VERSION = "3_tablet_version";

const char* ROOT_OCCUPY_SIZE  = "occupy_size";
const char* ROOT_RECORD_COUNT = "record_count";
const char* ROOT_CRC_SUM = "crc_sum";

char max_row_key[sb::common::OB_MAX_ROW_KEY_LENGTH];


const int ADD_ERROR = -1;
const int ADD_OK = 0;
const int SHOULD_DO_MIGRATE = 1;
const int CAN_NOT_FIND_SUTABLE_SERVER = 2;

const int MIGRATE_WAIT_SECONDS = 60;

const int NO_REPORTING = 0;
const int START_REPORTING = 1;

const int HB_RETRY_FACTOR = 100 * 1000;
const int MAX_TRIGEER_WAIT_SECONDS = 60 * 30;
}
}

namespace sb {
namespace nameserver {
const int NameServer::STATUS_INIT;
const int NameServer::STATUS_NEED_REPORT;
const int NameServer::STATUS_NEED_BUILD;
const int NameServer::STATUS_CHANGING;
const int NameServer::STATUS_NEED_BALANCE;
const int NameServer::STATUS_BALANCING;
const int NameServer::STATUS_SLEEP;
const int NameServer::STATUS_INTERRUPT_BALANCING;
const char* NameServer::ROOT_TABLE_EXT = "rtable";
const char* NameServer::CHUNKSERVER_LIST_EXT = "clist";

using namespace common;
NameServer::NameServer()
  : ups_inner_port_(0), lease_duration_(0), schema_manager_(NULL),
    root_table_for_query_(NULL), tablet_manager_for_query_(NULL),
    root_table_for_build_(NULL), tablet_manager_for_build_(NULL),
    have_inited_(false), new_table_created_(false), migrate_wait_seconds_(0),
    server_status_(STATUS_INIT), build_sync_flag_(BUILD_SYNC_FLAG_NONE),
    safe_lost_one_duration_(0), wait_init_time_(1000L * 1000L * 60),
    max_merge_duration_(1000L * 1000L * 7200), cs_merge_command_interval_mseconds_(60 * 1000L * 1000L),
    first_cs_had_registed_(false), receive_stop_(false), drop_this_build_(false),
    safe_copy_count_in_init_(2), safe_copy_count_in_merge_(2), create_table_in_init_(0),
    last_frozen_mem_version_(-1), pre_frozen_mem_version_(-1), last_frozen_time_(0),
    tablet_replicas_num_(DEFAULT_TABLET_REPLICAS_NUM),
    balance_worker_sleep_us_(MIN_BALANCE_WORKER_SLEEP_US),
    balance_worker_idle_sleep_us_(MIN_BALANCE_WORKER_SLEEP_US),
    balance_tolerance_count_(0), balance_start_time_us_(0),
    balance_timeout_us_(0), balance_timeout_us_delta_(DEFAULT_BALANCE_TIMEOUT_US_DELTA),
    balance_max_timeout_us_(0), balance_batch_migrate_count_(0),
    balance_batch_migrate_done_num_(0), balance_select_dest_start_pos_(0),
    balance_max_concurrent_migrate_num_(0), balance_max_migrate_out_per_cs_(0),
    enable_balance_(1), enable_rereplication_(1), balance_testing_(false),
    root_table_modifier_(this), balance_worker_(this), heart_beat_checker_(this) {
  worker_ = NULL;
  log_worker_ = NULL;
  time_stamp_changing_ = -1;
  frozen_mem_version_ = -1;
  config_file_name_[0] = '\0';
  schema_file_name_[0] = '\0';
  time(&start_time_);
}
NameServer::~NameServer() {
  if (schema_manager_) {
    delete schema_manager_;
    schema_manager_ = NULL;
  }
  if (root_table_for_query_) {
    delete root_table_for_query_;
    root_table_for_query_ = NULL;
  }
  if (root_table_for_build_) {
    delete root_table_for_build_;
    root_table_for_build_ = NULL;
  }
  if (tablet_manager_for_query_) {
    delete tablet_manager_for_query_;
    tablet_manager_for_query_ = NULL;
  }
  if (tablet_manager_for_build_) {
    delete tablet_manager_for_build_;
    tablet_manager_for_build_ = NULL;
  }
  have_inited_ = false;
}
bool NameServer::init(const char* config_file_name, const int64_t now, NameServerWorker* worker) {
  bool res = true;
  worker_ = worker;
  log_worker_ = worker_->get_log_manager()->get_log_worker();

  if (have_inited_ == false && config_file_name != NULL) {
    schema_manager_ = new(std::nothrow)ObSchemaManagerV2(now);
    TBSYS_LOG(INFO, "init schema_version=%ld", now);
    if (schema_manager_ == NULL) {
      TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
      res = false;
    }
    if (res) {
      tablet_manager_for_query_ = new(std::nothrow)ObTabletInfoManager();
      if (tablet_manager_for_query_ == NULL) {
        TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
        res = false;
      } else {
        root_table_for_query_ = new(std::nothrow)NameTable(tablet_manager_for_query_);
        TBSYS_LOG(INFO, "new root table created, root_table_for_query=%p", root_table_for_query_);
        if (root_table_for_query_ == NULL) {
          TBSYS_LOG(ERROR, "new NameTable error");
          res = false;
        }
      }
    }
    strncpy(config_file_name_, config_file_name, OB_MAX_FILE_NAME_LENGTH);
    config_file_name_[OB_MAX_FILE_NAME_LENGTH - 1] = '\0';
    if (res) {
      res = reload_config(config_file_name_);
    }
    //this means if init failed, you should aband this NameServer
    //do not call init again.
    have_inited_ = true;
  } else {
    if (have_inited_) {
      TBSYS_LOG(ERROR, "can not be inited again");
    } else {
      TBSYS_LOG(ERROR, "config file name can not be empty");
    }
    res = false;
  }

  return res;
}

void NameServer::start_threads() {
  root_table_modifier_.start();
  balance_worker_.start();
  heart_beat_checker_.start();
}

void NameServer::stop_threads() {
  TBSYS_LOG(INFO, "stop threads");
  receive_stop_ = true;
  TBSYS_LOG(INFO, "stop flag set");
  heart_beat_checker_.stop();
  heart_beat_checker_.wait();
  TBSYS_LOG(INFO, "heartbeat thread stopped");
  balance_worker_.stop();
  balance_worker_.wait();
  TBSYS_LOG(INFO, "balance thread stopped");
  root_table_modifier_.stop();
  root_table_modifier_.wait();
  TBSYS_LOG(INFO, "table_modifier thread stopped");
}

int NameServer::load_ups_list(tbsys::CConfig& config) {
  int ret = OB_SUCCESS;
  static const int MY_BUF_SIZE = 64;
  char conf_key[MY_BUF_SIZE];

  ups_list_.ups_count_ = config.getInt(STR_SECTION_UPDATE_SERVER, STR_UPS_COUNT, 0);
  TBSYS_LOG(INFO, "loading ups count=%d", ups_list_.ups_count_);
  for (int32_t i = 0; OB_SUCCESS == ret && i < ups_list_.ups_count_ && i < ups_list_.MAX_UPS_COUNT; ++i) {
    snprintf(conf_key, MY_BUF_SIZE, "ups%d_ip", i);
    const char* ip =  config.getString(STR_SECTION_UPDATE_SERVER, conf_key, NULL);
    snprintf(conf_key, MY_BUF_SIZE, "ups%d_port", i);
    int port = config.getInt(STR_SECTION_UPDATE_SERVER, conf_key, 0);
    snprintf(conf_key, MY_BUF_SIZE, "ups%d_inner_port", i);
    int inner_port = config.getInt(STR_SECTION_UPDATE_SERVER, conf_key, 0);
    snprintf(conf_key, MY_BUF_SIZE, "ups%d_ms_read_percentage", i);
    int ms_read_percentage = config.getInt(STR_SECTION_UPDATE_SERVER, conf_key, -1);
    snprintf(conf_key, MY_BUF_SIZE, "ups%d_cs_read_percentage", i);
    int cs_read_percentage = config.getInt(STR_SECTION_UPDATE_SERVER, conf_key, -1);
    if (!ups_list_.ups_array_[i].addr_.set_ipv4_addr(ip, port)) {
      TBSYS_LOG(ERROR, "invalid ups addr, addr=%s port=%d", ip, port);
      ret = OB_INVALID_ARGUMENT;
    } else if (0 >= inner_port) {
      TBSYS_LOG(ERROR, "invalid ups inner port, port=%d", inner_port);
      ret = OB_INVALID_ARGUMENT;
    } else if (0 > ms_read_percentage || 100 < ms_read_percentage) {
      TBSYS_LOG(ERROR, "invalid ups ms_read_percentage=%d", ms_read_percentage);
      ret = OB_INVALID_ARGUMENT;
    } else if (0 > cs_read_percentage || 100 < cs_read_percentage) {
      TBSYS_LOG(ERROR, "invalid ups cs_read_percentage=%d", cs_read_percentage);
      ret = OB_INVALID_ARGUMENT;
    } else {
      ups_list_.ups_array_[i].inner_port_ = inner_port;
      ups_list_.ups_array_[i].ms_read_percentage_ = ms_read_percentage;
      ups_list_.ups_array_[i].cs_read_percentage_ = cs_read_percentage;
    }
  } // end for

  if (OB_SUCCESS == ret) {
    ups_list_.print();
  }
  return ret;
}

int NameServer::load_client_config(tbsys::CConfig& config) {
  int ret = OB_SUCCESS;
  static const int MY_BUF_SIZE = 64;
  char conf_key[MY_BUF_SIZE];
  client_config_.BNL_alpha_ = config.getInt(STR_SECTION_CLIENT, STR_BNL_ALPHA,
                                            ObClientConfig::DEFAULT_BNL_ALPHA);
  client_config_.BNL_alpha_denominator_ = config.getInt(STR_SECTION_CLIENT, STR_BNL_ALPHA_DENOMINATOR,
                                                        ObClientConfig::DEFAULT_BNL_ALPHA_DENOMINATOR);
  client_config_.BNL_threshold_ = config.getInt(STR_SECTION_CLIENT, STR_BNL_THRESHOLD,
                                                ObClientConfig::DEFAULT_BNL_THRESHOLD);
  client_config_.BNL_threshold_denominator_ = config.getInt(STR_SECTION_CLIENT, STR_BNL_THRESHOLD_DENOMINATOR,
                                                            ObClientConfig::DEFAULT_BNL_THRESHOLD_DENOMINATOR);
  client_config_.obi_list_.obi_count_ = config.getInt(STR_SECTION_OB_INSTANCES, STR_OBI_COUNT, 0);
  TBSYS_LOG(INFO, "loading obi count=%d", client_config_.obi_list_.obi_count_);
  int32_t loaded_count = 0;
  for (int32_t i = 0; OB_SUCCESS == ret && i < client_config_.obi_list_.obi_count_ && i < client_config_.obi_list_.MAX_OBI_COUNT; ++i) {
    snprintf(conf_key, MY_BUF_SIZE, "obi%d_rs_vip", i);
    const char* ip =  config.getString(STR_SECTION_OB_INSTANCES, conf_key, NULL);
    snprintf(conf_key, MY_BUF_SIZE, "obi%d_rs_port", i);
    int port = config.getInt(STR_SECTION_OB_INSTANCES, conf_key, 0);
    snprintf(conf_key, MY_BUF_SIZE, "obi%d_read_percentage", i);
    int read_percentage = config.getInt(STR_SECTION_OB_INSTANCES, conf_key, -1);
    ObServer rs_addr;
    if (!rs_addr.set_ipv4_addr(ip, port)) {
      TBSYS_LOG(ERROR, "invalid ups addr, addr=%s port=%d", ip, port);
      ret = OB_INVALID_ARGUMENT;
    } else if (0 > read_percentage || 100 < read_percentage) {
      TBSYS_LOG(ERROR, "invalid obi read_percentage=%d", read_percentage);
      ret = OB_INVALID_ARGUMENT;
    } else {
      client_config_.obi_list_.conf_array_[i].set_rs_addr(rs_addr);
      client_config_.obi_list_.conf_array_[i].set_read_percentage(read_percentage);
      loaded_count++;
    }
  } // end for
  client_config_.obi_list_.obi_count_ = loaded_count;
  if (OB_SUCCESS == ret) {
    if (0 == loaded_count) {
      // no ob instances config, set myself as the only instance
      client_config_.obi_list_.conf_array_[0].set_rs_addr(my_addr_);
      client_config_.obi_list_.obi_count_ = 1;
    } else {
      bool found_myself = false;
      for (int i = 0; i < client_config_.obi_list_.obi_count_; ++i) {
        if (client_config_.obi_list_.conf_array_[i].get_rs_addr() == my_addr_) {
          found_myself = true;
          break;
        }
      }
      if (!found_myself) {
        TBSYS_LOG(ERROR, "this RS's address must be in the list of ob instances");
        client_config_.obi_list_.print();
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  if (OB_SUCCESS == ret) {
    if (NULL != schema_manager_) {
      strcpy(client_config_.app_name_, schema_manager_->get_app_name());
    }
    client_config_.print();
  }
  return ret;
}

bool NameServer::reload_config(const char* config_file_name) {
  int res = false;
  if (config_file_name != NULL) {
    res = true;
    tbsys::CConfig config;
    if (res) {
      if (EXIT_FAILURE == config.load(config_file_name)) {
        TBSYS_LOG(ERROR, "load config file %s error", config_file_name);
        res = false;
      }
    }
    if (!have_inited_) {
      //update server info; schema; and vip only can be changed in the first time we load config file
      if (res) {
        //get info of update server
        const char* ip =  config.getString(STR_SECTION_UPDATE_SERVER, STR_IP, NULL);
        int port = config.getInt(STR_SECTION_UPDATE_SERVER, STR_PORT, 0);
        if (ip == NULL ||  0 == port || !update_server_status_.server_.set_ipv4_addr(ip, port)) {
          TBSYS_LOG(ERROR, "load update server info error config file is %s ", config_file_name);
          res = false;
        }
        update_server_status_.status_ = ObServerStatus::STATUS_SERVING;

        if (res) {
          ups_inner_port_ = config.getInt(STR_SECTION_UPDATE_SERVER, STR_UPS_INNER_PORT, 0);
          if (0 == ups_inner_port_) {
            TBSYS_LOG(ERROR, "load update server inner port error");
            res = false;
          }
        }
      }
      UNUSED(STR_SECTION_ROOT_SERVER);
      //if (res)
      //{
      //  //get root server's vip
      //  const char* vip =  config.getString(STR_SECTION_ROOT_SERVER, STR_IP, NULL);

      //}
      if (res) {
        //give schema file name
        const char* schema_file_name = config.getString(STR_SECTION_SCHEMA_INFO, STR_FILE_NAME, NULL);
        if (NULL == schema_file_name) {
          TBSYS_LOG(ERROR, "load schema file name error config file is %s ", config_file_name);
          res = false;
        } else {
          strncpy(schema_file_name_, schema_file_name, OB_MAX_FILE_NAME_LENGTH);
          schema_file_name_[OB_MAX_FILE_NAME_LENGTH - 1] = '\0';
        }
      }
      if (res) {
        // init schema manager
        tbsys::CConfig config;
        if (!schema_manager_->parse_from_file(schema_file_name_, config)) {
          TBSYS_LOG(ERROR, "parse schema error chema file is %s ", schema_file_name_);
          res = false;
        } else {
          TBSYS_LOG(INFO, "load schema from file, file=%s", schema_file_name_);
        }
      }
    }

    if (res) {
      const char* p = config.getString(STR_SECTION_CHUNK_SERVER, STR_LEASE, "10000000");
      int64_t lease_duration = strtoll(p, NULL, 10);
      if (lease_duration < 1000000) {
        TBSYS_LOG(ERROR, "lease duration %ld is unacceptable", lease_duration);
        res = false;
      } else {
        lease_duration_ = lease_duration;
      }
    }

    if (res) {
      migrate_wait_seconds_ = config.getInt(STR_SECTION_ROOT_SERVER, STR_MIGRATE_WAIT_SECONDS, MIGRATE_WAIT_SECONDS);
      if (migrate_wait_seconds_ < MIGRATE_WAIT_SECONDS) {
        TBSYS_LOG(WARN, "change migrate_wait_seconds to %d second(s)", MIGRATE_WAIT_SECONDS);
        migrate_wait_seconds_ = MIGRATE_WAIT_SECONDS;
      }

      enable_balance_ = config.getInt(STR_SECTION_ROOT_SERVER, STR_ENABLE_BALANCE, 1);
      TBSYS_LOG(INFO, "enable_balance=%d", enable_balance_);
      enable_rereplication_ = config.getInt(STR_SECTION_ROOT_SERVER, STR_ENABLE_REREPLICATION, 1);
      TBSYS_LOG(INFO, "enable_rereplication=%d", enable_rereplication_);

      int64_t safe_lost_one_duration = config.getInt(STR_SECTION_CHUNK_SERVER, STR_SAFE_LOST_ONE_DURATION, 3600);
      TBSYS_LOG(INFO, "safe lost one duration is %ld second(s)", safe_lost_one_duration);
      safe_lost_one_duration_ = safe_lost_one_duration * 1000L * 1000L;

      int64_t wait_init_time = config.getInt(STR_SECTION_CHUNK_SERVER, STR_SAFE_WAIT_INIT_DURATION, 60);
      TBSYS_LOG(INFO, "wait_init_time is %ld second(s)", wait_init_time);
      wait_init_time_ = wait_init_time * 1000L * 1000L;

      int64_t max_merge_duration = config.getInt(STR_SECTION_CHUNK_SERVER, STR_MAX_MERGE_DURATION, 7200);
      TBSYS_LOG(INFO, "max_merge_duration is %ld second(s)", max_merge_duration);
      max_merge_duration_ = max_merge_duration * 1000L * 1000L;

      int64_t cs_merge_command_interval_mseconds = config.getInt(STR_SECTION_ROOT_SERVER, STR_CS_COMMAND_INTERVAL, 60 * 1000L * 1000L);
      TBSYS_LOG(INFO, "cs_merge_command_interval_mseconds is %ld m_second(s)", cs_merge_command_interval_mseconds);
      cs_merge_command_interval_mseconds_ = cs_merge_command_interval_mseconds;

      int64_t balance_tolerance = config.getInt(STR_SECTION_CHUNK_SERVER, STR_BALANCE_TOLERANCE,
                                                DEFAULT_BALANCE_TOLERANCE);
      if (MIN_BALANCE_TOLERANCE <= balance_tolerance) {
        balance_tolerance_count_ = balance_tolerance;
      } else {
        balance_tolerance_count_ = MIN_BALANCE_TOLERANCE;
      }
      TBSYS_LOG(INFO, "balance_tolerance_=%ld", balance_tolerance_count_);

      balance_max_concurrent_migrate_num_ = config.getInt(STR_SECTION_CHUNK_SERVER, STR_MAX_CONCURRENT_MIGRATE,
                                                          DEFAULT_MAX_CONCURRENT_MIGRATE);
      if (0 >= balance_max_concurrent_migrate_num_) {
        balance_max_concurrent_migrate_num_ = DEFAULT_MAX_CONCURRENT_MIGRATE;
      }
      TBSYS_LOG(INFO, "max_concurrent_migrate_per_cs=%d", balance_max_concurrent_migrate_num_);

      balance_max_migrate_out_per_cs_ = config.getInt(STR_SECTION_CHUNK_SERVER, STR_MAX_BATCH_MIGRATE_OUT_PER_CS,
                                                      DEFAULT_MAX_MIGRATE_OUT_PER_CS);
      if (0 >= balance_max_migrate_out_per_cs_) {
        balance_max_migrate_out_per_cs_ = DEFAULT_MAX_MIGRATE_OUT_PER_CS;
      }
      TBSYS_LOG(INFO, "max_batch_migrate_out_per_cs=%d", balance_max_migrate_out_per_cs_);
      int32_t max_migrate_timeout = config.getInt(STR_SECTION_CHUNK_SERVER, STR_MAX_BATCH_MIGRATE_TIMEOUT,
                                                  DEFAULT_MAX_MIGRATE_TIMEOUT);
      if (0 >= max_migrate_timeout) {
        max_migrate_timeout = DEFAULT_MAX_MIGRATE_TIMEOUT;
      }
      balance_max_timeout_us_ = max_migrate_timeout;
      balance_max_timeout_us_ *= 1000 * 1000;
      TBSYS_LOG(INFO, "max_migrate_timeout_us=%ld", balance_max_timeout_us_);

      int32_t balance_worker_idle_sleep_sec = config.getInt(STR_SECTION_ROOT_SERVER, STR_BALANCE_WORKER_IDLE_SLEEP_SEC,
                                                            DEFAULT_BALANCE_WORKER_IDLE_SLEEP_SEC);
      if (0 >= balance_worker_idle_sleep_sec) {
        balance_worker_idle_sleep_sec = DEFAULT_BALANCE_WORKER_IDLE_SLEEP_SEC;
      }
      balance_worker_idle_sleep_us_ = balance_worker_idle_sleep_sec;
      balance_worker_idle_sleep_us_ *= 1000 * 1000;
      if (balance_worker_idle_sleep_us_ < MIN_BALANCE_WORKER_SLEEP_US) {
        balance_worker_idle_sleep_us_ = MIN_BALANCE_WORKER_SLEEP_US;
      }
      TBSYS_LOG(INFO, "balance_worker_idle_sleep_us=%ld", balance_worker_idle_sleep_us_);

      int safe_copy_count_in_init = config.getInt(STR_SECTION_ROOT_SERVER, STR_SAFE_COPY_COUNT_IN_INIT, 2);
      if (safe_copy_count_in_init > 0 && safe_copy_count_in_init <= OB_SAFE_COPY_COUNT) {
        safe_copy_count_in_init_ = safe_copy_count_in_init;
      }
      TBSYS_LOG(INFO, "safe_copy_count_in_init_=%d", safe_copy_count_in_init_);
      int safe_copy_count_in_merge = config.getInt(STR_SECTION_ROOT_SERVER, STR_SAFE_COPY_COUNT_IN_MERGE, 2);
      if (safe_copy_count_in_merge > 0 && safe_copy_count_in_merge <= OB_SAFE_COPY_COUNT) {
        safe_copy_count_in_merge_ = safe_copy_count_in_merge;
      }
      int tmp = config.getInt(STR_SECTION_ROOT_SERVER, STR_TABLET_REPLICAS_NUM, DEFAULT_TABLET_REPLICAS_NUM);
      if (safe_copy_count_in_init_ > tmp) {
        TBSYS_LOG(ERROR, "tablet_replicas_num=%d safe_copy_count_in_init=%d", tmp, safe_copy_count_in_init_);
        res = false;
      } else if (0 < tmp && OB_SAFE_COPY_COUNT >= tmp) {
        tablet_replicas_num_ = tmp;
        TBSYS_LOG(INFO, "tablet_replicas_num=%d", tablet_replicas_num_);
      } else {
        TBSYS_LOG(WARN, "invalid tablet_replicas_num, num=%d", tmp);
      }
      create_table_in_init_ = config.getInt(STR_SECTION_ROOT_SERVER, STR_CREATE_TABLE_IN_INIT, 0);
    }
    if (res) {
      const char* ip =  config.getString(STR_SECTION_ROOT_SERVER, STR_IP, NULL);
      int port = config.getInt(STR_SECTION_ROOT_SERVER, STR_PORT, 0);
      if (!my_addr_.set_ipv4_addr(ip, port)) {
        TBSYS_LOG(ERROR, "invalid addr, addr=%s port=%d", ip, port);
        res = false;
      } else {
        char addr_buf[OB_IP_STR_BUFF];
        my_addr_.to_string(addr_buf, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "my addr=%s", addr_buf);
      }
    }
    if (res) {
      res = (OB_SUCCESS == load_ups_list(config));
    }
    if (res) {
      res = (OB_SUCCESS == load_client_config(config));
    }
    TBSYS_LOG(INFO, "reload config, file=%s ret=%c", config_file_name, res ? 'Y' : 'N');
  }
  return res;
}
bool NameServer::start_report(bool init_flag) {
  TBSYS_LOG(DEBUG, "start_report init flag is %d", init_flag);
  bool ret = false;

  //      int rc = OB_SUCCESS;
  //
  //      if (rc == OB_SUCCESS)
  //      {
  //        tbsys::CThreadGuard guard(&status_mutex_);
  //        switch (server_status_)
  //        {
  //          //case STATUS_INIT:
  //          //  if (init_flag == true)
  //          //  {
  //          //    server_status_ = STATUS_NEED_REPORT;
  //          //  }
  //          //  ret = true;
  //          //  break;
  //          case STATUS_SLEEP:
  //            server_status_ = STATUS_NEED_REPORT;
  //            ret = true;
  //            break;
  //          case STATUS_BALANCING:
  //            server_status_ = STATUS_INTERRUPT_BALANCING;
  //            break;
  //
  //          default:
  //            break;
  //        }
  //        if (is_master())
  //        {
  //          if (ret && 0 == time_stamp_changing_)
  //          {
  //            int64_t ts = tbsys::CTimeUtil::getTime();
  //            time_stamp_changing_ = ts; // set chaning timestamp
  //          }
  //          rc = log_worker_.start_report(time_stamp_changing_, init_flag);
  //        }
  //      }
  //
  TBSYS_LOG(DEBUG, "start_report return is %d", ret);
  return ret;
}
void NameServer::drop_current_build() {
  drop_this_build_ = true;
  if (is_master()) {
    log_worker_->drop_current_build();
  }
}
bool NameServer::start_switch() {
  bool ret = false;

  int rc = OB_SUCCESS;

  tbsys::CThreadGuard guard(&status_mutex_);
  int64_t ts = tbsys::CTimeUtil::getTime();


  if (rc == OB_SUCCESS) {
    TBSYS_LOG(INFO, " server_status_ = %d", server_status_);
    switch (server_status_) {
    case STATUS_SLEEP:
    case STATUS_NEED_REPORT:
    case STATUS_NEED_BUILD:
      if (is_master()) {
        time_stamp_changing_ = ts; // set chaning timestamp
        rc = log_worker_->start_switch_root_table(time_stamp_changing_);
      }
      if (rc == OB_SUCCESS) {
        server_status_ = STATUS_NEED_BUILD;
        ret = true;
      }
      break;
    case STATUS_BALANCING:
      if (is_master()) {
        time_stamp_changing_ = ts; // set chaning timestamp
        rc = log_worker_->start_switch_root_table(time_stamp_changing_);
      }
      if (rc == OB_SUCCESS) {
        server_status_ = STATUS_INTERRUPT_BALANCING;
      }
      break;
    default:
      break;
    }
  }

  return ret;
}

bool NameServer::get_schema(common::ObSchemaManagerV2& out_schema) const {
  bool res = false;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (schema_manager_ != NULL) {
    out_schema = *schema_manager_;
    res = true;
  }
  return res;
}
int64_t NameServer::get_schema_version() const {
  int64_t t1 = 0;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (schema_manager_ != NULL) {
    t1 = schema_manager_->get_version();
  }

  return t1;
}

void NameServer::print_alive_server() const {
  TBSYS_LOG(INFO, "start dump server info");
  //tbsys::CRLockGuard guard(server_manager_rwlock_); // do not need this lock
  ObChunkServerManager::const_iterator it = server_manager_.begin();
  int32_t index = 0;
  for (; it != server_manager_.end(); ++it) {
    it->dump(index++);
  }
  TBSYS_LOG(INFO, "dump server info complete");
  return;
}
int NameServer::get_cs_info(ObChunkServerManager* out_server_manager) const {
  int ret = OB_SUCCESS;
  tbsys::CRLockGuard guard(server_manager_rwlock_);
  if (out_server_manager != NULL) {
    *out_server_manager = server_manager_;
  } else {
    ret = OB_ERROR;
  }
  return ret;
}
void NameServer::dump_root_table() const {
  tbsys::CRLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "dump root table");
  if (root_table_for_query_ != NULL) {
    root_table_for_query_->dump();
  }
}

void NameServer::dump_unusual_tablets() const {
  tbsys::CRLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "dump unusual tablets");
  if (root_table_for_query_ != NULL) {
    root_table_for_query_->dump_unusual_tablets(last_frozen_mem_version_, tablet_replicas_num_);
  }
}

bool NameServer::reload_config(bool did_write_log) {
  bool ret = reload_config(config_file_name_);
  if (ret && did_write_log && is_master()) {
    ret = (OB_SUCCESS == log_worker_->set_ups_list(ups_list_));
    if (ret) {
      ret = (OB_SUCCESS == log_worker_->set_client_config(client_config_));
    }
  }
  return ret;
}

// @return 0 do not copy, 1 copy immediately, -1 delayed copy
inline int NameServer::need_copy(int32_t available_num, int32_t lost_num) {
  OB_ASSERT(0 <= available_num);
  OB_ASSERT(0 <= lost_num);
  int ret = 0;
  if (0 == available_num || 0 == lost_num) {
    ret = 0;
  } else if (1 == lost_num) {
    ret = -1;
  } else {                      // lost_num >= 2
    ret = 1;
  }
  return ret;
}

int32_t NameServer::nb_get_table_count() {
  int32_t ret = 0;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL == schema_manager_) {
    TBSYS_LOG(ERROR, "schema_manager is NULL");
  } else {
    const ObTableSchema* it = NULL;
    for (it = schema_manager_->table_begin(); schema_manager_->table_end() != it; ++it) {
      ret++;
    }
  }
  return ret;
}

uint64_t NameServer::nb_get_next_table_id(int32_t table_count, int32_t seq/*=-1*/) {
  uint64_t ret = OB_INVALID_ID;
  // for each table
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL == schema_manager_) {
    TBSYS_LOG(ERROR, "schema_manager is NULL");
  } else if (0 >= table_count) {
    // no table
  } else {
    if (0 > seq) {
      seq = balance_next_table_seq_;
      balance_next_table_seq_++;
    }
    int32_t idx = 0;
    const ObTableSchema* it = NULL;
    for (it = schema_manager_->table_begin(); schema_manager_->table_end() != it; ++it) {
      if (seq % table_count == idx) {
        ret = it->get_table_id();
        break;
      }
      idx++;
    }
  }
  return ret;
}

int NameServer::nb_find_dest_cs(NameTable::const_iterator meta, int64_t low_bound, int32_t cs_num,
                                int32_t& dest_cs_idx, ObChunkServerManager::iterator& dest_it) {
  int ret = OB_ENTRY_NOT_EXIST;
  dest_cs_idx = OB_INVALID_INDEX;
  if (0 < cs_num) {
    ObChunkServerManager::iterator it = server_manager_.begin();
    it += balance_select_dest_start_pos_ % cs_num;
    if (it >= server_manager_.end()) {
      it = server_manager_.begin();
    }
    ObChunkServerManager::iterator it_start_pos = it;
    int32_t cs_idx = OB_INVALID_INDEX;
    // scan from start_pos to end(), then from start() to start_pos
    while (true) {
      if (it->status_ != ObServerStatus::STATUS_DEAD
          && it->balance_info_.table_sstable_count_ < low_bound) {
        cs_idx = it - server_manager_.begin();
        // this cs does't have this tablet
        if (!meta->did_cs_have(cs_idx)) {
          dest_it = it;
          dest_cs_idx = cs_idx;
          TBSYS_LOG(DEBUG, "find dest cs, start_pos=%ld cs_idx=%d",
                    it_start_pos - server_manager_.begin(), dest_cs_idx);
          balance_select_dest_start_pos_ = dest_cs_idx + 1;
          ret = OB_SUCCESS;
          break;
        }
      }
      ++it;
      if (it == server_manager_.end()) {
        it = server_manager_.begin();
      }
      if (it == it_start_pos) {
        break;
      }
    } // end while
  }
  return ret;
}

int NameServer::nb_check_rereplication(NameTable::const_iterator it, RereplicationAction& act) {
  int ret = OB_SUCCESS;
  act = RA_NOP;
  if (enable_rereplication_) {
    int64_t last_version = 0;
    int32_t valid_replicas_num = 0;
    int32_t lost_copy = 0;
    for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++) {
      if (OB_INVALID_INDEX != it->server_info_indexes_[i]) {
        valid_replicas_num ++;
        if (it->tablet_version_[i] > last_version) {
          last_version = it->tablet_version_[i];
        }
      }
    }

    if (valid_replicas_num < tablet_replicas_num_) {
      lost_copy = tablet_replicas_num_ - valid_replicas_num;
      int did_need_copy = need_copy(valid_replicas_num, lost_copy);
      int64_t now = tbsys::CTimeUtil::getTime();
      if (1 == did_need_copy) {
        act = RA_COPY;
      } else if (-1 == did_need_copy) {
        if (now - it->last_dead_server_time_ > safe_lost_one_duration_) {
          act = RA_COPY;
        } else {
          TBSYS_LOG(DEBUG, "copy delayed, now=%ld lost_replica_time=%ld duration=%ld",
                    now, it->last_dead_server_time_, safe_lost_one_duration_);
        }
      }
    } else if (valid_replicas_num > tablet_replicas_num_) {
      act = RA_DELETE;
    }
  }
  return ret;
}

int NameServer::nb_select_copy_src(NameTable::const_iterator it,
                                   int32_t& src_cs_idx, ObChunkServerManager::iterator& src_it) {
  int ret = OB_ENTRY_NOT_EXIST;
  src_cs_idx = OB_INVALID_INDEX;
  int32_t min_count = INT32_MAX;
  // find cs with min migrate out count
  for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
    if (OB_INVALID_INDEX != it->server_info_indexes_[i]) {
      ObServerStatus* src_cs = server_manager_.get_server_status(it->server_info_indexes_[i]);
      int32_t migrate_count = src_cs->balance_info_.migrate_to_.count();
      if (migrate_count < min_count
          && migrate_count < balance_max_migrate_out_per_cs_) {
        min_count = migrate_count;
        src_cs_idx = src_cs - server_manager_.begin();
        src_it = src_cs;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int NameServer::nb_add_copy(NameTable::const_iterator it, const ObTabletInfo* tablet, int64_t low_bound, int32_t cs_num) {
  int ret = OB_ERROR;
  int32_t dest_cs_idx = OB_INVALID_INDEX;
  ObServerStatus* dest_it = NULL;
  if (OB_SUCCESS != nb_find_dest_cs(it, low_bound, cs_num, dest_cs_idx, dest_it)
      || OB_INVALID_INDEX == dest_cs_idx) {
    if (OB_SUCCESS != nb_find_dest_cs(it, INT64_MAX, cs_num, dest_cs_idx, dest_it)
        || OB_INVALID_INDEX == dest_cs_idx) {
      TBSYS_LOG(DEBUG, "cannot find dest cs");
    }
  }
  if (OB_INVALID_INDEX != dest_cs_idx) {
    int32_t src_cs_idx = OB_INVALID_INDEX;
    ObServerStatus* src_it = NULL;
    if (OB_SUCCESS != nb_select_copy_src(it, src_cs_idx, src_it)
        || OB_INVALID_INDEX == src_cs_idx) {
      TBSYS_LOG(DEBUG, "cannot find src cs");
    } else {
      int bm_ret = server_manager_.add_copy_info(*src_it, tablet->range_, dest_cs_idx);
      if (OB_SUCCESS == bm_ret) {
        // no locking
        dest_it->balance_info_.table_sstable_count_++;
        balance_batch_migrate_count_++;
        balance_batch_copy_count_++;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int NameServer::nb_check_add_migrate(NameTable::const_iterator it, const ObTabletInfo* tablet, int64_t avg_count, int32_t cs_num) {
  int ret = OB_SUCCESS;
  int64_t delta_count = balance_tolerance_count_;
  for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
    int32_t cs_idx = it->server_info_indexes_[i];
    if (OB_INVALID_INDEX != cs_idx) {
      ObServerStatus* src_cs = server_manager_.get_server_status(cs_idx);
      if (NULL != src_cs && ObServerStatus::STATUS_DEAD != src_cs->status_) {
        if (src_cs->balance_info_.table_sstable_count_ > avg_count + delta_count
            && src_cs->balance_info_.migrate_to_.count() < balance_max_migrate_out_per_cs_) {
          // move out this sstable
          // find dest cs, no locking
          int32_t dest_cs_idx = OB_INVALID_INDEX;
          ObServerStatus* dest_it = NULL;
          if (OB_SUCCESS != nb_find_dest_cs(it, avg_count - delta_count, cs_num, dest_cs_idx, dest_it)
              || OB_INVALID_INDEX == dest_cs_idx) {
            TBSYS_LOG(DEBUG, "cannot find dest cs");
          } else {
            int bm_ret = server_manager_.add_migrate_info(*src_cs, tablet->range_, dest_cs_idx);
            if (OB_SUCCESS == bm_ret) {
              // no locking
              src_cs->balance_info_.table_sstable_count_--;
              dest_it->balance_info_.table_sstable_count_++;
              balance_batch_migrate_count_++;
            }
          }
          break;
        }
      }
    }
  } // end for
  return ret;
}

int NameServer::nb_balance_by_table(const uint64_t table_id, bool& scan_next_table) {
  int ret = OB_SUCCESS;
  int64_t avg_size = 0;
  int64_t avg_count = 0;
  int64_t delta_count = balance_tolerance_count_;
  int32_t cs_num = 0;
  scan_next_table = true;

  if (OB_SUCCESS != (ret = nb_calculate_sstable_count(table_id, avg_size, avg_count, cs_num))) {
    TBSYS_LOG(WARN, "calculate table size error, err=%d", ret);
  } else if (0 < cs_num) {
    bool is_curr_table_balanced = nb_is_curr_table_balanced(avg_count);
    if (!is_curr_table_balanced) {
      TBSYS_LOG(DEBUG, "balance table, table_id=%lu avg_count=%ld", table_id, avg_count);
      nb_print_balance_info();
    }

    NameTable::const_iterator it;
    const ObTabletInfo* tablet = NULL;
    bool table_found = false;
    RereplicationAction ract;
    // scan the root table
    tbsys::CRLockGuard guard(root_table_rwlock_);
    for (it = root_table_for_query_->begin(); it != root_table_for_query_->sorted_end(); ++it) {
      tablet = root_table_for_query_->get_tablet_info(it);
      if (NULL != tablet) {
        if (tablet->range_.table_id_ == table_id) {
          if (!table_found) {
            table_found = true;
          }
          // check re-replication first
          int add_ret = OB_ERROR;
          if (OB_SUCCESS == nb_check_rereplication(it, ract)) {
            if (RA_COPY == ract) {
              add_ret = nb_add_copy(it, tablet, avg_count - delta_count, cs_num);
            } else if (RA_DELETE == ract) {
              // @todo
            }
          }
          // then do balnace if needed
          if (OB_SUCCESS != add_ret
              && !is_curr_table_balanced
              && enable_balance_) {
            nb_check_add_migrate(it, tablet, avg_count, cs_num);
          }
          // terminate condition
          if (server_manager_.is_migrate_infos_full()) {
            scan_next_table = false;
            break;
          }
        } else {
          if (table_found) {
            // another table
            break;
          }
        }
      } // end if tablet not NULL
    } // end for
  }
  return ret;
}

bool NameServer::nb_is_curr_table_balanced(int64_t avg_sstable_count, const ObServer& except_cs) const {
  bool ret = true;
  int64_t delta_count = balance_tolerance_count_;
  int32_t cs_out = 0;
  int32_t cs_in = 0;
  ObChunkServerManager::const_iterator it;
  for (it = server_manager_.begin(); it != server_manager_.end(); ++it) {
    if (it->status_ != ObServerStatus::STATUS_DEAD) {
      if (except_cs == it->server_) {
        // do not consider except_cs
        continue;
      }
      if ((avg_sstable_count + delta_count) < it->balance_info_.table_sstable_count_) {
        cs_out++;
      } else if ((avg_sstable_count - delta_count) > it->balance_info_.table_sstable_count_) {
        cs_in++;
      }
      if (0 < cs_out && 0 < cs_in) {
        ret = false;
        break;
      }
    }
  } // end for
  return ret;
}

bool NameServer::nb_is_curr_table_balanced(int64_t avg_sstable_count) const {
  ObServer not_exist_cs;
  return nb_is_curr_table_balanced(avg_sstable_count, not_exist_cs);
}

int NameServer::nb_calculate_sstable_count(const uint64_t table_id, int64_t& avg_size, int64_t& avg_count, int32_t& cs_count) {
  int ret = OB_SUCCESS;
  avg_size = 0;
  avg_count = 0;
  cs_count = 0;
  int64_t total_size = 0;
  int64_t total_count = 0; // total sstable count
  {
    // prepare
    tbsys::CWLockGuard guard(server_manager_rwlock_);
    server_manager_.reset_balance_info_for_table(cs_count);
  } // end lock
  {
    // calculate sum
    NameTable::const_iterator it;
    const ObTabletInfo* tablet = NULL;
    tbsys::CRLockGuard guard(root_table_rwlock_);
    bool table_found = false;
    for (it = root_table_for_query_->begin(); it != root_table_for_query_->sorted_end(); ++it) {
      tablet = root_table_for_query_->get_tablet_info(it);
      if (NULL != tablet) {
        if (tablet->range_.table_id_ == table_id) {
          if (!table_found) {
            table_found = true;
          }
          for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
            if (OB_INVALID_INDEX != it->server_info_indexes_[i]) {
              ObServerStatus* cs = server_manager_.get_server_status(it->server_info_indexes_[i]);
              if (NULL != cs && ObServerStatus::STATUS_DEAD != cs->status_) {
                cs->balance_info_.table_sstable_total_size_ += tablet->occupy_size_;
                total_size += tablet->occupy_size_;
                cs->balance_info_.table_sstable_count_++;
                total_count++;
              }
            }
          } // end for
        } else {
          if (table_found) {
            break;
          }
        }
      }
    } // end for
  }   // end lock
  if (0 != cs_count) {
    avg_size = total_size / cs_count;
    avg_count = total_count / cs_count;
    int64_t sstable_avg_size = -1;
    if (0 != total_count) {
      sstable_avg_size = total_size / total_count;
    }
    TBSYS_LOG(DEBUG, "sstable distribution, table_id=%lu total_size=%ld total_count=%ld "
              "cs_num=%d avg_size=%ld avg_count=%ld sstable_avg_size=%ld",
              table_id, total_size, total_count,
              cs_count, avg_size, avg_count, sstable_avg_size);
  }
  return ret;
}

void NameServer::nb_print_balance_info() const {
  char addr_buf[OB_IP_STR_BUFF];
  const ObServerStatus* it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it) {
    if (NULL != it && ObServerStatus::STATUS_DEAD != it->status_) {
      it->server_.to_string(addr_buf, OB_IP_STR_BUFF);
      TBSYS_LOG(DEBUG, "cs=%s sstables_count=%ld sstables_size=%ld migrate=%d",
                addr_buf, it->balance_info_.table_sstable_count_,
                it->balance_info_.table_sstable_total_size_,
                it->balance_info_.migrate_to_.count());
    }
  }
}

void NameServer::nb_print_balance_info(char* buf, const int64_t buf_len, int64_t& pos) const {
  char addr_buf[OB_IP_STR_BUFF];
  const ObServerStatus* it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it) {
    if (NULL != it && ObServerStatus::STATUS_DEAD != it->status_) {
      it->server_.to_string(addr_buf, OB_IP_STR_BUFF);
      databuff_printf(buf, buf_len, pos, "%s %ld %ld\n",
                      addr_buf, it->balance_info_.table_sstable_count_,
                      it->balance_info_.table_sstable_total_size_);
    }
  }
}

int NameServer::send_msg_migrate(const ObServer& src, const ObServer& dest, const ObRange& range, bool keep_src) {
  int ret = OB_SUCCESS;
  ret = worker_->get_rpc_stub().migrate_tablet(src, dest, range, keep_src, worker_->get_rpc_timeout());
  if (OB_SUCCESS == ret) {
    static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
    range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
    char f_server[OB_IP_STR_BUFF];
    char t_server[OB_IP_STR_BUFF];
    src.to_string(f_server, OB_IP_STR_BUFF);
    dest.to_string(t_server, OB_IP_STR_BUFF);
    TBSYS_LOG(INFO, "migrate tablet, tablet=%s src=%s dest=%s keep_src=%c",
              row_key_dump_buff, f_server, t_server, keep_src ? 'Y' : 'N');
  } else {
    TBSYS_LOG(WARN, "failed to send migrate msg, err=%d", ret);
  }
  return ret;
}

int NameServer::nb_start_batch_migrate() {
  int ret = OB_SUCCESS;
  int32_t sent_count = 0;
  int32_t batch_migrate_per_cs = balance_max_concurrent_migrate_num_;
  ObChunkServerManager::iterator it;
  for (it = server_manager_.begin(); it != server_manager_.end(); ++it) {
    if (it->status_ != ObServerStatus::STATUS_DEAD
        && 0 < it->balance_info_.migrate_to_.count()
        && it->balance_info_.curr_migrate_out_num_ < batch_migrate_per_cs) {
      // start the first K migrate
      ObMigrateInfo* minfo = it->balance_info_.migrate_to_.head();
      for (; NULL != minfo; minfo = minfo->next_) {
        if (OB_INVALID_INDEX != minfo->cs_idx_
            && ObMigrateInfo::STAT_INIT == minfo->stat_) {
          ObServerStatus* dest_cs = server_manager_.get_server_status(minfo->cs_idx_);
          if (NULL != dest_cs
              && ObServerStatus::STATUS_DEAD != dest_cs->status_
              && batch_migrate_per_cs > dest_cs->balance_info_.curr_migrate_in_num_) {
            if (OB_SUCCESS == send_msg_migrate(it->server_, dest_cs->server_, minfo->range_, 1 == minfo->keep_src_)) {
              minfo->stat_ = ObMigrateInfo::STAT_SENT;
              it->balance_info_.curr_migrate_out_num_++;
              dest_cs->balance_info_.curr_migrate_in_num_++;
              sent_count++;
              if (it->balance_info_.curr_migrate_out_num_ >= batch_migrate_per_cs) {
                break;
              }
            }
          }
        }
      } // end while each migrate candidates
    }
  } // end for
  TBSYS_LOG(INFO, "batch migrate sent_num=%d done=%d total=%d",
            sent_count, balance_batch_migrate_done_num_, balance_batch_migrate_count_);
  if (0 >= sent_count) {
    nb_print_migrate_infos();
  }
  return ret;
}

void NameServer::nb_print_migrate_infos() const {
  TBSYS_LOG(INFO, "print migrate infos:");
  char addr_buf1[OB_IP_STR_BUFF];
  char addr_buf2[OB_IP_STR_BUFF];
  static char range_buf[OB_MAX_ROW_KEY_LENGTH * 2];
  int32_t idx = 0;
  int32_t total_in = 0;
  int32_t total_out = 0;
  ObChunkServerManager::const_iterator it;
  for (it = server_manager_.begin(); it != server_manager_.end(); ++it) {
    if (it->status_ != ObServerStatus::STATUS_DEAD) {
      total_in += it->balance_info_.curr_migrate_in_num_;
      total_out += it->balance_info_.curr_migrate_out_num_;
      it->server_.to_string(addr_buf1, OB_IP_STR_BUFF);
      TBSYS_LOG(INFO, "balance info, cs=%s in=%d out=%d",
                addr_buf1, it->balance_info_.curr_migrate_in_num_,
                it->balance_info_.curr_migrate_out_num_);

      if (0 < it->balance_info_.migrate_to_.count()) {
        const ObMigrateInfo* minfo = it->balance_info_.migrate_to_.head();
        for (; NULL != minfo; minfo = minfo->next_) {
          if (OB_INVALID_INDEX != minfo->cs_idx_
              && ObMigrateInfo::STAT_DONE != minfo->stat_) {
            const ObServerStatus* dest_cs = server_manager_.get_server_status(minfo->cs_idx_);
            if (NULL != dest_cs
                && ObServerStatus::STATUS_DEAD != dest_cs->status_) {
              dest_cs->server_.to_string(addr_buf2, OB_IP_STR_BUFF);
              minfo->range_.to_string(range_buf, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(INFO, "migrate info, idx=%d src_cs=%s dest_cs=%s range=%s stat=%s src_out=%d dest_in=%d keep_src=%c",
                        idx, addr_buf1, addr_buf2, range_buf, minfo->get_stat_str(),
                        it->balance_info_.curr_migrate_out_num_,
                        dest_cs->balance_info_.curr_migrate_in_num_,
                        minfo->keep_src_ ? 'Y' : 'N');
              idx++;
            }
          }
        } // end while each migrate candidates
      }
    }
  } // end for
  if (total_in != total_out) {
    TBSYS_LOG(ERROR, "BUG total_in=%d total_out=%d", total_in, total_out);
  }
}

void NameServer::dump_migrate_info() const {
  TBSYS_LOG(INFO, "balance batch migrate infos, start_us=%ld timeout_us=%ld done=%d total=%d",
            balance_start_time_us_, balance_timeout_us_,
            balance_batch_migrate_done_num_, balance_batch_migrate_count_);
  nb_print_migrate_infos();
}

int NameServer::nb_check_migrate_timeout() {
  int ret = OB_SUCCESS;
  int64_t mnow = tbsys::CTimeUtil::getMonotonicTime();
  if (nb_is_in_batch_migrating()
      && balance_timeout_us_ + balance_timeout_us_delta_ + balance_start_time_us_ < mnow) {
    TBSYS_LOG(WARN, "balance batch migrate timeout, start_us=%ld prev_timeout_us=%ld done=%d total=%d",
              balance_start_time_us_, balance_timeout_us_,
              balance_batch_migrate_done_num_, balance_batch_migrate_count_);
    int64_t elapsed_us = 0;
    if (balance_start_time_us_ < balance_last_migrate_succ_time_) {
      elapsed_us = balance_last_migrate_succ_time_ - balance_start_time_us_;
    } else {
      elapsed_us = mnow - balance_start_time_us_;
    }
    // better guess of timeout
    if (0 >= balance_batch_migrate_done_num_) {
      balance_batch_migrate_done_num_ = 1;
    }
    balance_timeout_us_ = balance_batch_migrate_count_ * elapsed_us / balance_batch_migrate_done_num_;
    if (0 < balance_max_timeout_us_
        && balance_timeout_us_ > balance_max_timeout_us_) {
      balance_timeout_us_ = balance_max_timeout_us_;
    }
    // clear
    balance_start_time_us_ = 0;
    balance_batch_migrate_count_ = 0;
    balance_batch_copy_count_ = 0;
    balance_batch_migrate_done_num_ = 0;
    server_manager_.reset_balance_info(balance_max_migrate_out_per_cs_);
  }
  return ret;
}

bool NameServer::nb_is_in_batch_migrating() {
  return 0 != balance_start_time_us_;
}

void NameServer::nb_batch_migrate_done() {
  if (nb_is_in_batch_migrating()) {
    int64_t mnow = tbsys::CTimeUtil::getMonotonicTime();
    TBSYS_LOG(INFO, "balance batch migrate done, elapsed_us=%ld prev_timeout_us=%ld done=%d",
              mnow - balance_start_time_us_, balance_timeout_us_, balance_batch_migrate_count_);

    server_manager_.reset_balance_info(balance_max_migrate_out_per_cs_);
    balance_timeout_us_ = mnow - balance_start_time_us_;
    balance_start_time_us_ = 0;
    balance_batch_migrate_count_ = 0;
    balance_batch_copy_count_ = 0;
    balance_batch_migrate_done_num_ = 0;
  }
}

int NameServer::nb_trigger_next_migrate(const ObRange& range, int32_t src_cs_idx, int32_t dest_cs_idx, bool keep_src) {
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  ObServerStatus* src_cs = server_manager_.get_server_status(src_cs_idx);
  ObServerStatus* dest_cs = server_manager_.get_server_status(dest_cs_idx);
  if (NULL == src_cs || NULL == dest_cs) {
    TBSYS_LOG(WARN, "invalid cs, src_cs_idx=%d dest_cs_id=%d", src_cs_idx, dest_cs_idx);
    ret = OB_INVALID_ARGUMENT;
  } else if (ObServerStatus::STATUS_DEAD == src_cs->status_) {
    TBSYS_LOG(WARN, "src cs is offline, cs_idx=%d", src_cs_idx);
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!nb_is_in_batch_migrating()
             || OB_SUCCESS != (ret = src_cs->balance_info_.migrate_to_.set_migrate_done(range, dest_cs_idx))) {
    TBSYS_LOG(WARN, "not a recorded migrate, src_cs=%d dest_cs=%d", src_cs_idx, dest_cs_idx);
  } else {
    src_cs->balance_info_.curr_migrate_out_num_--;
    if (ObServerStatus::STATUS_DEAD != dest_cs->status_) {
      dest_cs->balance_info_.curr_migrate_in_num_--;
    }
    balance_batch_migrate_done_num_++;
    balance_last_migrate_succ_time_ = tbsys::CTimeUtil::getMonotonicTime();
    if (keep_src) {
      worker_->get_stat_manager().inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_COPY_COUNT);
    } else {
      worker_->get_stat_manager().inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_MIGRATE_COUNT);
    }
    if (balance_batch_migrate_done_num_ >= balance_batch_migrate_count_) {
      nb_batch_migrate_done();
    } else {
      // next migrate
      ret = nb_start_batch_migrate();
    }
  }
  return ret;
}

// 负载均衡入口函数
void NameServer::do_new_balance() {
  int ret = OB_SUCCESS;
  balance_batch_migrate_count_ = 0;
  balance_batch_copy_count_ = 0;
  balance_batch_migrate_done_num_ = 0;
  balance_last_migrate_succ_time_ = 0;
  server_manager_.reset_balance_info(balance_max_migrate_out_per_cs_);
  TBSYS_LOG(DEBUG, "new balance begin");
  int32_t table_count = nb_get_table_count();
  bool scan_next_table = true;
  for (int32_t i = 0; i < table_count && scan_next_table; ++i) { // for each table
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count))) {
      TBSYS_LOG(DEBUG, "balance next table, table_id=%lu table_count=%d", table_id, table_count);
      ret = nb_balance_by_table(table_id, scan_next_table);
    }
  }

  if (0 < balance_batch_migrate_count_) {
    TBSYS_LOG(INFO, "batch migrate begin, count=%d(copy=%d) timeout=%ld",
              balance_batch_migrate_count_, balance_batch_copy_count_, balance_timeout_us_);
    tbsys::CWLockGuard guard(server_manager_rwlock_);
    balance_start_time_us_ = tbsys::CTimeUtil::getMonotonicTime();
    ret = nb_start_batch_migrate();
    balance_worker_sleep_us_ = MIN_BALANCE_WORKER_SLEEP_US;
  } else {
    // idle
    balance_worker_sleep_us_ = balance_worker_idle_sleep_us_;
    TBSYS_LOG(INFO, "balance worker idle, sleep_us=%ld", balance_worker_sleep_us_);
  }
}

bool NameServer::nb_is_all_tables_balanced(const common::ObServer& except_cs) {
  bool ret = true;
  int32_t table_count = nb_get_table_count();
  for (int32_t i = 0; i < table_count; ++i) { // for each table
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count, i))) {
      int64_t avg_size = 0;
      int64_t avg_count = 0;
      int32_t cs_num = 0;
      if (OB_SUCCESS == nb_calculate_sstable_count(table_id, avg_size, avg_count, cs_num)) {
        nb_print_balance_info();
        ret = nb_is_curr_table_balanced(avg_count, except_cs);
        if (!ret) {
          TBSYS_LOG(DEBUG, "table not balanced, id=%lu", table_id);
          break;
        }
      } else {
        ret = false;
        break;
      }
    }
  }
  return ret;
}

bool NameServer::nb_is_all_tables_balanced() {
  ObServer not_exist_cs;
  return nb_is_all_tables_balanced(not_exist_cs);
}

void NameServer::nb_print_balance_infos(char* buf, const int64_t buf_len, int64_t& pos) {
  int32_t table_count = nb_get_table_count();
  for (int32_t i = 0; i < table_count; ++i) { // for each table
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count, i))) {
      int64_t avg_size = 0;
      int64_t avg_count = 0;
      int32_t cs_num = 0;
      if (OB_SUCCESS == nb_calculate_sstable_count(table_id, avg_size, avg_count, cs_num)) {
        databuff_printf(buf, buf_len, pos, "table_id=%lu avg_count=%ld avg_size=%ld cs_num=%d\n",
                        table_id, avg_count, avg_size, cs_num);
        databuff_printf(buf, buf_len, pos, "cs sstables_count sstables_size\n");
        nb_print_balance_info(buf, buf_len, pos);
        databuff_printf(buf, buf_len, pos, "--------\n");
      }
    }
  }
}

bool NameServer::nb_is_all_tablets_replicated(int32_t expected_replicas_num) {
  bool ret = true;
  NameTable::const_iterator it;
  int32_t replicas_num = 0;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  for (it = root_table_for_query_->begin(); it != root_table_for_query_->sorted_end(); ++it) {
    replicas_num = 0;
    for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
      if (OB_INVALID_INDEX != it->server_info_indexes_[i]) {
        replicas_num++;
      }
    }
    if (replicas_num < expected_replicas_num) {
      TBSYS_LOG(DEBUG, "tablet not replicated, num=%d expected=%d",
                replicas_num, expected_replicas_num);
      ret = false;
      break;
    }
  }
  return ret;
}

/*
 * 从本地读取新schema, 判断兼容性
 */
int NameServer::switch_schema(int64_t time_stamp) {
  TBSYS_LOG(INFO, "switch_schema time_stamp is %ld", time_stamp);
  int res = OB_SUCCESS;
  tbsys::CConfig config;
  common::ObSchemaManagerV2* new_schema_manager = NULL;
  new_schema_manager = new(std::nothrow)ObSchemaManagerV2(time_stamp);

  if (new_schema_manager == NULL) {
    TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
    res = OB_ERROR;
  } else if (! new_schema_manager->parse_from_file(schema_file_name_, config)) {
    TBSYS_LOG(ERROR, "parse schema error chema file is %s ", schema_file_name_);
    res = OB_ERROR;
  } else {
    tbsys::CWLockGuard guard(schema_manager_rwlock_);
    if (! schema_manager_->is_compatible(*new_schema_manager)) {
      TBSYS_LOG(ERROR, "new schema can not compitable with the old one"
                " schema file name is  %s ", schema_file_name_);
      res = OB_ERROR;
    } else {
      common::ObSchemaManagerV2* old_schema_manager = NULL;
      old_schema_manager = schema_manager_;
      schema_manager_ = new_schema_manager;
      new_schema_manager = old_schema_manager;
    }
  } //releae lock
  if (new_schema_manager != NULL) {
    delete new_schema_manager;
    new_schema_manager = NULL;
  }
  return res;
}

int NameServer::create_root_table_for_build() {
  int ret = OB_SUCCESS;
  if (tablet_manager_for_build_ != NULL) {
    delete tablet_manager_for_build_;
    tablet_manager_for_build_ = NULL;
  }
  if (root_table_for_build_ != NULL) {
    delete root_table_for_build_;
    root_table_for_build_ = NULL;
  }
  //root_table_for_query we need create new table
  tablet_manager_for_build_ = new(std::nothrow)ObTabletInfoManager;
  if (tablet_manager_for_build_ == NULL) {
    TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  root_table_for_build_ = new(std::nothrow)NameTable(tablet_manager_for_build_);
  if (NULL == root_table_for_build_) {
    TBSYS_LOG(ERROR, "new root table for build error");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  TBSYS_LOG(INFO, "new root_table_for_build=%p", root_table_for_build_);
  return ret;
}

int NameServer::init_root_table_by_report() {
  TBSYS_LOG(INFO, "[NOTICE] init_root_table_by_report begin");
  int  res = OB_SUCCESS;

  create_root_table_for_build();
  if (root_table_for_build_ == NULL) {
    TBSYS_LOG(ERROR, "new NameTable error");
    res = OB_ERROR;
  }
  {
    tbsys::CThreadGuard guard(&status_mutex_);
    server_status_ = STATUS_CHANGING;
    TBSYS_LOG(INFO, "server_status_ = %d", server_status_);
  }
  //wail until have cs registed
  while (OB_SUCCESS == res && !first_cs_had_registed_ && !receive_stop_) {
    TBSYS_LOG(INFO, "wait for the first cs regist");
    sleep(1);
  }
  bool finish_init = false;
  int round = 0;
  while (OB_SUCCESS == res && !finish_init && !receive_stop_) {
    TBSYS_LOG(INFO, "clock click, round=%d", ++round);

    ObTabletInfoManager* tablet_manager_tmp_ = NULL;
    NameTable* root_table_tmp_ = NULL;
    if (OB_SUCCESS == res) {
      tablet_manager_tmp_ = new(std::nothrow)ObTabletInfoManager;
      if (tablet_manager_tmp_ == NULL) {
        TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
        res = OB_ERROR;
      }
      root_table_tmp_ = new(std::nothrow)NameTable(tablet_manager_tmp_);
      if (root_table_tmp_ == NULL) {
        TBSYS_LOG(ERROR, "new NameTable error");
        res = OB_ERROR;
      }
    }
    if (OB_SUCCESS == res) {
      tbsys::CThreadGuard guard_build(&root_table_build_mutex_);
      TBSYS_LOG(INFO, "table_for_build is empty?%c", root_table_for_build_->is_empty() ? 'Y' : 'N');

      root_table_for_build_->sort();
      bool check_ok = false;
      check_ok = (OB_SUCCESS == root_table_for_build_->shrink_to(root_table_tmp_));
      if (check_ok) {
        root_table_tmp_->sort();
      } else {
        TBSYS_LOG(ERROR, "shrink table error");
      }
      if (check_ok) {
        check_ok = root_table_tmp_->check_lost_range();
        if (!check_ok) {
          TBSYS_LOG(WARN, "check root table failed we will wait for another %ld seconds", wait_init_time_ / (1000L * 1000L));
        }
      }
      if (check_ok) {
        check_ok = root_table_tmp_->check_tablet_copy_count(safe_copy_count_in_init_);
        if (!check_ok) {
          TBSYS_LOG(WARN, "check root table copy_count fail we will wait for another %ld seconds", wait_init_time_ / (1000L * 1000L));
        }
      }
      if (check_ok) {
        //if all server is reported and
        //we have no tablets, we create it

        bool all_server_is_reported = true;
        int32_t server_count = 0;
        {
          tbsys::CRLockGuard guard(server_manager_rwlock_);
          ObChunkServerManager::iterator it = server_manager_.begin();
          for (; it != server_manager_.end(); ++it) {
            if (it->port_cs_ != 0 && (it->status_ == ObServerStatus::STATUS_REPORTING ||
                                      it->status_ == ObServerStatus::STATUS_DEAD)) {
              all_server_is_reported = false;
              break;
            }
            ++server_count;
          }
        }
        TBSYS_LOG(INFO, "all_cs_reported=%c cs_num=%d", all_server_is_reported ? 'Y' : 'N', server_count);

        if (server_count > 0 && all_server_is_reported &&
            root_table_tmp_->is_empty() && (create_table_in_init_ != 0) && is_master()) {
          TBSYS_LOG(INFO, "chunkservers have no data,create new table");
          create_new_table_in_init(schema_manager_, root_table_tmp_);
        }

        //check we have we have enougth table
        tbsys::CRLockGuard guard(schema_manager_rwlock_);
        for (const ObTableSchema* it = schema_manager_->table_begin(); it != schema_manager_->table_end(); ++it) {
          if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table()) {
            if (!root_table_tmp_->table_is_exist(it->get_table_id())) {
              TBSYS_LOG(WARN, "table_id = %lu has not been created are you sure about this?", it->get_table_id());
              check_ok = false;
              break;
            }
          }
        }
      }
      if (check_ok) {
        delete root_table_for_build_;
        root_table_for_build_ = NULL;
        delete tablet_manager_for_build_;
        tablet_manager_for_build_ = NULL;
        tbsys::CWLockGuard guard(root_table_rwlock_);
        delete root_table_for_query_;
        root_table_for_query_ = root_table_tmp_;
        TBSYS_LOG(INFO, "new root table created, root_table_for_query=%p", root_table_for_query_);
        root_table_tmp_ = NULL;
        delete tablet_manager_for_query_;
        tablet_manager_for_query_ = tablet_manager_tmp_;
        tablet_manager_tmp_ = NULL;
        finish_init = true;
      }
    }
    if (tablet_manager_tmp_ != NULL) {
      delete tablet_manager_tmp_;
      tablet_manager_tmp_ = NULL;
    }
    if (root_table_tmp_ != NULL) {
      delete root_table_tmp_;
      root_table_tmp_ = NULL;
    }
    sleep(wait_init_time_ / (1000L * 1000L));
  }//end while
  if (root_table_for_build_ != NULL) {
    delete root_table_for_build_;
    root_table_for_build_ = NULL;
  }
  if (tablet_manager_for_build_ != NULL) {
    delete tablet_manager_for_build_;
    tablet_manager_for_build_ = NULL;
  }
  {
    tbsys::CWLockGuard guard(server_manager_rwlock_);
    ObChunkServerManager::iterator it = server_manager_.begin();
    for (; it != server_manager_.end(); ++it) {
      if (it->status_ == ObServerStatus::STATUS_REPORTED || it->status_ == ObServerStatus::STATUS_REPORTING) {
        it->status_ = ObServerStatus::STATUS_SERVING;
      }
    }
  }

  build_sync_flag_ = BUILD_SYNC_INIT_OK;
  {
    tbsys::CThreadGuard guard(&(status_mutex_));
    if (server_status_ == STATUS_CHANGING) {
      server_status_ = STATUS_SLEEP;
      TBSYS_LOG(INFO, "server_status_ = %d", server_status_);
    }
  }

  if (res == OB_SUCCESS && finish_init && is_master()) {
    TBSYS_LOG(INFO, "before do check point, res: %d", res);
    tbsys::CRLockGuard rt_guard(root_table_rwlock_);
    tbsys::CRLockGuard cs_guard(server_manager_rwlock_);
    tbsys::CThreadGuard st_guard(&status_mutex_);
    tbsys::CThreadGuard log_guard(worker_->get_log_manager()->get_log_sync_mutex());
    int ret = worker_->get_log_manager()->do_check_point();
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "do check point after init root table failed, err: %d", ret);
    } else {
      TBSYS_LOG(INFO, "do check point after init root table done");
    }
  }
  TBSYS_LOG(INFO, "[NOTICE] init_root_table_by_report finished, res=%d", res);
  return res;
}

/*
 * chunk serve和merege server注册
 * @param out status 0 do not start report 1 start report
 */
int NameServer::regist_server(const ObServer& server, bool is_merge, int32_t& status, int64_t time_stamp) {
  int ret = OB_NOT_INIT;
  if (server_status_ != STATUS_INIT) { // not in init
    ret = OB_SUCCESS;
    if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO) {
      char server_char[OB_IP_STR_BUFF];
      server.to_string(server_char, OB_IP_STR_BUFF);
      TBSYS_LOG(INFO, "regist server %s is_merge %d", server_char, is_merge);
    }
    if (is_master()) {
      if (time_stamp < 0)
        time_stamp = tbsys::CTimeUtil::getTime();

      if (is_merge) {
        ret = log_worker_->regist_ms(server, time_stamp);
      } else {
        ret = log_worker_->regist_cs(server, time_stamp);
      }
    }

    if (ret == OB_SUCCESS) {
      {
        time_stamp = tbsys::CTimeUtil::getMonotonicTime();
        int rc = 0;
        tbsys::CWLockGuard guard(server_manager_rwlock_);
        rc = server_manager_.receive_hb(server, time_stamp, is_merge, true);
      }
      first_cs_had_registed_ = true;
      //now we always want cs report its tablet
      status = START_REPORTING;
      if (!is_merge && START_REPORTING == status) {
        tbsys::CThreadGuard mutex_guard(&root_table_build_mutex_); //this for only one thread modify root_table
        tbsys::CWLockGuard root_table_guard(root_table_rwlock_);
        tbsys::CWLockGuard server_info_guard(server_manager_rwlock_);
        ObChunkServerManager::iterator it;
        it = server_manager_.find_by_ip(server);
        if (it != server_manager_.end()) {
          {
            //remove this server's tablet
            if (root_table_for_query_ != NULL) {
              root_table_for_query_->server_off_line(it - server_manager_.begin(), 0);
            }
          }
          if (it->status_ == ObServerStatus::STATUS_SERVING || it->status_ == ObServerStatus::STATUS_WAITING_REPORT) {
            // chunk server will start report
            it->status_ = ObServerStatus::STATUS_REPORTING;
          }
        }
      }

    }
    TBSYS_LOG(INFO, "regist ret %d", ret);
  }
  return ret;
}

/*
 * chunk server更新自己的磁盘情况信息
 */
int NameServer::update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used) {
  int ret = OB_SUCCESS;
  if (is_master()) {
    ret = log_worker_->report_cs_load(server, capacity, used);
  }

  if (ret == OB_SUCCESS) {
    ObServerDiskInfo disk_info;
    disk_info.set_capacity(capacity);
    disk_info.set_used(used);
    tbsys::CWLockGuard guard(server_manager_rwlock_);
    ret = server_manager_.update_disk_info(server, disk_info);
  }

  if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO) {
    char server_str[OB_IP_STR_BUFF];
    server.to_string(server_str, OB_IP_STR_BUFF);
    TBSYS_LOG(INFO, "server %s update capacity info capacity is %ld, used is %ld ret is %d", server_str, capacity, used, ret);
  }

  return ret;
}

/*
 * 迁移完成操作
 */
int NameServer::migrate_over(const ObRange& range, const common::ObServer& src_server, const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version) {
  int ret = OB_SUCCESS;
  if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO) {
    static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
    range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
    char f_server[OB_IP_STR_BUFF];
    char t_server[OB_IP_STR_BUFF];
    src_server.to_string(f_server, OB_IP_STR_BUFF);
    dest_server.to_string(t_server, OB_IP_STR_BUFF);
    TBSYS_LOG(INFO, "migrate_over received, src_cs=%s dest_cs=%s keep_src=%d range=%s",
              f_server, t_server, keep_src, row_key_dump_buff);
  }

  int src_server_index = get_server_index(src_server);
  int dest_server_index = get_server_index(dest_server);

  NameTable::const_iterator start_it;
  NameTable::const_iterator end_it;
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  tbsys::CWLockGuard guard(root_table_rwlock_);
  int find_ret = root_table_for_query_->find_range(range, start_it, end_it);
  if (OB_SUCCESS == find_ret && start_it == end_it) {
    start_it->migrate_monotonic_time_ = 0;
    if (keep_src) {
      if (OB_INVALID_INDEX == dest_server_index) {
        // dest cs is down
        TBSYS_LOG(WARN, "can not find cs, src=%d dest=%d", src_server_index, dest_server_index);
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        // add replica
        ret = root_table_for_query_->modify(start_it, dest_server_index, tablet_version);
      }
    } else {
      // dest_server_index and src_server_index may be INVALID
      ret = root_table_for_query_->replace(start_it, src_server_index, dest_server_index, tablet_version);
    }
    if (OB_SUCCESS == find_ret) {
      ObServerStatus* src_status = server_manager_.get_server_status(src_server_index);
      ObServerStatus* dest_status = server_manager_.get_server_status(dest_server_index);
      const common::ObTabletInfo* tablet_info = NULL;
      tablet_info = ((const NameTable*)root_table_for_query_)->get_tablet_info(start_it);
      if (src_status != NULL && dest_status != NULL && tablet_info != NULL) {
        if (!keep_src) {
          if (OB_INVALID_INDEX != src_server_index) {
            src_status->disk_info_.set_used(src_status->disk_info_.get_used() - tablet_info->occupy_size_);
          }
        }
        if (OB_INVALID_INDEX != dest_server_index) {
          dest_status->disk_info_.set_used(dest_status->disk_info_.get_used() + tablet_info->occupy_size_);
        }
      }
    }
    if (is_master()) {
      ret = log_worker_->cs_migrate_done(range, src_server, dest_server, keep_src, tablet_version);
    }
    if (is_master() || balance_testing_) {
      nb_trigger_next_migrate(range, src_server_index, dest_server_index, keep_src);
    }
  } else {
    TBSYS_LOG(INFO, "can not find the right range ignore this");
  }

  return ret;
}

int NameServer::make_out_cell(ObCellInfo& out_cell, NameTable::const_iterator first,
                              NameTable::const_iterator last, ObScanner& scanner, const int32_t max_row_count, const int32_t max_key_len) const {
  static ObString s_root_1_port(strlen(ROOT_1_PORT), strlen(ROOT_1_PORT), (char*)ROOT_1_PORT);
  static ObString s_root_1_ms_port(strlen(ROOT_1_MS_PORT), strlen(ROOT_1_MS_PORT), (char*)ROOT_1_MS_PORT);
  static ObString s_root_1_ipv6_1(strlen(ROOT_1_IPV6_1), strlen(ROOT_1_IPV6_1), (char*)ROOT_1_IPV6_1);
  static ObString s_root_1_ipv6_2(strlen(ROOT_1_IPV6_2), strlen(ROOT_1_IPV6_2), (char*)ROOT_1_IPV6_2);
  static ObString s_root_1_ipv6_3(strlen(ROOT_1_IPV6_3), strlen(ROOT_1_IPV6_3), (char*)ROOT_1_IPV6_3);
  static ObString s_root_1_ipv6_4(strlen(ROOT_1_IPV6_4), strlen(ROOT_1_IPV6_4), (char*)ROOT_1_IPV6_4);
  static ObString s_root_1_ipv4(strlen(ROOT_1_IPV4), strlen(ROOT_1_IPV4), (char*)ROOT_1_IPV4);
  static ObString s_root_1_tablet_version(strlen(ROOT_1_TABLET_VERSION), strlen(ROOT_1_TABLET_VERSION), (char*)ROOT_1_TABLET_VERSION);

  static ObString s_root_2_port(strlen(ROOT_2_PORT),    strlen(ROOT_2_PORT), (char*)ROOT_2_PORT);
  static ObString s_root_2_ms_port(strlen(ROOT_2_MS_PORT), strlen(ROOT_2_MS_PORT), (char*)ROOT_2_MS_PORT);
  static ObString s_root_2_ipv6_1(strlen(ROOT_2_IPV6_1),  strlen(ROOT_2_IPV6_1), (char*)ROOT_2_IPV6_1);
  static ObString s_root_2_ipv6_2(strlen(ROOT_2_IPV6_2),  strlen(ROOT_2_IPV6_2), (char*)ROOT_2_IPV6_2);
  static ObString s_root_2_ipv6_3(strlen(ROOT_2_IPV6_3),  strlen(ROOT_2_IPV6_3), (char*)ROOT_2_IPV6_3);
  static ObString s_root_2_ipv6_4(strlen(ROOT_2_IPV6_4),  strlen(ROOT_2_IPV6_4), (char*)ROOT_2_IPV6_4);
  static ObString s_root_2_ipv4(strlen(ROOT_2_IPV4),    strlen(ROOT_2_IPV4), (char*)ROOT_2_IPV4);
  static ObString s_root_2_tablet_version(strlen(ROOT_2_TABLET_VERSION), strlen(ROOT_2_TABLET_VERSION), (char*)ROOT_2_TABLET_VERSION);

  static ObString s_root_3_port(strlen(ROOT_3_PORT),    strlen(ROOT_3_PORT), (char*)ROOT_3_PORT);
  static ObString s_root_3_ms_port(strlen(ROOT_3_MS_PORT), strlen(ROOT_3_MS_PORT), (char*)ROOT_3_MS_PORT);
  static ObString s_root_3_ipv6_1(strlen(ROOT_3_IPV6_1),  strlen(ROOT_3_IPV6_1), (char*)ROOT_3_IPV6_1);
  static ObString s_root_3_ipv6_2(strlen(ROOT_3_IPV6_2),  strlen(ROOT_3_IPV6_2), (char*)ROOT_3_IPV6_2);
  static ObString s_root_3_ipv6_3(strlen(ROOT_3_IPV6_3),  strlen(ROOT_3_IPV6_3), (char*)ROOT_3_IPV6_3);
  static ObString s_root_3_ipv6_4(strlen(ROOT_3_IPV6_4),  strlen(ROOT_3_IPV6_4), (char*)ROOT_3_IPV6_4);
  static ObString s_root_3_ipv4(strlen(ROOT_3_IPV4),    strlen(ROOT_3_IPV4), (char*)ROOT_3_IPV4);
  static ObString s_root_3_tablet_version(strlen(ROOT_3_TABLET_VERSION), strlen(ROOT_3_TABLET_VERSION), (char*)ROOT_3_TABLET_VERSION);

  static ObString s_root_occupy_size(strlen(ROOT_OCCUPY_SIZE), strlen(ROOT_OCCUPY_SIZE), (char*)ROOT_OCCUPY_SIZE);
  static ObString s_root_record_count(strlen(ROOT_RECORD_COUNT), strlen(ROOT_RECORD_COUNT), (char*)ROOT_RECORD_COUNT);
  static ObString s_root_crc_sum(strlen(ROOT_CRC_SUM), strlen(ROOT_CRC_SUM), (char*)ROOT_CRC_SUM);
  static char c = 0;

  int ret = OB_SUCCESS;
  if (c == 0) {
    memset(max_row_key, 0xff, OB_MAX_ROW_KEY_LENGTH);
    c = 1;
  }

  const common::ObTabletInfo* tablet_info = NULL;
  int count = 0;
  NameTable::const_iterator it = first;
  for (; it <= last; it++) {
    if (count > max_row_count) break;
    tablet_info = ((const NameTable*)root_table_for_query_)->get_tablet_info(it);
    if (tablet_info == NULL) {
      TBSYS_LOG(ERROR, "you should not reach this bugs");
      break;
    }
    out_cell.row_key_ = tablet_info->range_.end_key_;
    if (tablet_info->range_.border_flag_.is_max_value()) {
      out_cell.row_key_.assign(max_row_key, max_key_len);
    }
    TBSYS_LOG(DEBUG, "add a row key to out cell, length = %d", out_cell.row_key_.length());
    count++;
    //start one row
    out_cell.column_name_ = s_root_occupy_size;
    out_cell.value_.set_int(tablet_info->occupy_size_);
    if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
      break;
    }

    out_cell.column_name_ = s_root_record_count;
    out_cell.value_.set_int(tablet_info->row_count_);
    if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
      break;
    }

    //out_cell.column_name_ = s_root_crc_sum;
    //out_cell.value_.set_int(tablet_info->crc_sum_);
    //if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
    //{
    //  break;
    //}

    const ObServerStatus* server_status = NULL;
    if (it->server_info_indexes_[0] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[0])) != NULL) {
      out_cell.column_name_ = s_root_1_port;
      out_cell.value_.set_int(server_status->port_cs_);

      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4) {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_1_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }

      out_cell.column_name_ = s_root_1_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }

      out_cell.column_name_ = s_root_1_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[0]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }
    } else {
      TBSYS_LOG(INFO, "make_out_cell:%d", it->server_info_indexes_[0]);
    }
    if (it->server_info_indexes_[1] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[1])) != NULL) {
      out_cell.column_name_ = s_root_2_port;
      out_cell.value_.set_int(server_status->port_cs_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4) {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_2_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }

      out_cell.column_name_ = s_root_2_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }

      out_cell.column_name_ = s_root_2_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[1]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }
    }
    if (it->server_info_indexes_[2] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[2])) != NULL) {
      out_cell.column_name_ = s_root_3_port;
      out_cell.value_.set_int(server_status->port_cs_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4) {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_3_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }

      out_cell.column_name_ = s_root_3_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }

      out_cell.column_name_ = s_root_3_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[2]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell))) {
        break;
      }
    }

  }
  return ret;
}

int NameServer::find_root_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner) const {
  int ret = OB_SUCCESS;
  const ObCellInfo* cell = NULL;

  if (NULL == (cell = get_param[0])) {
    TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
    ret = OB_INVALID_ARGUMENT;
  } else if (get_param.get_is_read_consistency() && obi_role_.get_role() != ObiRole::MASTER) {
    TBSYS_LOG(WARN, "we are not a master instance");
    ret = OB_NOT_MASTER;
  } else {
    int8_t rt_type = 0;
    UNUSED(rt_type); // for now we ignore this; OP_RT_TABLE_TYPE or OP_RT_TABLE_INDEX_TYPE

    if (cell->table_id_ != 0 && cell->table_id_ != OB_INVALID_ID) {
      char table_name_buff[OB_MAX_TABLE_NAME_LENGTH];
      ObString table_name(OB_MAX_TABLE_NAME_LENGTH, 0, table_name_buff);
      int32_t max_key_len = 0;
      if (OB_SUCCESS != (ret = get_table_info(cell->table_id_, table_name, max_key_len))) {
        TBSYS_LOG(WARN, "failed to get table name, err=%d table_id=%lu", ret, cell->table_id_);
      } else if (OB_SUCCESS != (ret = find_root_table_key(cell->table_id_, table_name, max_key_len, cell->row_key_, scanner))) {
        TBSYS_LOG(WARN, "failed to get tablet, err=%d table_id=%lu", ret, cell->table_id_);
      }
    } else {
      int32_t max_key_len = 0;
      uint64_t table_id = get_table_info(cell->table_name_, max_key_len);
      if (OB_INVALID_ID == table_id) {
        TBSYS_LOG(WARN, "failed to get table id, err=%d", ret);
      } else if (OB_SUCCESS != (ret = find_root_table_key(table_id, cell->table_name_, max_key_len, cell->row_key_, scanner))) {
        TBSYS_LOG(WARN, "failed to get tablet, err=%d table_id=%lu", ret, cell->table_id_);
      }
    }
  }
  return ret;
}

int NameServer::find_root_table_key(const uint64_t table_id, const ObString& table_name, const int32_t max_key_len, const common::ObString& key, ObScanner& scanner) const {
  int ret = OB_SUCCESS;
  if (table_id == OB_INVALID_ID || 0 == table_id) {
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == ret) {
    ObCellInfo out_cell;
    out_cell.table_name_ = table_name;
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_for_query_ == NULL) {
      ret = OB_NOT_INIT;
    } else {
      NameTable::const_iterator first;
      NameTable::const_iterator last;
      NameTable::const_iterator ptr;
      ret = root_table_for_query_->find_key(table_id, key, RETURN_BACH_COUNT, first, last, ptr);
      TBSYS_LOG(DEBUG, "first %p last %p ptr %p", first, last, ptr);
      if (ret == OB_SUCCESS) {
        if (first == ptr) {
          // make a fake startkey
          out_cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
          ret = scanner.add_cell(out_cell);
          out_cell.value_.reset();
        }
        if (OB_SUCCESS == ret) {
          ret = make_out_cell(out_cell, first, last, scanner, RETURN_BACH_COUNT, max_key_len);
        }
      }
    }
  }
  return ret;
}

int NameServer::find_root_table_range(const common::ObScanParam& scan_param, ObScanner& scanner) const {

  int ret = OB_SUCCESS;
  const ObString& table_name = scan_param.get_table_name();
  const ObRange& key_range = *scan_param.get_range();
  int32_t max_key_len = 0;
  uint64_t table_id = get_table_info(table_name, max_key_len);

  if (0 == table_id || OB_INVALID_ID == table_id) {
    TBSYS_LOG(WARN, "table name are invaild");
    ret = OB_INVALID_ARGUMENT;
  } else if (scan_param.get_is_read_consistency() && obi_role_.get_role() != ObiRole::MASTER) {
    TBSYS_LOG(INFO, "we are not a master instance");
    ret = OB_NOT_MASTER;
  } else {
    ObCellInfo out_cell;
    out_cell.table_name_ = table_name;
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_for_query_ == NULL) {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "scan request in initialize phase");
    } else {
      NameTable::const_iterator first;
      NameTable::const_iterator last;
      ObRange search_range = key_range;
      search_range.table_id_ = table_id;
      ret = root_table_for_query_->find_range(search_range, first, last);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(WARN, "cann't find this range,ret[%d]", ret);
      } else {
        if ((ret = make_out_cell(out_cell, first, last, scanner,
                                 MAX_RETURN_BACH_ROW_COUNT, max_key_len)) != OB_SUCCESS) {
          TBSYS_LOG(WARN, "make out cell failed,ret[%d]", ret);
        }
      }
    }
  }
  return ret;
}

int NameServer::echo_update_server_freeze_mem() {
  int res = OB_ERROR;
  if (update_server_status_.status_ == ObServerStatus::STATUS_SERVING) {
    if (is_master()) {
      log_worker_->us_mem_freezing(update_server_status_.server_, frozen_mem_version_);
    }
    update_server_status_.status_ = ObServerStatus::STATUS_REPORTING;
    res = OB_SUCCESS;
  }
  return res;
}
int NameServer::echo_start_merge_received(const common::ObServer& server) {
  int res = OB_ERROR;
  ObChunkServerManager::iterator it;
  it = server_manager_.find_by_ip(server);
  if (it != server_manager_.end()) {
    if (it->status_ == ObServerStatus::STATUS_SERVING || it->status_ == ObServerStatus::STATUS_WAITING_REPORT) {
      if (is_master()) {
        log_worker_->cs_start_merging(server);
      }

      it->status_ = ObServerStatus::STATUS_REPORTING;
      res = OB_SUCCESS;
    }
  }
  return res;
}

/*
 * 切换过程中, update server冻结内存表 或者chunk server 进行merge等耗时操作完成
 * 发送消息调用此函数
 */
int NameServer::waiting_jsb_done(const common::ObServer& server, const int64_t frozen_mem_version) {
  int res = OB_ERROR;
  UNUSED(frozen_mem_version);
  if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO) {
    char f_server[OB_IP_STR_BUFF];
    server.to_string(f_server, OB_IP_STR_BUFF);
    TBSYS_LOG(INFO, "waiting_jsb_done server is %s update_server_status_.status_ %d",
              f_server, update_server_status_.status_);
  }
  if (update_server_status_.server_ == server) {
    // updateserver
    if (update_server_status_.status_ == ObServerStatus::STATUS_REPORTING) {
      if (is_master()) {
        log_worker_->us_mem_frozen(server, frozen_mem_version);
      }
      update_server_status_.status_ = ObServerStatus::STATUS_REPORTED;
      res = OB_SUCCESS;
    }
  } else {
    // chunkserver
    if (is_master()) {
      log_worker_->cs_merge_over(server, frozen_mem_version);
    }
    ObChunkServerManager::iterator it;
    tbsys::CWLockGuard guard(server_manager_rwlock_);
    it = server_manager_.find_by_ip(server);
    if (it != server_manager_.end()) {
      TBSYS_LOG(INFO, "waiting_jsb_done it->status_ ObServerStatus::STATUS_REPORTING %d %d,froze version:%ld",
                it->status_, ObServerStatus::STATUS_REPORTING, frozen_mem_version_);
      if (it->status_ == ObServerStatus::STATUS_REPORTING) {
        it->status_ = ObServerStatus::STATUS_REPORTED;
        if (frozen_mem_version_ == -1) { // not in merge process
          it->status_ = ObServerStatus::STATUS_SERVING;
        }
      }

    }
    res = OB_SUCCESS;
  }
  return res;
}
int NameServer::echo_unload_received(const common::ObServer& server) {
  int res = OB_ERROR;
  if (update_server_status_.server_ == server) {
    if (update_server_status_.status_ == ObServerStatus::STATUS_REPORTED) {
      if (is_master()) {
        log_worker_->unload_us_done(server);
      }
      update_server_status_.status_ = ObServerStatus::STATUS_SERVING;
      res = OB_SUCCESS;
    }
  } else {
    ObChunkServerManager::iterator it;
    it = server_manager_.find_by_ip(server);
    if (it != server_manager_.end()) {
      if (it->status_ == ObServerStatus::STATUS_REPORTED || it->status_ == ObServerStatus::STATUS_REPORTING) {
        if (is_master()) {
          log_worker_->unload_cs_done(server);
        }
        it->status_ = ObServerStatus::STATUS_SERVING;
        res = OB_SUCCESS;
      }

    }
  }
  return res;
}

int NameServer::get_server_index(const common::ObServer& server) const {
  int ret = OB_INVALID_INDEX;
  ObChunkServerManager::const_iterator it;
  tbsys::CRLockGuard guard(server_manager_rwlock_);
  it = server_manager_.find_by_ip(server);
  if (it != server_manager_.end()) {
    if (ObServerStatus::STATUS_DEAD != it->status_) {
      ret = it - server_manager_.begin();
    }
  }
  return ret;
}
int NameServer::report_tablets(const ObServer& server, const ObTabletReportInfoList& tablets, const int64_t frozen_mem_version) {
  int return_code = OB_SUCCESS;
  char t_server[OB_IP_STR_BUFF];
  server.to_string(t_server, OB_IP_STR_BUFF);
  int server_index = get_server_index(server);
  if (server_index == OB_INVALID_INDEX) {
    TBSYS_LOG(WARN, "can not find server's info, server=%s", t_server);
    return_code = OB_ENTRY_NOT_EXIST;
  } else {
    TBSYS_LOG_US(INFO, "[NOTICE] report tablets, server=%d ip=%s count=%ld version=%ld",
                 server_index, t_server, tablets.tablet_list_.get_array_index(), frozen_mem_version);
    if (is_master()) {
      log_worker_->report_tablets(server, tablets, frozen_mem_version);
    }
    return_code = got_reported(tablets, server_index, frozen_mem_version);
    TBSYS_LOG_US(INFO, "got_reported over");
  }
  return return_code;
}
/*
 * 收到汇报消息后调用
 */
int NameServer::got_reported(const ObTabletReportInfoList& tablets, const int server_index, const int64_t frozen_mem_version) {
  int64_t index = tablets.tablet_list_.get_array_index();
  ObTabletReportInfo* p_table_info = NULL;
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&root_table_build_mutex_);
  if (NULL != root_table_for_build_) {
    TBSYS_LOG(INFO, "will add tablet info to root_table_for_build");
    for (int64_t i = 0; i < index; ++i) {
      p_table_info = tablets.tablet_list_.at(i);
      if (p_table_info != NULL) {
        ret = got_reported_for_build(p_table_info->tablet_info_, server_index,
                                     p_table_info->tablet_location_.tablet_version_);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(WARN, "report_tablets error code is %d", ret);
          break;
        }
      }
    }
  } else {
    TBSYS_LOG(INFO, "will add tablet info to root_table_for_query");
    got_reported_for_query_table(tablets, server_index, frozen_mem_version);
  }
  return ret;
}
/*
 * 处理汇报消息, 直接写到当前的root table中
 * 如果发现汇报消息中有对当前root table的tablet的分裂或者合并
 * 要调用采用写拷贝机制的处理函数
 */
int NameServer::got_reported_for_query_table(const ObTabletReportInfoList& tablets,
                                             const int32_t server_index, const int64_t frozen_mem_version) {
  UNUSED(frozen_mem_version);
  int ret = OB_SUCCESS;
  int64_t have_done_index = 0;
  bool need_split = false;
  bool need_add = false;

  ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
  if (new_server_status == NULL) {
    TBSYS_LOG(ERROR, "can not find server");
  } else {
    ObTabletReportInfo* p_table_info = NULL;
    NameTable::const_iterator first;
    NameTable::const_iterator last;
    int64_t index = tablets.tablet_list_.get_array_index();
    int find_ret = OB_SUCCESS;
    common::ObTabletInfo* tablet_info = NULL;
    int range_pos_type = NameTable::POS_TYPE_ERROR;

    tbsys::CRLockGuard guard(root_table_rwlock_);
    for (have_done_index = 0; have_done_index < index; ++have_done_index) {
      p_table_info = tablets.tablet_list_.at(have_done_index);
      if (p_table_info != NULL) {
        //TODO(maoqi) check the table of this tablet is exist in schema
        //if (NULL == schema_manager_->get_table_schema(p_table_info->tablet_info_.range_.table_id_))
        //{
        //  continue;
        //}
        if (!p_table_info->tablet_info_.range_.border_flag_.is_left_open_right_closed()) {
          static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
          p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
          TBSYS_LOG(WARN, "cs reported illegal tablet, server=%d range=%s", server_index, row_key_dump_buff);
        }
        tablet_info = NULL;
        find_ret = root_table_for_query_->find_range(p_table_info->tablet_info_.range_, first, last);
        TBSYS_LOG(DEBUG, "root_table_for_query_->find_range ret = %d", find_ret);
        if (OB_SUCCESS == find_ret) {
          tablet_info = root_table_for_query_->get_tablet_info(first);
        }
        range_pos_type = NameTable::POS_TYPE_ERROR;
        if (NULL != tablet_info) {
          range_pos_type = root_table_for_query_->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
        }
        TBSYS_LOG(DEBUG, " range_pos_type = %d", range_pos_type);
        if (range_pos_type == NameTable::POS_TYPE_SPLIT_RANGE) {
          need_split = true;  //will create a new table to deal with the left
          break;
        }
        //else if (range_pos_type == NameTable::POS_TYPE_ADD_RANGE)
        //{
        //  need_add = true;
        //  break;
        //}
        if (NULL != tablet_info &&
            (range_pos_type == NameTable::POS_TYPE_SAME_RANGE || range_pos_type == NameTable::POS_TYPE_MERGE_RANGE)
           ) {
          if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                                                         p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_for_query_)) {
            TBSYS_LOG(ERROR, "write new tablet error");
            static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
            p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(INFO, "%s", row_key_dump_buff);
            //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
          }
        } else {
          TBSYS_LOG(INFO, "can not found range ignore this");
          static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
          p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
          TBSYS_LOG(INFO, "%s", row_key_dump_buff);
          //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
        }
      }
    }
  } //end else, release lock
  if (need_split || need_add) {
    ret = got_reported_with_copy(tablets, server_index, have_done_index);
  }
  return ret;
}
/*
 * 写拷贝机制的,处理汇报消息
 */
int NameServer::got_reported_with_copy(const ObTabletReportInfoList& tablets,
                                       const int32_t server_index, const int64_t have_done_index) {
  int ret = OB_SUCCESS;
  common::ObTabletInfo* tablet_info = NULL;
  ObTabletReportInfo* p_table_info = NULL;
  ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
  NameTable::const_iterator first;
  NameTable::const_iterator last;
  TBSYS_LOG(DEBUG, "root table write on copy");
  if (new_server_status == NULL) {
    TBSYS_LOG(ERROR, "can not find server");
    ret = OB_ERROR;
  } else {
    NameTable* root_table_for_split = new NameTable(NULL);
    if (root_table_for_split == NULL) {
      TBSYS_LOG(ERROR, "new NameTable error");
      ret = OB_ERROR;
    } else {
      *root_table_for_split = *root_table_for_query_;
      int range_pos_type = NameTable::POS_TYPE_ERROR;
      for (int64_t index = have_done_index; OB_SUCCESS == ret && index < tablets.tablet_list_.get_array_index(); index++) {
        p_table_info = tablets.tablet_list_.at(index);
        if (p_table_info != NULL) {
          tablet_info = NULL;
          int find_ret = root_table_for_split->find_range(p_table_info->tablet_info_.range_, first, last);
          if (OB_SUCCESS == find_ret) {
            tablet_info = root_table_for_split->get_tablet_info(first);
          }
          range_pos_type = NameTable::POS_TYPE_ERROR;
          if (NULL != tablet_info) {
            range_pos_type = root_table_for_split->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
          }
          if (NULL != tablet_info) {
            if (range_pos_type == NameTable::POS_TYPE_SAME_RANGE || range_pos_type == NameTable::POS_TYPE_MERGE_RANGE) {
              if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                                                             p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_for_split)) {
                TBSYS_LOG(ERROR, "write new tablet error");
                static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              }
            } else if (range_pos_type == NameTable::POS_TYPE_SPLIT_RANGE) {
              if (NameTable::get_max_tablet_version(first) >= p_table_info->tablet_location_.tablet_version_) {
                TBSYS_LOG(ERROR, "same version different range error !! version %ld",
                          p_table_info->tablet_location_.tablet_version_);
                static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              } else {
                ret = root_table_for_split->split_range(p_table_info->tablet_info_, first,
                                                        p_table_info->tablet_location_.tablet_version_, server_index);
                if (OB_SUCCESS != ret) {
                  TBSYS_LOG(ERROR, "split range error, ret = %d", ret);
                  static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                  p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                  TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                  //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                }
              }
            }
            //else if (range_pos_type == NameTable::POS_TYPE_ADD_RANGE)
            //{
            //  ret = root_table_for_split->add_range(p_table_info->tablet_info_, first,
            //      p_table_info->tablet_location_.tablet_version_, server_index);
            //}
            else {
              TBSYS_LOG(INFO, "error range be ignored range_pos_type =%d", range_pos_type);
              static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
              p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(INFO, "%s", row_key_dump_buff);
              //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
            }
          }
        }
      }
    }

    if (OB_SUCCESS == ret && root_table_for_split != NULL) {
      tbsys::CWLockGuard guard(root_table_rwlock_);
      delete root_table_for_query_;
      root_table_for_query_ = root_table_for_split;
      root_table_for_split = NULL;
    } else {
      if (root_table_for_split != NULL) {
        delete root_table_for_split;
        root_table_for_split = NULL;
      }
    }
  }
  return ret;
}

/*
 * 在一个tabelt的各份拷贝中, 寻找合适的备份替换掉
 */
int NameServer::write_new_info_to_root_table(
  const ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index,
  NameTable::const_iterator& first, NameTable::const_iterator& last, NameTable* p_root_table) {
  int ret = OB_SUCCESS;
  int32_t found_it_index = OB_INVALID_INDEX;
  int64_t max_tablet_version = 0;
  ObServerStatus* server_status = NULL;
  ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
  if (new_server_status == NULL) {
    TBSYS_LOG(ERROR, "can not find server");
    ret = OB_ERROR;
  } else {
    for (NameTable::const_iterator it = first; it <= last; it++) {
      ObTabletInfo* p_tablet_write = p_root_table->get_tablet_info(it);
      ObTabletCrcHistoryHelper* crc_helper = p_root_table->get_crc_helper(it);
      if (crc_helper == NULL) {
        TBSYS_LOG(ERROR, "%s", "get src helper error should not reach this bugs!!");
        ret = OB_ERROR;
        break;
      }
      max_tablet_version = NameTable::get_max_tablet_version(it);
      if (tablet_version >= max_tablet_version) {
        if (first != last) {
          TBSYS_LOG(ERROR, "we should not have merge tablet max tabelt is %ld this one is %ld",
                    max_tablet_version, tablet_version);
          ret = OB_ERROR;
          break;
        }
      }
      if (first == last) {
        //check crc sum
        ret = crc_helper->check_and_update(tablet_version, tablet_info.crc_sum_);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "check crc sum error crc is %lu tablet is", tablet_info.crc_sum_);
          static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
          tablet_info.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
          TBSYS_LOG(INFO, "%s", row_key_dump_buff);
          it->dump();
          //tablet_info.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
          break;
        }
      }
      //try to over write dead server or old server;
      found_it_index = NameTable::find_suitable_pos(it, server_index, tablet_version);
      if (found_it_index == OB_INVALID_INDEX) {
        //find the serve who have max free disk
        for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++) {
          server_status = server_manager_.get_server_status(it->server_info_indexes_[i]);
          if (server_status != NULL &&
              new_server_status->disk_info_.get_percent() > 0 &&
              server_status->disk_info_.get_percent() > new_server_status->disk_info_.get_percent()) {
            found_it_index = i;
          }
        }
      }
      if (found_it_index != OB_INVALID_INDEX) {
        TBSYS_LOG(DEBUG, "write a tablet to root_table found_it_index = %d server_index =%d tablet_version = %ld",
                  found_it_index, server_index, tablet_version);

        //tablet_info.range_.hex_dump(TBSYS_LOG_LEVEL_DEBUG);
        static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
        tablet_info.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        TBSYS_LOG(DEBUG, "%s", row_key_dump_buff);
        //over write
        atomic_exchange((uint32_t*) & (it->server_info_indexes_[found_it_index]), server_index);
        atomic_exchange((uint64_t*) & (it->tablet_version_[found_it_index]), tablet_version);
        if (p_tablet_write != NULL) {
          atomic_exchange((uint64_t*) & (p_tablet_write->row_count_), tablet_info.row_count_);
          atomic_exchange((uint64_t*) & (p_tablet_write->occupy_size_), tablet_info.occupy_size_);
        }
      }
    }
  }
  return ret;
}


/**
 * @brief check the version of tablets
 *
 * @return
 */
bool NameServer::all_tablet_is_the_last_frozen_version() const {
  NameTable::iterator it;
  bool ret = true;
  int32_t new_copy = 0;

  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    for (it = root_table_for_query_->begin(); it < root_table_for_query_->sorted_end(); ++it) {
      new_copy = 0;

      for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++) {
        if (OB_INVALID_INDEX == it->server_info_indexes_[i]) { //the server is down
        } else if (it->tablet_version_[i] >= last_frozen_mem_version_) {
          ++new_copy;
        }
      }

      if (new_copy < safe_copy_count_in_init_) {
        ret = false;
        break;
      }
    } //end all tablet
  } //lock

  return ret;
}


/*
 * 系统初始化的时候, 处理汇报消息,
 * 信息放到root table for build 结构中
 */
int NameServer::got_reported_for_build(const ObTabletInfo& tablet, const int32_t server_index, const int64_t version) {
  int ret = OB_SUCCESS;
  if (root_table_for_build_ == NULL) {
    TBSYS_LOG(ERROR, "no root_table_for_build_ ignore this");
    ret = OB_ERROR;
  } else {
    static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
    tablet.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
    if (!tablet.range_.border_flag_.is_left_open_right_closed()) {
      TBSYS_LOG(WARN, "cs reported illegal tablet, server=%d crc=%lu range=%s", server_index, tablet.crc_sum_, row_key_dump_buff);
    }
    TBSYS_LOG(DEBUG, "add a tablet, server=%d crc=%lu version=%ld range=%s", server_index, tablet.crc_sum_, version, row_key_dump_buff);
    //TODO(maoqi) check the table of this tablet is exist in schema
    //if (schema_manager_->get_table_schema(tablet.range_.table_id_) != NULL)
    //{
    ret = root_table_for_build_->add(tablet, server_index, version);
    //}
  }
  return ret;
}

// the array size of server_index should be larger than expected_num
void NameServer::get_available_servers_for_new_table(int* server_index, int32_t expected_num, int32_t& results_num) {
  //tbsys::CThreadGuard guard(&server_manager_mutex_);
  results_num = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end() && results_num < expected_num; ++it) {
    if (it->status_ != ObServerStatus::STATUS_DEAD) {
      server_index[results_num] = it - server_manager_.begin();
      results_num++;
    }
  }
}

void NameServer::use_new_schema() {
  int ret = OB_SUCCESS;
  int64_t schema_timestamp = tbsys::CTimeUtil::getTime();
  if (!is_master()) {
    TBSYS_LOG(WARN, "cannot switch schema as a slave");
  } else if (OB_SUCCESS != (ret = switch_schema(schema_timestamp))) {
    TBSYS_LOG(ERROR, "failed to load and switch new schema, err=%d", ret);
  } else {
    ret = log_worker_->sync_schema(schema_timestamp);
    if (OB_SUCCESS == ret) {
      TBSYS_LOG(INFO, "sync schema succ, ts=%ld", schema_timestamp);
    } else {
      TBSYS_LOG(ERROR, "sync schema error, err=%d", ret);
    }
    this->create_new_table();
    ret = worker_->get_rpc_stub().switch_schema(update_server_status_.server_, *schema_manager_, worker_->get_rpc_timeout());
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "up_switch_schema error, ret=%d schema_manager_=%p, "
                "this may be caused by OBI state, switch schema in slave cluster "
                "will not affect Updateserver schema",
                ret, schema_manager_);
    }
  }
}

void NameServer::create_new_table() {
  if (is_master()) {
    create_new_table(schema_manager_);
  } else {
    new_table_created_ = false;
    build_sync_flag_ = BUILD_SYNC_FLAG_CAN_ACCEPT_NEW_TABLE;
    while (!is_master() && !new_table_created_) {
      usleep(SLAVE_SLEEP_TIME);
    }
    //if (!new_table_created_)
    //{
    //  create_new_table(schema_manager_);
    //}
    new_table_created_ = false;
  }
}
int NameServer::slave_create_new_table(const common::ObTabletInfo& tablet, const int32_t* t_server_index, const int32_t replicas_num, const int64_t mem_version) {
  int ret = OB_SUCCESS;
  if (NULL != root_table_for_build_) {
    tbsys::CThreadGuard guard_build(&root_table_build_mutex_);
    TBSYS_LOG(INFO, "replay log create_new_table and we are initializing");
    ret = root_table_for_build_->create_table(tablet, t_server_index, replicas_num, mem_version);
  } else {
    NameTable* root_table_for_create = NULL;
    root_table_for_create = new(std::nothrow) NameTable(NULL);
    if (root_table_for_create == NULL) {
      TBSYS_LOG(ERROR, "new NameTable error");
      ret = OB_ERROR;
    }
    if (NULL != root_table_for_create) {
      *root_table_for_create = *root_table_for_query_;
    }
    if (OB_SUCCESS == ret) {
      root_table_for_create->create_table(tablet, t_server_index, replicas_num, mem_version);
      tbsys::CWLockGuard guard(root_table_rwlock_);
      TBSYS_LOG(INFO, "delete old query table, addr=%p", root_table_for_query_);
      delete root_table_for_query_;
      root_table_for_query_ = root_table_for_create;
      TBSYS_LOG(INFO, "new root table created, root_table_for_query=%p", root_table_for_query_);
      root_table_for_create = NULL;
    }
    if (root_table_for_create != NULL) {
      delete root_table_for_create;
    }
  }
  return ret;
}

void NameServer::create_new_table_in_init(common::ObSchemaManagerV2* schema, NameTable* root_table_tmp) {
  int32_t server_index[OB_SAFE_COPY_COUNT];
  ObTabletInfo tablet;
  tablet.range_.border_flag_.set_inclusive_end();
  tablet.range_.border_flag_.set_min_value();
  tablet.range_.border_flag_.set_max_value();
  ObServerStatus* server_status = NULL;
  ObServer server;
  int created_count = 0;
  int32_t t_server_index[OB_SAFE_COPY_COUNT];
  NameTable* root_table_for_create = root_table_tmp;
  int ret = OB_SUCCESS;

  if (ObiRole::INIT == obi_role_.get_role()) {
    TBSYS_LOG(WARN, "cannot create new table when obi_role=INIT");
    ret = OB_NOT_INIT;
  }

  if (OB_SUCCESS == ret && root_table_for_create != NULL) {
    int64_t mem_froze_version = 0;
    int retry_time = 0;
    while (OB_SUCCESS != (ret = worker_->get_rpc_stub().get_last_frozen_version(update_server_status_.server_, worker_->get_rpc_timeout(), mem_froze_version))) {
      retry_time++;
      if (retry_time >= 3) break;
      sleep(WAIT_SECONDS * retry_time);
    }

    if (OB_SUCCESS == ret && 0 <= mem_froze_version) {
      for (const ObTableSchema* it = schema->table_begin(); it != schema->table_end(); ++it) {
        if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table()) {
          if (!root_table_for_create->table_is_exist(it->get_table_id())) {
            tablet.range_.table_id_ = it->get_table_id();
            for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
              server_index[i] = OB_INVALID_INDEX;
              t_server_index[i] = OB_INVALID_INDEX;
            }
            int32_t results_num = -1;
            get_available_servers_for_new_table(server_index, tablet_replicas_num_, results_num);
            created_count = 0;
            for (int i = 0; i < results_num; ++i) {
              TBSYS_LOG(INFO, "in create_table_in_init,server_index[i]:%d", server_index[i]);
              if (server_index[i] != OB_INVALID_INDEX) {
                server_status = server_manager_.get_server_status(server_index[i]);
                if (server_status != NULL) {
                  server = server_status->server_;
                  server.set_port(server_status->port_cs_);
                  if (OB_SUCCESS == worker_->get_rpc_stub().create_tablet(server, tablet.range_, mem_froze_version, worker_->get_rpc_timeout())) {
                    t_server_index[created_count] = server_index[i];
                    created_count++;
                    TBSYS_LOG(INFO, "create tablet replica, table_id=%lu server=%d version=%ld",
                              tablet.range_.table_id_, server_index[i], mem_froze_version);
                  }
                } else {
                  TBSYS_LOG(WARN, "get server status failed");
                  server_index[i] = OB_INVALID_INDEX;
                }
              }
            } // end for
            if (created_count > 0) {
              ret = root_table_for_create->create_table(tablet, t_server_index, created_count, mem_froze_version);
              if (is_master()) {
                log_worker_->add_new_tablet(created_count, tablet, t_server_index, mem_froze_version);
              }
            }
          }
        }
      } // end for
    } else {
      TBSYS_LOG(ERROR, "get ups_get_last_frozen_memtable_version error, err=%d frozen_version=%ld", ret, mem_froze_version);
    }
  }
  if (is_master()) {
    log_worker_->create_table_done();
  }
}

void NameServer::create_new_table(common::ObSchemaManagerV2* schema) {
  int32_t server_index[OB_SAFE_COPY_COUNT];
  ObTabletInfo tablet;
  tablet.range_.border_flag_.set_inclusive_end();
  tablet.range_.border_flag_.set_min_value();
  tablet.range_.border_flag_.set_max_value();
  ObServerStatus* server_status = NULL;
  ObServer server;
  int created_count = 0;
  int32_t t_server_index[OB_SAFE_COPY_COUNT];
  NameTable* root_table_for_create = NULL;
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&root_table_build_mutex_);
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_for_query_ != NULL) {
      for (const ObTableSchema* it = schema->table_begin(); it != schema->table_end(); ++it) {
        if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table()) {
          if (!root_table_for_query_->table_is_exist(it->get_table_id())) {
            root_table_for_create = new(std::nothrow) NameTable(NULL);
            if (root_table_for_create == NULL) {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "new NameTable error");
            }
            break;
          }
        }
      }
      if (NULL != root_table_for_create) {
        *root_table_for_create = *root_table_for_query_;
      }
    }
  }
  if (root_table_for_create != NULL) {
    int64_t mem_froze_version = 0;
    int retry_time = 0;
    while (OB_SUCCESS != (ret = worker_->get_rpc_stub().get_last_frozen_version(update_server_status_.server_, worker_->get_rpc_timeout(), mem_froze_version))) {
      retry_time++;
      if (retry_time >= 3) break;
      sleep(WAIT_SECONDS);
    }
    if (OB_SUCCESS == ret && 0 <= mem_froze_version) {
      for (const ObTableSchema* it = schema->table_begin(); it != schema->table_end(); ++it) {
        if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table()) {
          if (!root_table_for_create->table_is_exist(it->get_table_id())) {
            tablet.range_.table_id_ = it->get_table_id();
            for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
              server_index[i] = OB_INVALID_INDEX;
              t_server_index[i] = OB_INVALID_INDEX;
            }
            int32_t results_num = -1;
            get_available_servers_for_new_table(server_index, tablet_replicas_num_, results_num);
            created_count = 0;
            for (int i = 0; i < results_num; ++i) {
              if (server_index[i] != OB_INVALID_INDEX) {
                server_status = server_manager_.get_server_status(server_index[i]);
                if (server_status != NULL) {
                  server = server_status->server_;
                  server.set_port(server_status->port_cs_);
                  if (OB_SUCCESS == worker_->get_rpc_stub().create_tablet(server, tablet.range_, mem_froze_version, worker_->get_rpc_timeout())) {
                    t_server_index[created_count] = server_index[i];
                    created_count++;
                    TBSYS_LOG(INFO, "create tablet replica, table_id=%lu server=%d version=%ld",
                              tablet.range_.table_id_, server_index[i], mem_froze_version);
                  }
                } else {
                  server_index[i] = OB_INVALID_INDEX;
                }
              }
            } // end for
            if (created_count > 0) {
              ret = root_table_for_create->create_table(tablet, t_server_index, created_count, mem_froze_version);
              if (is_master()) {
                log_worker_->add_new_tablet(created_count, tablet, t_server_index, mem_froze_version);
              }
            }
          } else {
            TBSYS_LOG(WARN, "table already exist, table_id=%ld", it->get_table_id());
          }
        }
      } // end for
      tbsys::CWLockGuard guard(root_table_rwlock_);
      delete root_table_for_query_;
      root_table_for_query_ = root_table_for_create;
      root_table_for_create = NULL;
    } else {
      TBSYS_LOG(ERROR, "get ups_get_last_frozen_memtable_version error, err=%d frozen_version=%ld", ret, mem_froze_version);
    }
  }
  if (is_master()) {
    log_worker_->create_table_done();
  }
}

common::ObServer NameServer::get_update_server_info() const {
  return update_server_status_.server_;
}

int32_t NameServer::get_update_server_inner_port() const {
  return ups_inner_port_;
}

int64_t NameServer::get_merge_delay_interval() const {
  return cs_merge_command_interval_mseconds_ * server_manager_.get_array_length();
}

uint64_t NameServer::get_table_info(const common::ObString& table_name, int32_t& max_row_key_length) const {
  uint64_t table_id = OB_INVALID_ID;
  max_row_key_length = 0;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (schema_manager_ != NULL) {
    const ObTableSchema* table_schema = schema_manager_->get_table_schema(table_name);
    if (table_schema != NULL) {
      table_id = table_schema->get_table_id();
      max_row_key_length = table_schema->get_rowkey_max_length();
    }
  }
  return table_id;
}
int NameServer::get_table_info(const uint64_t table_id, common::ObString& table_name, int32_t& max_row_key_length) const {
  int ret = OB_ERROR;
  max_row_key_length = 0;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (schema_manager_ != NULL) {
    if (table_id > 0 && table_id != OB_INVALID_ID) {
      const ObTableSchema* table_schema = schema_manager_->get_table_schema(table_id);
      if (table_schema != NULL) {
        max_row_key_length = table_schema->get_rowkey_max_length();
        int table_name_len = strlen(table_schema->get_table_name());
        if (table_name_len == table_name.write(table_schema->get_table_name(), table_name_len)) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int NameServer::do_check_point(const uint64_t ckpt_id) {
  int ret = OB_SUCCESS;

  const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
  char filename[OB_MAX_FILE_NAME_LENGTH];

  int err = 0;
  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH) {
    TBSYS_LOG(ERROR, "generate root table file name [%s] failed, error: %s", filename, strerror(errno));
    ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    ret = root_table_for_query_->write_to_file(filename);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "write root table to file [%s] failed, err=%d", filename, ret);
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH) {
    TBSYS_LOG(ERROR, "generate chunk server list file name [%s] failed, error: %s", filename, strerror(errno));
    ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    ret = server_manager_.write_to_file(filename);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "write chunkserver list to file [%s] failed, err=%d", filename, ret);
    }
  }

  return ret;
}

int NameServer::recover_from_check_point(const int server_status, const uint64_t ckpt_id) {
  int ret = OB_SUCCESS;

  server_status_ = server_status;
  TBSYS_LOG(INFO, "server status recover from check point is %d", server_status_);

  const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
  char filename[OB_MAX_FILE_NAME_LENGTH];

  int err = 0;
  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH) {
    TBSYS_LOG(ERROR, "generate root table file name [%s] failed, error: %s", filename, strerror(errno));
    ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    ret = root_table_for_query_->read_from_file(filename);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "recover root table from file [%s] failed, err=%d", filename, ret);
    } else {
      TBSYS_LOG(INFO, "recover root table, size=%ld", root_table_for_query_->end() - root_table_for_query_->begin());
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH) {
    TBSYS_LOG(ERROR, "generate chunk server list file name [%s] failed, error: %s", filename, strerror(errno));
    ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    int32_t cs_num = 0;
    int32_t ms_num = 0;

    ret = server_manager_.read_from_file(filename, cs_num, ms_num);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "recover chunkserver list from file [%s] failed, err=%d", filename, ret);
    } else {
      TBSYS_LOG(INFO, "recover server list, cs_num=%d ms_num=%d", cs_num, ms_num);
    }
    if (0 < cs_num) {
      first_cs_had_registed_ = true;
    }
  }

  TBSYS_LOG(INFO, "recover finished with ret: %d, ckpt_id: %ld", ret, ckpt_id);

  return ret;
}

int NameServer::report_frozen_memtable(const int64_t frozen_version, bool did_replay) {
  int ret = OB_SUCCESS;

  if (frozen_version < 0 || frozen_version <= last_frozen_mem_version_) {
    TBSYS_LOG(WARN, "invalid froze_version, version=%ld last_frozen_version=%ld",
              frozen_version, last_frozen_mem_version_);
    ret = OB_ERROR;
  } else {
    if (!did_replay && is_master() && (!all_tablet_is_the_last_frozen_version())) {
      TBSYS_LOG(WARN, "merge is too slow, last_version=%ld curr_version=%ld",
                last_frozen_mem_version_, frozen_version); //just warn
    }
  }

  if (OB_SUCCESS == ret) {
    pre_frozen_mem_version_ = last_frozen_mem_version_;
    last_frozen_mem_version_ = frozen_version;
    last_frozen_time_ = did_replay ? 0 : tbsys::CTimeUtil::getMonotonicTime();
    TBSYS_LOG(INFO, "frozen_version=%ld last_frozen_time=%ld did_replay=%d",
              last_frozen_mem_version_, last_frozen_time_, did_replay);
  }

  if (OB_SUCCESS == ret) {
    if (is_master() && !did_replay) {
      log_worker_->sync_us_frozen_version(last_frozen_mem_version_);
    }
  }
  return ret;
}

int NameServer::get_server_status() const {
  return server_status_;
}
int64_t NameServer::get_time_stamp_changing() const {
  return time_stamp_changing_;
}
int64_t NameServer::get_lease() const {
  return lease_duration_;
}

void NameServer::wait_init_finished() {
  static const unsigned long sleep_us = 200 * 1000;
  while (true) {
    {
      tbsys::CThreadGuard guard(&(status_mutex_));
      if (STATUS_INIT != server_status_) {
        break;
      }
    }
    usleep(sleep_us);           // sleep 200ms
  }
  TBSYS_LOG(INFO, "nameserver2 init finished");
}

const ObiRole& NameServer::get_obi_role() const {
  return obi_role_;
}

int NameServer::set_obi_role(const ObiRole& role) {
  int ret = OB_SUCCESS;
  // send request to the updateserver
  if (ObiRole::MASTER != role.get_role()
      && ObiRole::SLAVE != role.get_role()) {
    TBSYS_LOG(WARN, "invalid obi role, role=%d", role.get_role());
    ret = OB_INVALID_ARGUMENT;
  } else if (obi_role_ == role) {
    TBSYS_LOG(WARN, "obi role already is %s", role.get_role_str());
    ret = OB_INIT_TWICE;
  } else {
    if (is_master()) {
      if (OB_SUCCESS != (ret = worker_->get_rpc_stub().set_obi_role(update_server_status_.server_, role, worker_->get_rpc_timeout()))) {
        TBSYS_LOG(INFO, "failed to set updateserver's obi role, err=%d", ret);
      }
    }
    if (OB_SUCCESS == ret) {
      obi_role_.set_role(role.get_role());
    }
  }
  TBSYS_LOG(INFO, "set obi role, role=%s ret=%d", role.get_role() == ObiRole::MASTER ? "MASTER" : "SLAVE", ret);
  return ret;
}

int NameServer::get_obi_config(common::ObiConfig& obi_config) const {
  int ret = OB_ENTRY_NOT_EXIST;
  for (int i = 0; i < client_config_.obi_list_.obi_count_; ++i) {
    if (client_config_.obi_list_.conf_array_[i].get_rs_addr() == my_addr_) {
      obi_config = client_config_.obi_list_.conf_array_[i];
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int NameServer::set_obi_config(const common::ObiConfig& conf) {
  return set_obi_config(my_addr_, conf);
}

int NameServer::set_obi_config(const common::ObServer& rs_addr, const common::ObiConfig& conf) {
  int ret = OB_SUCCESS;
  if (0 > conf.get_read_percentage() || 100 < conf.get_read_percentage()) {
    TBSYS_LOG(WARN, "invalid param, read_percentage=%d", conf.get_read_percentage());
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    for (int i = 0; i < client_config_.obi_list_.obi_count_; ++i) {
      if (client_config_.obi_list_.conf_array_[i].get_rs_addr() == rs_addr) {
        client_config_.obi_list_.conf_array_[i].set_read_percentage(conf.get_read_percentage());
        client_config_.obi_list_.print();
        if (is_master()) {
          if (OB_SUCCESS != (ret = log_worker_->set_client_config(client_config_))) {
            TBSYS_LOG(ERROR, "write log error, err=%d", ret);
          }
        }
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

void NameServer::do_stat_common(char* buf, const int64_t buf_len, int64_t& pos) {
  do_stat_start_time(buf, buf_len, pos);
  do_stat_local_time(buf, buf_len, pos);
  databuff_printf(buf, buf_len, pos, "prog_version: %s(%s)\n", PACKAGE_STRING, RELEASEID);
  databuff_printf(buf, buf_len, pos, "pid: %d\n", getpid());
  databuff_printf(buf, buf_len, pos, "obi_role: %s\n", obi_role_.get_role_str());
}

void NameServer::do_stat_start_time(char* buf, const int64_t buf_len, int64_t& pos) {
  databuff_printf(buf, buf_len, pos, "start_time: %s", ctime(&start_time_));
}

void NameServer::do_stat_local_time(char* buf, const int64_t buf_len, int64_t& pos) {
  time_t now = time(NULL);
  databuff_printf(buf, buf_len, pos, "local_time: %s", ctime(&now));
}

void NameServer::do_stat_schema_version(char* buf, const int64_t buf_len, int64_t& pos) {
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  int64_t schema_version = schema_manager_->get_version();
  tbutil::Time schema_time = tbutil::Time::microSeconds(schema_version);
  struct timeval schema_tv(schema_time);
  struct tm stm;
  localtime_r(&schema_tv.tv_sec, &stm);
  char time_buf[32];
  strftime(time_buf, sizeof(time_buf), "%F %H:%M:%S", &stm);
  databuff_printf(buf, buf_len, pos, "schema_version: %lu(%s)", schema_version, time_buf);
}

void NameServer::do_stat_frozen_time(char* buf, const int64_t buf_len, int64_t& pos) {
  tbutil::Time frozen_time = tbutil::Time::microSeconds(last_frozen_time_);
  struct timeval frozen_tv(frozen_time);
  struct tm stm;
  localtime_r(&frozen_tv.tv_sec, &stm);
  char time_buf[32];
  strftime(time_buf, sizeof(time_buf), "%F %H:%M:%S", &stm);
  databuff_printf(buf, buf_len, pos, "frozen_time: %lu(%s)", last_frozen_time_, time_buf);
}

void NameServer::do_stat_mem(char* buf, const int64_t buf_len, int64_t& pos) {
  struct mallinfo minfo = mallinfo();
  databuff_printf(buf, buf_len, pos, "mem: arena=%d ordblks=%d hblkhd=%d uordblks=%d fordblks=%d keepcost=%d",
                  minfo.arena, minfo.ordblks, minfo.hblkhd, minfo.uordblks, minfo.fordblks, minfo.keepcost);
}

void NameServer::do_stat_table_num(char* buf, const int64_t buf_len, int64_t& pos) {
  int num = -1;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL != schema_manager_) {
    num = schema_manager_->table_end() - schema_manager_->table_begin();
  }
  databuff_printf(buf, buf_len, pos, "table_num: %d", num);
}

void NameServer::do_stat_tablet_num(char* buf, const int64_t buf_len, int64_t& pos) {
  int num = -1;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != tablet_manager_for_query_) {
    num = tablet_manager_for_query_->end() - tablet_manager_for_query_->begin();
  }
  databuff_printf(buf, buf_len, pos, "tablet_num: %d", num);
}

void NameServer::do_stat_cs(char* buf, const int64_t buf_len, int64_t& pos) {
  char server_str[OB_IP_STR_BUFF];
  ObServer tmp_server;
  databuff_printf(buf, buf_len, pos, "chunkservers: ");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it) {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD) {
      tmp_server = it->server_;
      tmp_server.set_port(it->port_cs_);
      tmp_server.to_string(server_str, OB_IP_STR_BUFF);
      databuff_printf(buf, buf_len, pos, "%s ", server_str);
    }
  }
}

void NameServer::do_stat_ms(char* buf, const int64_t buf_len, int64_t& pos) {
  char server_str[OB_IP_STR_BUFF];
  ObServer tmp_server;
  databuff_printf(buf, buf_len, pos, "mergeservers: ");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it) {
    if (it->port_ms_ != 0
        && it->ms_status_ != ObServerStatus::STATUS_DEAD) {
      tmp_server = it->server_;
      tmp_server.set_port(it->port_ms_);
      tmp_server.to_string(server_str, OB_IP_STR_BUFF);
      databuff_printf(buf, buf_len, pos, "%s ", server_str);
    }
  }
}

void NameServer::do_stat_ups(char* buf, const int64_t buf_len, int64_t& pos) {
  char server_str[OB_IP_STR_BUFF];
  update_server_status_.server_.to_string(server_str, OB_IP_STR_BUFF);
  databuff_printf(buf, buf_len, pos, "ups: %s(%d master)|", server_str, ups_inner_port_);
  ups_list_.print(buf, buf_len, pos);
}

void NameServer::do_stat_client_config(char* buf, const int64_t buf_len, int64_t& pos) {
  databuff_printf(buf, buf_len, pos, "client_config:\n");
  client_config_.print(buf, buf_len, pos);
}

int NameServer::do_stat(int stat_key, char* buf, const int64_t buf_len, int64_t& pos) {
  int ret = OB_SUCCESS;
  switch (stat_key) {
  case OB_RS_STAT_COMMON:
    do_stat_common(buf, buf_len, pos);
    break;
  case OB_RS_STAT_START_TIME:
    do_stat_start_time(buf, buf_len, pos);
    break;
  case OB_RS_STAT_LOCAL_TIME:
    do_stat_local_time(buf, buf_len, pos);
    break;
  case OB_RS_STAT_PROGRAM_VERSION:
    databuff_printf(buf, buf_len, pos, "prog_version: %s(%s)", PACKAGE_STRING, RELEASEID);
    break;
  case OB_RS_STAT_PID:
    databuff_printf(buf, buf_len, pos, "pid: %d", getpid());
    break;
  case OB_RS_STAT_MEM:
    do_stat_mem(buf, buf_len, pos);
    break;
  case OB_RS_STAT_RS_STATUS:
    databuff_printf(buf, buf_len, pos, "rs_status: %d", server_status_);
    break;
  case OB_RS_STAT_FROZEN_VERSION:
    databuff_printf(buf, buf_len, pos, "frozen_version: %d", last_frozen_mem_version_);
    break;
  case OB_RS_STAT_SCHEMA_VERSION:
    do_stat_schema_version(buf, buf_len, pos);
    break;
  case OB_RS_STAT_LOG_SEQUENCE:
    databuff_printf(buf, buf_len, pos, "log_seq: %d", log_worker_->get_cur_log_seq());
    break;
  case OB_RS_STAT_LOG_FILE_ID:
    databuff_printf(buf, buf_len, pos, "log_file_id: %d", log_worker_->get_cur_log_file_id());
    break;
  case OB_RS_STAT_TABLE_NUM:
    do_stat_table_num(buf, buf_len, pos);
    break;
  case OB_RS_STAT_TABLET_NUM:
    do_stat_tablet_num(buf, buf_len, pos);
    break;
  case OB_RS_STAT_CS:
    do_stat_cs(buf, buf_len, pos);
    break;
  case OB_RS_STAT_MS:
    do_stat_ms(buf, buf_len, pos);
    break;
  case OB_RS_STAT_UPS:
    do_stat_ups(buf, buf_len, pos);
    break;
  case OB_RS_STAT_FROZEN_TIME:
    do_stat_frozen_time(buf, buf_len, pos);
    break;
  case OB_RS_STAT_CLIENT_CONF:
    do_stat_client_config(buf, buf_len, pos);
    break;
  case OB_RS_STAT_SSTABLE_DIST:
    nb_print_balance_infos(buf, buf_len, pos);
    break;
  case OB_RS_STAT_CS_NUM:
  case OB_RS_STAT_MS_NUM:
  case OB_RS_STAT_RS_SLAVE:
  case OB_RS_STAT_OPS_GET:
  case OB_RS_STAT_OPS_SCAN:
  case OB_RS_STAT_REPLICAS_NUM:
  default:
    databuff_printf(buf, buf_len, pos, "unknown or not implemented yet, stat_key=%d", stat_key);
    break;
  }
  return ret;
}


int NameServer::make_checkpointing() {
  tbsys::CRLockGuard rt_guard(root_table_rwlock_);
  tbsys::CRLockGuard cs_guard(server_manager_rwlock_);
  tbsys::CThreadGuard st_guard(&status_mutex_);
  tbsys::CThreadGuard log_guard(worker_->get_log_manager()->get_log_sync_mutex());
  int ret = worker_->get_log_manager()->do_check_point();
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "failed to make checkpointing, err=%d", ret);
  } else {
    TBSYS_LOG(INFO, "made checkpointing");
  }
  return ret;
}

const common::ObUpsList& NameServer::get_ups_list() const {
  return ups_list_;
}

const common::ObClientConfig& NameServer::get_client_config() const {
  return client_config_;
}

int NameServer::set_ups_config(const common::ObServer& ups, int32_t ms_read_percentage, int32_t cs_read_percentage) {
  int ret = OB_ENTRY_NOT_EXIST;
  if (0 > ms_read_percentage || 100 < ms_read_percentage) {
    TBSYS_LOG(WARN, "invalid param, ms_read_percentage=%d", ms_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  } else if (0 > cs_read_percentage || 100 < cs_read_percentage) {
    TBSYS_LOG(WARN, "invalid param, cs_read_percentage=%d", cs_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int32_t i = 0; i < ups_list_.ups_count_; ++i) {
      if (ups_list_.ups_array_[i].addr_ == ups) {
        ups_list_.ups_array_[i].ms_read_percentage_ = ms_read_percentage;
        ups_list_.ups_array_[i].cs_read_percentage_ = cs_read_percentage;
        char addr_buf[OB_IP_STR_BUFF];
        ups_list_.ups_array_[i].addr_.to_string(addr_buf, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "set ups config, idx=%d addr=%s inner_port=%d ms_read_percentage=%hhd cs_read_percentage=%hhd",
                  i, addr_buf,
                  ups_list_.ups_array_[i].inner_port_,
                  ups_list_.ups_array_[i].ms_read_percentage_,
                  ups_list_.ups_array_[i].cs_read_percentage_);
        ret = OB_SUCCESS;
        if (is_master()) {
          if (OB_SUCCESS != (ret = log_worker_->set_ups_list(ups_list_))) {
            TBSYS_LOG(ERROR, "write log error, err=%d", ret);
          }
        }
        break;
      }
    }
  }
  return ret;
}

int NameServer::set_ups_list(const common::ObUpsList& ups_list) {
  int ret = OB_SUCCESS;
  ups_list_ = ups_list;
  ups_list_.print();
  return ret;
}

int NameServer::set_client_config(const common::ObClientConfig& client_conf) {
  int ret = OB_SUCCESS;
  client_config_ = client_conf;
  client_config_.print();
  return ret;
}

int NameServer::serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const {
  return server_manager_.serialize_cs_list(buf, buf_len, pos);
}

int NameServer::serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const {
  return server_manager_.serialize_ms_list(buf, buf_len, pos);
}


////////////////////////////////////////////////////////////////
NameServer::rootTableModifier::rootTableModifier(NameServer* name_server): name_server_(name_server) {
}

void NameServer::rootTableModifier::run(tbsys::CThread* thread, void* arg) {
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(INFO, "[NOTICE] root table modifier thread start");
  bool init_process = false;
  {
    tbsys::CThreadGuard guard(&(name_server_->status_mutex_));
    if (name_server_->server_status_ == STATUS_INIT) {
      init_process  = true;  //execute init process only the first time the system start up;
    }
  }

  //for now report, switch, and balance are all in this thread, maybe we will use multithread later
  if (init_process) {
    TBSYS_LOG(INFO, "init process");
    if ((OB_SUCCESS != name_server_->init_root_table_by_report()) ||
        (name_server_->build_sync_flag_ != BUILD_SYNC_INIT_OK)) {
      TBSYS_LOG(ERROR, "system init error");
      exit(0);
    }
  } else {
    name_server_->build_sync_flag_ = BUILD_SYNC_INIT_OK;
    TBSYS_LOG(INFO, "don't need init process, server_status_=%d", name_server_->server_status_);
  }
  TBSYS_LOG(INFO, "[NOTICE] start service now");

  while (!_stop) {
    int64_t now = tbsys::CTimeUtil::getMonotonicTime();
    if (name_server_->is_master() && (name_server_->last_frozen_time_ > 0)) {
      if (((name_server_->max_merge_duration_ + name_server_->last_frozen_time_) < now) &&
          !name_server_->all_tablet_is_the_last_frozen_version()) {
        TBSYS_LOG(ERROR, "merge is too slow,start at:%ld,now:%ld,max_merge_duration_:%ld", name_server_->last_frozen_time_,
                  now, name_server_->max_merge_duration_);
      } else if (name_server_->all_tablet_is_the_last_frozen_version()) {
        TBSYS_LOG(INFO, "build new root table ok"); //for qa
        name_server_->last_frozen_time_ = 0;
        // checkpointing after done merge
        name_server_->make_checkpointing();
      }
    }
    sleep(1);
  }
  TBSYS_LOG(INFO, "[NOTICE] root table modifier thread exit");
}

NameServer::balanceWorker::balanceWorker(NameServer* name_server): name_server_(name_server) {
}
void NameServer::balanceWorker::run(tbsys::CThread* thread, void* arg) {
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(INFO, "[NOTICE] balance worker thread start, wait_seconds=%d",
            name_server_->migrate_wait_seconds_);
  //TODO if this is not the first start up we will do sleep
  for (int i = 0; i < name_server_->migrate_wait_seconds_ && !_stop; i++) {
    sleep(1);
  }
  TBSYS_LOG(INFO, "[NOTICE] balance working");
  while (!_stop) {
    if (name_server_->is_master() || name_server_->balance_testing_) {
      if (name_server_->enable_balance_ || name_server_->enable_rereplication_) {
        if (name_server_->nb_is_in_batch_migrating()) {
          name_server_->nb_check_migrate_timeout();
        } else {
          name_server_->do_new_balance();
        }
      }
    } else {
      TBSYS_LOG(DEBUG, "not the master");
    }
    int sleep_ms = name_server_->balance_worker_sleep_us_ / 1000;
    name_server_->balance_worker_sleep_cond_.wait(sleep_ms);
  }
  TBSYS_LOG(INFO, "[NOTICE] balance worker thread exit");
}
NameServer::heartbeatChecker::heartbeatChecker(NameServer* name_server): name_server_(name_server) {
}

bool NameServer::is_master() const {
  ObRoleMgr::Role role = worker_->get_role_manager()->get_role();
  ObRoleMgr::State state = worker_->get_role_manager()->get_state();
  return (role == ObRoleMgr::MASTER) && (state == ObRoleMgr::ACTIVE);
}

int NameServer::receive_hb(const common::ObServer& server, ObRole role) {
  int64_t  now = tbsys::CTimeUtil::getMonotonicTime();
  return server_manager_.receive_hb(server, now, role == OB_MERGESERVER ? true : false);
}

void NameServer::heartbeatChecker::run(tbsys::CThread* thread, void* arg) {
  UNUSED(thread);
  UNUSED(arg);
  ObServer tmp_server;
  int64_t now = 0;
  int64_t preview_rotate_time = 0;
  bool need_report = false;
  bool need_balance = false;
  TBSYS_LOG(INFO, "[NOTICE] heart beat checker thread start");
  while (!_stop) {
    need_report = false;
    need_balance = false;
    now = tbsys::CTimeUtil::getTime();
    if ((now > preview_rotate_time + 10 * 1000 * 1000) &&
        ((now / (1000 * 1000)  - timezone) % (24 * 3600)  == 0)) {
      preview_rotate_time = now;
      TBSYS_LOG(INFO, "rotateLog");
      TBSYS_LOGGER.rotateLog(NULL, NULL);
    }
    int64_t monotonic_now = tbsys::CTimeUtil::getMonotonicTime();
    //server_need_hb.init(will_heart_beat, ObChunkServerManager::MAX_SERVER_COUNT);
    if (name_server_->is_master()) {
      ObChunkServerManager::iterator it = name_server_->server_manager_.begin();
      char server_str[OB_IP_STR_BUFF];
      for (; it != name_server_->server_manager_.end(); ++it) {
        if (it->status_ != ObServerStatus::STATUS_DEAD) {
          if (it->is_alive(monotonic_now, name_server_->lease_duration_)) {
            if (monotonic_now - it->last_hb_time_ >= (name_server_->lease_duration_ / 2 + it->hb_retry_times_ * HB_RETRY_FACTOR)) {
              it->hb_retry_times_ ++;
              tmp_server = it->server_;
              tmp_server.set_port(it->port_cs_);
              //need hb
              //server_need_hb.push_back(tmp_server);
              if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG) {
                tmp_server.to_string(server_str, OB_IP_STR_BUFF);
                //TBSYS_LOG(DEBUG, "send hb to %s", server_str);
              }
              if (name_server_->worker_->get_rpc_stub().heartbeat_to_cs(tmp_server, name_server_->lease_duration_,
                                                                        name_server_->last_frozen_mem_version_) == OB_SUCCESS) {
                //do nothing
              }
            }
          } else {
            TBSYS_LOG(INFO, "server is down,monotonic_now:%ld,lease_duration:%ld", monotonic_now, name_server_->lease_duration_);
            name_server_->server_manager_.set_server_down(it);
            name_server_->log_worker_->server_is_down(it->server_, now);

            tbsys::CThreadGuard mutex_guard(&(name_server_->root_table_build_mutex_)); //this for only one thread modify root_table
            tbsys::CWLockGuard guard(name_server_->root_table_rwlock_);
            if (name_server_->root_table_for_query_ != NULL) {
              name_server_->root_table_for_query_->server_off_line(it - name_server_->server_manager_.begin(), now);
              // some cs is down, signal the balance worker
              name_server_->balance_worker_sleep_cond_.broadcast();
            } else {
              TBSYS_LOG(ERROR, "root_table_for_query_ = NULL, server_index=%ld",
                        it - name_server_->server_manager_.begin());
            }
          }
        }

        if (it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0) {
          if (it->is_ms_alive(monotonic_now, name_server_->lease_duration_)) {
            if (monotonic_now - it->last_hb_time_ms_ >
                (name_server_->lease_duration_ / 2)) {
              //hb to ms
              tmp_server = it->server_;
              tmp_server.set_port(it->port_ms_);
              name_server_->worker_->get_rpc_stub().heartbeat_to_ms(tmp_server, name_server_->lease_duration_,
                                                                    name_server_->get_schema_version(), name_server_->get_obi_role());
            }
          } else {
            name_server_->server_manager_.set_server_down_ms(it);
          }
        }
      } //end for
    } //end if master
    //async heart beat
    usleep(10000);
  }
  TBSYS_LOG(INFO, "[NOTICE] heart beat checker thread exit");
}

}
}

