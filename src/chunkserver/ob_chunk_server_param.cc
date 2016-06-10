/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_chunk_server_param.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     maoqi <maoqi@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *
 */

#include "ob_chunk_server_param.h"
#include "config.h"

namespace {
const char* OBCS_SECTION  = "chunk_server";
const char* OBCS_PORT = "port";
const char* OBCS_DEVNAME = "dev_name";
const char* OBCS_TASK_QUEUE_SIZE = "task_queue_size";
const char* OBCS_TASK_THREAD_COUNT = "task_thread_count";
const char* OBCS_MAX_MIGRATE_TASK_COUNT = "max_migrate_task_count";
const char* OBCS_DATADIR_PATH = "datadir_path";
const char* OBCS_APP_NAME = "application_name";
const char* OBCS_HEARTBEAT_TIMEOUT = "heartbeat_timeout";
const char* OBCS_NETWORK_TIMEOUT = "network_timeout";
const char* OBCS_LEASE_CHECK_INTERVAL = "lease_check_interval";
const char* OBCS_RETRY_TIMES = "retry_times";
const char* OBCS_MAX_TABLETS_NUM = "max_tablets_num";
const char* OBCS_MAX_SSTABLE_SIZE = "max_sstable_size";
const char* OBCS_RSYNC_BAND_LIMIT = "migrate_band_limit_kbytes";
const char* OBCS_MERGE_MEM_LIMIT = "merge_mem_limit";
const char* OBCS_MERGE_THREAD_PER_DISK = "merge_thread_per_disk";
const char* OBCS_MAX_MERGE_THREAD_NUM  = "max_merge_thread_num";
const char* OBCS_RESERVE_SSTABLE_COPY = "reserve_sstable_copy";
const char* OBCS_MERGE_THRESHOLD_LOAD_HIGH = "merge_load_threshold_high";
const char* OBCS_MERGE_THRESHOLD_REQUEST_HIGH = "merge_threshold_request_high";
const char* OBCS_MERGE_DELAY_INTERVAL_MINUTES = "merge_delay_interval_minutes";
const char* OBCS_MERGE_DELAY_FOR_LSYNC_SECOND = "merge_delay_for_lsync_second";
const char* OBCS_MERGE_SCAN_USE_PREREAD = "merge_scan_use_preread";
const char* OBCS_MIN_MERGE_INTERVAL_SECOND = "min_merge_interval_second";
const char* OBCS_MIN_DROP_CACHE_WAIT_SECOND = "min_drop_cache_wait_second";
const char* OBCS_MERGE_TIMEOUT = "merge_timeout";
const char* OBCS_MERGE_PAUSE_ROW_COUNT = "merge_pause_row_count";
const char* OBCS_MERGE_PAUSE_SLEEP_TIME = "merge_pause_sleep_time_us";
const char* OBCS_MERGE_HIGHLOAD_SLEEP_TIME = "merge_high_load_sleep_time_us";

const char* OBCS_MERGE_ADJUST_RATIO = "merge_adjust_ratio";
const char* OBCS_MAX_VERSION_GAP = "max_version_gap";
const char* OBCS_FETCH_UPSLIST_INTERVAL = "upslist_interval_us";
const char* OBCS_TASK_LEFT_TIME = "task_left_time_us";
const char* OBCS_WRITE_SSTABLE_IO_TYPE = "write_sstable_io_type";

const char* OBRS_SECTION = "name_server";
const char* OBRS_IP = "vip";
const char* OBRS_PORT = "port";

const char* OB_CACHE_SECTION = "memory_cache";
const char* OBCE_BLOCKCACHE_MEMSIZE_MB = "block_cache_memsize_mb";
const char* OBCE_FICACHE_MAX_CACHE_NUM = "file_info_cache_max_cache_num";
const char* OBCE_BICACHE_MEMSIZE = "block_index_cache_memsize_mb";
const char* OBCE_JCACHE_MEMSIZE = "join_cache_memsize_mb";

const int64_t BLOCKCACHE_MEMSIZE_MB = 1024L;
const int64_t FICACHE_MAX_CACHE_NUM = 4096L;
const int64_t BICACHE_MEMSIZE = 512L;
const int64_t JCACHE_MEMSIZE = 512L;

const int64_t DEFAULT_MERGE_DELAY_FOR_LSYNC = 60; // 60 seconds
const int64_t DEFAULT_MERGE_SCAN_USE_PREREAD = 1;
const int64_t DEFAULT_MERGE_PAUSE_ROW_COUNT = 2000;
const int64_t DEFAULT_MERGE_PAUSE_SLEEP_TIME = 0; //  donot pause merge;
const int64_t DEFAULT_MERGE_HIGHLOAD_SLEEP_TIME = 2 * 1000 * 1000L; // 2 seconds

}

using namespace sb::common;
using namespace sb::sstable;

namespace sb {
namespace chunkserver {
ObChunkServerParam::ObChunkServerParam() {
  memset(this, 0, sizeof(ObChunkServerParam));
}

ObChunkServerParam::~ObChunkServerParam() {
}

int ObChunkServerParam::load_string(char* dest, const int32_t size,
                                    const char* section, const char* name, bool not_null) {
  int ret = OB_SUCCESS;
  if (NULL == dest || 0 >= size || NULL == section || NULL == name) {
    ret = OB_ERROR;
  }

  const char* value = NULL;
  if (OB_SUCCESS == ret) {
    value = TBSYS_CONFIG.getString(section, name);
    if (not_null && (NULL == value || 0 >= strlen(value))) {
      TBSYS_LOG(ERROR, "%s.%s has not been set.", section, name);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret && NULL != value) {
    if ((int32_t)strlen(value) >= size) {
      TBSYS_LOG(ERROR, "%s.%s too long, length (%d) > %d",
                section, name, (int32_t)strlen(value), size);
      ret = OB_SIZE_OVERFLOW;
    } else {
      memset(dest, 0, size);
      strncpy(dest, value, strlen(value));
    }
  }

  return ret;
}

int ObChunkServerParam::load_from_config() {
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == ret) {
    ret = load_string(datadir_path_,
                      OB_MAX_FILE_NAME_LENGTH, OBCS_SECTION, OBCS_DATADIR_PATH, true);
    TBSYS_LOG(DEBUG, "datadir_path:%s\n", datadir_path_);
  }

  if (OB_SUCCESS == ret) {
    ret = load_string(application_name_,
                      OB_MAX_APP_NAME_LENGTH, OBCS_SECTION, OBCS_APP_NAME, true);
  }

  char root_server_ip[OB_MAX_IP_SIZE];
  int32_t root_server_port = 0;
  if (OB_SUCCESS == ret) {
    ret = load_string(root_server_ip,
                      OB_MAX_IP_SIZE, OBRS_SECTION, OBRS_IP, true);
  }

  if (OB_SUCCESS == ret) {
    root_server_port = TBSYS_CONFIG.getInt(OBRS_SECTION, OBRS_PORT, 0);
    if (root_server_port <= 0) {
      TBSYS_LOG(ERROR, "root server port (%d) cannot <= 0.", root_server_port);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    bool res = name_server_.set_ipv4_addr(root_server_ip, root_server_port);
    if (!res) {
      TBSYS_LOG(ERROR, "root server ip %s, port:%d is invalid.", root_server_ip, root_server_port);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret) {
    ret = load_string(dev_name_, OB_MAX_IP_SIZE, OBCS_SECTION, OBCS_DEVNAME, true);
  }

  if (OB_SUCCESS == ret) {
    chunk_server_port_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_PORT, 0);
    if (chunk_server_port_ <= 0) {
      TBSYS_LOG(ERROR, "chunk server port (%d) cannot <= 0.", chunk_server_port_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    task_queue_size_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_TASK_QUEUE_SIZE, 1000);
    if (task_queue_size_ <= 0) {
      TBSYS_LOG(ERROR, "chunkserver task queue size (%d) cannot <= 0.", task_queue_size_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    task_thread_count_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_TASK_THREAD_COUNT, 20);
    if (task_thread_count_ <= 0) {
      TBSYS_LOG(ERROR, "chunkserver task thread count (%d) cannot <= 0." ,
                task_thread_count_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    int32_t default_count = task_thread_count_ / 10;
    if (default_count < 1) default_count = 1;
    max_migrate_task_count_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MAX_MIGRATE_TASK_COUNT, default_count);
    if (max_migrate_task_count_ <= 0) {
      TBSYS_LOG(ERROR, "chunkserver task migrate task count (%d) cannot <= 0." ,
                max_migrate_task_count_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    heartbeat_time_out_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_HEARTBEAT_TIMEOUT, 1000 * 1000);
    if (heartbeat_time_out_ <= 0) {
      TBSYS_LOG(ERROR, "chunkserver heartbeat timeout (%ld) cannot <= 0." ,
                heartbeat_time_out_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    retry_times_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_RETRY_TIMES, 3);
    if (retry_times_ <= 0) {
      TBSYS_LOG(WARN, "chunkserver retry times (%d) cannot < 0", retry_times_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    reserve_sstable_copy_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_RESERVE_SSTABLE_COPY, 3);
    if (reserve_sstable_copy_ <= 0) {
      TBSYS_LOG(WARN, "chunkserver reserver_sstable_copy_ (%d) can't < 0", reserve_sstable_copy_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    network_time_out_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_NETWORK_TIMEOUT, 2000 * 1000);
    if (network_time_out_ <= 100 * 1000L) { // at least 100ms
      TBSYS_LOG(ERROR, "chunkserver network timeout (%ld) cannot <= 100,000." ,
                network_time_out_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    lease_check_interval_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_LEASE_CHECK_INTERVAL, 5000 * 1000);
    if (lease_check_interval_ <= 0) {
      TBSYS_LOG(ERROR, "chunkserver lease check interval (%ld) cannot <= 0." ,
                lease_check_interval_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    max_tablets_num_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MAX_TABLETS_NUM, 10000);
    if (max_tablets_num_ <= 0) {
      TBSYS_LOG(ERROR, "chunkserver max tablets num (%ld) cannot <= 0." ,
                max_tablets_num_);
      ret = OB_INVALID_ARGUMENT;
    }

    max_sstable_size_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MAX_SSTABLE_SIZE, 256 * 1024 * 1024);
    if (max_sstable_size_ <= 0) {
      TBSYS_LOG(ERROR, "chunkserver max sstable size (%ld) cannot <= 0." ,
                max_sstable_size_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    rsync_band_limit_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_RSYNC_BAND_LIMIT, 50 * 1024); // 50M
    if (rsync_band_limit_ <= 0) {
      TBSYS_LOG(ERROR, "chunkserver rsync band limit KBps(%ld) cannot <= 0." ,
                rsync_band_limit_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_mem_limit_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MERGE_MEM_LIMIT, 64 * 1024 * 1024); //64M
    if (merge_mem_limit_ <= 0) {
      TBSYS_LOG(WARN, "chunkserver merge memory limit (%ld) <=0,set it to 64MB", merge_mem_limit_);
      merge_mem_limit_ = 64L * 1024 * 1024;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_thread_per_disk_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MERGE_THREAD_PER_DISK, 1);
    if (merge_thread_per_disk_ <= 0) {
      TBSYS_LOG(WARN, "merge thread per disk(%ld) can't < 0,set it to 1", merge_thread_per_disk_);
      merge_thread_per_disk_ = 1;
    }
  }

  if (OB_SUCCESS == ret) {
    max_merge_thread_num_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MAX_MERGE_THREAD_NUM, 1);
    if (max_merge_thread_num_ <= 0) {
      TBSYS_LOG(WARN, "max merge thread(%ld) can't <= 0,set it to 1", max_merge_thread_num_);
      max_merge_thread_num_ = 1;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_threshold_load_high_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MERGE_THRESHOLD_LOAD_HIGH, 16);
    if (merge_threshold_load_high_ <= 0) {
      TBSYS_LOG(WARN, "merge_threshold_load_high_(%ld) cann't <= 0,set it to 16", merge_threshold_load_high_);
      merge_threshold_load_high_ = 16;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_threshold_request_high_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MERGE_THRESHOLD_REQUEST_HIGH, 3000);
    if (merge_threshold_request_high_ <= 0) {
      TBSYS_LOG(WARN, "merge_threshold_request_high_(%ld) cann't <= 0,set it to 3000", merge_threshold_request_high_);
      merge_threshold_request_high_ = 3000;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_delay_interval_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MERGE_DELAY_INTERVAL_MINUTES, 10);
    merge_delay_interval_ *= (60L * 60 * 1000 * 1000);  //minutes to us
    if (merge_delay_interval_ <= 0) {
      TBSYS_LOG(WARN, "merge_delay_interval (%ld) cann't <= 0,set it to 10 minutes", merge_delay_interval_);
      merge_delay_interval_ = (10 * 60L * 60 * 1000 * 1000);
    }
  }

  if (OB_SUCCESS == ret) {
    merge_delay_for_lsync_ = TBSYS_CONFIG.getInt(OBCS_SECTION,
                                                 OBCS_MERGE_DELAY_FOR_LSYNC_SECOND, DEFAULT_MERGE_DELAY_FOR_LSYNC);
    merge_delay_for_lsync_ *= (1000 * 1000L);
    if (merge_delay_for_lsync_ <= 0) {
      TBSYS_LOG(WARN, "merge_delay_for_lsync_ (%ld) cann't <= 0,set it to 1 minutes", merge_delay_for_lsync_);
      merge_delay_for_lsync_ = DEFAULT_MERGE_DELAY_FOR_LSYNC * 1000 * 1000L;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_scan_use_preread_ = TBSYS_CONFIG.getInt(OBCS_SECTION,
                                                  OBCS_MERGE_SCAN_USE_PREREAD, DEFAULT_MERGE_SCAN_USE_PREREAD);
    if (merge_scan_use_preread_ != 0 && merge_scan_use_preread_ != 1) {
      TBSYS_LOG(WARN, "merge_scan_use_preread_ (%ld) not 0 or 1", merge_scan_use_preread_);
      merge_scan_use_preread_ = DEFAULT_MERGE_SCAN_USE_PREREAD;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_timeout_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MERGE_TIMEOUT, 3000000);
    if (merge_timeout_ <= 0) {
      TBSYS_LOG(WARN, "merge_timeout_ (%ld) cann't <= 0,set it to 3s", merge_timeout_);
    }
  }

  if (OB_SUCCESS == ret) {
    merge_pause_row_count_ = TBSYS_CONFIG.getInt(OBCS_SECTION,
                                                 OBCS_MERGE_PAUSE_ROW_COUNT, DEFAULT_MERGE_PAUSE_ROW_COUNT);
    if (merge_pause_row_count_ <= 0) {
      TBSYS_LOG(WARN, "merge_pause_row_count_ (%ld) cann't <= 0,set it to %ld",
                merge_pause_row_count_, DEFAULT_MERGE_PAUSE_ROW_COUNT);
      merge_pause_row_count_ = DEFAULT_MERGE_PAUSE_ROW_COUNT;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_pause_sleep_time_ = TBSYS_CONFIG.getInt(OBCS_SECTION,
                                                  OBCS_MERGE_PAUSE_SLEEP_TIME, DEFAULT_MERGE_PAUSE_SLEEP_TIME);
    if (merge_pause_sleep_time_ < 0) {
      TBSYS_LOG(WARN, "merge_pause_sleep_time_ (%ld) cann't < 0,set it to %ld",
                merge_pause_sleep_time_, DEFAULT_MERGE_PAUSE_SLEEP_TIME);
      merge_pause_sleep_time_ = DEFAULT_MERGE_PAUSE_SLEEP_TIME;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_highload_sleep_time_ = TBSYS_CONFIG.getInt(OBCS_SECTION,
                                                     OBCS_MERGE_HIGHLOAD_SLEEP_TIME, DEFAULT_MERGE_HIGHLOAD_SLEEP_TIME);
    if (merge_highload_sleep_time_ <= 0) {
      TBSYS_LOG(WARN, "merge_highload_sleep_time_ (%ld) cann't <= 0,set it to %ld",
                merge_highload_sleep_time_, DEFAULT_MERGE_HIGHLOAD_SLEEP_TIME);
      merge_highload_sleep_time_ = DEFAULT_MERGE_HIGHLOAD_SLEEP_TIME;
    }
  }

  if (OB_SUCCESS == ret) {
    merge_adjust_ratio_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MERGE_ADJUST_RATIO, 80);
    if (merge_adjust_ratio_ <= 0) {
      TBSYS_LOG(WARN, "merge_adjust_ratio_(%ld) cann't <= 0,set it to 80", merge_adjust_ratio_);
      merge_adjust_ratio_ = 80;
    }
  }

  if (OB_SUCCESS == ret) {
    max_version_gap_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MAX_VERSION_GAP, 3);
    if (max_version_gap_ <= 0) {
      TBSYS_LOG(WARN, "max_version_gap_(%ld) cann't <= 0,set if to default(3)");
      max_version_gap_ = 3;
    }
  }

  if (OB_SUCCESS == ret) {
    min_merge_interval_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MIN_MERGE_INTERVAL_SECOND, 600);
    min_merge_interval_ *= (1000 * 1000L);  //second to us
    if (min_merge_interval_ <= 0) {
      TBSYS_LOG(WARN, "min_merge_interval_ (%ld) cann't <= 0, "
                "set it to 600 second(10 mintues)", min_merge_interval_);
      min_merge_interval_ = (600 * 1000 * 1000L);
    }
  }

  if (OB_SUCCESS == ret) {
    min_drop_cache_wait_time_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_MIN_DROP_CACHE_WAIT_SECOND, 300);
    min_drop_cache_wait_time_ *= (1000 * 1000L);   //minutes to us
    if (min_drop_cache_wait_time_ <= 0) {
      TBSYS_LOG(WARN, "min_drop_cache_wait_time_ (%ld) cann't <= 0,"
                "set it to 300 second(5 minutes)", min_drop_cache_wait_time_);
      min_drop_cache_wait_time_ = (300 * 1000 * 1000L);
    }
  }

  if (OB_SUCCESS == ret) {
    fetch_ups_interval_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_FETCH_UPSLIST_INTERVAL, 60 * 1000 * 1000L);
    if (fetch_ups_interval_ <= 0) {
      TBSYS_LOG(ERROR, "fetch ups list interval time must > 0, got (%ld)", fetch_ups_interval_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    task_left_time_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_TASK_LEFT_TIME, 300 * 1000);
    if (task_left_time_ < 0) {
      TBSYS_LOG(ERROR, "task left time must >= 0, got (%d)", task_left_time_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    write_sstable_io_type_ = TBSYS_CONFIG.getInt(OBCS_SECTION, OBCS_WRITE_SSTABLE_IO_TYPE, 0);
    if (write_sstable_io_type_ < 0) {
      TBSYS_LOG(ERROR, "write sstable io type must >= 0, got (%d)", write_sstable_io_type_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    bc_conf_.block_cache_memsize_mb = TBSYS_CONFIG.getInt(
                                        OB_CACHE_SECTION, OBCE_BLOCKCACHE_MEMSIZE_MB,
                                        BLOCKCACHE_MEMSIZE_MB);
    bc_conf_.ficache_max_num = TBSYS_CONFIG.getInt(
                                 OB_CACHE_SECTION, OBCE_FICACHE_MAX_CACHE_NUM, FICACHE_MAX_CACHE_NUM);
  }

  if (OB_SUCCESS == ret) {
    bic_conf_.cache_mem_size = TBSYS_CONFIG.getInt(
                                 OB_CACHE_SECTION, OBCE_BICACHE_MEMSIZE, BICACHE_MEMSIZE);
    bic_conf_.cache_mem_size <<= 20; // mbytes to bytes
  }

  if (OB_SUCCESS == ret) {
    jc_conf_.cache_mem_size = TBSYS_CONFIG.getInt(
                                OB_CACHE_SECTION, OBCE_JCACHE_MEMSIZE, JCACHE_MEMSIZE);
    jc_conf_.cache_mem_size <<= 20; //mbytes to bytes
  }

  return ret;

}

#define CHANGE_CONFIG_ITEM_INT_TYPE(int_type, item_name, item_var, unit_multiple, illegal_pred) \
    do \
    { \
      if (OB_SUCCESS == ret) \
      { \
        int_type newval = config.getInt(OBCS_SECTION, item_name, item_var / unit_multiple); \
        newval *= unit_multiple; \
        if (illegal_pred)  \
        { \
          TBSYS_LOG(WARN, "reload conf %s (%ld) is invalid %s", #item_var, newval, #illegal_pred); \
        } \
        else if (newval != item_var) \
        { \
          TBSYS_LOG(INFO, "reload conf %s switch (%ld) to (%ld)", #item_var, item_var, newval); \
          item_var = newval; \
        } \
      } \
    }  while(0);

int ObChunkServerParam::reload_from_config(const char* config_file_name) {
  int ret = OB_SUCCESS;

  tbsys::CConfig config;
  if (config.load(config_file_name)) {
    fprintf(stderr, "load file %s error\n", config_file_name);
    ret = OB_ERROR;
  }

  CHANGE_CONFIG_ITEM_INT_TYPE(int32_t, OBCS_RETRY_TIMES, retry_times_, 1, (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_NETWORK_TIMEOUT, network_time_out_, 1, (newval < 100 * 1000L));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_RSYNC_BAND_LIMIT, rsync_band_limit_, 1, (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_MEM_LIMIT, merge_mem_limit_, 1, (newval < 1 * 1024 * 1024L));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_THRESHOLD_LOAD_HIGH, merge_threshold_load_high_, 1, (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_THRESHOLD_REQUEST_HIGH, merge_threshold_request_high_, 1, (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_DELAY_INTERVAL_MINUTES, merge_delay_interval_,
                              (60 * 60 * 1000 * 1000L), (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_DELAY_FOR_LSYNC_SECOND, merge_delay_for_lsync_,
                              (1000 * 1000L), (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_SCAN_USE_PREREAD, merge_scan_use_preread_,
                              1, (newval != 0 && newval != 1));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_TIMEOUT, merge_timeout_, 1, (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_ADJUST_RATIO, merge_adjust_ratio_, 1, (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MIN_MERGE_INTERVAL_SECOND, min_merge_interval_,
                              (1000 * 1000L), (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MIN_DROP_CACHE_WAIT_SECOND, min_drop_cache_wait_time_,
                              (1000 * 1000L), (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_FETCH_UPSLIST_INTERVAL, fetch_ups_interval_, 1, (newval <= 0));

  CHANGE_CONFIG_ITEM_INT_TYPE(int32_t, OBCS_TASK_QUEUE_SIZE, task_queue_size_, 1, (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_TASK_LEFT_TIME, task_left_time_, 1, (newval < 0));

  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_PAUSE_ROW_COUNT, merge_pause_row_count_, 1, (newval <= 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_PAUSE_SLEEP_TIME, merge_pause_sleep_time_, 1, (newval < 0));
  CHANGE_CONFIG_ITEM_INT_TYPE(int64_t, OBCS_MERGE_HIGHLOAD_SLEEP_TIME, merge_highload_sleep_time_, 1, (newval <= 0));

  return ret;
}

} // end namespace chunkserver
} // end namespace sb



