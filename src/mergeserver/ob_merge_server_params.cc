/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_server_params.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "ob_merge_server_params.h"
#include "config.h"
#include "common/ob_malloc.h"
#include <unistd.h>

using namespace sb::common;

namespace {
const char* OBMS_MS_SECTION = "merge_server";
const char* OBMS_RS_SECTION = "root_server";
const char* OBMS_PORT = "port";
const char* OBMS_VIP = "vip";
const char* OBMS_DEVNAME = "dev_name";
const char* OBMS_RETRY_TIMES = "retry_times";
const char* OBMS_TASK_QUEUE_SIZE = "task_queue_size";
const char* OBMS_TASK_THREAD_COUNT = "task_thread_count";
const char* OBMS_TASK_LEFT_TIME = "task_left_time_us";
const char* OBMS_NETWORK_TIMEOUT = "network_timeout_us";
const char* OBMS_LEASE_INTERVAL = "lease_interval_us";
const char* OBMS_MONITOR_INTERVAL = "monitor_interval_us";
const char* OBMS_UPSLIST_INTERVAL = "upslist_interval_us";
const char* OBMS_BLACKLIST_TIMEOUT = "blacklist_timeout_us";
const char* OBMS_BLACKLIST_FAIL_COUNT = "blacklist_fail_count";
const char* OBMS_LOCATION_CACHE_SIZE = "location_cache_size_mb";
const char* OBMS_LOCATION_CACHE_TIMEOUT = "location_cache_timeout_us";
const char* OBMS_INTERMEDIATE_BUFFER_SIZE = "intermediate_buffer_size_mbyte";
const char* OBMS_MEMORY_SIZE_LIMIG_PERCENT = "memory_size_limit_percent";
const char* OBMS_ALLOW_RETURN_UNCOMPLETE_RESULT = "allow_return_uncomplete_result";
const int64_t OB_MS_MAX_MEMORY_SIZE_LIMIT = 80;
const int64_t OB_MS_MIN_MEMORY_SIZE_LIMIT = 10;
}

namespace sb {
namespace mergeserver {
ObMergeServerParams::ObMergeServerParams() {
  memset(this, 0, sizeof(ObMergeServerParams));
}

int ObMergeServerParams::load_from_config() {
  int ret = OB_SUCCESS;

  ret = load_string(root_server_ip_, OB_MAX_IP_SIZE, OBMS_RS_SECTION, OBMS_VIP);

  if (ret == OB_SUCCESS) {
    ret = load_string(dev_name_, OB_MAX_IP_SIZE, OBMS_MS_SECTION, OBMS_DEVNAME);
  }


  if (ret == OB_SUCCESS) {
    server_listen_port_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_PORT, 0);
    if (server_listen_port_ <= 0) {
      TBSYS_LOG(ERROR, "mergeserver listen port must > 0, got (%d)", server_listen_port_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    root_server_port_ = TBSYS_CONFIG.getInt(OBMS_RS_SECTION, OBMS_PORT, 0);
    if (root_server_port_ <= 0) {
      TBSYS_LOG(ERROR, "root server's port must > 0, got (%d)", root_server_port_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    task_queue_size_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_TASK_QUEUE_SIZE, 1000);
    if (task_queue_size_ <= 0) {
      TBSYS_LOG(ERROR, "task queue size must > 0, got (%d)", task_queue_size_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    task_thread_count_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_TASK_THREAD_COUNT, 20);
    if (task_thread_count_ <= 0) {
      TBSYS_LOG(ERROR, "task thread count must > 0, got (%d)", task_thread_count_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    task_left_time_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_TASK_LEFT_TIME, 100 * 1000);
    if (task_left_time_ < 0) {
      TBSYS_LOG(ERROR, "task left time must >= 0, got (%d)", task_left_time_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    network_time_out_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_NETWORK_TIMEOUT, 2000);
    if (network_time_out_ <= 0) {
      TBSYS_LOG(ERROR, "network timeout must > 0, got (%ld)", network_time_out_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    ups_blacklist_timeout_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_BLACKLIST_TIMEOUT, 60 * 1000 * 1000L);
    if (ups_blacklist_timeout_ <= 0) {
      TBSYS_LOG(ERROR, "ups in blacklist timeout must > 0, got (%ld)", ups_blacklist_timeout_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    ups_fail_count_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_BLACKLIST_FAIL_COUNT, 20);
    if (ups_fail_count_ <= 0) {
      TBSYS_LOG(ERROR, "ups fail count into blacklist must > 0, got (%ld)", ups_fail_count_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    fetch_ups_interval_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_UPSLIST_INTERVAL, 60 * 1000 * 1000L);
    if (fetch_ups_interval_ <= 0) {
      TBSYS_LOG(ERROR, "fetch ups list interval time must > 0, got (%ld)", fetch_ups_interval_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    check_lease_interval_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_LEASE_INTERVAL, 6 * 1000 * 1000L);
    if (check_lease_interval_ <= 0) {
      TBSYS_LOG(ERROR, "check lease interval time must > 0, got (%ld)", check_lease_interval_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    monitor_interval_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_MONITOR_INTERVAL, 600 * 1000 * 1000L);
    if (monitor_interval_ <= 0) {
      TBSYS_LOG(ERROR, "monitor interval time must > 0, got (%ld)", monitor_interval_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    retry_times_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_RETRY_TIMES, 3);
    if (retry_times_ < 0) {
      TBSYS_LOG(ERROR, "retry_times must >= 0, got (%d)", retry_times_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    location_cache_timeout_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_LOCATION_CACHE_TIMEOUT, 1000 * 1000 * 600L);
    if (location_cache_timeout_ <= 0) {
      TBSYS_LOG(ERROR, "tablet location cache timeout must > 0, got (%ld)", location_cache_timeout_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    location_cache_size_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_LOCATION_CACHE_SIZE);
    if (location_cache_size_ <= 0) {
      TBSYS_LOG(ERROR, "tablet location cache size should great than 0 got (%d)",
                location_cache_size_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    intermediate_buffer_size_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_INTERMEDIATE_BUFFER_SIZE, 4);
    if (intermediate_buffer_size_ <= 0) {
      TBSYS_LOG(ERROR, "intermediate_buffer_size_ must > 0, got (%d)", intermediate_buffer_size_);
      ret = OB_INVALID_ARGUMENT;
    } else {
      intermediate_buffer_size_ *= 1024 * 1024;
    }
  }

  if (ret == OB_SUCCESS) {
    allow_return_uncomplete_result_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_ALLOW_RETURN_UNCOMPLETE_RESULT, 0);
    if ((allow_return_uncomplete_result_ != 0) && (allow_return_uncomplete_result_ != 1)) {
      TBSYS_LOG(ERROR, "allow_return_uncomplete_result must 0 or 1 it's [%d]", allow_return_uncomplete_result_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS) {
    memory_size_limit_ = TBSYS_CONFIG.getInt(OBMS_MS_SECTION, OBMS_MEMORY_SIZE_LIMIG_PERCENT, 15);
    if (memory_size_limit_ < OB_MS_MIN_MEMORY_SIZE_LIMIT
        || OB_MS_MAX_MEMORY_SIZE_LIMIT < memory_size_limit_) {
      TBSYS_LOG(ERROR, "memory_size_limit_percent between [%d,%d], got (%d)",
                OB_MS_MIN_MEMORY_SIZE_LIMIT,
                OB_MS_MAX_MEMORY_SIZE_LIMIT,
                memory_size_limit_);
      ret = OB_INVALID_ARGUMENT;
    } else {
      memory_size_limit_ = memory_size_limit_ * sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE) / 100;
      ob_set_memory_size_limit(memory_size_limit_);
      TBSYS_LOG(DEBUG, "set memory size limit [limit:%ld]", memory_size_limit_);
    }
  }
  return ret;
}

int ObMergeServerParams::load_string(char* dest, const int32_t size,
                                     const char* section, const char* name, bool require) {
  int ret = OB_SUCCESS;
  if (NULL == dest || 0 >= size || NULL == section || NULL == name) {
    ret = OB_ERROR;
  }

  const char* value = NULL;
  if (OB_SUCCESS == ret) {
    value = TBSYS_CONFIG.getString(section, name);
    if (require && (NULL == value || 0 >= strlen(value))) {
      TBSYS_LOG(ERROR, "%s.%s has not been set.", section, name);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret && NULL != value) {
    if ((int32_t)strlen(value) >= size) {
      TBSYS_LOG(ERROR, "%s.%s too long, length (%ld) > %d",
                section, name, strlen(value), size);
      ret = OB_SIZE_OVERFLOW;
    } else {
      strncpy(dest, value, strlen(value));
    }
  }

  return ret;
}

void ObMergeServerParams::dump_config() {
  TBSYS_LOG(INFO, "%s", "mergeserver config:");
  TBSYS_LOG(INFO, "listen port => %d", server_listen_port_);
  TBSYS_LOG(INFO, "device name => %s", dev_name_);
  TBSYS_LOG(INFO, "task queue size => %d", task_queue_size_);
  TBSYS_LOG(INFO, "task min left time => %ld", task_left_time_);
  TBSYS_LOG(INFO, "task thread count => %d", task_thread_count_);
  TBSYS_LOG(INFO, "network time out => %ld", network_time_out_);
  TBSYS_LOG(INFO, "timeout retry times => %ld", retry_times_);
  TBSYS_LOG(INFO, "check lease time interval => %ld", check_lease_interval_);
  TBSYS_LOG(INFO, "monitor time interval => %ld", monitor_interval_);
  TBSYS_LOG(INFO, "fetch ups list time interval => %ld", fetch_ups_interval_);
  TBSYS_LOG(INFO, "ups fail count into blacklist => %ld", ups_fail_count_);
  TBSYS_LOG(INFO, "ups blacklist time out => %ld", ups_blacklist_timeout_);
  TBSYS_LOG(INFO, "tablet location cache size in Mbyte => %d", location_cache_size_);
  TBSYS_LOG(INFO, "tablet location cache time out in ms => %ld", location_cache_timeout_);
  TBSYS_LOG(INFO, "nameserver address => %s:%d", root_server_ip_, root_server_port_);
  TBSYS_LOG(INFO, "memory size limit => %ld", memory_size_limit_);
  TBSYS_LOG(INFO, "allow_return_uncomplete_result => %d", allow_return_uncomplete_result_);
}

} /* mergeserver */
} /* sb */


