/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_server_params.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_
#define OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_
#include <stdint.h>

namespace sb {
namespace mergeserver {
class ObMergeServerParams {
 public:
  static const int32_t OB_MAX_IP_SIZE = 64;
  ObMergeServerParams();

 public:
  int load_from_config();

  inline const char* get_device_name() const { return dev_name_; }
  inline const char* get_root_server_ip() const { return root_server_ip_; }
  inline int32_t get_root_server_port() const { return root_server_port_; }
  inline int32_t get_listen_port() const { return server_listen_port_; }

  inline int32_t get_task_queue_size() const { return task_queue_size_; }
  inline int32_t get_task_thread_size() const { return task_thread_count_; }
  inline int32_t get_task_left_time() const { return task_left_time_; }
  inline int32_t get_retry_times() const { return retry_times_; }
  inline int64_t get_network_timeout() const { return network_time_out_; }
  inline int64_t get_lease_check_interval() const { return check_lease_interval_; }
  inline int64_t get_monitor_interval() const { return monitor_interval_; }
  inline int64_t get_fetch_ups_interval() const { return fetch_ups_interval_; }
  inline int64_t get_ups_fail_count() const { return ups_fail_count_; }
  inline int64_t get_ups_blacklist_timeout() const { return ups_blacklist_timeout_; }
  inline int32_t get_tablet_location_cache_size() const {return location_cache_size_;}
  inline int64_t get_tablet_location_cache_timeout() const {return location_cache_timeout_;}
  inline int64_t get_intermediate_buffer_size() const {return intermediate_buffer_size_; }

  inline bool allow_return_uncomplete_result()const {return allow_return_uncomplete_result_; }
  void dump_config();

 private:
  int load_string(char* dest, const int32_t size,
                  const char* section, const char* name, bool require = true);

 private:
  char dev_name_[OB_MAX_IP_SIZE];
  char root_server_ip_[OB_MAX_IP_SIZE];
  int32_t root_server_port_;
  int32_t server_listen_port_;
  int32_t task_queue_size_;
  int32_t task_thread_count_;
  int64_t task_left_time_;
  int64_t network_time_out_;
  int64_t fetch_ups_interval_;
  int64_t ups_fail_count_;
  int64_t ups_blacklist_timeout_;
  int64_t monitor_interval_;
  int64_t check_lease_interval_;
  int32_t retry_times_;
  int32_t location_cache_size_;
  int64_t location_cache_timeout_;
  int64_t intermediate_buffer_size_;
  int64_t memory_size_limit_;

  int32_t allow_return_uncomplete_result_;
  // if new config item added, don't forget add it into dump_config
};
} /* mergeserver */
} /* sb */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_ */


