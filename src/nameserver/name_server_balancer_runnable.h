/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * name_server_balancer_runnable.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_BALANCER_RUNNABLE_H
#define _OB_ROOT_BALANCER_RUNNABLE_H 1
#include "name_server_balancer.h"
#include "name_server_server_config.h"
#include "common/ob_role_mgr.h"
namespace sb {
namespace nameserver {
class NameServerBalancer;
class NameServerBalancerRunnable : public tbsys::CDefaultRunnable {
 public:
  NameServerBalancerRunnable(NameServerServerConfig& config,
                         NameServerBalancer& balancer,
                         common::ObRoleMgr& role_mgr);
  virtual ~NameServerBalancerRunnable();
  void run(tbsys::CThread* thread, void* arg);
  void wakeup();
 private:
  bool is_master() const;
  // disallow copy
  NameServerBalancerRunnable(const NameServerBalancerRunnable& other);
  NameServerBalancerRunnable& operator=(const NameServerBalancerRunnable& other);
 private:
  // data members
  NameServerServerConfig& config_;
  NameServerBalancer& balancer_;
  common::ObRoleMgr& role_mgr_;
  tbsys::CThreadCond balance_worker_sleep_cond_;
};
} // end namespace nameserver
} // end namespace sb

#endif /* _OB_ROOT_BALANCER_RUNNABLE_H */

