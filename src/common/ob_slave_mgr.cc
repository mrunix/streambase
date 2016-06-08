/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_slave_mgr.cc for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "ob_slave_mgr.h"

#include "ob_malloc.h"
#include "ob_result.h"

using namespace sb::common;

const int ObSlaveMgr::DEFAULT_VERSION = 1;
const int ObSlaveMgr::DEFAULT_LOG_SYNC_TIMEOUT = 500 * 1000;
const int ObSlaveMgr::GRANT_LEASE_TIMEOUT = 1000000;
const int ObSlaveMgr::CHECK_LEASE_VALID_INTERVAL = 10000;
const int ObSlaveMgr::MASTER_LEASE_CHECK_REDUNDANCE = 1000000;

ObSlaveMgr::ObSlaveMgr() {
  is_initialized_ = false;
  slave_num_ = 0;
  rpc_stub_ = NULL;
  lease_interval_ = 0;
  lease_reserved_time_ = 0;
}

ObSlaveMgr::~ObSlaveMgr() {
  ServerNode* node = NULL;
  ObDLink* p = slave_head_.server_list_link.next();
  while (p != &slave_head_.server_list_link) {
    node = (ServerNode*)p;
    p = p->next();
    p->prev()->remove();
    ob_free(node);
  }
}

int ObSlaveMgr::init(const uint32_t vip,
                     ObCommonRpcStub* rpc_stub,
                     int64_t log_sync_timeout,
                     int64_t lease_interval,
                     int64_t lease_reserved_time,
                     int64_t send_retry_times/* = DEFAULT_SEND_LOG_RETRY_TIMES*/,
                     bool exist_wait_lease_on/* = false*/) {
  int ret = OB_SUCCESS;

  if (is_initialized_) {
    ret = OB_INIT_TWICE;
  } else {
    if (NULL == rpc_stub) {
      TBSYS_LOG(ERROR, "Parameters is invlid[rpc_stub=%p]", rpc_stub);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    vip_ = vip;
    rpc_stub_ = rpc_stub;
    log_sync_timeout_ = log_sync_timeout;
    lease_interval_ = lease_interval;
    lease_reserved_time_ = lease_reserved_time;
    send_retry_times_ = send_retry_times;
    slave_fail_wait_lease_on_ = exist_wait_lease_on;
    is_initialized_ = true;
  }

  return ret;
}

int ObSlaveMgr::add_server(const ObServer& server) {
  int ret = OB_SUCCESS;

  slave_info_mutex_.lock();
  ServerNode* node = find_server_(server);
  if (NULL != node) {
    char addr_buf[BUFSIZ];
    if (!server.to_string(addr_buf, BUFSIZ)) {
      strcpy(addr_buf, "Get Server IP failed");
    }
    TBSYS_LOG(INFO, "slave[%s] is already existed", addr_buf);
  } else {
    ServerNode* item = (ServerNode*)ob_malloc(sizeof(ServerNode));
    if (NULL == item) {
      TBSYS_LOG(ERROR, "slave_list_(ObVector) push_back error[%d]", ret);
      ret = OB_ERROR;
    } else {
      item->reset();

      item->server = server;

      slave_head_.server_list_link.insert_prev(item->server_list_link);

      slave_num_ ++;

      char addr_buf[BUFSIZ];
      if (!server.to_string(addr_buf, BUFSIZ)) {
        strcpy(addr_buf, "Get Server IP failed");
      }
      TBSYS_LOG(INFO, "add a slave[%s], remaining slave number[%d]", addr_buf, slave_num_);
    }
  }
  slave_info_mutex_.unlock();

  return ret;
}

int ObSlaveMgr::delete_server(const ObServer& server) {
  int ret = OB_SUCCESS;

  slave_info_mutex_.lock();
  ServerNode* node = find_server_(server);

  char addr_buf[BUFSIZ];
  if (!server.to_string(addr_buf, BUFSIZ)) {
    strcpy(addr_buf, "Get Server IP failed");
  }

  if (NULL == node) {
    TBSYS_LOG(WARN, "Server[%s] is not found", addr_buf);
    ret = OB_ERROR;
  } else {
    node->server_list_link.remove();
    ob_free(node);
    slave_num_ --;

    TBSYS_LOG(INFO, "delete server[%s], remaining slave number[%d]", addr_buf, slave_num_);
  }
  slave_info_mutex_.unlock();

  return ret;
}

int ObSlaveMgr::send_data(const char* data, int64_t length) {
  int ret = check_inner_stat();
  ObDataBuffer send_buf;
  ServerNode failed_head;

  if (OB_SUCCESS == ret) {
    if (NULL == data || length < 0) {
      TBSYS_LOG(ERROR, "parameters are invalid[data=%p length=%ld]", data, length);
      ret = OB_INVALID_ARGUMENT;
    } else {
      send_buf.set_data(const_cast<char*>(data), length);
      send_buf.get_position() = length;
    }
  }

  slave_info_mutex_.lock();

  if (OB_SUCCESS == ret) {
    ServerNode* slave_node = NULL;
    ObDLink* p = slave_head_.server_list_link.next();
    while (OB_SUCCESS == ret && p != NULL && p != &slave_head_.server_list_link) {
      slave_node = (ServerNode*)(p);
      int err = 0;

      for (int64_t i = 0; i < send_retry_times_; i++) {
        int64_t send_beg_time = tbsys::CTimeUtil::getMonotonicTime();
        err = rpc_stub_->send_log(slave_node->server, send_buf, log_sync_timeout_);
        if (OB_SUCCESS == err) {
          break;
        } else if (i + 1 < send_retry_times_) {
          int64_t send_elsp_time = tbsys::CTimeUtil::getMonotonicTime() - send_beg_time;
          if (send_elsp_time < log_sync_timeout_) {
            usleep(log_sync_timeout_ - send_elsp_time);
          }
        }
      }

      if (OB_SUCCESS != err) {
        if (!tbsys::CNetUtil::isLocalAddr(vip_)) {
          TBSYS_LOG(ERROR, "VIP has gone");
          ret = OB_ERROR;
          p = p->next();
        } else {
          char addr_buf[BUFSIZ];
          if (!slave_node->server.to_string(addr_buf, BUFSIZ)) {
            strcpy(addr_buf, "Get Server IP failed");
          }
          TBSYS_LOG(WARN, "send_data to slave[%s] error[err=%d]", addr_buf, err);

          ObDLink* to_del = p;
          p = p->next();
          to_del->remove();
          slave_num_ --;
          failed_head.server_list_link.insert_prev(*to_del);
        }
      } else {
        p = p->next();
      }

    } // end of loop

    if (NULL == p) {
      TBSYS_LOG(ERROR, "Server list encounter NULL pointer, this should not be reached");
      ret = OB_ERROR;
    }
  }

  slave_info_mutex_.unlock();

  if (OB_SUCCESS == ret) {
    ServerNode* slave_node = NULL;
    ObDLink* p = failed_head.server_list_link.next();
    while (p != NULL && p != &failed_head.server_list_link) {
      slave_node = (ServerNode*)(p);
      if (slave_fail_wait_lease_on_) {
        // wait slave lease timeout when switch is on
        while (slave_node->is_lease_valid(MASTER_LEASE_CHECK_REDUNDANCE)) {
          usleep(CHECK_LEASE_VALID_INTERVAL);
        }
      }

      char addr_buf[BUFSIZ];
      if (!slave_node->server.to_string(addr_buf, BUFSIZ)) {
        strcpy(addr_buf, "Get Server IP failed");
      }
      if (slave_fail_wait_lease_on_) {
        TBSYS_LOG(WARN, "Slave[%s]'s lease is expired and has been removed", addr_buf);
      } else {
        TBSYS_LOG(WARN, "Slave[%s] has been removed without "
                  "waiting lease timeout", addr_buf);
      }

      p = p->next();
      p->prev()->remove();
      ob_free(slave_node);
      ret = OB_PARTIAL_FAILED;
    }
    if (NULL == p) {
      TBSYS_LOG(ERROR, "Server list encounter NULL pointer, this should not be reached");
      ret = OB_ERROR;
    }
  }

  return ret;
}

int ObSlaveMgr::extend_lease(const ObServer& server, ObLease& lease) {
  int ret = OB_SUCCESS;

  slave_info_mutex_.lock();

  ServerNode* node = find_server_(server);
  if (NULL == node) {
    slave_info_mutex_.unlock();

    char addr_buf[BUFSIZ];
    if (!server.to_string(addr_buf, BUFSIZ)) {
      strcpy(addr_buf, "Get Server IP failed");
    }
    TBSYS_LOG(WARN, "Server[%s] is not found", addr_buf);
    ret = OB_ERROR;
  } else {
    node->lease.lease_time = tbsys::CTimeUtil::getTime();
    node->lease.lease_interval = lease_interval_;
    node->lease.renew_interval = lease_reserved_time_;
    lease = node->lease;

    slave_info_mutex_.unlock();

    ret = rpc_stub_->grant_lease(server, lease, GRANT_LEASE_TIMEOUT);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "grant_lease error, ret=%d", ret);
    } else {
      char addr_buf[BUFSIZ];
      if (!server.to_string(addr_buf, BUFSIZ)) {
        strcpy(addr_buf, "Get Server IP failed");
      }
      TBSYS_LOG(DEBUG, "grant lease to Slave[%s], lease_time=%ld lease_internval=%ld renew_interval=%ld",
                addr_buf, lease.lease_time, lease.lease_interval, lease.renew_interval);
    }
  }

  return ret;
}

int ObSlaveMgr::check_lease_expiration() {
  //TODO: lease机制实现, yanran
  TBSYS_LOG(DEBUG, "TODO: check_lease_expiration");
  return OB_SUCCESS;
}

bool ObSlaveMgr::is_lease_valid(const ObServer& server) const {
  //TODO: lease机制实现, yanran
  char addr_buf[BUFSIZ];
  if (!server.to_string(addr_buf, BUFSIZ)) {
    strcpy(addr_buf, "Get Server IP failed");
  }
  TBSYS_LOG(DEBUG, "TODO: is_lease_valid of Slave[%s]", addr_buf);
  return false;
}

ObSlaveMgr::ServerNode* ObSlaveMgr::find_server_(const ObServer& server) {
  ServerNode* res = NULL;

  ServerNode* node = NULL;
  ObDLink* p = slave_head_.server_list_link.next();
  while (p != &slave_head_.server_list_link) {
    node = (ServerNode*)p;
    if (node->server == server) {
      res = node;
      break;
    }

    p = p->next();
  }

  return res;
}

