/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   rongxuan<rongxuan.lc@alipay.com>
 *     - some work details here
 */

#include "name_server_server_state.h"
#include "name_server_server.h"
using namespace sb::common;
using namespace sb::nameserver;

int NameServerServerState::init(NameServer* name_server, tbsys::CRWLock* server_manager_lock, const ObChunkServerManager* server_manager) {
  int ret = OB_SUCCESS;
  if (NULL == server_manager_lock || NULL == server_manager
      || NULL == name_server) {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid argument. lock=%p, server_manager=%p, name_server=%p",
              server_manager_lock, server_manager, name_server);
  }
  if (OB_SUCCESS == ret) {
    name_server_ = name_server;
    server_manager_rwlock_ = server_manager_lock;
    server_manager_ = const_cast<ObChunkServerManager*>(server_manager);
  }
  return ret;
}

int NameServerServerState::set_daily_merge_error(const char* err_msg, const int64_t length) {
  int ret = OB_ERROR;
  if (err_msg == NULL || length > OB_MAX_ERROR_MSG_LEN) {
    TBSYS_LOG(WARN, "invalid argument. err_msg=%p, len=%ld", err_msg, length);
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == ret) {
    tbsys::CThreadGuard guard(&mutex_);
    if (is_daily_merge_error_) {
      TBSYS_LOG(WARN, "already have daily_merge error.");
    } else {
      is_daily_merge_error_ = true;
      memcpy(err_msg_, err_msg, length);
      err_msg_[length] = '\0';
    }
  }
  return ret;
}
bool NameServerServerState::is_daily_merge_tablet_error()const {
  tbsys::CThreadGuard guard(&mutex_);
  return is_daily_merge_error_;
}
char* NameServerServerState::get_error_msg() {
  tbsys::CThreadGuard guard(&mutex_);
  return err_msg_;
}
void NameServerServerState::clean_daily_merge_error() {
  tbsys::CThreadGuard guard(&mutex_);
  is_daily_merge_error_ = false;
}
