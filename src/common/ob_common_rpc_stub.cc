/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_common_rpc_stub.cc for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "ob_common_rpc_stub.h"

using namespace sb::common;

const int32_t ObCommonRpcStub :: DEFAULT_VERSION = 1;

ObCommonRpcStub :: ObCommonRpcStub() {
  client_mgr_ = NULL;
}

ObCommonRpcStub :: ~ObCommonRpcStub() {
}

int ObCommonRpcStub :: init(const ObClientManager* client_mgr) {
  int err = OB_SUCCESS;

  if (NULL == client_mgr) {
    TBSYS_LOG(WARN, "invalid param, client_mgr=%p", client_mgr);
    err = OB_INVALID_ARGUMENT;
  } else {
    client_mgr_ = client_mgr;
  }

  return err;
}

int ObCommonRpcStub :: send_log(const ObServer& ups_slave, ObDataBuffer& log_data,
                                const int64_t timeout_us) {
  int err = OB_SUCCESS;
  ObDataBuffer out_buff;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(out_buff);
  }

  // step 1. send log data to slave
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(ups_slave,
                                    OB_SEND_LOG, DEFAULT_VERSION, timeout_us, log_data, out_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "send log data to slave failed "
                "data_len[%ld] err[%d].", log_data.get_position(), err);
    }
  }

  // step 2. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err) {
    ObResultCode result_code;
    err = result_code.deserialize(out_buff.get_data(), out_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    } else {
      err = result_code.result_code_;
    }
  }

  return err;
}

int ObCommonRpcStub :: renew_lease(const common::ObServer& master,
                                   const common::ObServer& slave_addr, const int64_t timeout_us) {
  int err = OB_SUCCESS;

  ObDataBuffer data_buff;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. serialize slave addr
  if (OB_SUCCESS == err) {
    err = slave_addr.serialize(data_buff.get_data(), data_buff.get_capacity(),
                               data_buff.get_position());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to serialize slave addr, err[%d].", err);
    }
  }


  // step 2. send renew lease request
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(master,
                                    OB_RENEW_LEASE_REQUEST, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "send renew_lease failed, err[%d].", err);
    }
  }

  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err) {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    } else {
      err = result_code.result_code_;
    }
  }

  return err;
}

int ObCommonRpcStub :: grant_lease(const common::ObServer& slave,
                                   const ObLease& lease, const int64_t timeout_us) {
  int err = OB_SUCCESS;

  ObDataBuffer data_buff;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. serialize lease
  if (OB_SUCCESS == err) {
    err = lease.serialize(data_buff.get_data(), data_buff.get_capacity(),
                          data_buff.get_position());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to serialize lease, err[%d].", err);
    }
  }

  // step 2. send grant lease request
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(slave,
                                    OB_GRANT_LEASE_REQUEST, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "send grant lease failed, err[%d].", err);
    }
  }

  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err) {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    } else {
      err = result_code.result_code_;
    }
  }

  return err;
}

int ObCommonRpcStub::slave_quit(const common::ObServer& master, const common::ObServer& slave_addr,
                                const int64_t timeout_us) {
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. serialize slave addr
  if (OB_SUCCESS == err) {
    err = slave_addr.serialize(data_buff.get_data(), data_buff.get_capacity(),
                               data_buff.get_position());
  }

  // step 2. send request to register
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(master,
                                    OB_SLAVE_QUIT, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "send request to register failed"
                "err[%d].", err);
    }
  }

  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err) {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    } else {
      err = result_code.result_code_;
    }
  }

  char addr_buf_master[BUFSIZ];
  char addr_buf_slave[BUFSIZ];
  if (!master.to_string(addr_buf_master, BUFSIZ)) {
    strcpy(addr_buf_master, "Get Server IP failed");
  }
  if (!slave_addr.to_string(addr_buf_slave, BUFSIZ)) {
    strcpy(addr_buf_slave, "Get Server IP failed");
  }
  TBSYS_LOG(INFO, "send slave(%s) quit info to Master(%s), err[%d].", addr_buf_slave, addr_buf_master, err);

  return err;
}

int ObCommonRpcStub :: get_obi_role(const common::ObServer& rs, common::ObiRole& obi_role, const int64_t timeout_us) {
  int err = OB_SUCCESS;

  ObDataBuffer data_buff;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. send get obi role request
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(rs,
                                    OB_GET_OBI_ROLE, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "send get obi role failed, err[%d].", err);
    }
  }

  // step 2. deserialize the ObObiRole and response code
  int64_t pos = 0;
  if (OB_SUCCESS == err) {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    } else {
      err = result_code.result_code_;
      if (OB_SUCCESS != err) {
        TBSYS_LOG(ERROR, "get obi response error, err=%d", err);
      }
    }
  }

  if (OB_SUCCESS == err) {
    err = obi_role.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "deserialize ObObiRole failed:pos[%ld], err[%d].", pos, err);
    }
  }

  return err;
}

int ObCommonRpcStub :: get_thread_buffer_(ObDataBuffer& data_buff) {
  int err = OB_SUCCESS;

  ThreadSpecificBuffer::Buffer* rpc_buffer = NULL;
  // get buffer for rpc send and receive
  rpc_buffer = thread_buffer_.get_buffer();
  if (NULL == rpc_buffer) {
    TBSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
    err = OB_ERROR;
  } else {
    rpc_buffer->reset();
    data_buff.set_data(rpc_buffer->current(), rpc_buffer->remain());
  }

  return err;
}

