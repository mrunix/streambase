/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_rpc_stub.cc for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#include "ob_ups_rpc_stub.h"
#include "ob_update_server.h"
#include "ob_update_server_main.h"

namespace sb {
namespace updateserver {
using namespace sb::common;

ObUpsRpcStub :: ObUpsRpcStub() {
  client_mgr_ = NULL;
}

ObUpsRpcStub :: ~ObUpsRpcStub() {
}

int ObUpsRpcStub :: fetch_schema(const ObServer& name_server, const int64_t timestamp,
                                 CommonSchemaManagerWrapper& schema_mgr, const int64_t timeout_us) {
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(data_buff);
  }
  // step 1. serialize timestamp to data_buff
  if (OB_SUCCESS == err) {
    err = common::serialization::encode_vi64(data_buff.get_data(),
                                             data_buff.get_capacity(), data_buff.get_position(), timestamp);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "serialize timestamp failed:timestamp[%ld], err[%d].",
                timestamp, err);
    }
  }

  // step 2. send request to fetch new schema
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(name_server,
                                    OB_FETCH_SCHEMA, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "send request to root server for fetch schema failed"
                ":timestamp[%ld], err[%d].", timestamp, err);
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
  // step 4. deserialize the table schema
  if (OB_SUCCESS == err) {
    err = schema_mgr.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "deserialize schema from buff failed"
                "timestamp[%ld], pos[%ld], err[%d].", timestamp, pos, err);
    }
  }

  return err;
}

int ObUpsRpcStub :: sync_schema(const ObServer& ups_master, ObSchemaManagerWrapper& schema,
                                const int64_t timeout_us) {
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. serialize schema to data_buff
  if (OB_SUCCESS == err) {
    err = schema.serialize(data_buff.get_data(), data_buff.get_capacity(),
                           data_buff.get_position());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "serialize schema failed:err[%d]", err);
    }
  }

  // step 2. send request to sync schema
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(ups_master,
                                    OB_SYNC_SCHEMA, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "send request to ups slave to sync schema failed"
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

  return err;
}

int ObUpsRpcStub :: slave_register_followed(const ObServer& master, const ObSlaveInfo& slave_info,
                                            ObUpsFetchParam& fetch_param, const int64_t timeout_us) {
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
    err = slave_info.serialize(data_buff.get_data(), data_buff.get_capacity(),
                               data_buff.get_position());
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "ObSlaveInfo serialize error, err=%d", err);
    }
  }

  // step 2. send request to register
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(master,
                                    OB_SLAVE_REG, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(WARN, "send request to register failed err[%d].", err);
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

  // step 3. deserialize fetch param
  if (OB_SUCCESS == err) {
    err = fetch_param.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "deserialize fetch param failed, err[%d]", err);
    }
  }

  return err;
}

int ObUpsRpcStub :: slave_register_standalone(const ObServer& master,
                                              const uint64_t log_id, const uint64_t log_seq,
                                              uint64_t& log_id_res, uint64_t& log_seq_res, const int64_t timeout_us) {
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
    if (OB_SUCCESS != (err = serialization::encode_i64(data_buff.get_data(), data_buff.get_capacity(),
                                                       data_buff.get_position(), (int64_t)log_id))) {
      TBSYS_LOG(WARN, "log_id serialize error, err=%d", err);
    } else if (OB_SUCCESS != (err = serialization::encode_i64(data_buff.get_data(), data_buff.get_capacity(),
                                                              data_buff.get_position(), (int64_t)log_seq))) {
      TBSYS_LOG(WARN, "log_seq serialize error, err=%d", err);
    }
  }

  // step 2. send request to register
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(master,
                                    OB_SLAVE_REG, DEFAULT_VERSION, timeout_us, data_buff);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "send request to register failed err[%d].", err);
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
      if (OB_SUCCESS == err) {
        if (OB_SUCCESS != (err = serialization::decode_i64(data_buff.get_data(), data_buff.get_position(), pos, (int64_t*)&log_id_res))) {
          TBSYS_LOG(ERROR, "deserialize log_id_res error, err=%d", err);
        } else if (OB_SUCCESS != (err = serialization::decode_i64(data_buff.get_data(), data_buff.get_position(), pos, (int64_t*)&log_seq_res))) {
          TBSYS_LOG(ERROR, "deserialize log_seq_res error, err=%d", err);
        }
      }
    }
  }

  return err;
}

int ObUpsRpcStub :: send_freeze_memtable_resp(const ObServer& name_server,
                                              const ObServer& ups_master, const int64_t schema_timestamp, const int64_t timeout_us) {
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;
  ObServer update_server;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(data_buff);
  }

  // serialize ups_master
  if (OB_SUCCESS == err) {
    err = ups_master.serialize(data_buff.get_data(), data_buff.get_capacity(),
                               data_buff.get_position());
  }

  // serialize timestamp
  if (OB_SUCCESS == err) {
    err = common::serialization::encode_vi64(data_buff.get_data(),
                                             data_buff.get_capacity(), data_buff.get_position(), schema_timestamp);
  }

  // step 1. send freeze memtable resp
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(name_server,
                                    OB_WAITING_JOB_DONE, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "send freeze memtable failed, err[%d].", err);
    }
  }

  // step 2. deserialize the response code
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

int ObUpsRpcStub :: report_freeze(const common::ObServer& name_server,
                                  const common::ObServer& ups_master, const int64_t frozen_version, const int64_t timeout_us) {
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;
  ObServer update_server;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(data_buff);
  }

  // serialize ups_master
  if (OB_SUCCESS == err) {
    err = ups_master.serialize(data_buff.get_data(), data_buff.get_capacity(),
                               data_buff.get_position());
  }

  // serialize timestamp
  if (OB_SUCCESS == err) {
    err = common::serialization::encode_vi64(data_buff.get_data(),
                                             data_buff.get_capacity(), data_buff.get_position(), frozen_version);
  }

  // step 1. send freeze memtable resp
  if (OB_SUCCESS == err) {
    err = client_mgr_->send_request(name_server,
                                    OB_UPDATE_SERVER_REPORT_FREEZE, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "send freeze memtable failed, err[%d].", err);
    }
  }

  // step 2. deserialize the response code
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

int ObUpsRpcStub :: fetch_lsync(const common::ObServer& lsync, const uint64_t log_id, const uint64_t log_seq,
                                char*& log_data, int64_t& log_len, const int64_t timeout_us) {
  int err = OB_SUCCESS;

  ObDataBuffer data_buff;

  if (NULL == client_mgr_) {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  } else {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. serialize info
  if (OB_SUCCESS == err) {
    if (OB_SUCCESS != (err = serialization::encode_i64(data_buff.get_data(), data_buff.get_capacity(),
                                                       data_buff.get_position(), (int64_t)log_id))) {
      TBSYS_LOG(WARN, "Serialize log_id error, err=%d", err);
    } else if (OB_SUCCESS != (err = serialization::encode_i64(data_buff.get_data(), data_buff.get_capacity(),
                                                              data_buff.get_position(), (int64_t)log_seq))) {
      TBSYS_LOG(WARN, "Serialize log_seq error, err=%d", err);
    }
  }

  // step 2. send request to register
  if (OB_SUCCESS == err) {
    int64_t send_bgn_time = tbsys::CTimeUtil::getMonotonicTime();

    err = client_mgr_->send_request(lsync,
                                    OB_LSYNC_FETCH_LOG, DEFAULT_VERSION, timeout_us, data_buff);
    if (OB_SUCCESS != err && OB_RESPONSE_TIME_OUT != err) {
      TBSYS_LOG(ERROR, "send request to lsync failed err[%d].", err);
    } else if (OB_RESPONSE_TIME_OUT == err) {
      int64_t send_end_time = tbsys::CTimeUtil::getMonotonicTime();
      int64_t left_time = timeout_us - (send_end_time - send_bgn_time);
      if (left_time > 0) {
        usleep(left_time);
      }
    }
  }

  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err) {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    } else if (OB_SUCCESS != (err = result_code.result_code_) && OB_NEED_RETRY != err) {
      TBSYS_LOG(ERROR, "result_code is error: result_code_=%d", result_code.result_code_);
    } else if (OB_NEED_RETRY == err) {
      TBSYS_LOG(DEBUG, "no data, retry");
    } else {
      if (OB_SUCCESS != (err = serialization::decode_i64(data_buff.get_data(), data_buff.get_position(), pos, (int64_t*)&log_len))) {
        TBSYS_LOG(WARN, "Deserialize log_len error, err=%d", err);
      } else {
        log_data = data_buff.get_data() + pos;
        TBSYS_LOG(DEBUG, "log_data=%p log_len=%ld", log_data, log_len);
      }
    }
  }

  return err;
}

int ObUpsRpcStub :: get_thread_buffer_(ObDataBuffer& data_buff) {
  int err = OB_SUCCESS;

  ThreadSpecificBuffer::Buffer* rpc_buffer = NULL;
  // get buffer for rpc send and receive
  ObUpdateServerMain* obj = ObUpdateServerMain::get_instance();
  if (NULL == obj) {
    TBSYS_LOG(ERROR, "get ObUpdateServerMain instance failed.");
  } else {
    const ObUpdateServer& server = obj->get_update_server();
    rpc_buffer = server.get_rpc_buffer();
    if (NULL == rpc_buffer) {
      TBSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
      err = OB_ERROR;
    } else {
      rpc_buffer->reset();
      data_buff.set_data(rpc_buffer->current(), rpc_buffer->remain());
    }
  }

  return err;
}
}
}



