/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include "nameserver/nameserver_worker.h"
#include "nameserver/nameserver_rpc_stub.h"
#include "common/ob_schema.h"
#include "common/ob_define.h"
namespace sb {
namespace nameserver {
using namespace common;
NameServerRpcStub::NameServerRpcStub()
  : thread_buffer_(NULL) {
}

NameServerRpcStub::~NameServerRpcStub() {
}

int NameServerRpcStub::init(const ObClientManager* client_mgr, common::ThreadSpecificBuffer* tsbuffer) {
  OB_ASSERT(NULL != client_mgr);
  OB_ASSERT(NULL != tsbuffer);
  ObCommonRpcStub::init(client_mgr);
  thread_buffer_ = tsbuffer;
  return OB_SUCCESS;
}

int NameServerRpcStub::slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout) {
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
                                    OB_SLAVE_REG, DEFAULT_VERSION, timeout, data_buff);
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

  // step 3. deserialize fetch param
  if (OB_SUCCESS == err) {
    err = fetch_param.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "deserialize fetch param failed, err[%d]", err);
    }
  }

  return err;
}

int NameServerRpcStub::get_thread_buffer_(common::ObDataBuffer& data_buffer) {
  int ret = OB_SUCCESS;
  if (NULL == thread_buffer_) {
    TBSYS_LOG(ERROR, "thread_buffer_ = NULL");
    ret = OB_ERROR;
  } else {
    common::ThreadSpecificBuffer::Buffer* buff = thread_buffer_->get_buffer();
    if (NULL == buff) {
      TBSYS_LOG(ERROR, "thread_buffer_ = NULL");
      ret = OB_ERROR;
    } else {
      buff->reset();
      data_buffer.set_data(buff->current(), buff->remain());
    }
  }
  return ret;
}

int NameServerRpcStub::set_obi_role(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us) {
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_) {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf))) {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  } else if (OB_SUCCESS != (ret = role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    TBSYS_LOG(WARN, "failed to serialize role, err=%d", ret);
  } else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_SET_OBI_ROLE, DEFAULT_VERSION, timeout_us, msgbuf))) {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  } else {
    ObResultCode result_code;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    } else if (OB_SUCCESS != result_code.result_code_) {
      TBSYS_LOG(WARN, "failed to set obi role, err=%d", result_code.result_code_);
      ret = result_code.result_code_;
    }
  }
  return ret;
}

int NameServerRpcStub::switch_schema(const common::ObServer& ups, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us) {
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_) {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf))) {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  } else if (OB_SUCCESS != (ret = schema_manager.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    TBSYS_LOG(ERROR, "failed to serialize schema, err=%d", ret);
  } else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_SWITCH_SCHEMA, DEFAULT_VERSION, timeout_us, msgbuf))) {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  } else {
    ObResultCode result;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    } else if (OB_SUCCESS != result.result_code_) {
      TBSYS_LOG(WARN, "failed to switch schema, err=%d", result.result_code_);
      ret = result.result_code_;
    } else {
      char server_buf[OB_IP_STR_BUFF];
      ups.to_string(server_buf, OB_IP_STR_BUFF);
      TBSYS_LOG(INFO, "send up_switch_schema, ups=%s schema_version=%ld", server_buf, schema_manager.get_version());
    }
  }
  return ret;
}

int NameServerRpcStub::migrate_tablet(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObRange& range, bool keey_src, const int64_t timeout_us) {
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_) {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf))) {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  } else if (OB_SUCCESS != (ret = range.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    TBSYS_LOG(ERROR, "failed to serialize rage, err=%d", ret);
  } else if (OB_SUCCESS != (ret = dest_cs.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    TBSYS_LOG(ERROR, "failed to serialize dest_cs, err=%d", ret);
  } else if (OB_SUCCESS != (ret = common::serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), keey_src))) {
    TBSYS_LOG(ERROR, "failed to serialize keey_src, err=%d", ret);
  } else if (OB_SUCCESS != (ret = client_mgr_->send_request(src_cs, OB_CS_MIGRATE, DEFAULT_VERSION, timeout_us, msgbuf))) {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  } else {
    ObResultCode result;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    } else if (OB_SUCCESS != result.result_code_) {
      TBSYS_LOG(WARN, "failed to migrate tablet, err=%d", result.result_code_);
      ret = result.result_code_;
    } else {
    }
  }
  return ret;
}

int NameServerRpcStub::create_tablet(const common::ObServer& cs, const common::ObRange& range, const int64_t mem_version, const int64_t timeout_us) {
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_) {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf))) {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  } else if (OB_SUCCESS != (ret = range.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    TBSYS_LOG(ERROR, "failed to serialize rage, err=%d", ret);
  } else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), mem_version))) {
    TBSYS_LOG(ERROR, "failed to serialize keey_src, err=%d", ret);
  } else if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_CS_CREATE_TABLE, DEFAULT_VERSION, timeout_us, msgbuf))) {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  } else {
    ObResultCode result;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    } else if (OB_SUCCESS != result.result_code_) {
      TBSYS_LOG(WARN, "failed to create tablet, err=%d", result.result_code_);
      ret = result.result_code_;
    } else {
    }
  }
  return ret;
}

int NameServerRpcStub::get_last_frozen_version(const common::ObServer& ups, const int64_t timeout_us, int64_t& frozen_version) {
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  frozen_version = -1;

  if (NULL == client_mgr_) {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf))) {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  } else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_UPS_GET_LAST_FROZEN_VERSION, DEFAULT_VERSION, timeout_us, msgbuf))) {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  } else {
    ObResultCode result;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    } else if (OB_SUCCESS != result.result_code_) {
      TBSYS_LOG(WARN, "failed to create tablet, err=%d", result.result_code_);
      ret = result.result_code_;
    } else if (OB_SUCCESS != (ret = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &frozen_version))) {
      TBSYS_LOG(WARN, "failed to deserialize frozen version ,err=%d", ret);
      frozen_version = -1;
    } else {
      TBSYS_LOG(INFO, "last_frozen_version=%ld", frozen_version);
    }
  }
  return ret;
}

int NameServerRpcStub::get_obi_role(const common::ObServer& master, const int64_t timeout_us, common::ObiRole& obi_role) {
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_) {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf))) {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  } else if (OB_SUCCESS != (ret = client_mgr_->send_request(master, OB_GET_OBI_ROLE, DEFAULT_VERSION, timeout_us, msgbuf))) {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  } else {
    ObResultCode result;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    } else if (OB_SUCCESS != result.result_code_) {
      TBSYS_LOG(WARN, "failed to get obi_role, err=%d", result.result_code_);
      ret = result.result_code_;
    } else if (OB_SUCCESS != (ret = obi_role.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      TBSYS_LOG(WARN, "failed to deserialize frozen version ,err=%d", ret);
    } else {
      TBSYS_LOG(INFO, "get obi_role from master, obi_role=%s", obi_role.get_role_str());
    }
  }
  return ret;
}

int NameServerRpcStub::heartbeat_to_cs(const common::ObServer& cs, const int64_t lease_time, const int64_t frozen_mem_version) {
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 2;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_) {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf))) {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  } else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), lease_time))) {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  } else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), frozen_mem_version))) {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  } else if (OB_SUCCESS != (ret = client_mgr_->post_request(cs, OB_REQUIRE_HEARTBEAT, MY_VERSION, msgbuf))) {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  } else {
    // success
  }
  return ret;
}

int NameServerRpcStub::heartbeat_to_ms(const common::ObServer& ms, const int64_t lease_time, const int64_t schema_version, const common::ObiRole& role) {
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  static const int MY_VERSION = 3;

  if (NULL == client_mgr_) {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf))) {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  } else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), lease_time))) {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  } else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), schema_version))) {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  } else if (OB_SUCCESS != (ret = role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  } else if (OB_SUCCESS != (ret = client_mgr_->post_request(ms, OB_REQUIRE_HEARTBEAT, MY_VERSION, msgbuf))) {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  } else {
    // success
  }
  return ret;
}


} /* nameserver */
} /* sb */

