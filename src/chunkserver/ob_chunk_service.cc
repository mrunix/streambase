/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_chunk_service.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     maoqi <maoqi@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *     fangji <fangji.hcm@taobao.com>
 *
 */

#include "ob_chunk_service.h"
#include "ob_chunk_server.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/file_directory_utils.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_aio_buffer_mgr.h"
#include "ob_tablet.h"
#include "ob_chunk_server_main.h"

using namespace sb::common;
using namespace sb::sstable;

namespace sb {
namespace chunkserver {
ObChunkService::ObChunkService()
  : chunk_server_(NULL), inited_(false),
    service_started_(false), in_register_process_(false),
    service_expired_time_(0), merge_delay_interval_(1), migrate_task_count_(0),
    lease_checker_(this), merge_task_(this), fetch_ups_task_(this) {
}

ObChunkService::~ObChunkService() {
  destroy();
}

/**
 * use ObChunkService after initialized.
 */
int ObChunkService::initialize(ObChunkServer* chunk_server) {
  int rc = OB_SUCCESS;
  if (inited_) {
    rc = OB_INIT_TWICE;
  } else if (NULL == chunk_server) {
    rc = OB_INVALID_ARGUMENT;
  } else {
    chunk_server_ = chunk_server;
    inited_ = true;
  }

  if (OB_SUCCESS == rc) {
    rc = timer_.init();
  }

  inited_ = (OB_SUCCESS == rc);
  return rc;
}

/*
 * stop service, before chunkserver stop working thread.
 */
int ObChunkService::destroy() {
  int rc = OB_SUCCESS;
  if (inited_) {
    inited_ = false;
    timer_.destroy();
    service_started_ = false;
    in_register_process_ = false;
    chunk_server_ = NULL;
  } else {
    rc = OB_NOT_INIT;
  }

  return rc;
}

/**
 * ChunkServer must fetch schema from RootServer first.
 * then provide service.
 */
int ObChunkService::start() {
  int rc = OB_SUCCESS;
  if (!inited_) {
    rc = OB_NOT_INIT;
  } else {
    rc = load_tablets();
    if (OB_SUCCESS != rc) {
      TBSYS_LOG(ERROR, "load local tablets error, rc=%d", rc);
    }
  }

  if (OB_SUCCESS == rc) {
    rc = register_self();
  }

  if (OB_SUCCESS == rc) {
    rc = timer_.schedule(lease_checker_,
                         chunk_server_->get_param().get_lease_check_interval(), false);
  }

  if (OB_SUCCESS == rc) {
    get_merge_delay_interval();
  }

  if (OB_SUCCESS == rc) {
    //for the sake of simple,just update the stat per second
    rc = timer_.schedule(stat_updater_, 1000000, true);
  }

  if (OB_SUCCESS == rc) {
    rc = timer_.schedule(fetch_ups_task_, chunk_server_->get_param().get_fetch_ups_interval(), false);
  }
  return rc;
}

int ObChunkService::load_tablets() {
  int rc = OB_SUCCESS;
  if (!inited_) {
    rc = OB_NOT_INIT;
  }

  ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
  // load tablets;
  if (OB_SUCCESS == rc) {
    int32_t size = 0;
    const int32_t* disk_no_array = tablet_manager.get_disk_manager().get_disk_no_array(size);
    if (disk_no_array != NULL && size > 0) {
      rc = tablet_manager.load_tablets(disk_no_array, size);
    } else {
      rc = OB_ERROR;
      TBSYS_LOG(ERROR, "get disk no array failed.");
    }
  }

  if (OB_SUCCESS == rc) {
    rc = tablet_manager.start_merge_thread();
    if (rc != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "start merge thread failed.");
    }
  }

  return rc;
}

int ObChunkService::register_self_busy_wait(int32_t& status) {
  int rc = OB_SUCCESS;
  status = 0;
  ObRootServerRpcStub& rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
  while (inited_) {
    rc = rs_rpc_stub.register_server(chunk_server_->get_self(), false, status);
    if (OB_SUCCESS == rc) break;
    if (OB_RESPONSE_TIME_OUT != rc && OB_NOT_INIT != rc) {
      TBSYS_LOG(ERROR, "register self to nameserver failed, rc=%d", rc);
      break;
    }
    usleep(chunk_server_->get_param().get_network_time_out());
  }
  return rc;
}

int ObChunkService::report_tablets_busy_wait() {
  int rc = OB_SUCCESS;
  ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
  while (inited_) {
    rc = tablet_manager.report_tablets();
    if (OB_SUCCESS == rc) break;
    if (OB_RESPONSE_TIME_OUT != rc) {
      TBSYS_LOG(ERROR, "report tablets to nameserver failed, rc=%d", rc);
      break;
    }
    usleep(chunk_server_->get_param().get_network_time_out());
  }
  return rc;
}

int ObChunkService::fetch_schema_busy_wait(ObSchemaManagerV2* schema) {
  int rc = OB_SUCCESS;
  if (NULL == schema) {
    TBSYS_LOG(ERROR, "invalid argument,sceham is null");
    rc = OB_INVALID_ARGUMENT;
  } else {
    while (inited_) {
      rc = chunk_server_->get_rs_rpc_stub().fetch_schema(0, *schema);
      if (OB_SUCCESS == rc) break;
      if (OB_RESPONSE_TIME_OUT != rc) {
        TBSYS_LOG(ERROR, "report tablets to nameserver failed, rc=%d", rc);
        break;
      }
      usleep(chunk_server_->get_param().get_network_time_out());
    }
  }
  return rc;
}

int ObChunkService::register_self() {
  int rc = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(ERROR, "service not initialized, cannot register_self.");
    rc = OB_NOT_INIT;
  }

  if (in_register_process_) {
    TBSYS_LOG(ERROR, "another thread is registering.");
    rc = OB_ERROR;
  } else {
    in_register_process_ = true;
  }

  ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
  //const ObChunkServerParam & param = chunk_server_->get_param();
  //ObRootServerRpcStub & rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
  int32_t status = 0;
  // register self to nameserver until success.
  if (OB_SUCCESS == rc) {
    rc = register_self_busy_wait(status);
  }

  if (OB_SUCCESS == rc) {
    // TODO init lease 10s for first startup.
    service_expired_time_ = tbsys::CTimeUtil::getTime() + 10000000;
    int64_t current_data_version = tablet_manager.get_serving_data_version();
    if (0 == status) {
      TBSYS_LOG(INFO, "system startup on first time, wait nameserver start new schema,"
                "current data version=%ld", current_data_version);
      // start chunkserver on very first time, do nothing, wait nameserver
      // launch the start_new_schema process.
      //service_started_ = true;
    } else {
      TBSYS_LOG(INFO, "chunk service start, current data version: %ld", current_data_version);
      rc = report_tablets_busy_wait();
      if (OB_SUCCESS == rc) {
        tablet_manager.report_capacity_info();
        service_started_ = true;
      }
    }
  }

  in_register_process_ = false;

  return rc;
}

/*
 * after initialize() & start(), then can handle the request.
 */
int ObChunkService::do_request(
  const int32_t packet_code,
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  int rc = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
    rc = OB_NOT_INIT;
  }

  if (OB_SUCCESS == rc) {
    if (!service_started_
        && packet_code != OB_START_MERGE
        && packet_code != OB_REQUIRE_HEARTBEAT) {
      TBSYS_LOG(ERROR, "service not started, only accept "
                "start schema message or heatbeat from nameserver.");
      rc = OB_CS_SERVICE_NOT_STARTED;
    }
  }
  if (OB_SUCCESS == rc) {
    //check lease valid.
    if (!is_valid_lease()
        && (!in_register_process_)
        && packet_code != OB_REQUIRE_HEARTBEAT) {
      // TODO re-register self??
      TBSYS_LOG(WARN, "lease expired, wait timer schedule re-register self to nameserver.");
    }
  }

  if (OB_SUCCESS != rc) {
    TBSYS_LOG(ERROR, "call func error packet_code is %d return code is %d",
              packet_code, rc);
    common::ObResultCode result;
    result.result_code_ = rc;
    // send response.
    int serialize_ret = result.serialize(out_buffer.get_data(),
                                         out_buffer.get_capacity(), out_buffer.get_position());
    if (OB_SUCCESS != serialize_ret) {
      TBSYS_LOG(ERROR, "serialize result code object failed.");
    } else {
      chunk_server_->send_response(
        packet_code + 1, version,
        out_buffer, connection, channel_id);
    }
  } else {
    switch (packet_code) {
    case OB_GET_REQUEST:
      rc = cs_get(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_SCAN_REQUEST:
      rc = cs_scan(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_BATCH_GET_REQUEST:
      rc = cs_batch_get(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_DROP_OLD_TABLETS:
      rc = cs_drop_old_tablets(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_REQUIRE_HEARTBEAT:
      rc = cs_heart_beat(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_CS_MIGRATE:
      rc = cs_migrate_tablet(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_MIGRATE_OVER:
      rc = cs_load_tablet(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_CS_CREATE_TABLE:
      rc = cs_create_tablet(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_CS_GET_MIGRATE_DEST_LOC:
      rc = cs_get_migrate_dest_loc(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_CS_DUMP_TABLET_IMAGE:
      rc = cs_dump_tablet_image(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_FETCH_STATS:
      rc = cs_fetch_stats(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_CS_START_GC:
      rc = cs_start_gc(version, channel_id, connection, in_buffer, out_buffer);
      break;
    case OB_UPS_RELOAD_CONF:
      rc = cs_reload_conf(version, channel_id, connection, in_buffer, out_buffer);
      break;
    default:
      rc = OB_ERROR;
      break;
    }
  }
  return rc;
}

int ObChunkService::cs_batch_get(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  // TODO  not implement yet.
  UNUSED(version);
  UNUSED(channel_id);
  UNUSED(connection);
  UNUSED(in_buffer);
  UNUSED(out_buffer);
  return OB_SUCCESS;
}

int ObChunkService::cs_get(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_GET_VERSION = 1;
  uint64_t table_id = OB_INVALID_ID;
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  //char msg_buff[MAX_RESULT_MESSAGE_LENGTH];
  //result_msg.message_.assign_buffer(msg_buff, MAX_RESULT_MESSAGE_LENGTH);
  if (version != CS_GET_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  ObScanner* scanner = GET_TSI(ObScanner);
  ObGetParam* get_param_ptr = GET_TSI(ObGetParam);

  if (NULL == scanner || NULL == get_param_ptr) {
    TBSYS_LOG(ERROR, "failed to get thread local get_param or scanner, "
              "scanner=%, get_param_ptr=%p", scanner, get_param_ptr);
    rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
  }

  if (OB_SUCCESS == rc.result_code_) {
    scanner->reset();
    rc.result_code_ = get_param_ptr->deserialize(
                        in_buffer.get_data(), in_buffer.get_capacity(),
                        in_buffer.get_position());

    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_get input param error.");
    }
  }

  if (OB_SUCCESS == rc.result_code_) {
    if (get_param_ptr->get_cell_size() <= 0) {
      TBSYS_LOG(WARN, "invalid param, cell_size=%ld", get_param_ptr->get_cell_size());
      rc.result_code_ = OB_INVALID_ARGUMENT;
    } else if (NULL == get_param_ptr->get_row_index() || get_param_ptr->get_row_size() <= 0) {
      TBSYS_LOG(WARN, "invalid get param, row_index=%p, row_size=%ld",
                get_param_ptr->get_row_index(), get_param_ptr->get_row_size());
      rc.result_code_ = OB_INVALID_ARGUMENT;
    } else {
      // FIXME: the count is not very accurate,we just inc the get count of the first table
      table_id = (*get_param_ptr)[0]->table_id_;
      OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_GET_COUNT);
      OB_CHUNK_STAT(inc, ObChunkServerStatManager::META_TABLE_ID, ObChunkServerStatManager::INDEX_META_REQUEST_COUNT);
      rc.result_code_ =
        chunk_server_->get_tablet_manager().get(*get_param_ptr, *scanner);
    }
  }

  // send response. return result code anyway.
  int serialize_ret = rc.serialize(out_buffer.get_data(),
                                   out_buffer.get_capacity(), out_buffer.get_position());
  if (OB_SUCCESS != serialize_ret) {
    TBSYS_LOG(ERROR, "serialize result code object failed.");
  }

  // if get return success, we can return the scanner.
  if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret) {
    serialize_ret = scanner->serialize(out_buffer.get_data(),
                                       out_buffer.get_capacity(), out_buffer.get_position());
    if (OB_SUCCESS != serialize_ret) {
      TBSYS_LOG(ERROR, "serialize ObScanner failed.");
    }
  }

  if (OB_SUCCESS == serialize_ret) {
    OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_GET_BYTES, out_buffer.get_position());
    chunk_server_->send_response(
      OB_GET_RESPONSE, CS_GET_VERSION,
      out_buffer, connection, channel_id);
  }
  int64_t end_time = tbsys::CTimeUtil::getTime();
  OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_GET_TIME, end_time - start_time);

  return rc.result_code_;
}

int ObChunkService::cs_scan(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_SCAN_VERSION = 1;
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  if (version != CS_SCAN_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  uint64_t  table_id = OB_INVALID_ID; //for stat
  ObScanner* scanner = GET_TSI(ObScanner);
  common::ObScanParam* scan_param_ptr = GET_TSI(ObScanParam);

  if (NULL == scanner || NULL == scan_param_ptr) {
    TBSYS_LOG(ERROR, "failed to get thread local scan_param or scanner, "
              "scanner=%, scan_param_ptr=%p", scanner, scan_param_ptr);
    rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
  }

  if (OB_SUCCESS == rc.result_code_) {
    scanner->reset();
    rc.result_code_ = scan_param_ptr->deserialize(
                        in_buffer.get_data(), in_buffer.get_capacity(),
                        in_buffer.get_position());
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_scan input scan param error.");
    } else {
      // force every client scan request use preread mode.
      // scan_param_ptr->set_read_mode(ObScanParam::PREREAD);
    }
  }

  if (OB_SUCCESS == rc.result_code_) {
    table_id = scan_param_ptr->get_table_id();
    OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_SCAN_COUNT);
    OB_CHUNK_STAT(inc, ObChunkServerStatManager::META_TABLE_ID, ObChunkServerStatManager::INDEX_META_REQUEST_COUNT);
    rc.result_code_ = chunk_server_->get_tablet_manager().scan(*scan_param_ptr, *scanner);
  }

  // send response.
  int serialize_ret = rc.serialize(out_buffer.get_data(),
                                   out_buffer.get_capacity(), out_buffer.get_position());
  if (OB_SUCCESS != serialize_ret) {
    TBSYS_LOG(ERROR, "serialize result code object failed.");
  }

  // if scan return success , we can return scanner.
  if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret) {
    serialize_ret = scanner->serialize(out_buffer.get_data(),
                                       out_buffer.get_capacity(), out_buffer.get_position());
    if (OB_SUCCESS != serialize_ret) {
      TBSYS_LOG(ERROR, "serialize ObScanner failed.");
    }
  }

  if (OB_SUCCESS == serialize_ret) {
    OB_CHUNK_STAT(inc, table_id,
                  ObChunkServerStatManager::INDEX_SCAN_BYTES, out_buffer.get_position());
    chunk_server_->send_response(
      OB_SCAN_RESPONSE, CS_SCAN_VERSION,
      out_buffer, connection, channel_id);
  }

  int64_t end_time = tbsys::CTimeUtil::getTime();
  OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_SCAN_TIME, end_time - start_time);

  if (NULL != scan_param_ptr && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG) {
    char range_buf[OB_RANGE_STR_BUFSIZ];
    scan_param_ptr->get_range()->to_string(range_buf, OB_RANGE_STR_BUFSIZ);
    TBSYS_LOG(DEBUG, "scan request:table_id:%lu, range=%s, "
              "scan size=%ld, column size=%ld, direction=%d, read mode=%d, rc=%d, consume=%ld",
              table_id, range_buf,
              scan_param_ptr->get_scan_size(), scan_param_ptr->get_column_id_size(),
              scan_param_ptr->get_scan_direction(), scan_param_ptr->get_read_mode(),
              rc.result_code_, end_time - start_time);
  }

  int status = OB_SUCCESS;
  ObThreadAIOBufferMgrArray* aio_buf_mgr_array = GET_TSI(ObThreadAIOBufferMgrArray);
  if (NULL != aio_buf_mgr_array) {
    status = aio_buf_mgr_array->wait_all_aio_buf_mgr_free(10 * 1000000);
    if (OB_AIO_TIMEOUT == status) {
      //TODO:stop current thread.
      TBSYS_LOG(WARN, "failed to wait all aio buffer manager free, stop current thread");
    }
  }

  return rc.result_code_;
}

int ObChunkService::cs_drop_old_tablets(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_DROP_OLD_TABLES_VERSION = 1;
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;
  //char msg_buff[24];
  //rc.message_.assign(msg_buff, 24);
  if (version != CS_DROP_OLD_TABLES_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  int64_t memtable_frozen_version = 0;

  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = common::serialization::decode_vi64(
                        in_buffer.get_data(), in_buffer.get_capacity(),
                        in_buffer.get_position(), &memtable_frozen_version);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse drop_old_tablets input memtable_frozen_version param error.");
    }
  }

  TBSYS_LOG(INFO, "drop_old_tablets: memtable_frozen_version:%ld", memtable_frozen_version);

  int serialize_ret = rc.serialize(out_buffer.get_data(),
                                   out_buffer.get_capacity(), out_buffer.get_position());
  if (serialize_ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "rc.serialize error");
  }

  if (OB_SUCCESS == serialize_ret) {
    chunk_server_->send_response(
      OB_DROP_OLD_TABLETS_RESPONSE,
      CS_DROP_OLD_TABLES_VERSION,
      out_buffer, connection, channel_id);
  }


  // call tablet_manager_ drop tablets.
  //ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
  //rc.result_code_ = tablet_manager.drop_tablets(memtable_frozen_version);

  return rc.result_code_;
}

/*
 * int cs_heart_beat(const int64_t lease_duration);
 */
int ObChunkService::cs_heart_beat(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_HEART_BEAT_VERSION = 2;
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;
  //char msg_buff[24];
  //rc.message_.assign(msg_buff, 24);
  UNUSED(channel_id);
  UNUSED(connection);
  UNUSED(out_buffer);
  //if (version != CS_HEART_BEAT_VERSION)
  //{
  //  rc.result_code_ = OB_ERROR_FUNC_VERSION;
  //}

  int64_t lease_duration = 0;
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = common::serialization::decode_vi64(
                        in_buffer.get_data(), in_buffer.get_capacity(),
                        in_buffer.get_position(), &lease_duration);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_heart_beat input lease_duration param error.");
    } else {
      TBSYS_LOG(DEBUG, "cs_heart_beat: lease_duration=%ld", lease_duration);
    }
  }

  TBSYS_LOG(DEBUG, "cs_heart_beat,version:%d,CS_HEART_BEAT_VERSION:%d", version, CS_HEART_BEAT_VERSION);

  if (version == CS_HEART_BEAT_VERSION) {
    int64_t frozen_version = 0;
    if (OB_SUCCESS == rc.result_code_) {
      rc.result_code_ = common::serialization::decode_vi64(
                          in_buffer.get_data(), in_buffer.get_capacity(),
                          in_buffer.get_position(), &frozen_version);
      if (OB_SUCCESS != rc.result_code_) {
        TBSYS_LOG(ERROR, "parse cs_heart_beat input frozen_version param error.");
      } else {
        TBSYS_LOG(DEBUG, "cs_heart_beat: frozen_version=%ld", frozen_version);
      }
    }

    if (OB_SUCCESS == rc.result_code_ && service_started_) {
      int64_t wait_time = 0;
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
      if (frozen_version > chunk_server_->get_tablet_manager().get_serving_data_version()) {
        if (frozen_version > merge_task_.get_last_frozen_version()) {
          TBSYS_LOG(INFO, "pending a new frozen version need merge:%ld,last:%ld",
                    frozen_version, merge_task_.get_last_frozen_version());
          merge_task_.set_frozen_version(frozen_version);
        }
        if (!merge_task_.is_scheduled()
            && tablet_manager.get_chunk_merge().can_launch_next_round(frozen_version)) {
          srand(tbsys::CTimeUtil::getTime());
          wait_time = random() % merge_delay_interval_;
          // wait one more minute for ensure slave updateservers sync frozen version.
          wait_time += chunk_server_->get_param().get_merge_delay_for_lsync();
          TBSYS_LOG(INFO, "launch a new merge process after wait %ld us.", wait_time);
          timer_.schedule(merge_task_, wait_time, false);  //async
          merge_task_.set_scheduled();
        }
      }
    }
  }

  // send heartbeat request to root_server
  if (OB_SUCCESS == rc.result_code_) {
    ObRootServerRpcStub& rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
    rc.result_code_ = rs_rpc_stub.async_heartbeat(chunk_server_->get_self());
  }

  if (OB_SUCCESS == rc.result_code_) {
    service_expired_time_ = tbsys::CTimeUtil::getTime() + lease_duration;
  }

  /*
  int serialize_ret = rc.serialize(out_buffer.get_data(),
      out_buffer.get_capacity(), out_buffer.get_position());
  if (serialize_ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "rc.serialize error");
  }

  if (OB_SUCCESS == serialize_ret)
  {
    chunk_server_->send_response(
        OB_HEARTBEAT_RESPONSE,
        CS_HEART_BEAT_VERSION,
        out_buffer, connection, channel_id);
  }
  */
  return rc.result_code_;
}

int ObChunkService::cs_create_tablet(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_CREATE_TABLE_VERSION = 1;
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;
  if (version != CS_CREATE_TABLE_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  }


  ObRange range;
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = range.deserialize(
                        in_buffer.get_data(), in_buffer.get_capacity(),
                        in_buffer.get_position());
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_create_tablet input range param error.");
    }
  }

  TBSYS_LOG(INFO, "cs_create_tablet, dump input range below:");
  range.hex_dump(TBSYS_LOG_LEVEL_INFO);

  // get last frozen memtable version for update
  int64_t last_frozen_memtable_version = 0;
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = serialization::decode_vi64(
                        in_buffer.get_data(), in_buffer.get_capacity(),
                        in_buffer.get_position(), &last_frozen_memtable_version);
    /*
    ObServer update_server;
    rc.result_code_ = chunk_server_->get_rs_rpc_stub().get_update_server(update_server);
    ObRootServerRpcStub update_stub;
    if (OB_SUCCESS == rc.result_code_)
    {
      rc.result_code_ = update_stub.init(update_server, &chunk_server_->get_client_manager());
    }
    if (OB_SUCCESS == rc.result_code_)
    {
      rc.result_code_ = update_stub.get_last_frozen_memtable_version(last_frozen_memtable_version);
    }
    */
  }

  if (OB_SUCCESS == rc.result_code_) {
    TBSYS_LOG(DEBUG, "create tablet, last_frozen_memtable_version=%ld",
              last_frozen_memtable_version);
    rc.result_code_ = chunk_server_->get_tablet_manager().create_tablet(
                        range, last_frozen_memtable_version);
  }

  int serialize_ret = rc.serialize(out_buffer.get_data(),
                                   out_buffer.get_capacity(), out_buffer.get_position());
  if (serialize_ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "cs_create_tablet rc.serialize error");
  }

  if (OB_SUCCESS == serialize_ret) {
    chunk_server_->send_response(
      OB_CS_CREATE_TABLE_RESPONSE,
      CS_CREATE_TABLE_VERSION,
      out_buffer, connection, channel_id);
  }


  // call tablet_manager_ drop tablets.

  return rc.result_code_;
}


int ObChunkService::cs_load_tablet(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_LOAD_TABLET_VERSION = 1;
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;
  if (version != CS_LOAD_TABLET_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  ObRange range;
  int64_t num_file = 0;
  //deserialize ObRange
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = range.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
                                        in_buffer.get_position());
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_load_tablet range param error.");
    } else {
      char range_buf[OB_RANGE_STR_BUFSIZ];
      range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(DEBUG, "cs_load_tablet dump range <%s>", range_buf);
    }
  }

  int32_t dest_disk_no = 0;
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = serialization::decode_vi32(in_buffer.get_data(), in_buffer.get_capacity(),
                                                 in_buffer.get_position(), &dest_disk_no);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse dest_disk_no range param error.");
    } else {
      TBSYS_LOG(INFO, "cs_load_tablet dest_disk_no=%d ", dest_disk_no);
    }
  }

  // deserialize tablet_version;
  int64_t tablet_version = 0;
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
                                                 in_buffer.get_capacity(), in_buffer.get_position(), &tablet_version);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_load_tablet tablet_version param error.");
    } else {
      TBSYS_LOG(INFO, "cs_load_tablet tablet_version = %ld ", tablet_version);
    }
  }

  uint64_t crc_sum = 0;
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
                                                 in_buffer.get_capacity(), in_buffer.get_position(), (int64_t*)(&crc_sum));
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_load_tablet crc_sum param error.");
    } else {
      TBSYS_LOG(INFO, "cs_load_tablet crc_sum = %lu ", crc_sum);
    }
  }

  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = common::serialization::decode_vi64(in_buffer.get_data(),
                                                         in_buffer.get_capacity(), in_buffer.get_position(), &num_file);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_load_tablet number of sstable  param error.");
    } else {
      TBSYS_LOG(INFO, "cs_load_tablet num_file = %ld ", num_file);
    }
  }

  ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
  if (OB_SUCCESS == rc.result_code_ && num_file > 0) {
    char (*path)[OB_MAX_FILE_NAME_LENGTH];
    char* path_buf = static_cast<char*>(ob_malloc(num_file * OB_MAX_FILE_NAME_LENGTH));
    if (NULL == path_buf) {
      TBSYS_LOG(ERROR, "failed to allocate memory for path array.");
      rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      path = new(path_buf)char[num_file][OB_MAX_FILE_NAME_LENGTH];
    }

    int64_t len = 0;
    if (OB_SUCCESS == rc.result_code_) {
      for (int64_t idx = 0; idx < num_file; idx++) {
        if (NULL == common::serialization::decode_vstr(in_buffer.get_data(),
                                                       in_buffer.get_capacity(), in_buffer.get_position(),
                                                       path[idx], OB_MAX_FILE_NAME_LENGTH, &len)) {
          rc.result_code_ = OB_ERROR;
          TBSYS_LOG(ERROR, "parse cs_load_tablet dest_path param error.");
          break;
        } else {
          TBSYS_LOG(INFO, "parse cs_load_tablet dest_path [%ld] = %s", idx, path[idx]);
        }
      }
    }

    if (OB_SUCCESS == rc.result_code_) {
      rc.result_code_ = tablet_manager.dest_load_tablet(range, path, num_file, tablet_version, dest_disk_no, crc_sum);
      if (OB_SUCCESS != rc.result_code_ && OB_CS_MIGRATE_IN_EXIST != rc.result_code_) {
        TBSYS_LOG(ERROR, "ObTabletManager::dest_load_tablet error, rc=%d", rc.result_code_);
      }
    }

    if (NULL != path_buf) {
      ob_free(path_buf);
    }
  } else if (OB_SUCCESS == rc.result_code_ && num_file == 0) {
    rc.result_code_ = tablet_manager.dest_load_tablet(range, NULL, 0, tablet_version, dest_disk_no, crc_sum);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "ObTabletManager::dest_load_tablet error, rc=%d", rc.result_code_);
    }
  }

  //send response to src chunkserver
  int serialize_ret = rc.serialize(out_buffer.get_data(),
                                   out_buffer.get_capacity(), out_buffer.get_position());
  if (serialize_ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "rc.serialize error");
  }

  if (OB_SUCCESS == serialize_ret) {
    chunk_server_->send_response(
      OB_CS_MIGRATE_RESPONSE,
      CS_LOAD_TABLET_VERSION,
      out_buffer, connection, channel_id);
  }
  return rc.result_code_;
}

int ObChunkService::cs_migrate_tablet(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_MIGRATE_TABLET_VERSION = 1;
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;

  if (version != CS_MIGRATE_TABLET_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  }


  ObRange range;
  ObServer dest_server;
  bool keep_src = false;

  //deserialize ObRange
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = range.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
                                        in_buffer.get_position());
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_migrate_tablet range param error.");
    }
  }

  //deserialize destination chunkserver
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = dest_server.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
                                              in_buffer.get_position());
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_migrate_tablet dest_server param error.");
    }
  }

  //deserialize migrate type(copy or move)
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = common::serialization::decode_bool(in_buffer.get_data(), in_buffer.get_capacity(),
                                                         in_buffer.get_position(), &keep_src);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_migrate_tablet keep_src param error.");
    }
  }

  char range_buf[OB_RANGE_STR_BUFSIZ];
  if (OB_SUCCESS == rc.result_code_) {
    char ip_addr_string[OB_IP_STR_BUFF];
    range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
    dest_server.to_string(ip_addr_string, OB_IP_STR_BUFF);
    TBSYS_LOG(INFO, "begin migrate_tablet %s, dest_server=%s, keep_src=%d",
              range_buf, ip_addr_string, keep_src);
  }

  int32_t max_migrate_task_count = chunk_server_->get_param().get_max_migrate_task_count();
  if (migrate_task_count_ > (uint32_t)max_migrate_task_count) {
    TBSYS_LOG(ERROR, "current migrate task count = %u, exceeded max = %d",
              migrate_task_count_, max_migrate_task_count);
    rc.result_code_ = OB_ERROR;
  }

  atomic_inc(&migrate_task_count_);


  //response to root server first
  int serialize_ret = rc.serialize(out_buffer.get_data(),
                                   out_buffer.get_capacity(), out_buffer.get_position());
  if (serialize_ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "migrate_tablet rc.serialize error");
  }

  if (OB_SUCCESS == serialize_ret) {
    chunk_server_->send_response(
      OB_MIGRATE_OVER_RESPONSE,
      CS_MIGRATE_TABLET_VERSION,
      out_buffer, connection, channel_id);
  }

  ObRootServerRpcStub cs_rpc_stub;

  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = cs_rpc_stub.init(dest_server, &(chunk_server_->get_client_manager()));
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "migrate_tablet init cs_rpc_stub error.");
    }
  }

  ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
  char (*dest_path)[OB_MAX_FILE_NAME_LENGTH] = NULL;
  char (*src_path)[OB_MAX_FILE_NAME_LENGTH] = NULL;
  char* dest_path_buf = static_cast<char*>(ob_malloc(ObTablet::MAX_SSTABLE_PER_TABLET * OB_MAX_FILE_NAME_LENGTH));
  char* src_path_buf  = static_cast<char*>(ob_malloc(ObTablet::MAX_SSTABLE_PER_TABLET * OB_MAX_FILE_NAME_LENGTH));
  if (NULL == src_path_buf || NULL == dest_path_buf) {
    TBSYS_LOG(ERROR, "migrate_tablet failed to allocate memory for path array.");
    rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    src_path = new(src_path_buf)char[ObTablet::MAX_SSTABLE_PER_TABLET][OB_MAX_FILE_NAME_LENGTH];
    dest_path = new(dest_path_buf)char[ObTablet::MAX_SSTABLE_PER_TABLET][OB_MAX_FILE_NAME_LENGTH];
  }

  int64_t num_file = 0;
  int64_t tablet_version = 0;
  int32_t dest_disk_no = 0;
  uint64_t crc_sum = 0;

  ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = tablet_manager.migrate_tablet(range,
                                                    dest_server, src_path, dest_path, num_file, tablet_version, dest_disk_no, crc_sum);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "ObTabletManager::migrate_tablet <%s> error, rc.result_code_=%d",
                range_buf, rc.result_code_);
    }
  }


  //send request to load sstable
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = cs_rpc_stub.dest_load_tablet(dest_server,
                                                   range, dest_disk_no, tablet_version, crc_sum, num_file, dest_path);
    if (OB_CS_MIGRATE_IN_EXIST == rc.result_code_) {
      TBSYS_LOG(INFO, "dest server already hold this tablet, consider it done.");
      rc.result_code_ = OB_SUCCESS;
    }
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "dest server load tablet <%s> error ,rc.code=%d",
                range_buf, rc.result_code_);
    }
  }


  ObRootServerRpcStub& rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
  //report migrate info to root server
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = rs_rpc_stub.migrate_over(range,
                                               chunk_server_->get_self(), dest_server, keep_src, tablet_version);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "report migrate tablet <%s> over error, rc.code=%d",
                range_buf, rc.result_code_);
    } else {
      TBSYS_LOG(INFO, "report migrate tablet <%s> over to nameserver success.",
                range_buf);
    }
  }

  if (OB_SUCCESS == rc.result_code_ && false == keep_src) {
    // migrate/move , set local tablet merged,
    // will be discarded in next time merge process .
    ObTablet* src_tablet = NULL;
    rc.result_code_ = tablet_image.acquire_tablet(range,
                                                  ObMultiVersionTabletImage::SCAN_FORWARD, 0, src_tablet);
    if (OB_SUCCESS == rc.result_code_ && NULL != src_tablet) {
      TBSYS_LOG(INFO, "src tablet set merged, version=%ld,disk=%d, range:",
                src_tablet->get_data_version(), src_tablet->get_disk_no());
      src_tablet->get_range().hex_dump(TBSYS_LOG_LEVEL_INFO);
      src_tablet->set_merged();
      tablet_image.write(
        src_tablet->get_data_version(), src_tablet->get_disk_no());
    }

    if (NULL != src_tablet) {
      tablet_image.release_tablet(src_tablet);
    }

  }

  if (NULL != src_path_buf) {
    ob_free(src_path_buf);
  }
  if (NULL != dest_path_buf) {
    ob_free(dest_path_buf);
  }
  TBSYS_LOG(INFO, "migrate_tablet finish rc.code=%d", rc.result_code_);
  atomic_dec(&migrate_task_count_);
  return rc.result_code_;
}

int ObChunkService::cs_get_migrate_dest_loc(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_GET_MIGRATE_DEST_LOC_VERSION = 1;
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;

  if (version != CS_GET_MIGRATE_DEST_LOC_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  }


  int64_t occupy_size = 0;
  //deserialize occupy_size
  if (OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
                                                 in_buffer.get_capacity(), in_buffer.get_position(), &occupy_size);
    if (OB_SUCCESS != rc.result_code_) {
      TBSYS_LOG(ERROR, "parse cs_get_migrate_dest_loc occupy_size param error.");
    } else {
      TBSYS_LOG(INFO, "cs_get_migrate_dest_loc occupy_size =%ld", occupy_size);
    }
  }

  int32_t disk_no = 0;
  char dest_directory[OB_MAX_FILE_NAME_LENGTH];

  ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
  disk_no = tablet_manager.get_disk_manager().get_disk_for_migrate();
  if (disk_no <= 0) {
    TBSYS_LOG(ERROR, "get wrong disk no =%d", disk_no);
    rc.result_code_ = OB_ERROR;
  } else {
    rc.result_code_ = get_sstable_directory(disk_no, dest_directory, OB_MAX_FILE_NAME_LENGTH);
  }

  //response to root server first
  int serialize_ret = rc.serialize(out_buffer.get_data(),
                                   out_buffer.get_capacity(), out_buffer.get_position());
  if (serialize_ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "rc.serialize error");
  }

  // ifreturn success , we can return disk_no & dest_directory.
  if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret) {
    serialize_ret = serialization::encode_vi32(out_buffer.get_data(),
                                               out_buffer.get_capacity(), out_buffer.get_position(), disk_no);
    if (OB_SUCCESS != serialize_ret) {
      TBSYS_LOG(ERROR, "serialize disk_no failed.");
    }
  }

  if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret) {
    ObString dest_string(OB_MAX_FILE_NAME_LENGTH,
                         strlen(dest_directory), dest_directory);
    serialize_ret = dest_string.serialize(out_buffer.get_data(),
                                          out_buffer.get_capacity(), out_buffer.get_position());
    if (OB_SUCCESS != serialize_ret) {
      TBSYS_LOG(ERROR, "serialize dest_directory failed.");
    }
  }

  if (OB_SUCCESS == serialize_ret) {
    chunk_server_->send_response(
      OB_CS_GET_MIGRATE_DEST_LOC_RESPONSE,
      CS_GET_MIGRATE_DEST_LOC_VERSION,
      out_buffer, connection, channel_id);
  }

  return rc.result_code_;
}

int ObChunkService::cs_dump_tablet_image(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_DUMP_TABLET_IMAGE_VERSION = 1;
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;

  int32_t index = 0;
  int32_t disk_no = 0;

  char* dump_buf = NULL;
  const int64_t dump_size = 1024 * 1024LL;

  int64_t pos = 0;
  ObTabletImage* tablet_image = NULL;

  if (version != CS_DUMP_TABLET_IMAGE_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  } else if (OB_SUCCESS != (rc.result_code_ =
                              serialization::decode_vi32(in_buffer.get_data(),
                                                         in_buffer.get_capacity(), in_buffer.get_position(), &index))) {
    TBSYS_LOG(WARN, "parse cs_dump_tablet_image index param error.");
  } else if (OB_SUCCESS != (rc.result_code_ =
                              serialization::decode_vi32(in_buffer.get_data(),
                                                         in_buffer.get_capacity(), in_buffer.get_position(), &disk_no))) {
    TBSYS_LOG(WARN, "parse cs_dump_tablet_image disk_no param error.");
  } else if (disk_no <= 0) {
    TBSYS_LOG(WARN, "cs_dump_tablet_image input param error, "
              "disk_no=%d", disk_no);
    rc.result_code_ = OB_INVALID_ARGUMENT;
  } else if (NULL == (dump_buf = static_cast<char*>(ob_malloc(dump_size)))) {
    rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(ERROR, "allocate memory for serialization failed.");
  } else if (OB_SUCCESS != (rc.result_code_ =
                              chunk_server_->get_tablet_manager().get_serving_tablet_image().
                              serialize(index, disk_no, dump_buf, dump_size, pos))) {
    TBSYS_LOG(WARN, "serialize tablet image failed. disk_no=%d", disk_no);
  }

  //response to root server first
  int serialize_ret = rc.serialize(out_buffer.get_data(),
                                   out_buffer.get_capacity(), out_buffer.get_position());
  if (serialize_ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "rc.serialize error");
  }

  // ifreturn success , we can return dump_buf
  if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret) {
    ObString return_dump_obj(pos, pos, dump_buf);
    serialize_ret = return_dump_obj.serialize(out_buffer.get_data(),
                                              out_buffer.get_capacity(), out_buffer.get_position());
    if (OB_SUCCESS != serialize_ret) {
      TBSYS_LOG(ERROR, "serialize return_dump_obj failed.");
    }
  }

  if (OB_SUCCESS == serialize_ret) {
    chunk_server_->send_response(
      OB_CS_DUMP_TABLET_IMAGE_RESPONSE,
      CS_DUMP_TABLET_IMAGE_VERSION,
      out_buffer, connection, channel_id);
  }

  if (NULL != dump_buf) {
    ob_free(dump_buf);
    dump_buf = NULL;
  }
  if (NULL != tablet_image) {
    delete(tablet_image);
    tablet_image = NULL;
  }

  return rc.result_code_;
}

int ObChunkService::cs_fetch_stats(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  UNUSED(in_buffer);
  const int32_t CS_FETCH_STATS_VERSION = 1;
  int ret = OB_SUCCESS;

  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;

  if (version != CS_FETCH_STATS_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "rc.serialize error");
  }

  // ifreturn success , we can return dump_buf
  if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == ret) {
    ret = chunk_server_->get_stat_manager().serialize(
            out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  }

  if (OB_SUCCESS == ret) {
    chunk_server_->send_response(
      OB_FETCH_STATS_RESPONSE,
      CS_FETCH_STATS_VERSION,
      out_buffer, connection, channel_id);
  }
  return ret;
}

int ObChunkService::cs_start_gc(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  const int32_t CS_START_GC_VERSION = 1;
  int ret = OB_SUCCESS;
  int64_t recycle_version = 0;

  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;

  if (version != CS_START_GC_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  if (OB_SUCCESS == ret && OB_SUCCESS == rc.result_code_) {
    rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(), in_buffer.get_capacity(),
                                                 in_buffer.get_position(), &recycle_version);
  }

  ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "rc.serialize error");
  }

  if (OB_SUCCESS == ret) {
    chunk_server_->send_response(
      OB_RESULT,
      CS_START_GC_VERSION,
      out_buffer, connection, channel_id);
  }
  chunk_server_->get_tablet_manager().start_gc(recycle_version);
  return ret;
}

int ObChunkService::cs_reload_conf(
  const int32_t version,
  const int32_t channel_id,
  tbnet::Connection* connection,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer) {
  int ret = OB_SUCCESS;
  const int32_t CS_RELOAD_CONF_VERSION = 1;

  ObString conf_file;
  char config_file_str[OB_MAX_FILE_NAME_LENGTH];
  common::ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;

  if (version != CS_RELOAD_CONF_VERSION) {
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
  } else {
    if (OB_SUCCESS != (rc.result_code_ =
                         conf_file.deserialize(in_buffer.get_data(),
                                               in_buffer.get_capacity(), in_buffer.get_position()))) {
      TBSYS_LOG(WARN, "deserialize conf file error, ret=%d", ret);
    } else {
      int64_t length = conf_file.length();
      strncpy(config_file_str, conf_file.ptr(), length);
      config_file_str[length] = '\0';
      TBSYS_LOG(INFO, "reload conf from file %s", config_file_str);
    }
  }

  if (OB_SUCCESS == rc.result_code_) {
    if (OB_SUCCESS != (rc.result_code_ =
                         chunk_server_->get_param().reload_from_config(config_file_str))) {
      TBSYS_LOG(WARN, "failed to reload config, ret=%d", ret);
    } else {
      chunk_server_->get_tablet_manager().get_chunk_merge().set_config_param();
      chunk_server_->set_default_queue_size(chunk_server_->get_param().get_task_queue_size());
      chunk_server_->set_min_left_time(chunk_server_->get_param().get_task_left_time());
    }
  }

  ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "rc.serialize error");
  } else {
    chunk_server_->send_response(
      OB_UPS_RELOAD_CONF_RESPONSE,
      CS_RELOAD_CONF_VERSION,
      out_buffer, connection, channel_id);
  }

  return ret;
}

bool ObChunkService::is_valid_lease() {
  int64_t current_time = tbsys::CTimeUtil::getTime();
  return current_time < service_expired_time_;
}

void ObChunkService::get_merge_delay_interval() {
  int64_t interval = 0;
  int rc = OB_SUCCESS;
  ObRootServerRpcStub& rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
  rc = rs_rpc_stub.get_merge_delay_interval(interval);
  if (OB_SUCCESS == rc) {
    merge_delay_interval_ = interval;
  } else {
    merge_delay_interval_ = chunk_server_->get_param().get_merge_delay_interval();
  }
  TBSYS_LOG(INFO, "merge_delay_interval_ is %ld", merge_delay_interval_);
}

void ObChunkService::LeaseChecker::runTimerTask() {
  if (NULL != service_ && service_->inited_) {
    if (!service_->is_valid_lease() && !service_->in_register_process_) {
      TBSYS_LOG(INFO, "lease expired, re-register to root_server");
      service_->register_self();
    }

    // memory usage stats
    ObTabletManager& manager = service_->chunk_server_->get_tablet_manager();
    OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID,
                  ObChunkServerStatManager::INDEX_MU_DEFAULT,
                  ob_get_mod_memory_usage(ObModIds::OB_MOD_DEFAULT));
    OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID,
                  ObChunkServerStatManager::INDEX_MU_NETWORK,
                  ob_get_mod_memory_usage(ObModIds::OB_COMMON_NETWORK));
    OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID,
                  ObChunkServerStatManager::INDEX_MU_THREAD_BUFFER,
                  ob_get_mod_memory_usage(ObModIds::OB_THREAD_BUFFER));
    OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID,
                  ObChunkServerStatManager::INDEX_MU_TABLET,
                  ob_get_mod_memory_usage(ObModIds::OB_CS_TABLET_IMAGE));
    OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID,
                  ObChunkServerStatManager::INDEX_MU_BI_CACHE,
                  manager.get_serving_block_index_cache().get_cache_mem_size());
    OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID,
                  ObChunkServerStatManager::INDEX_MU_BLOCK_CACHE,
                  manager.get_serving_block_cache().size());
    OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID,
                  ObChunkServerStatManager::INDEX_MU_BI_CACHE_UNSERVING,
                  manager.get_unserving_block_index_cache().get_cache_mem_size());
    OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID,
                  ObChunkServerStatManager::INDEX_MU_BLOCK_CACHE_UNSERVING,
                  manager.get_unserving_block_cache().size());
    if (manager.get_join_cache().is_inited()) {
      OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID,
                    ObChunkServerStatManager::INDEX_MU_JOIN_CACHE,
                    manager.get_join_cache().get_cache_mem_size());
    }

    // reschedule
    service_->timer_.schedule(*this,
                              service_->chunk_server_->get_param().get_lease_check_interval(), false);
  }
}

void ObChunkService::StatUpdater::runTimerTask() {
  ObStat* stat = NULL;
  THE_CHUNK_SERVER.get_stat_manager().get_stat(ObChunkServerStatManager::META_TABLE_ID, stat);
  if (NULL == stat) {
    TBSYS_LOG(DEBUG, "get stat failed");
  } else {
    int64_t request_count = stat->get_value(ObChunkServerStatManager::INDEX_META_REQUEST_COUNT);
    current_request_count_ = request_count - pre_request_count_;
    stat->set_value(ObChunkServerStatManager::INDEX_META_REQUEST_COUNT_PER_SECOND, current_request_count_);
    pre_request_count_ = request_count;
  }
}

void ObChunkService::MergeTask::runTimerTask() {
  int err = OB_SUCCESS;
  ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
  unset_scheduled();

  if (frozen_version_ <= 0) {
    //initialize phase or slave rs switch to master but get frozen version failed
  } else if (frozen_version_  < tablet_manager.get_serving_tablet_image().get_newest_version()) {
    TBSYS_LOG(ERROR, "mem frozen version (%ld) < newest local tablet version (%ld),exit", frozen_version_,
              tablet_manager.get_serving_tablet_image().get_newest_version());
    kill(getpid(), 2);
  } else if (tablet_manager.get_chunk_merge().can_launch_next_round(frozen_version_)) {
    err = tablet_manager.merge_tablets(frozen_version_);
    if (err != OB_SUCCESS) {
      frozen_version_ = 0; //failed,wait for the next schedule
    }
    service_->get_merge_delay_interval();
  } else {
    tablet_manager.get_chunk_merge().set_newest_frozen_version(frozen_version_);
  }
}

void ObChunkService::FetchUpsTask::runTimerTask() {
  ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
  tablet_manager.get_chunk_merge().fetch_update_server_list();
  // reschedule fetch updateserver list task with new interval.
  service_->timer_.schedule(*this, service_->chunk_server_->get_param().get_fetch_ups_interval(), false);
}

} // end namespace chunkserver
} // end namespace sb



