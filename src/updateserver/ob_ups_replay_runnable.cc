/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_replay_runnable.cc for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "ob_ups_replay_runnable.h"

#include "common/utility.h"
#include "ob_update_server.h"
#include "ob_update_server_main.h"

using namespace sb::common;
using namespace sb::updateserver;

ObUpsReplayRunnable::ObUpsReplayRunnable() {
  is_initialized_ = false;
}

ObUpsReplayRunnable::~ObUpsReplayRunnable() {
}

int ObUpsReplayRunnable::replay(LogCommand cmd, uint64_t seq, const char* log_data, const int64_t data_len) {
  UNUSED(seq);

  int ret = 0;

  ObUpdateServerMain* ups_main = ObUpdateServerMain::get_instance();

  if (NULL == ups_main) {
    TBSYS_LOG(ERROR, "instance of ObUpdateServerMain is NULL!!");
    ret = OB_ERROR;
  } else if (OB_LOG_UPS_MUTATOR == cmd) {
    ObUpsMutator mutator;
    int64_t pos = 0;
    ret = mutator.deserialize(log_data, data_len, pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "UpsMutator deserialize error[ret=%d log_data=%p data_len=%ld]", ret, log_data, data_len);
      common::hex_dump(log_data, data_len, false, TBSYS_LOG_LEVEL_WARN);
    } else {
      if (pos != data_len) {
        TBSYS_LOG(WARN, "log_data is longer than expected[log_data=%p data_len=%ld pos=%ld]", log_data, data_len, pos);
        common::hex_dump(log_data, data_len, false, TBSYS_LOG_LEVEL_WARN);
      }

      ObUpsTableMgr& table_mgr = ups_main->get_update_server().get_table_mgr();
      ret = table_mgr.replay(mutator);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "UpsTableMgr replay error[ret=%d]", ret);
      }
    }
  } else if (OB_UPS_SWITCH_SCHEMA == cmd) {
    CommonSchemaManagerWrapper schema;
    int64_t pos = 0;
    schema.deserialize(log_data, data_len, pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "ObSchemaManagerWrapper deserialize error[ret=%d log_data=%p data_len=%ld]",
                ret, log_data, data_len);
      common::hex_dump(log_data, data_len, false, TBSYS_LOG_LEVEL_WARN);
    } else {
      ObUpsTableMgr& table_mgr = ups_main->get_update_server().get_table_mgr();
      ret = table_mgr.set_schemas(schema);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "UpsTableMgr set_schemas error, ret=%d schema_version=%ld", ret, schema.get_version());
      } else {
        TBSYS_LOG(INFO, "switch schema succ");
      }
    }
  } else {
    TBSYS_LOG(ERROR, "Unknown log type, LogCommand=%d", cmd);
  }

  return ret;
}



