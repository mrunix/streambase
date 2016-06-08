/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_log_mgr.cc for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "ob_ups_log_mgr.h"

#include "common/file_utils.h"
#include "common/file_directory_utils.h"
#include "common/ob_log_dir_scanner.h"
#include "common/ob_log_reader.h"
#include "common/utility.h"

using namespace sb::common;
using namespace sb::updateserver;

const char* ObUpsLogMgr::UPS_LOG_REPLAY_POINT_FILE = "log_replay_point";
const int ObUpsLogMgr::UINT64_MAX_LEN = 30;

ObUpsLogMgr::ObUpsLogMgr() {
  role_mgr_ = NULL;
  replay_point_ = 0;
  max_log_id_ = 0;
  is_initialized_ = false;
  is_log_dir_empty_ = false;
  replay_point_fn_[0] = '\0';
  log_dir_[0] = '\0';
}

ObUpsLogMgr::~ObUpsLogMgr() {
}

int ObUpsLogMgr::init(const char* log_dir, const int64_t log_file_max_size, ObSlaveMgr* slave_mgr, ObRoleMgr* role_mgr, int64_t log_sync_type) {
  int ret = OB_SUCCESS;

  if (is_initialized_) {
    TBSYS_LOG(ERROR, "ObUpsLogMgr has been initialized");
    ret = OB_INIT_TWICE;
  }

  if (OB_SUCCESS == ret) {
    if (NULL == log_dir || NULL == role_mgr) {
      TBSYS_LOG(ERROR, "Arguments are invalid[log_dir=%p role_mgr=%p]", log_dir, role_mgr);
      ret = OB_INVALID_ARGUMENT;
    } else {
      int log_dir_len = strlen(log_dir);
      if (log_dir_len >= OB_MAX_FILE_NAME_LENGTH) {
        TBSYS_LOG(ERROR, "Argument is invalid[log_dir_len=%d log_dir=%s]", log_dir_len, log_dir);
        ret = OB_INVALID_ARGUMENT;
      } else {
        strncpy(log_dir_, log_dir, log_dir_len);
        log_dir_[log_dir_len] = '\0';

        int err = 0;
        err = snprintf(replay_point_fn_, OB_MAX_FILE_NAME_LENGTH, "%s/%s", log_dir, UPS_LOG_REPLAY_POINT_FILE);
        if (err < 0) {
          TBSYS_LOG(ERROR, "snprintf replay_point_fn_ error[%s][log_dir_=%s UPS_LOG_REPLAY_POINT_FILE=%s]",
                    strerror(errno), log_dir, UPS_LOG_REPLAY_POINT_FILE);
          ret = OB_ERROR;
        } else if (err >= OB_MAX_FILE_NAME_LENGTH) {
          TBSYS_LOG(ERROR, "replay_point_fn_ is too long[err=%d OB_MAX_FILE_NAME_LENGTH=%ld]",
                    err, OB_MAX_FILE_NAME_LENGTH);
          ret = OB_ERROR;
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    int load_ret = OB_SUCCESS;
    load_ret = load_replay_point_();
    if (OB_SUCCESS != load_ret && OB_FILE_NOT_EXIST != load_ret) {
      ret = OB_ERROR;
    } else {
      ObLogDirScanner scanner;

      ret = scanner.init(log_dir);
      if (OB_SUCCESS != ret && OB_DISCONTINUOUS_LOG != ret) {
        TBSYS_LOG(ERROR, "ObLogDirScanner init error");
      } else {
        // if replay point does not exist, the minimum log is replay point
        // else check the correctness
        if (OB_FILE_NOT_EXIST == load_ret) {
          if (OB_ENTRY_NOT_EXIST == scanner.get_min_log_id(replay_point_)) {
            replay_point_ = 1;
            max_log_id_ = 1;
            is_log_dir_empty_ = true;
          } else {
            ret = scanner.get_max_log_id(max_log_id_);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(ERROR, "ObLogDirScanner get_max_log_id error[ret=%d]", ret);
            }
          }
          if (OB_SUCCESS == ret) {
            TBSYS_LOG(INFO, "replay_point_file does not exist, take min_log_id as replay_point[replay_point_=%lu]", replay_point_);
          }
        } else {
          uint64_t min_log_id;
          ret = scanner.get_min_log_id(min_log_id);
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(ERROR, "get_min_log_id error[ret=%d]", ret);
            ret = OB_ERROR;
          } else {
            if (min_log_id > replay_point_) {
              TBSYS_LOG(ERROR, "missing log file[min_log_id=%lu replay_point_=%lu", min_log_id, replay_point_);
              ret = OB_ERROR;
            }
          }

          if (OB_SUCCESS == ret) {
            ret = scanner.get_max_log_id(max_log_id_);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(ERROR, "get_max_log_id error[ret=%d]", ret);
              ret = OB_ERROR;
            }
          }
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    ret = ObLogWriter::init(log_dir, log_file_max_size, slave_mgr, log_sync_type);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "ObLogWriter init failed[ret=%d]", ret);
    } else {
      role_mgr_ = role_mgr;
      is_initialized_ = true;
      TBSYS_LOG(INFO, "ObUpsLogMgr[this=%p] init succ[replay_point_=%lu max_log_id_=%lu]", this, replay_point_, max_log_id_);
    }
  }

  return ret;
}

int ObUpsLogMgr::replay_log(ObUpsTableMgr& table_mgr) {
  int ret = OB_SUCCESS;

  ObLogReader log_reader;
  ObUpsMutator mutator;
  int64_t pos = 0;
  char* log_data = NULL;
  int64_t data_len = 0;
  LogCommand cmd = OB_LOG_UNKNOWN;
  uint64_t seq;

  ret = check_inner_stat();

  if ((OB_SUCCESS == ret) && (!is_log_dir_empty_)) {
    ret = log_reader.init(log_dir_, replay_point_, 0, false);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "ObLogReader init error[ret=%d], ObLogReplayRunnable init failed", ret);
    } else {
      ret = log_reader.read_log(cmd, seq, log_data, data_len);
      if (OB_FILE_NOT_EXIST != ret && OB_READ_NOTHING != ret && OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]", ret);
      }
      while (OB_SUCCESS == ret && ObRoleMgr::INIT == role_mgr_->get_state()) {
        set_cur_log_seq(seq);

        if (OB_LOG_UPS_MUTATOR == cmd) {
          pos = 0;
          ret = mutator.deserialize(log_data, data_len, pos);
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(ERROR, "UpsMutator deserialize error[ret=%d log_data=%p data_len=%ld]", ret, log_data, data_len);
            common::hex_dump(log_data, data_len, false, TBSYS_LOG_LEVEL_WARN);
            break;
          } else {
            if (pos != data_len) {
              TBSYS_LOG(WARN, "log_data is longer than expected[log_data=%p data_len=%ld pos=%ld]", log_data, data_len, pos);
              common::hex_dump(log_data, data_len, false, TBSYS_LOG_LEVEL_WARN);
              break;
            }

            ret = table_mgr.replay(mutator);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(ERROR, "UpsTableMgr replay error[ret=%d]", ret);
              break;
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
            ret = table_mgr.set_schemas(schema);
            if (OB_SUCCESS != ret) {
              TBSYS_LOG(ERROR, "UpsTableMgr set_schemas error, ret=%d schema_version=%ld", ret, schema.get_version());
            } else {
              TBSYS_LOG(INFO, "switch schema succ");
            }
          }
        } else if (OB_LOG_NOP != cmd) {
          TBSYS_LOG(ERROR, "Unknown cmd=%d", cmd);
          ret = OB_ERROR;
          break;
        }

        ret = log_reader.read_log(cmd, seq, log_data, data_len);
        if (OB_FILE_NOT_EXIST != ret && OB_READ_NOTHING != ret && OB_SUCCESS != ret) {
          TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]", ret);
        }
      }

      if (ObRoleMgr::INIT != role_mgr_->get_state()) {
        TBSYS_LOG(ERROR, "Updateserver state error[state=%d]", role_mgr_->get_state());
        ret = OB_ERROR;
      }

      // handle exception, when the last log file contain SWITCH_LOG entry
      // but the next log file is missing
      if (OB_FILE_NOT_EXIST == ret) {
        max_log_id_++;
        ret = OB_SUCCESS;
      }

      // reach the end of commit log
      if (OB_READ_NOTHING == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObUpsLogMgr::write_replay_point(uint64_t replay_point) {
  int ret = 0;

  ret = check_inner_stat();

  if (OB_SUCCESS == ret) {
    int err = 0;
    FileUtils rplpt_file;
    err = rplpt_file.open(replay_point_fn_, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if (err < 0) {
      TBSYS_LOG(ERROR, "open file[\"%s\"] error[%s]", replay_point_fn_, strerror(errno));
      ret = OB_ERROR;
    } else {
      char rplpt_str[UINT64_MAX_LEN];
      int rplpt_str_len = 0;
      rplpt_str_len = snprintf(rplpt_str, UINT64_MAX_LEN, "%lu", replay_point);
      if (rplpt_str_len < 0) {
        TBSYS_LOG(ERROR, "snprintf rplpt_str error[%s][replay_point=%lu]", strerror(errno), replay_point);
        ret = OB_ERROR;
      } else if (rplpt_str_len >= UINT64_MAX_LEN) {
        TBSYS_LOG(ERROR, "rplpt_str is too long[rplpt_str_len=%d UINT64_MAX_LEN=%d", rplpt_str_len, UINT64_MAX_LEN);
        ret = OB_ERROR;
      } else {
        err = rplpt_file.write(rplpt_str, rplpt_str_len);
        if (err < 0) {
          TBSYS_LOG(ERROR, "write error[%s][rplpt_str=%p rplpt_str_len=%d]", strerror(errno), rplpt_str, rplpt_str_len);
          ret = OB_ERROR;
        }
      }

      rplpt_file.close();
    }
  }

  if (OB_SUCCESS == ret) {
    replay_point_ = replay_point;
    TBSYS_LOG(INFO, "set replay point to %lu", replay_point_);
  }

  return ret;
}

int ObUpsLogMgr::add_slave(const ObServer& server, uint64_t& new_log_file_id) {
  int ret = OB_SUCCESS;

  ObSlaveMgr* slave_mgr = get_slave_mgr();
  if (NULL == slave_mgr) {
    TBSYS_LOG(ERROR, "slaev_mgr is NULL");
    ret = OB_ERROR;
  } else {
    ret = slave_mgr->add_server(server);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "ObSlaveMgreadd_server error[ret=%d]", ret);
    } else {
      ret = switch_log_file(new_log_file_id);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "switch_log_file error[ret=%d]", ret);
      }
    }
  }

  return ret;
}

int ObUpsLogMgr::load_replay_point_() {
  int ret = OB_SUCCESS;

  int err = 0;
  char rplpt_str[UINT64_MAX_LEN];
  int rplpt_str_len = 0;

  if (!FileDirectoryUtils::exists(replay_point_fn_)) {
    TBSYS_LOG(INFO, "replay point file[\"%s\"] does not exist", replay_point_fn_);
    ret = OB_FILE_NOT_EXIST;
  } else {
    FileUtils rplpt_file;
    err = rplpt_file.open(replay_point_fn_, O_RDONLY);
    if (err < 0) {
      TBSYS_LOG(ERROR, "open file[\"%s\"] error[%s]", replay_point_fn_, strerror(errno));
      ret = OB_ERROR;
    } else {
      rplpt_str_len = rplpt_file.read(rplpt_str, UINT64_MAX_LEN);
      if (rplpt_str_len < 0) {
        TBSYS_LOG(ERROR, "read file error[%s]", strerror(errno));
        ret = OB_ERROR;
      } else if ((rplpt_str_len >= UINT64_MAX_LEN) || (rplpt_str_len == 0)) {
        TBSYS_LOG(ERROR, "data contained in replay point file is invalid[rplpt_str_len=%d]", rplpt_str_len);
        ret = OB_ERROR;
      } else {
        rplpt_str[rplpt_str_len] = '\0';

        const int STRTOUL_BASE = 10;
        char* endptr;
        replay_point_ = strtoul(rplpt_str, &endptr, STRTOUL_BASE);
        if ('\0' != *endptr) {
          TBSYS_LOG(ERROR, "non-digit exist in replay point file[rplpt_str=%.*s]", rplpt_str_len, rplpt_str);
          ret = OB_ERROR;
        } else if (ERANGE == errno) {
          TBSYS_LOG(ERROR, "replay point contained in replay point file is out of range");
          ret = OB_ERROR;
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    TBSYS_LOG(INFO, "load replay point succ[replay_point_=%lu] from file[\"%s\"]", replay_point_, replay_point_fn_);
  }

  return ret;
}



