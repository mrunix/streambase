/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_log_reader.cc for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "ob_log_reader.h"

using namespace sb::common;

ObLogReader::ObLogReader() {
  is_initialized_ = false;
  cur_log_file_id_ = 0;
  is_wait_ = false;
}

ObLogReader::~ObLogReader() {
}

int ObLogReader::init(const char* log_dir, const uint64_t log_file_id_start, const uint64_t log_seq, bool is_wait) {
  int ret = OB_SUCCESS;

  if (is_initialized_) {
    TBSYS_LOG(ERROR, "ObLogReader has been initialized before");
    ret = OB_INIT_TWICE;
  } else {
    if (NULL == log_dir) {
      TBSYS_LOG(ERROR, "Parameter is invalid[log_dir=%p]", log_dir);
      ret = OB_INVALID_ARGUMENT;
    } else {
      ret = log_file_reader_.init(log_dir);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "log_file_reader_ init[log_dir=%s] error[ret=%d]", log_dir, ret);
      } else {
        cur_log_file_id_ = log_file_id_start;
        cur_log_seq_id_ = log_seq;
        max_log_file_id_ = 0;
        is_wait_ = is_wait;
        has_max_ = false;
        is_initialized_ = true;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    if (0 != log_seq) {
      if (OB_SUCCESS != (ret = seek(log_seq))) {
        TBSYS_LOG(ERROR, "seek log seq error, log_seq=%lu, ret=%d", log_seq, ret);
      }
    }
  }

  if (OB_SUCCESS != ret) {
    is_initialized_ = false;
  }

  return ret;
}

int ObLogReader::read_log(LogCommand& cmd, uint64_t& seq, char*& log_data, int64_t& data_len) {
  int ret = OB_SUCCESS;

  if (!is_initialized_) {
    TBSYS_LOG(ERROR, "ObLogReader has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    if (!log_file_reader_.is_opened()) {
      ret = open_log_(cur_log_file_id_);
    }
    if (OB_SUCCESS == ret) {
      ret = read_log_(cmd, seq, log_data, data_len);
      if (OB_SUCCESS == ret) {
        while (OB_SUCCESS == ret && OB_LOG_SWITCH_LOG == cmd) {
          TBSYS_LOG(INFO, "reach the end of log[cur_log_file_id_=%lu]", cur_log_file_id_);
          ret = log_file_reader_.close();
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(ERROR, "log_file_reader_ close error[ret=%d]", ret);
          } else {
            cur_log_file_id_ ++;
            ret = open_log_(cur_log_file_id_, seq);
            if (OB_SUCCESS == ret) {
              ret = log_file_reader_.read_log(cmd, seq, log_data, data_len);
              if (OB_SUCCESS != ret && OB_READ_NOTHING != ret) {
                TBSYS_LOG(ERROR, "log_file_reader_ read_log error[ret=%d]", ret);
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    cur_log_seq_id_ = seq;
  }
  return ret;
}

int ObLogReader::seek(uint64_t log_seq) {
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == ret) {
    LogCommand cmd;
    uint64_t seq = 0;
    char* log_data;
    int64_t data_len;

    ret = read_log(cmd, seq, log_data, data_len);
    if (OB_READ_NOTHING == ret) {
      ret = OB_SUCCESS;
    } else if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "seek failed, log_seq=%lu", log_seq);
    } else if (seq >= log_seq) {
      TBSYS_LOG(WARN, "seek failed, the initial seq is bigger than log_seq, "
                "seq=%lu log_seq=%lu", seq, log_seq);
      ret = OB_ERROR;
    } else {
      while (seq < log_seq) {
        ret = read_log(cmd, seq, log_data, data_len);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(ERROR, "read_log failed, ret=%d", ret);
          break;
        }
      }
    }
  }
  return ret;
}

int ObLogReader::open_log_(const uint64_t log_file_id, const uint64_t last_log_seq/* = 0*/) {
  int ret = OB_SUCCESS;

  if (is_wait_ && has_max_ && log_file_id > max_log_file_id_) {
    ret = OB_READ_NOTHING;
  } else {
    ret = log_file_reader_.open(log_file_id, last_log_seq);
    if (is_wait_) {
      if (OB_FILE_NOT_EXIST == ret) {
        TBSYS_LOG(DEBUG, "log file doesnot exist, id=%lu", log_file_id);
        ret = OB_READ_NOTHING;
      } else if (ret != OB_SUCCESS) {
        TBSYS_LOG(WARN, "log_file_reader_ open[id=%lu] error[ret=%d]", cur_log_file_id_, ret);
      }
    } else {
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(WARN, "log_file_reader_ open[id=%lu] error[ret=%d]", cur_log_file_id_, ret);
      }
    }
  }

  return ret;
}

int ObLogReader::read_log_(LogCommand& cmd, uint64_t& log_seq, char*& log_data, int64_t& data_len) {
  int ret = OB_SUCCESS;

  int err = log_file_reader_.read_log(cmd, log_seq, log_data, data_len);
  //while (OB_READ_NOTHING == err && is_wait_)
  //{
  //  usleep(WAIT_TIME);
  //  err = log_file_reader_.read_log(cmd, log_seq, log_data, data_len);
  //}
  ret = err;
  if (OB_SUCCESS != ret && OB_READ_NOTHING != ret) {
    TBSYS_LOG(WARN, "log_file_reader_ read_log error[ret=%d]", ret);
  }

  return ret;
}

