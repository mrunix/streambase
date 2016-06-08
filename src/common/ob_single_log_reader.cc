/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_single_log_reader.cc for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "ob_single_log_reader.h"

using namespace sb::common;

const int64_t ObSingleLogReader::LOG_BUFFER_MAX_LENGTH = 1 << 21;

ObSingleLogReader::ObSingleLogReader() {
  is_initialized_ = false;
  dio_ = true;
  file_name_[0] = '\0';
  file_id_ = 0;
  last_log_seq_ = 0;
  log_buffer_.reset();
}

ObSingleLogReader::~ObSingleLogReader() {
  if (NULL != log_buffer_.get_data()) {
    ob_free(log_buffer_.get_data());
    log_buffer_.reset();
  }
}

int ObSingleLogReader::init(const char* log_dir) {
  int ret = OB_SUCCESS;

  if (is_initialized_) {
    TBSYS_LOG(ERROR, "ObSingleLogReader has been initialized");
    ret = OB_INIT_TWICE;
  } else {
    if (NULL == log_dir) {
      TBSYS_LOG(ERROR, "Parameter is invalid[log_dir=%p]", log_dir);
      ret = OB_INVALID_ARGUMENT;
    } else {
      int log_dir_len = strlen(log_dir);
      if (log_dir_len >= OB_MAX_FILE_NAME_LENGTH) {
        TBSYS_LOG(ERROR, "log_dir is too long[len=%d log_dir=%s]", log_dir_len, log_dir);
        ret = OB_INVALID_ARGUMENT;
      } else {
        strncpy(log_dir_, log_dir, log_dir_len + 1);
      }
    }
  }

  if (OB_SUCCESS == ret) {
    if (NULL == log_buffer_.get_data()) {
      char* buf = static_cast<char*>(ob_malloc(LOG_BUFFER_MAX_LENGTH));
      if (NULL == buf) {
        TBSYS_LOG(ERROR, "ob_malloc for log_buffer_ failed");
        ret = OB_ERROR;
      } else {
        log_buffer_.set_data(buf, LOG_BUFFER_MAX_LENGTH);
      }
    }
  }

  if (OB_SUCCESS == ret) {
    TBSYS_LOG(INFO, "ObSingleLogReader initialize successfully");
    last_log_seq_ = 0;

    is_initialized_ = true;
  }

  return ret;
}

int ObSingleLogReader::open(const uint64_t file_id, const uint64_t last_log_seq/* = 0*/) {
  int ret = check_inner_stat_();

  if (OB_SUCCESS == ret) {
    int err = snprintf(file_name_, OB_MAX_FILE_NAME_LENGTH, "%s/%lu", log_dir_, file_id);
    if (err < 0) {
      TBSYS_LOG(ERROR, "snprintf file_name[log_dir_=%s file_id=[%lu] error[%s]", log_dir_, file_id, strerror(errno));
      ret = OB_ERROR;
    } else if (err >= OB_MAX_FILE_NAME_LENGTH) {
      TBSYS_LOG(ERROR, "snprintf file_name[file_id=%lu] error[%s]", file_id, strerror(errno));
      ret = OB_ERROR;
    } else {
      int fn_len = strlen(file_name_);
      ret = file_.open(ObString(fn_len, fn_len, file_name_), dio_);
      if (OB_SUCCESS == ret) {
        file_id_ = file_id;
        last_log_seq_ = last_log_seq;
        pos = 0;
        pread_pos_ = 0;
        log_buffer_.get_position() = 0;
        log_buffer_.get_limit() = 0;
        TBSYS_LOG(INFO, "open log file(name=%s id=%lu)", file_name_, file_id);
      } else if (OB_FILE_NOT_EXIST == ret) {
        TBSYS_LOG(DEBUG, "log file(name=%s id=%lu) not found", file_name_, file_id);
      } else {
        TBSYS_LOG(WARN, "open file[name=%s] error[%s]", file_name_, strerror(errno));
      }
    }
  }

  return ret;
}

int ObSingleLogReader::close() {
  int ret = check_inner_stat_();

  if (OB_SUCCESS == ret) {
    file_.close();
    if (last_log_seq_ == 0) {
      TBSYS_LOG(INFO, "close file[name=%s], read no data from this log", file_name_);
    } else {
      TBSYS_LOG(INFO, "close file[name=%s] successfully, the last log sequence is %lu",
                file_name_, last_log_seq_);
    }
  }

  return ret;
}

int ObSingleLogReader::reset() {
  int ret = check_inner_stat_();

  if (OB_SUCCESS == ret) {
    ret = close();
    if (OB_SUCCESS == ret) {
      ob_free(log_buffer_.get_data());
      log_buffer_.reset();
      is_initialized_ = false;
    }
  }

  return ret;
}

int ObSingleLogReader::read_log(LogCommand& cmd, uint64_t& log_seq, char*& log_data, int64_t& data_len) {
  int ret = OB_SUCCESS;

  ObLogEntry entry;
  if (!is_initialized_) {
    TBSYS_LOG(ERROR, "ObSingleLogReader has not been initialized, please initialize first");
    ret = OB_NOT_INIT;
  } else {
    ret = entry.deserialize(log_buffer_.get_data(), log_buffer_.get_limit(), log_buffer_.get_position());
    if (OB_SUCCESS != ret) {
      ret = read_log_();
      if (OB_READ_NOTHING == ret) {
        // comment this log due to too frequent invoke by replay thread
        //TBSYS_LOG(DEBUG, "do not get a full entry, when reading ObLogEntry");
      } else if (OB_SUCCESS == ret) {
        ret = entry.deserialize(log_buffer_.get_data(), log_buffer_.get_limit(), log_buffer_.get_position());
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(DEBUG, "do not get a full entry, when reading ObLogEntry");
          ret = OB_READ_NOTHING;
        }
      }
    }

    if (OB_SUCCESS == ret) {
      if (is_entry_zeroed_(entry)) {
        log_buffer_.get_position() -= entry.get_serialize_size();
        pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
        log_buffer_.get_limit() = log_buffer_.get_position();
        ret = OB_READ_NOTHING;
      } else if (OB_SUCCESS != entry.check_header_integrity()) {
        log_buffer_.get_position() -= entry.get_serialize_size();
        pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
        log_buffer_.get_limit() = log_buffer_.get_position();
        TBSYS_LOG(ERROR, "Log entry header is corrupted, file_id_=%lu pread_pos_=%ld pos=%ld last_log_seq_=%lu",
                  file_id_, pread_pos_, pos, last_log_seq_);
        TBSYS_LOG(ERROR, "log_buffer_ position_=%ld limit_=%ld capacity_=%ld",
                  log_buffer_.get_position(), log_buffer_.get_limit(), log_buffer_.get_capacity());
        ret = OB_ERROR;
      } else if (entry.get_log_data_len() > log_buffer_.get_remain_data_len()) {
        //TBSYS_LOG(DEBUG, "do not get a full entry, when checking log data");
        log_buffer_.get_position() -= entry.get_serialize_size();

        ret = read_log_();
        if (OB_READ_NOTHING == ret) {
          TBSYS_LOG(DEBUG, "do not get a full entry, when reading ObLogEntry");
        } else {
          ret = entry.deserialize(log_buffer_.get_data(), log_buffer_.get_limit(), log_buffer_.get_position());
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(ERROR, "ObLogEntry deserialize error[ret=%d]", ret);
          } else {
            if (entry.get_log_data_len() > log_buffer_.get_remain_data_len()) {
              TBSYS_LOG(DEBUG, "do not get a full entry, when checking log data");
              TBSYS_LOG(DEBUG, "log_data_len=%d remaining=%ld", entry.get_log_data_len(), log_buffer_.get_remain_data_len());
              TBSYS_LOG(DEBUG, "limit=%ld pos=%ld", log_buffer_.get_limit(), log_buffer_.get_position());
              hex_dump(log_buffer_.get_data(), log_buffer_.get_limit());
              log_buffer_.get_position() -= entry.get_serialize_size();
              ret = OB_READ_NOTHING;
            }
          }
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    if (OB_SUCCESS == ret) {
      ret = entry.check_data_integrity(log_buffer_.get_data() + log_buffer_.get_position());
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "data corrupt, when check_data_integrity, file_id_=%lu pread_pos_=%ld pos=%ld last_log_seq_=%lu",
                  file_id_, pread_pos_, pos, last_log_seq_);
        TBSYS_LOG(ERROR, "log_buffer_ position_=%ld limit_=%ld capacity_=%ld",
                  log_buffer_.get_position(), log_buffer_.get_limit(), log_buffer_.get_capacity());
        hex_dump(log_buffer_.get_data(), log_buffer_.get_limit(), true, TBSYS_LOG_LEVEL_ERROR);
        ret = OB_ERROR;
      }
    }

    if (OB_SUCCESS == ret) {
      if (OB_LOG_SWITCH_LOG != entry.cmd_ && last_log_seq_ != 0 && (last_log_seq_ + 1) != entry.seq_) {
        TBSYS_LOG(ERROR, "the log sequence is not continuous[%lu => %lu]", last_log_seq_, entry.seq_);
        ret = OB_ERROR;
      }
    }

    if (OB_SUCCESS == ret) {
      if (OB_LOG_SWITCH_LOG != entry.cmd_) {
        last_log_seq_ = entry.seq_;
      }
      cmd = static_cast<LogCommand>(entry.cmd_);
      log_seq = entry.seq_;
      log_data = log_buffer_.get_data() + log_buffer_.get_position();
      data_len = entry.get_log_data_len();
      log_buffer_.get_position() += data_len;
    }
  }

  if (OB_SUCCESS == ret) {
    TBSYS_LOG(DEBUG, "LOG ENTRY: SEQ[%lu] CMD[%d] DATA_LEN[%ld] POS[%ld]", entry.seq_, cmd, data_len, pos);
    pos += entry.header_.header_length_ + entry.header_.data_length_;
  }

  return ret;
}

int ObSingleLogReader::read_log_() {
  int ret = OB_SUCCESS;

  if (log_buffer_.get_remain_data_len() > 0) {
    if (all_zero(log_buffer_.get_data() + log_buffer_.get_position(),
                 log_buffer_.get_limit() - log_buffer_.get_position())) {
      pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
      log_buffer_.get_limit() = log_buffer_.get_position();
    } else {
      memmove(log_buffer_.get_data(), log_buffer_.get_data() + log_buffer_.get_position(), log_buffer_.get_remain_data_len());
      log_buffer_.get_limit() = log_buffer_.get_remain_data_len();
      log_buffer_.get_position() = 0;
    }
  } else {
    log_buffer_.get_limit() = log_buffer_.get_position() = 0;
  }

  int64_t read_size = 0;
  ret = file_.pread(log_buffer_.get_data() + log_buffer_.get_limit(),
                    log_buffer_.get_capacity() - log_buffer_.get_limit(),
                    pread_pos_, read_size);
  TBSYS_LOG(DEBUG, "pread:: pread_pos=%ld read_size=%ld buf_pos=%ld buf_limit=%ld", pread_pos_, read_size, log_buffer_.get_position(), log_buffer_.get_limit());
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "read log file[file_id=%lu] ret=%d", file_id_, ret);
  } else {
    // comment this log due to too frequent invoke by replay thread
    //TBSYS_LOG(DEBUG, "read %dB amount data from log file[file_id=%lu fd=%d]", err, file_id_, log_fd_);
    log_buffer_.get_limit() += read_size;
    pread_pos_ += read_size;

    if (0 == read_size) {
      // comment this log due to too frequent invoke by replay thread
      //TBSYS_LOG(DEBUG, "reach end of log file[file_id=%d]", file_id_);
      ret = OB_READ_NOTHING;
    }
  }

  return ret;
}

bool ObSingleLogReader::is_entry_zeroed_(const ObLogEntry& entry) {
  return    entry.header_.magic_ == 0
            && entry.header_.header_length_ == 0
            && entry.header_.version_ == 0
            && entry.header_.header_checksum_ == 0
            && entry.header_.reserved_ == 0
            && entry.header_.data_length_ == 0
            && entry.header_.data_zlength_ == 0
            && entry.header_.data_checksum_ == 0
            && entry.seq_ == 0
            && entry.cmd_ == 0;
}

