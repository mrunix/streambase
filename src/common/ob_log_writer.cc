/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_log_writer.cc for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "ob_log_writer.h"

#include "ob_trace_log.h"
#include "ob_malloc.h"

using namespace sb::common;

ObLogWriter::ObLogWriter() {
  is_initialized_ = false;
  dio_ = true;
  cur_log_size_ = 0;
  log_file_max_size_ = 0;
  log_dir_[0] = '\0';
  cur_log_file_id_ = 0;
  cur_log_seq_ = 0;
  slave_mgr_ = NULL;
  log_sync_type_ = OB_LOG_NOSYNC;
  is_log_start_ = false;
  memset(empty_log_, 0x00, sizeof(empty_log_));
}

ObLogWriter::~ObLogWriter() {
  file_.close();

  if (NULL != log_buffer_.get_data()) {
    ob_free(log_buffer_.get_data());
    log_buffer_.reset();
  }
}

int ObLogWriter::init(const char* log_dir, const int64_t log_file_max_size, ObSlaveMgr* slave_mgr, int64_t log_sync_type) {
  int ret = OB_SUCCESS;

  int log_dir_len = 0;
  if (is_initialized_) {
    TBSYS_LOG(ERROR, "ObLogWriter has been initialized");
    ret = OB_INIT_TWICE;
  } else {
    if (NULL == log_dir || NULL == slave_mgr) {
      TBSYS_LOG(ERROR, "Parameter are invalid[log_dir=%p slave_mgr=%p]", log_dir, slave_mgr);
      ret = OB_INVALID_ARGUMENT;
    } else {
      log_dir_len = strlen(log_dir);
      if (log_dir_len >= OB_MAX_FILE_NAME_LENGTH) {
        TBSYS_LOG(ERROR, "log_dir is too long[log_dir_len=%d]", log_dir_len);
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    strncpy(log_dir_, log_dir, log_dir_len + 1);
    log_file_max_size_ = log_file_max_size;
    slave_mgr_ = slave_mgr;
    log_sync_type_ = log_sync_type;

    void* buf = ob_malloc(LOG_BUFFER_SIZE);
    if (NULL == buf) {
      TBSYS_LOG(ERROR, "ob_malloc[length=%ld] error", LOG_BUFFER_SIZE);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      log_buffer_.set_data(static_cast<char*>(buf), LOG_BUFFER_SIZE);
    }
  }

  if (OB_SUCCESS == ret) {
    is_initialized_ = true;
    TBSYS_LOG(INFO, "ObLogWriter initialize successfully[log_dir_=%s log_file_max_size_=%ld cur_log_file_id_=%lu "
              "cur_log_seq_=%lu slave_mgr_=%p log_sync_type_=%ld]",
              log_dir_, log_file_max_size_, cur_log_file_id_, cur_log_seq_, slave_mgr_, log_sync_type_);
  }

  return ret;
}

void ObLogWriter::reset_log() {
  if (file_.is_opened()) {
    file_.close();
  }

  cur_log_size_ = 0;
  cur_log_file_id_ = 0;
  cur_log_seq_ = 0;
}

int ObLogWriter::start_log(const uint64_t log_file_max_id, const uint64_t log_max_seq) {
  int ret = OB_SUCCESS;

  ret = check_inner_stat();

  if (OB_SUCCESS == ret) {
    cur_log_file_id_ = log_file_max_id;
    cur_log_seq_ = log_max_seq;

    ret = open_log_file_(cur_log_file_id_, false);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "open_log_file_ error[ret=%d]", ret);
    } else {
      // get current log file size
      cur_log_size_ = file_.get_file_pos();
      if (-1 == cur_log_size_) {
        TBSYS_LOG(ERROR, "get_file_pos error[%s]", strerror(errno));
        ret = OB_ERROR;
      } else {
        TBSYS_LOG(INFO, "commit log cur_log_file_id_=%lu cur_log_size_=%ld", cur_log_file_id_, cur_log_size_);
        is_log_start_ = true;
      }
    }
  }

  return ret;
}

int ObLogWriter::start_log(const uint64_t log_file_max_id) {
  int ret = OB_SUCCESS;

  ret = check_inner_stat();

  if (OB_SUCCESS == ret) {
    cur_log_file_id_ = log_file_max_id + 1;

    ret = open_log_file_(cur_log_file_id_, true);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "open_log_file_ error[ret=%d]", ret);
    } else {
      is_log_start_ = true;
    }
  }

  return ret;
}

int ObLogWriter::write_log(const LogCommand cmd, const char* log_data, const int64_t data_len) {
  int ret = check_inner_stat();

  if (OB_SUCCESS == ret) {
    if ((NULL == log_data && data_len != 0)
        || (NULL != log_data && data_len <= 0)) {
      TBSYS_LOG(ERROR, "Parameters are invalid[log_data=%p data_len=%ld]", log_data, data_len);
      ret = OB_INVALID_ARGUMENT;
    } else {
      if (0 == log_buffer_.get_position()) {
        ret = check_log_file_size_(LOG_BUFFER_SIZE);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(ERROR, "check_log_file_size_[cur_log_size_=%ld new_length=%ld] error[ret=%d]",
                    cur_log_size_, LOG_BUFFER_SIZE, ret);
        }
      }

      if (OB_SUCCESS == ret) {
        ret = serialize_log_(cmd, log_data, data_len);
      }
    }
  }

  return ret;
}

int ObLogWriter::flush_log() {
  int ret = check_inner_stat();

  if (OB_SUCCESS == ret) {
    if (log_buffer_.get_position() > 0) {
      ret = serialize_nop_log_();
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "serialize_nop_log_ error, ret=%d", ret);
      } else {
        int64_t send_start_time = tbsys::CTimeUtil::getTime();
        ret = slave_mgr_->send_data(log_buffer_.get_data(), log_buffer_.get_position());
        last_net_elapse_ = tbsys::CTimeUtil::getTime() - send_start_time;
        if (OB_SUCCESS == ret || OB_PARTIAL_FAILED == ret) {
          int64_t store_start_time = tbsys::CTimeUtil::getTime();
          ret = store_log(log_buffer_.get_data(), log_buffer_.get_position());
          last_disk_elapse_ = tbsys::CTimeUtil::getTime() - store_start_time;
          if (OB_SUCCESS == ret) {
            log_buffer_.get_position() = 0;
          }
        }
      }
    }
  }

  return ret;
}

int ObLogWriter::write_and_flush_log(const LogCommand cmd, const char* log_data, const int64_t data_len) {
  int ret = check_inner_stat();

  if (OB_SUCCESS == ret) {
    // check whether remaining data
    // if so, clear it
    if (log_buffer_.get_position() > 0) {
      log_buffer_.get_position() = 0;
    }

    ret = write_log(cmd, log_data, data_len);
    if (OB_SUCCESS == ret) {
      ret = flush_log();
    }
  }

  return ret;
}

int ObLogWriter::store_log(const char* buf, const int64_t buf_len) {
  int ret = OB_SUCCESS;

  ret = check_inner_stat();

  if (OB_SUCCESS == ret) {
    if (NULL == buf || buf_len <= 0) {
      TBSYS_LOG(ERROR, "parameters are invalid[buf=%p buf_len=%ld]", buf, buf_len);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == ret) {
    ret = file_.append(buf, buf_len, OB_LOG_NOSYNC == log_sync_type_ ? false : true);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "write data[buf_len=%ld] to commit log error[%s]", buf_len, strerror(errno));
    } else {
      cur_log_size_ += buf_len;
      TBSYS_LOG(DEBUG, "write %ld bytes to log[%lu] [cur_log_size_=%ld]", buf_len, cur_log_file_id_, cur_log_size_);
    }
  }

  return ret;
}

int ObLogWriter::switch_log_file(uint64_t& new_log_file_id) {
  int ret = check_inner_stat();

  if (OB_SUCCESS == ret) {
    ret = flush_log();
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "flush_log error[ret=%d]", ret);
    } else {
      const int64_t buf_len = LOG_FILE_ALIGN_SIZE - ObLogEntry::get_header_size();
      char buf[buf_len];
      int64_t buf_pos = 0;
      ret = serialization::encode_i64(buf, buf_len, buf_pos, cur_log_file_id_);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "encode_i64[cur_log_file_id_=%lu] error[ret=%d]", cur_log_file_id_, ret);
      } else {
        ret = serialize_log_(OB_LOG_SWITCH_LOG, buf, buf_len);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(ERROR, "serialize_log_ cur_log_file_id_[%lu] error[ret=%d]", cur_log_file_id_, ret);
        } else {
          ret = flush_log();
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(ERROR, "flush_log error[ret=%d]", ret);
          } else {
            ret = switch_to_log_file(cur_log_file_id_ + 1);
            if (OB_SUCCESS == ret) {
              new_log_file_id = cur_log_file_id_;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObLogWriter::switch_to_log_file(const uint64_t log_file_id) {
  int ret = check_inner_stat();

  if (OB_SUCCESS == ret) {
    if ((cur_log_file_id_ + 1) != log_file_id) {
      TBSYS_LOG(ERROR, "log_file_id is not continous[cur_log_file_id_=%lu log_file_id=%lu]", cur_log_file_id_, log_file_id);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret) {
    file_.close();
  }

  if (OB_SUCCESS == ret) {
    ret = open_log_file_(log_file_id, true);
    if (OB_SUCCESS == ret) {
      TBSYS_LOG(INFO, "switch_log_file successfully from %lu to %lu", cur_log_file_id_, log_file_id);
      cur_log_file_id_ = log_file_id;
    }
  }

  return ret;
}

int ObLogWriter::write_checkpoint_log(uint64_t& log_file_id) {
  int ret = check_inner_stat();

  log_file_id = 0;
  if (OB_SUCCESS == ret) {
    ret = flush_log();
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "flush_log error[ret=%d]", ret);
    } else {
      const int64_t buf_len = sizeof(uint64_t);
      char buf[buf_len];
      int64_t buf_pos = 0;
      ret = serialization::encode_i64(buf, buf_len, buf_pos, cur_log_file_id_);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "encode_i64[cur_log_file_id_=%lu] error[ret=%d]", cur_log_file_id_, ret);
      } else {
        ret = serialize_log_(OB_LOG_CHECKPOINT, buf, buf_pos);
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(ERROR, "serialize_log_ cur_log_file_id_[%lu] error[ret=%d]", cur_log_file_id_, ret);
        } else {
          ret = flush_log();
          if (OB_SUCCESS != ret) {
            TBSYS_LOG(ERROR, "flush_log error[ret=%d]", ret);
          } else {
            TBSYS_LOG(INFO, "write_checkpoint_log successfully[cur_log_file_id_=%lu]", cur_log_file_id_);
            uint64_t new_log_file_id = 0;
            ret = switch_log_file(new_log_file_id);
            if (OB_SUCCESS == ret) {
              log_file_id = cur_log_file_id_ - 1;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogWriter::open_log_file_(const uint64_t log_file_id, bool is_trunc) {
  int ret = OB_SUCCESS;

  struct stat file_info;
  int err = stat(log_dir_, &file_info);
  if (err != 0) { // log_dir does not exist
    err = mkdir(log_dir_, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    if (err != 0) {
      TBSYS_LOG(ERROR, "create \"%s\" directory error[%s]", log_dir_, strerror(errno));
      ret = OB_ERROR;
    } else {
      TBSYS_LOG(INFO, "create log directory[\"%s\"]", log_dir_);
    }
  }

  if (OB_SUCCESS == ret) {
    char file_name[OB_MAX_FILE_NAME_LENGTH];
    err = snprintf(file_name, OB_MAX_FILE_NAME_LENGTH, "%s/%lu", log_dir_, log_file_id);
    if (err < 0) {
      TBSYS_LOG(ERROR, "snprintf log filename error[%s]", strerror(errno));
      ret = OB_ERROR;
    } else if (err >= OB_MAX_FILE_NAME_LENGTH) {
      TBSYS_LOG(ERROR, "generated filename is too long[length=%d]", err);
      ret = OB_ERROR;
    } else {
      int fn_len = strlen(file_name);
      ret = file_.open(ObString(fn_len, fn_len, file_name), dio_, true, is_trunc, LOG_FILE_ALIGN_SIZE);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "open commit log file[file_name=%s] ret=%d", file_name, ret);
      } else {
        cur_log_size_ = 0;
      }
    }
  }

  return ret;
}

int ObLogWriter::serialize_log_(const LogCommand cmd, const char* log_data, const int64_t data_len) {
  int ret = OB_SUCCESS;

  ObLogEntry entry;
  if ((NULL == log_data && data_len != 0)
      || (NULL != log_data && data_len <= 0)) {
    TBSYS_LOG(ERROR, "Parameters are invalid[log_data=%p data_len=%ld]", log_data, data_len);
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t new_log_seq = 0;
    if (OB_LOG_SWITCH_LOG == cmd) {
      new_log_seq = cur_log_seq_;
    } else {
      new_log_seq = cur_log_seq_ + 1;
    }
    entry.set_log_seq(new_log_seq);
    entry.set_log_command(cmd);
    ret = entry.fill_header(log_data, data_len);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "ObLogEntry fill_header error[ret=%d log_data=%p data_len=%ld]", ret, log_data, data_len);
    } else {
      if ((data_len + entry.get_serialize_size() + LOG_FILE_ALIGN_SIZE) > log_buffer_.get_remain()) {
        TBSYS_LOG(DEBUG, "log_buffer_ remaining length[%ld] is less then \"data_len[%ld] + ObLogEntry[%ld]\"",
                  log_buffer_.get_remain(), data_len, entry.get_serialize_size());
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        ret = entry.serialize(log_buffer_.get_data(), log_buffer_.get_capacity(), log_buffer_.get_position());
        if (OB_SUCCESS != ret) {
          TBSYS_LOG(ERROR, "ObLogEntry serialize error[ret=%d buffer_remain=%ld]", ret, log_buffer_.get_remain());
        } else {
          if (NULL != log_data) {
            memcpy(log_buffer_.get_data() + log_buffer_.get_position(), log_data, data_len);
            log_buffer_.get_position() += data_len;
          }
          cur_log_seq_ = new_log_seq;
        }
      }
    }
  }

  return ret;
}

int ObLogWriter::serialize_nop_log_() {
  int ret = OB_SUCCESS;

  ObLogEntry entry;

  if (log_buffer_.get_position() == (log_buffer_.get_position() & LOG_FILE_ALIGN_MASK)) {
    TBSYS_LOG(DEBUG, "The log is aligned");
  } else {
    uint64_t new_log_seq = cur_log_seq_ + 1;
    int64_t data_len = (log_buffer_.get_position() & LOG_FILE_ALIGN_MASK)
                       + LOG_FILE_ALIGN_SIZE - log_buffer_.get_position();
    if (data_len <= entry.get_header_size()) {
      data_len += LOG_FILE_ALIGN_SIZE;
    }
    data_len -= entry.get_header_size();

    entry.set_log_seq(new_log_seq);
    entry.set_log_command(OB_LOG_NOP);
    ret = entry.fill_header(empty_log_, data_len);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "ObLogEntry fill_header error[ret=%d data_len=%ld]", ret, data_len);
    } else {
      ret = entry.serialize(log_buffer_.get_data(), log_buffer_.get_capacity(), log_buffer_.get_position());
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "ObLogEntry serialize error[ret=%d buffer_remain=%ld]", ret, log_buffer_.get_remain());
      } else {
        memcpy(log_buffer_.get_data() + log_buffer_.get_position(), empty_log_, data_len);
        log_buffer_.get_position() += data_len;
        cur_log_seq_ = new_log_seq;
      }
    }
  }

  return ret;
}

int ObLogWriter::check_log_file_size_(const int64_t new_length) {
  int ret = OB_SUCCESS;

  if ((cur_log_size_ + new_length) > log_file_max_size_) {
    uint64_t new_log_file_id = 0;
    ret = switch_log_file(new_log_file_id);
  }

  return ret;
}


