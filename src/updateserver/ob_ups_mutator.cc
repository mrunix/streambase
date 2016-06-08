/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_mutator.cc for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#include "ob_ups_mutator.h"

namespace sb {
namespace updateserver {
using namespace sb::common;

ObUpsMutator :: ObUpsMutator() {
  version_ = 0;
  flag_ = NORMAL_FLAG;
  mutate_timestamp_ = 0;
  memtable_checksum_before_mutate_ = 0;
  memtable_checksum_after_mutate_ = 0;
}

ObUpsMutator :: ~ObUpsMutator() {
  // empty
}

ObMutator& ObUpsMutator:: get_mutator() {
  return mutator_;
}

int ObUpsMutator :: set_freeze_memtable() {
  int err = OB_SUCCESS;

  if (NORMAL_FLAG != flag_) {
    TBSYS_LOG(WARN, "invalid status, flag_=%d", flag_);
    err = OB_ERROR;
  } else {
    flag_ = FREEZE_FLAG;
  }

  return err;
}

int ObUpsMutator :: set_drop_memtable() {
  int err = OB_SUCCESS;

  if (NORMAL_FLAG != flag_) {
    TBSYS_LOG(WARN, "invalid status, flag_=%d", flag_);
    err = OB_ERROR;
  } else {
    flag_ = DROP_FLAG;
  }

  return err;
}

int ObUpsMutator :: set_first_start() {
  int err = OB_SUCCESS;

  if (NORMAL_FLAG != flag_) {
    TBSYS_LOG(WARN, "invalid status, flag_=%d", flag_);
    err = OB_ERROR;
  } else {
    flag_ = START_FLAG;
  }

  return err;
}

bool ObUpsMutator :: is_normal_mutator() const {
  return flag_ == NORMAL_FLAG;
}

bool ObUpsMutator :: is_freeze_memtable() const {
  return flag_ == FREEZE_FLAG;
}

bool ObUpsMutator :: is_drop_memtable() const {
  return flag_ == DROP_FLAG;
}

bool ObUpsMutator :: is_first_start() const {
  return flag_ == START_FLAG;
}

void ObUpsMutator :: set_mutate_timestamp(const int64_t timestamp) {
  mutate_timestamp_ = timestamp;
}

int64_t ObUpsMutator :: get_mutate_timestamp() const {
  return mutate_timestamp_;
}

void ObUpsMutator :: set_memtable_checksum_before_mutate(const int64_t checksum) {
  memtable_checksum_before_mutate_ = checksum;
}

int64_t ObUpsMutator :: get_memtable_checksum_before_mutate() const {
  return memtable_checksum_before_mutate_;
}

void ObUpsMutator :: set_memtable_checksum_after_mutate(const int64_t checksum) {
  memtable_checksum_after_mutate_ = checksum;
}

int64_t ObUpsMutator :: get_memtable_checksum_after_mutate() const {
  return memtable_checksum_after_mutate_;
}

void ObUpsMutator :: reset_iter() {
  mutator_.reset_iter();
}

int ObUpsMutator :: next_cell() {
  return mutator_.next_cell();
}

int ObUpsMutator :: get_cell(ObMutatorCellInfo** cell) {
  return mutator_.get_cell(cell);
}

int ObUpsMutator :: serialize(char* buf, const int64_t buf_len, int64_t& pos) const {
  int err = OB_SUCCESS;

  if (NULL == buf || buf_len <= 0 || pos >= buf_len) {
    TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
    err = OB_INVALID_ARGUMENT;
  } else {
    if ((pos + 2 * (int64_t) sizeof(int32_t) + 2 * (int64_t) sizeof(int64_t)) >= buf_len) {
      TBSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
      err = OB_ERROR;
    } else {
      *(reinterpret_cast<int32_t*>(buf + pos)) = version_;
      pos += sizeof(int32_t);
      *(reinterpret_cast<int32_t*>(buf + pos)) = flag_;
      pos += sizeof(int32_t);
      *(reinterpret_cast<int64_t*>(buf + pos)) = mutate_timestamp_;
      pos += sizeof(int64_t);
      *(reinterpret_cast<int64_t*>(buf + pos)) = memtable_checksum_before_mutate_;
      pos += sizeof(int64_t);
      *(reinterpret_cast<int64_t*>(buf + pos)) = memtable_checksum_after_mutate_;
      pos += sizeof(int64_t);

      err = mutator_.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to serialize mutator, err=%d", err);
      }
    }
  }

  return err;
}

int ObUpsMutator :: deserialize(const char* buf, const int64_t buf_len, int64_t& pos) {
  int err = OB_SUCCESS;

  if (NULL == buf || buf_len <= 0 || pos >= buf_len) {
    TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
    err = OB_INVALID_ARGUMENT;
  } else {
    version_ = *(reinterpret_cast<const int32_t*>(buf + pos));
    pos += sizeof(int32_t);
    flag_ = *(reinterpret_cast<const int32_t*>(buf + pos));
    pos += sizeof(int32_t);
    mutate_timestamp_ = *(reinterpret_cast<const int64_t*>(buf + pos));
    pos += sizeof(int64_t);
    memtable_checksum_before_mutate_ = *(reinterpret_cast<const int64_t*>(buf + pos));
    pos += sizeof(int64_t);
    memtable_checksum_after_mutate_ = *(reinterpret_cast<const int64_t*>(buf + pos));
    pos += sizeof(int64_t);

    err = mutator_.deserialize(buf, buf_len, pos);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to deserialize mutator, err=%d", err);
    }
  }

  return err;
}

int64_t ObUpsMutator :: get_serialize_size(void) const {
  return mutator_.get_serialize_size() + 2 * sizeof(int32_t) + 2 * sizeof(int64_t);
}
}
}



