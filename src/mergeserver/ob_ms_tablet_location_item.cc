/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_tablet_location_item.cc for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#include "ob_ms_tablet_location_item.h"
#include "common/ob_tablet_info.h"
#include "common/serialization.h"

using namespace sb::common;
using namespace sb::mergeserver;


ObMergerTabletLocationList::ObMergerTabletLocationList() {
  cur_count_ = 0;
  timestamp_ = 0;
}

ObMergerTabletLocationList::~ObMergerTabletLocationList() {
}

// list size is too small so using bubble sort asc
int ObMergerTabletLocationList::sort(const ObServer& server) {
  // asc
  int ret = OB_SUCCESS;
  if (cur_count_ > 0) {
    int32_t server_ip = server.get_ipv4();
    ObMergerTabletLocation temp;
    for (int64_t i = cur_count_ - 1; i > 0; --i) {
      for (int64_t j = 0; j < i; ++j) {
        if (abs(locations_[j].server_.chunkserver_.get_ipv4() - server_ip) >
            abs(locations_[j + 1].server_.chunkserver_.get_ipv4() - server_ip)) {
          temp = locations_[j];
          locations_[j] = locations_[j + 1];
          locations_[j + 1] = temp;
        }
      }
    }
  }
  return ret;
}

void ObMergerTabletLocationList::set_item_valid(const int64_t timestamp) {
  timestamp_ = timestamp;
  for (int64_t i = 0; i < cur_count_; ++i) {
    locations_[i].err_times_ = 0;
  }
}

int64_t ObMergerTabletLocationList::get_valid_count(void) const {
  int64_t ret = 0;
  for (int64_t i = 0; i < cur_count_; ++i) {
    if (locations_[i].err_times_ < ObMergerTabletLocation::MAX_ERR_TIMES) {
      ++ret;
    }
  }
  return ret;
}

int ObMergerTabletLocationList::set_item_invalid(const ObMergerTabletLocation& location) {
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < cur_count_; ++i) {
    if ((locations_[i].server_.tablet_version_ == location.server_.tablet_version_)
        && (locations_[i].server_.chunkserver_ == location.server_.chunkserver_)) {
      // set to max err times
      locations_[i].err_times_ = ObMergerTabletLocation::MAX_ERR_TIMES;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}


int ObMergerTabletLocationList::del(const int64_t index, ObMergerTabletLocation& location) {
  int ret = OB_SUCCESS;
  if ((index < 0) || (index >= cur_count_)) {
    TBSYS_LOG(ERROR, "check index failed:index[%ld], count[%ld]", index, cur_count_);
    ret = OB_INPUT_PARAM_ERROR;
  } else {
    location = locations_[index];
    if (index < (MAX_REPLICA_COUNT - 1)) {
      memmove(&(locations_[index]), &(locations_[index + 1]),
              (cur_count_ - index - 1) * sizeof(ObTabletLocation));
    }
    --cur_count_;
  }
  return ret;
}


int ObMergerTabletLocationList::add(const ObTabletLocation& location) {
  int ret = OB_SUCCESS;
  if (cur_count_ < MAX_REPLICA_COUNT) {
    locations_[cur_count_].server_ = location;
    locations_[cur_count_].err_times_ = 0;
    ++cur_count_;
  } else {
    TBSYS_LOG(ERROR, "the items is full:count[%ld]", cur_count_);
    ret = OB_NO_EMPTY_ENTRY;
  }
  return ret;
}

void ObMergerTabletLocationList::print_info(void) const {
  TBSYS_LOG(DEBUG, "print tablet location servers:count[%ld], timestamp[%ld]",
            cur_count_, timestamp_);
  for (int64_t i = 0; i < cur_count_; ++i) {
    TBSYS_LOG(DEBUG, "check server error status:times[%lu]", locations_[i].err_times_);
    ObTabletLocation::dump(locations_[i].server_);
  }
}

// ObMergerTabletLocationList
DEFINE_SERIALIZE(ObMergerTabletLocationList) {
  int ret = OB_SUCCESS;
  ret = serialization::encode_vi64(buf, buf_len, pos, timestamp_);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "serialize timestamp failed:timestamp[%ld], ret[%d]",
              timestamp_, ret);
  } else {
    ret = serialization::encode_vi64(buf, buf_len, pos, cur_count_);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "serialize cur count failed:count[%ld], ret[%d]",
                cur_count_, ret);
    }
  }
  if (OB_SUCCESS == ret) {
    for (int64_t i = 0; i < cur_count_; ++i) {
      ret = serialization::encode_vi64(buf, buf_len, pos, locations_[i].err_times_);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "serialize err times failed:index[%ld], ret[%d]", i, ret);
        break;
      } else {
        ret = locations_[i].server_.serialize(buf, buf_len, pos);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "serailize location failed:index[%ld], ret[%d]", i, ret);
          break;
        }
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObMergerTabletLocationList) {
  int ret = OB_SUCCESS;
  ret = serialization::decode_vi64(buf, data_len, pos, &timestamp_);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "deserialize timestamp failed:ret[%d]", ret);
  } else {
    ret = serialization::decode_vi64(buf, data_len, pos, &cur_count_);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "deserialize cur count failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret) {
    for (int64_t i = 0; i < cur_count_; ++i) {
      ret = serialization::decode_vi64(buf, data_len, pos, &(locations_[i].err_times_));
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "deserialize err times failed:index[%ld], ret[%d]", i, ret);
        break;
      } else {
        ret = locations_[i].server_.deserialize(buf, data_len, pos);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "deserialize location failed:index[%ld], ret[%d]", i, ret);
          break;
        }
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObMergerTabletLocationList) {
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(timestamp_);
  total_size += serialization::encoded_length_vi64(cur_count_);
  for (int64_t i = 0; i < cur_count_; ++i) {
    total_size += serialization::encoded_length_vi64(locations_[i].err_times_);
    total_size += locations_[i].server_.get_serialize_size();
  }
  return total_size;
}





