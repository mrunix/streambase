/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include <algorithm>
#include <tbsys.h>
#include "nameserver/nameserver_meta.h"
#include "nameserver/tablet_info_manager.h"

namespace sb {
using namespace common;
namespace nameserver {

NameMeta::NameMeta(): tablet_info_index_(OB_INVALID_INDEX), last_dead_server_time_(0), migrate_monotonic_time_(0) {
  for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
    server_info_indexes_[i] = OB_INVALID_INDEX;
    tablet_version_[i] = 0;
  }
}
void NameMeta::dump() const {
  for (int32_t i = 0; i < common::OB_SAFE_COPY_COUNT; i++) {
    TBSYS_LOG(INFO, "server_info_index = %d, tablet_version = %ld",
              server_info_indexes_[i], tablet_version_[i]);
  }
  TBSYS_LOG(INFO, "last_dead_server_time = %ld migrate_monotonic_time = %ld",
            last_dead_server_time_, migrate_monotonic_time_);

}
void NameMeta::dump_as_hex(FILE* stream) const {
  if (stream != NULL) {
    fprintf(stream, "tablet_info_index %d ", tablet_info_index_);
  }
  for (int32_t i = 0; i < common::OB_SAFE_COPY_COUNT; i++) {
    fprintf(stream, "server_info_index %d tablet_version %ld ",
            server_info_indexes_[i], tablet_version_[i]);
  }
  fprintf(stream, "last_dead_server_time %ld\n", last_dead_server_time_);
  return;
}
void NameMeta::read_from_hex(FILE* stream) {
  if (stream != NULL) {
    fscanf(stream, "tablet_info_index %d ", &tablet_info_index_);
  }
  for (int32_t i = 0; i < common::OB_SAFE_COPY_COUNT; i++) {
    fscanf(stream, "server_info_index %d tablet_version %ld ",
           &server_info_indexes_[i], &tablet_version_[i]);
  }
  fscanf(stream, "last_dead_server_time %ld\n", &last_dead_server_time_);
  return;
}
DEFINE_SERIALIZE(NameMeta) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, tablet_info_index_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, last_dead_server_time_);
  }

  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
    if (OB_SUCCESS == ret) {
      ret = serialization::encode_vi32(buf, buf_len, tmp_pos, server_info_indexes_[i]);
    }
    if (OB_SUCCESS == ret) {
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, tablet_version_[i]);
    }
  }

  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_DESERIALIZE(NameMeta) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &tablet_info_index_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &last_dead_server_time_);
  }
  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
    if (OB_SUCCESS == ret) {
      ret = serialization::decode_vi32(buf, data_len, tmp_pos, &(server_info_indexes_[i]));
    }
    if (OB_SUCCESS == ret) {
      ret = serialization::decode_vi64(buf, data_len, tmp_pos, &(tablet_version_[i]));
    }
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_GET_SERIALIZE_SIZE(NameMeta) {
  int64_t len = serialization::encoded_length_vi32(tablet_info_index_);
  len += serialization::encoded_length_vi64(last_dead_server_time_);
  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++) {
    len += serialization::encoded_length_vi32(server_info_indexes_[i]);
    len += serialization::encoded_length_vi64(tablet_version_[i]);
  }
  return len;
}
ObRootMeta2CompareHelper::ObRootMeta2CompareHelper(ObTabletInfoManager* otim): tablet_info_manager_(otim) {
}
int ObRootMeta2CompareHelper::compare(const int32_t r1, const int32_t r2) const {
  int ret = 0;
  if (tablet_info_manager_ != NULL) {
    const ObTabletInfo* p_r1 = NULL;
    const ObTabletInfo* p_r2 = NULL;
    p_r1 = tablet_info_manager_->get_tablet_info(r1);
    p_r2 = tablet_info_manager_->get_tablet_info(r2);
    if (p_r1 != p_r2) {
      if (p_r1 == NULL) {
        TBSYS_LOG(WARN, "no tablet info, idx=%d", r1);
        ret = -1;
      } else if (p_r2 == NULL) {
        TBSYS_LOG(WARN, "no tablet info, idx=%d", r2);
        ret = 1;
      } else {
        ret = p_r1->range_.compare_with_endkey(p_r2->range_);
      }
    }
  } else {
    TBSYS_LOG(WARN, "NULL info manager");
  }
  return ret;
}
bool ObRootMeta2CompareHelper::operator()(const NameMeta& r1, const NameMeta& r2) const {
  return compare(r1.tablet_info_index_, r2.tablet_info_index_) < 0;
}

} // end namespace nameserver
} // end namespace sb

