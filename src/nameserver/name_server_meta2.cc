/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *
 *   Version: 0.1
 *
 *   Authors:
 *      daoan <daoan@taobao.com>
 *
 */
#include <algorithm>
#include <tbsys.h>
#include "nameserver/name_server_meta2.h"
#include "nameserver/ob_tablet_info_manager.h"

namespace sb {
using namespace common;
namespace nameserver {

NameServerMeta2::NameServerMeta2(): tablet_info_index_(OB_INVALID_INDEX), last_dead_server_time_(0), last_migrate_time_(0) {
  for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
    server_info_indexes_[i] = OB_INVALID_INDEX;
    tablet_version_[i] = 0;
  }
}
void NameServerMeta2::dump() const {
  for (int32_t i = 0; i < common::OB_SAFE_COPY_COUNT; i++) {
    TBSYS_LOG(INFO, "server_info_index = %d, tablet_version = %ld",
              server_info_indexes_[i], tablet_version_[i]);
  }
  TBSYS_LOG(INFO, "last_dead_server_time = %ld last_migrate_time = %ld",
            last_dead_server_time_, last_migrate_time_);

}

void NameServerMeta2::dump(const int server_index, int64_t& tablet_num) const {
  for (int32_t i = 0; i < common::OB_SAFE_COPY_COUNT; i++) {
    if (server_index == server_info_indexes_[i]) {
      tablet_num ++;
      TBSYS_LOG(INFO, "tablet_number = %ld, server_info_index = %d, tablet_version = %ld",
                tablet_num, server_info_indexes_[i], tablet_version_[i]);
    }
  }
}
void NameServerMeta2::dump_as_hex(FILE* stream) const {
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
void NameServerMeta2::read_from_hex(FILE* stream) {
  int __attribute__((unused)) ret;
  if (stream != NULL) {
    ret = fscanf(stream, "tablet_info_index %d ", &tablet_info_index_);
  }
  for (int32_t i = 0; i < common::OB_SAFE_COPY_COUNT; i++) {
    ret = fscanf(stream, "server_info_index %d tablet_version %ld ",
                 &server_info_indexes_[i], &tablet_version_[i]);
  }
  ret = fscanf(stream, "last_dead_server_time %ld\n", &last_dead_server_time_);
  return;
}
DEFINE_SERIALIZE(NameServerMeta2) {
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
DEFINE_DESERIALIZE(NameServerMeta2) {
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
DEFINE_GET_SERIALIZE_SIZE(NameServerMeta2) {
  int64_t len = serialization::encoded_length_vi32(tablet_info_index_);
  len += serialization::encoded_length_vi64(last_dead_server_time_);
  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++) {
    len += serialization::encoded_length_vi32(server_info_indexes_[i]);
    len += serialization::encoded_length_vi64(tablet_version_[i]);
  }
  return len;
}

void NameServerMeta2::has_been_migrated() {
  last_migrate_time_ = tbsys::CTimeUtil::getTime();
}

bool NameServerMeta2::can_be_migrated_now(int64_t disabling_period_us) const {
  bool ret = false;
  if (0 >= last_migrate_time_) {
    ret = true;
  } else {
    int64_t now = tbsys::CTimeUtil::getTime();
    ret = (now > last_migrate_time_ + disabling_period_us);
  }
  return ret;
}

NameServerMeta2CompareHelper::NameServerMeta2CompareHelper(ObTabletInfoManager* otim): tablet_info_manager_(otim) {
  if (NULL == otim) {
    TBSYS_LOG(ERROR, "tablet_info_manager_ is NULL");
  }
}

int NameServerMeta2CompareHelper::compare(const int32_t r1, const int32_t r2) const {
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
bool NameServerMeta2CompareHelper::operator()(const NameServerMeta2& r1, const NameServerMeta2& r2) const {
  int res = compare(r1.tablet_info_index_, r2.tablet_info_index_);
  if (0 == res) {
    //res = static_cast<int> (&r1 - &r2);
    res = static_cast<int>(r1.tablet_info_index_ - r2.tablet_info_index_);
  }
  return res < 0;
}

NameServerMeta2RangeLessThan::NameServerMeta2RangeLessThan(ObTabletInfoManager* tim) {
  tablet_info_manager_ = tim;
}

bool NameServerMeta2RangeLessThan::operator()(const NameServerMeta2& r1, const ObNewRange& r2) const {
  bool ret = true;
  const ObTabletInfo* tablet_info = NULL;
  if (NULL == tablet_info_manager_) {
    TBSYS_LOG(ERROR, "tablet_info_manager_ is NULL");
  } else {
    if (NULL == (tablet_info = tablet_info_manager_->get_tablet_info(r1.tablet_info_index_))) {
      TBSYS_LOG(ERROR, "tablet_info not exist");
    } else {
      ret = (tablet_info->range_.compare_with_endkey(r2) < 0);
    }
  }
  return ret;
}

NameServerMeta2TableIdLessThan::NameServerMeta2TableIdLessThan(ObTabletInfoManager* tim) {
  tablet_info_manager_ = tim;
}

bool NameServerMeta2TableIdLessThan::operator()(const NameServerMeta2& r1, const ObNewRange& r2) const {
  bool ret = true;
  const ObTabletInfo* tablet_info = NULL;
  if (NULL == tablet_info_manager_) {
    TBSYS_LOG(ERROR, "tablet_info_manager_ is NULL");
  } else {
    if (NULL == (tablet_info = tablet_info_manager_->get_tablet_info(r1.tablet_info_index_))) {
      TBSYS_LOG(ERROR, "tablet_info not exist");
    } else {
      ret = (tablet_info->range_.table_id_ < r2.table_id_);
    }
  }
  return ret;
}

} // end namespace nameserver
} // end namespace sb
