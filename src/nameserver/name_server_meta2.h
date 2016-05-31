/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1
 *
 *   Authors:
 *      daoan <daoan@taobao.com>
 *
 */
#ifndef OCEANBASE_ROOTSERVER_OB_ROOTMETA2_H_
#define OCEANBASE_ROOTSERVER_OB_ROOTMETA2_H_
#include <stdio.h>

#include "common/page_arena.h"
#include "common/ob_tablet_info.h"
#include "common/ob_define.h"
#include "common/ob_range2.h"
namespace sb {
namespace nameserver {
class ObTabletInfoManager;
struct NameServerMeta2 {
  int32_t tablet_info_index_;
  //index of chunk server manager. so we can find out the server' info by this
  mutable int32_t server_info_indexes_[common::OB_SAFE_COPY_COUNT];
  mutable int64_t tablet_version_[common::OB_SAFE_COPY_COUNT];
  mutable int64_t last_dead_server_time_;
  mutable int64_t last_migrate_time_; // don't serialize

  NameServerMeta2();
  void dump() const;
  void dump(const int server_index, int64_t& tablet_num) const;
  void dump_as_hex(FILE* stream) const;
  void read_from_hex(FILE* stream);
  bool did_cs_have(const int32_t cs_idx) const;
  bool can_be_migrated_now(int64_t disabling_period_us) const;
  void has_been_migrated();
  int64_t get_max_tablet_version() const;
  int32_t get_copy_count() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
};

inline int32_t NameServerMeta2::get_copy_count() const {
  int32_t count = 0;
  for (int32_t i = 0 ; i < common::OB_SAFE_COPY_COUNT; ++i) {
    if (common::OB_INVALID_INDEX != server_info_indexes_[i]) {
      ++count;
    }
  }
  return count;
}

inline bool NameServerMeta2::did_cs_have(const int32_t cs_idx) const {
  bool ret = false;
  for (int i = 0; i < common::OB_SAFE_COPY_COUNT; ++i) {
    if (cs_idx == server_info_indexes_[i]) {
      ret = true;
      break;
    }
  }
  return ret;
}

inline int64_t NameServerMeta2::get_max_tablet_version() const {
  int64_t max_tablet_version = 0;
  for (int32_t i = 0 ; i < common::OB_SAFE_COPY_COUNT; i++) {
    if (this->tablet_version_[i] > max_tablet_version) {
      max_tablet_version = this->tablet_version_[i];
    }
  }
  return max_tablet_version;
}

class NameServerMeta2CompareHelper {
 public:
  explicit NameServerMeta2CompareHelper(ObTabletInfoManager* otim);
  bool operator()(const NameServerMeta2& r1, const NameServerMeta2& r2) const;
  int compare(const int32_t r1, const int32_t r2) const;
 private:
  ObTabletInfoManager* tablet_info_manager_;
};

struct NameServerMeta2RangeLessThan {
 public:
  explicit NameServerMeta2RangeLessThan(ObTabletInfoManager* tim);
  bool operator()(const NameServerMeta2& r1, const common::ObNewRange& r2) const;
 private:
  ObTabletInfoManager* tablet_info_manager_;
};

struct NameServerMeta2TableIdLessThan {
 public:
  explicit NameServerMeta2TableIdLessThan(ObTabletInfoManager* tim);
  bool operator()(const NameServerMeta2& r1, const common::ObNewRange& r2) const;
 private:
  ObTabletInfoManager* tablet_info_manager_;
};

} // end namespace nameserver
} // end namespace sb


#endif
