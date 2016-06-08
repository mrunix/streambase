/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_info.h for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_TABLET_INFO_H_
#define OCEANBASE_COMMON_OB_TABLET_INFO_H_

#include "ob_define.h"
#include "ob_range.h"
#include "ob_server.h"
#include "ob_array_helper.h"

namespace sb {
namespace common {
struct ObTabletLocation {
  int64_t tablet_version_;
  ObServer chunkserver_;

  ObTabletLocation() : tablet_version_(0) { }
  ObTabletLocation(int64_t tablet_version, const ObServer& server)
    : tablet_version_(tablet_version), chunkserver_(server) { }

  static void dump(const ObTabletLocation& location);

  NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObTabletInfo {
  ObRange range_;
  int64_t row_count_;
  int64_t occupy_size_;
  uint64_t crc_sum_;

  ObTabletInfo()
    : range_(), row_count_(0), occupy_size_(0), crc_sum_(0) {}
  ObTabletInfo(const ObRange& r, const int32_t rc, const int32_t sz, const uint64_t crc_sum = 0)
    : range_(r), row_count_(rc), occupy_size_(sz), crc_sum_(crc_sum) {}

  inline bool equal(const ObTabletInfo& tablet) const {
    return range_.equal(tablet.range_);
  }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObTabletReportInfo {
  ObTabletInfo tablet_info_;
  ObTabletLocation tablet_location_;

  NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObTabletReportInfoList {
  ObTabletReportInfo tablets_[OB_MAX_TABLET_LIST_NUMBER];
  ObArrayHelper<ObTabletReportInfo> tablet_list_;

  ObTabletReportInfoList() {
    reset();
  }

  void reset() {
    tablet_list_.init(OB_MAX_TABLET_LIST_NUMBER, tablets_);
  }

  inline int add_tablet(const ObTabletReportInfo& tablet) {
    int ret = OB_SUCCESS;

    if (!tablet_list_.push_back(tablet))
      ret = OB_ARRAY_OUT_OF_RANGE;

    return ret;
  }

  inline int64_t get_tablet_size() const { return tablet_list_.get_array_index(); }
  inline const ObTabletReportInfo* get_tablet() const { return tablets_; }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObTabletInfoList {
  ObTabletInfo tablets[OB_MAX_TABLET_LIST_NUMBER];
  ObArrayHelper<ObTabletInfo> tablet_list;

  ObTabletInfoList() {
    reset();
  }

  void reset() {
    tablet_list.init(OB_MAX_TABLET_LIST_NUMBER, tablets);
  }

  inline int add_tablet(const ObTabletInfo& tablet) {
    int ret = OB_SUCCESS;

    if (!tablet_list.push_back(tablet))
      ret = OB_ARRAY_OUT_OF_RANGE;

    return ret;
  }

  inline int64_t get_tablet_size() const { return tablet_list.get_array_index(); }
  inline const ObTabletInfo* get_tablet() const { return tablets; }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

} // end namespace common
} // end namespace sb

#endif //OCEANBASE_ROOTSERVER_COMMONDATA_H_


