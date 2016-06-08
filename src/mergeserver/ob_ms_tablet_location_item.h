/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_tablet_location_item.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef OB_MERGER_TABLET_LOCATION_ITEM_H_
#define OB_MERGER_TABLET_LOCATION_ITEM_H_

#include "common/ob_tablet_info.h"

namespace sb {
namespace mergeserver {
/// server and access err times
struct ObMergerTabletLocation {
  static const int64_t MAX_ERR_TIMES = 30;
  int64_t err_times_;
  common::ObTabletLocation server_;
  ObMergerTabletLocation() : err_times_(0) {}
};

/// tablet location item info
class ObMergerTabletLocationList {
 public:
  ObMergerTabletLocationList();
  virtual ~ObMergerTabletLocationList();

 public:
  static const int64_t MAX_REPLICA_COUNT = 3;

  /// add a tablet location to item list
  int add(const common::ObTabletLocation& location);

  /// del the index pos TabletLocation
  int del(const int64_t index, ObMergerTabletLocation& location);

  /// set item invalid status
  int set_item_invalid(const ObMergerTabletLocation& location);

  /// set item valid status
  void set_item_valid(const int64_t timestamp);

  /// get valid item count
  int64_t get_valid_count(void) const;

  /// operator random access
  ObMergerTabletLocation& operator[](const uint64_t index);

  /// sort the server list
  int sort(const common::ObServer& server);

  /// current tablet locastion server count
  int64_t size(void) const;

  /// clear all items
  void clear(void);

  /// get modify timestamp
  int64_t get_timestamp(void) const;
  /// set timestamp
  void set_timestamp(const int64_t timestamp);

  /// dump all info
  void print_info(void) const;

  /// serailize or deserialization
  NEED_SERIALIZE_AND_DESERIALIZE;

 private:
  int64_t cur_count_;
  int64_t timestamp_;
  ObMergerTabletLocation locations_[MAX_REPLICA_COUNT];
};

inline int64_t ObMergerTabletLocationList::size(void) const {
  return cur_count_;
}

inline void ObMergerTabletLocationList::clear(void) {
  timestamp_ = 0;
  cur_count_ = 0;
}

inline int64_t ObMergerTabletLocationList::get_timestamp(void) const {
  return timestamp_;
}

inline void ObMergerTabletLocationList::set_timestamp(const int64_t timestamp) {
  timestamp_ = timestamp;
}

inline ObMergerTabletLocation& ObMergerTabletLocationList::operator[](const uint64_t index) {
  return locations_[index];
}
}
}



#endif // OB_MERGER_TABLET_LOCATION_ITEM_H_




