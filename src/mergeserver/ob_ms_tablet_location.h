/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_tablet_location.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef OB_MS_TABLET_LOCATION_CACHE_H_
#define OB_MS_TABLET_LOCATION_CACHE_H_


#include "ob_ms_cache_table.h"
#include "common/ob_string.h"
#include "common/ob_range.h"
#include "ob_ms_tablet_location_item.h"

namespace sb {
namespace mergeserver {
// cache
class ObMergerTabletLocationCache {
 public:
  /// construction
  ObMergerTabletLocationCache();
  /// deconstruction
  virtual ~ObMergerTabletLocationCache();

 public:
  /// cache item timeout when error occured
  static const int64_t CACHE_ERROR_TIMEOUT = 1000 * 1000 * 2L; // 2s

  // cache item alive timeout interval
  static const int64_t DEFAULT_ALIVE_TIMEOUT = 1000 * 1000 * 60 * 10L; // 10minutes

  /// init the cache item count
  int init(const uint64_t mem_size, const uint64_t count, const int64_t timeout);

  /// get cache item timeout for washout
  int64_t get_timeout(void) const;

  /// get table_id.rowkey location server list
  int get(const uint64_t table_id, const common::ObString& rowkey,
          ObMergerTabletLocationList& location);

  /// set the new range location, if range not valid, then del the old info
  int set(const common::ObRange& range, const ObMergerTabletLocationList& location);

  /// update the exist range location, if not exist return error
  int update(const uint64_t table_id, const common::ObString& rowkey,
             const ObMergerTabletLocationList& location);

  /// delete the cache item according to table_id.rowkey
  int del(const uint64_t table_id, const common::ObString& rowkey);

  /// cache item count
  uint64_t size(void);

  /// clear all items for sys admin
  int clear(void);

 private:
  // init cache
  bool init_;
  // cache alive timeout
  int64_t cache_timeout_;
  // tablet location cache
  common::ObVarCache<int, int, ObCBtreeTable> tablet_cache_;
};

inline int64_t ObMergerTabletLocationCache::get_timeout(void) const {
  return cache_timeout_;
}
}
}


#endif // OB_MS_TABLET_LOCATION_CACHE_H_




