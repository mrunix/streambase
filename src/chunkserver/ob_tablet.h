/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_tablet.h
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     rizhao <rizhao.ych@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_TABLET_H__
#define __OCEANBASE_CHUNKSERVER_OB_TABLET_H__

#include "common/ob_range.h"
#include "common/ob_array_helper.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_sstable_reader.h"

namespace sb {
namespace chunkserver {
class sstable::ObSSTableReader;
class ObTabletImage;

struct ObTabletRangeInfo {
  int16_t start_key_size_;
  int16_t end_key_size_;
  int16_t is_merged_;
  int16_t border_flag_;
  int64_t table_id_;

  NEED_SERIALIZE_AND_DESERIALIZE;
};

class ObTablet {
 public:
  static const int32_t MAX_SSTABLE_PER_TABLET = 64;
 public:
  ObTablet();
  ~ObTablet();
 public:
  int find_sstable(const common::ObRange& range,
                   sstable::ObSSTableReader* sstable[], int32_t& size) const ;
  int find_sstable(const common::ObString& key,
                   sstable::ObSSTableReader* sstable[], int32_t& size) const;
  int find_sstable(const sstable::ObSSTableId& sstable_id,
                   sstable::ObSSTableReader*& reader) const;
  int64_t get_max_sstable_file_seq() const;
  int64_t get_row_count() const;
  int64_t get_occupy_size() const;
 public:
  int include_sstable(const sstable::ObSSTableId& sstable_id) const;
  int add_sstable_by_id(const sstable::ObSSTableId& sstable_id);
  inline const common::ObArrayHelper<sstable::ObSSTableId>&
  get_sstable_id_list() const { return sstable_id_list_; }
  inline const common::ObArrayHelper<sstable::ObSSTableReader*>&
  get_sstable_reader_list() const { return sstable_reader_list_; }
  int load_sstable(ObTabletImage* tablet_image);
  int dump(const bool dump_sstable = false) const;

 public:
  inline void set_range(const common::ObRange& range) {
    range_ = range;
  }
  inline const common::ObRange& get_range(void) const {
    return range_;
  }

  inline void set_data_version(const int64_t version) {
    data_version_ = version;
  }
  inline int64_t get_data_version(void) const {
    return data_version_;
  }
  inline int32_t get_disk_no() const {
    return disk_no_;
  }
  inline void set_disk_no(int32_t disk_no) {
    disk_no_ = disk_no;
  }
  inline void set_merged(int status = 1) { merged_ = status; }
  inline bool is_merged() const { return merged_ > 0; }
  inline int32_t get_merge_count() const { return merge_count_; }
  inline void inc_merge_count() { ++merge_count_; }


  int get_checksum(uint64_t& tablet_checksum) const;
  void get_range_info(ObTabletRangeInfo& info) const;
  int set_range_by_info(const ObTabletRangeInfo& info,
                        char* row_key_stream_ptr, const int64_t row_key_stream_size);

 public:
  NEED_SERIALIZE_AND_DESERIALIZE;

 private:
  void destroy();
  void reset();
 private:
  common::ObRange range_;
  int32_t sstable_loaded_;
  int32_t merged_; // merge succeed
  int32_t merge_count_;
  int32_t disk_no_;
  int64_t data_version_;
  sstable::ObSSTableId sstable_id_inventory_[MAX_SSTABLE_PER_TABLET];
  sstable::ObSSTableReader* sstable_reader_inventory_[MAX_SSTABLE_PER_TABLET];
  common::ObArrayHelper<sstable::ObSSTableId> sstable_id_list_;
  common::ObArrayHelper<sstable::ObSSTableReader*> sstable_reader_list_;
};
}
}

#endif //__OB_TABLET_H__

