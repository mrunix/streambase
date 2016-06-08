/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_chunk_merge.h
 *
 * Authors:
 *     maoqi <maoqi@taobao.com>
 * Changes:
 *     qushan <qushan@taobao.com>
 *
 */
#ifndef OB_CHUNKSERVER_OB_CHUNK_MERGE_H_
#define OB_CHUNKSERVER_OB_CHUNK_MERGE_H_
#include <tbsys.h>
#include <Mutex.h>
#include <Monitor.h>
#include "common/ob_define.h"
#include "common/ob_bitmap.h"
#include "common/ob_schema.h"
#include "common/ob_vector.h"
#include "common/thread_buffer.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_row.h"
#include "ob_tablet_manager.h"
#include "ob_file_recycle.h"

#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "mergeserver/ob_cell_stream.h"
#include "mergeserver/ob_merge_join_agent.h"
#include "ob_merge_reader.h"
#include "mergeserver/ob_cs_get_cell_stream_wrapper.h"
#include "ob_chunk_server_merger_proxy.h"

namespace sb {
namespace chunkserver {
class ObChunkMerge {
 public:
  ObChunkMerge();
  ~ObChunkMerge() {}

  int init(ObTabletManager* manager);
  void destroy();

  /**
   * the new frozen_version come,
   * we will wake up all merge thread to start merge
   *
   */
  int schedule(const int64_t frozen_version);

  inline const bool is_pending_in_upgrade() const {
    return pending_in_upgrade_;
  }

  inline bool is_merge_stoped() const {
    return 0 == active_thread_num_;
  }

  inline void set_newest_frozen_version(const int64_t frozen_version) {
    if (frozen_version > newest_frozen_version_)
      newest_frozen_version_ = frozen_version;
  }

  inline mergeserver::ObMSUpsStreamWrapper& get_ups_stream_wrapper() {
    return *ups_stream_wrapper_;
  }

  int fetch_update_server_list();

  // for compatible with nameserver interface;
  int fetch_update_server_for_merge();

  void set_config_param();

  bool can_launch_next_round(const int64_t frozen_version);

  int create_merge_threads(const int32_t max_merge_thread);

 private:
  static void* run(void* arg);
  void merge_tablets();
  int get_tablets(ObTablet*& tablet);

  bool have_new_version_in_othercs(const ObTablet* tablet);

  int start_round(const int64_t frozen_version);
  int finish_round(const int64_t frozen_version);
  int fetch_frozen_time_busy_wait(const int64_t frozen_version, int64_t& frozen_time);

 public:
  struct RowKeySwap {
    RowKeySwap(): last_end_key_buf(NULL), start_key_buf(NULL),
      last_end_key_buf_len(0), last_end_key_len(0),
      start_key_buf_len(0), start_key_len(0) {}

    char* last_end_key_buf;
    char* start_key_buf;

    int32_t last_end_key_buf_len;
    int32_t last_end_key_len;

    int32_t start_key_buf_len;
    int32_t start_key_len;
  };

  bool check_load();
 private:
  friend class ObTabletMerger;
  const static int32_t MAX_MERGE_THREAD = 16;
  const static int32_t TABLET_COUNT_PER_MERGE = 1024;
  const static uint32_t MAX_MERGE_PER_DISK = 2;
 private:
  volatile bool inited_;
  pthread_cond_t cond_;
  pthread_mutex_t mutex_;

  pthread_t tid_[MAX_MERGE_THREAD];

  ObTablet* tablet_array_[TABLET_COUNT_PER_MERGE];
  int32_t tablets_num_;
  int32_t tablet_index_;
  int32_t thread_num_;

  volatile int32_t tablets_have_got_;
  volatile int64_t active_thread_num_;

  volatile int64_t frozen_version_; //frozen_version_ in merge
  int64_t frozen_timestamp_; //current frozen_timestamp_ in merge;
  volatile int64_t newest_frozen_version_; //the last frozen version in updateserver

  volatile int64_t merge_start_time_;    //this version start time
  volatile int64_t merge_last_end_time_;    //this version merged complete time

  ObTabletManager* tablet_manager_;

  common::ThreadSpecificBuffer last_end_key_buffer_;
  common::ThreadSpecificBuffer cur_row_key_buffer_;

  volatile uint32_t pending_merge_[common::OB_MAX_DISK_NUMBER]; //use atomic op

  volatile bool round_start_;
  volatile bool round_end_;
  volatile bool pending_in_upgrade_;

  int64_t merge_load_high_;
  int64_t request_count_high_;
  int64_t merge_adjust_ratio_;
  int64_t merge_load_adjust_;
  int64_t merge_pause_row_count_;
  int64_t merge_pause_sleep_time_;
  int64_t merge_highload_sleep_time_;

  common::ObSchemaManagerV2 last_schema_;
  common::ObSchemaManagerV2 current_schema_;
  mergeserver::ObMSUpsStreamWrapper* ups_stream_wrapper_;
};


class ObTabletMerger {
 public:
  ObTabletMerger(ObChunkMerge& chunk_merge, ObTabletManager& manager);
  ~ObTabletMerger() {}

  int init(ObChunkMerge::RowKeySwap* swap);
  int merge(ObTablet* tablet, int64_t frozen_version);

 private:
  typedef common::ObBitmap<char, common::ModuleArena> ExpireRowFilter;

  void reset();

  DISALLOW_COPY_AND_ASSIGN(ObTabletMerger);

  enum RowStatus {
    ROW_START = 0,
    ROW_GROWING = 1,
    ROW_END = 2
  };

  struct ExpireColumnInfo {
    uint64_t column_id_;
    int64_t column_group_id_;
    int64_t duration_;
  };

  int fill_sstable_schema(const ObTableSchema& common_schema, sstable::ObSSTableSchema& sstable_schema);
  int update_range_start_key();
  int update_range_end_key();
  int create_new_sstable();
  int finish_sstable();
  int update_meta();
  int report_tablets(ObTablet* tablet_list[], int32_t tablet_size);
  bool maybe_change_sstable() const;

  int fill_scan_param(const uint64_t column_group_id);
  int wait_aio_buffer() const;
  int merge_column_group(
    const int64_t column_group_id,
    const int64_t column_group_num,
    const ExpireColumnInfo& expire_column_info,
    const int64_t split_row_pos,
    const int64_t tablet_after_merge,
    ExpireRowFilter& expire_row_filter,
    bool& is_tablet_splited
  );

  int save_current_row(const bool current_row_expired);
  int check_row_count_in_column_group();
  void reset_for_next_column_group();
  int check_expire_column(const common::ObSchemaManagerV2& schema,
                          const uint64_t table_id, ExpireColumnInfo& expire_column_info);
  bool is_expired_cell(const common::ObCellInfo& cell,
                       const int64_t expire_duration, const int64_t frozen_timestamp);


 private:
  ObChunkMerge&            chunk_merge_;
  ObTabletManager&         manager_;

  ObChunkMerge::RowKeySwap*   row_key_swap_;

  ObCellInfo*     cell_;
  ObTablet*       old_tablet_;
  const ObTableSchema* new_table_schema_;

  sstable::ObSSTableRow    row_;
  sstable::ObSSTableSchema sstable_schema_;
  sstable::ObSSTableWriter writer_;
  common::ObRange new_range_;
  sstable::ObSSTableId     sstable_id_;

  int64_t max_sstable_size_;
  int64_t frozen_version_;
  int64_t current_sstable_size_;
  int64_t row_num_;
  int64_t pre_column_group_row_num_;

  char             path_[common::OB_MAX_FILE_NAME_LENGTH];
  common::ObString path_string_;
  common::ObString compressor_string_;

  ObChunkServerMergerProxy cs_proxy_;
  common::ObScanParam      scan_param_;

  mergeserver::ObMSGetCellStreamWrapper  ms_wrapper_;
  mergeserver::ObMergeJoinAgent          merge_join_agent_;
  common::ObVector<ObTablet*>           tablet_array_;
  common::ModuleArena  expire_filter_allocator_;
};
} /* chunkserver */
} /* sb */
#endif
