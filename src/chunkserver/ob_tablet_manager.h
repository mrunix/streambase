/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_tablet_manager.h
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     huating <huating.zmq@taobao.com>
 *     maoqi <maoqi@taobao.com>
 *     rizhao <rizhao.ych@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_TABLET_MANAGER_H__
#define __OCEANBASE_CHUNKSERVER_OB_TABLET_MANAGER_H__

#include "common/thread_buffer.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_seq_sstable_scanner.h"
#include "sstable/ob_sstable_getter.h"
#include "sstable/ob_sstable_reader.h"
#include "mergeserver/ob_join_cache.h"
#include "ob_fileinfo_cache.h"
#include "ob_disk_manager.h"
#include "ob_tablet_image.h"
#include "ob_chunk_server_param.h"
#include "ob_chunk_server_stat.h"
#include "ob_chunk_merge.h"
#include "ob_switch_cache_utility.h"

namespace sb {
namespace common {
class ObSchemaManager;
class ObSchemaManagerV2;
class ObGetParam;
class ObScanParam;
class ObScanner;
class ObRange;
class ObTabletReportInfo;
class ObTabletReportInfoList;
class ObScanner;
class ObIterator;
class ObServer;
}
namespace chunkserver {
class ObChunkServerParam;

class ObTabletManager {
 public:
  static const int32_t MAX_COMMAND_LENGTH = 1024 * 2;
 private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletManager);

 public:
  ObTabletManager();
  ~ObTabletManager();
  int init(const ObChunkServerParam* param);
  int init(const sstable::ObBlockCacheConf& bc_conf,
           const sstable::ObBlockIndexCacheConf& bic_conf,
           const int64_t max_tablets_num = 0, const char* data_dir = "/data",
           const int64_t max_sstable_size = 256L * 1024L * 1024L);
  int start_merge_thread();
  int load_tablets(const int32_t* disk_no_array, const int32_t size);
  void destroy();

 public:
  int get(const common::ObGetParam& get_param, common::ObScanner& scanner);

  int scan(const common::ObScanParam& scan_param, common::ObScanner& scanner);

 public:
  // get timestamp of current serving tablets
  int64_t get_serving_data_version(void) const;

  int prepare_merge_tablets(const int64_t memtable_frozen_version);
  int prepare_tablet_image(const int64_t memtable_frozen_version);

  int merge_tablets(const int64_t memtable_frozen_version);
  ObChunkMerge& get_chunk_merge() ;

  int report_tablets();

  int report_capacity_info();

  int create_tablet(const common::ObRange& range, const int64_t data_version);

  int migrate_tablet(const common::ObRange& range,
                     const common::ObServer& dest_server,
                     char (*src_path)[OB_MAX_FILE_NAME_LENGTH],
                     char (*dest_path)[common::OB_MAX_FILE_NAME_LENGTH],
                     int64_t& num_file,
                     int64_t& tablet_version,
                     int32_t& dest_disk_no,
                     uint64_t& crc_sum);

  int dest_load_tablet(const common::ObRange& range,
                       char (*dest_path)[common::OB_MAX_FILE_NAME_LENGTH],
                       const int64_t num_file,
                       const int64_t tablet_version,
                       const int32_t dest_disk_no,
                       const uint64_t crc_sum);

  void start_gc(const int64_t recycle_version);

 public:
  FileInfoCache& get_fileinfo_cache();
  sstable::ObBlockCache& get_serving_block_cache();
  sstable::ObBlockCache& get_unserving_block_cache();
  sstable::ObBlockIndexCache& get_serving_block_index_cache();
  sstable::ObBlockIndexCache& get_unserving_block_index_cache();
  ObMultiVersionTabletImage& get_serving_tablet_image();
  ObDiskManager& get_disk_manager();
  ObRegularRecycler& get_regular_recycler();
  ObScanRecycler& get_scan_recycler();
  mergeserver::ObJoinCache& get_join_cache();

  const ObMultiVersionTabletImage& get_serving_tablet_image() const;

  /**
   * only after the new tablet image is loaded, and the tablet
   * manager is also using the old tablet image, this function can
   * be called. this function will duplicate serving cache to
   * unserving cache.
   *
   * @return int if success,return OB_SUCCESS, else return
   *         OB_ERROR
   */
  int build_unserving_cache();
  int build_unserving_cache(const sstable::ObBlockCacheConf& bc_conf,
                            const sstable::ObBlockIndexCacheConf& bic_conf);

  /**
   * after switch to the new tablet image, call this function to
   * drop the unserving cache.
   *
   * @return int if success,return OB_SUCCESS, else return
   *         OB_ERROR
   */
  int drop_unserving_cache();

 public:
  int dump();

 public:
  inline bool is_stoped() { return !is_init_; }

 private:
  int internal_scan(const common::ObScanParam& scan_param, common::ObScanner& scanner);
  int init_seq_scanner(const common::ObScanParam& scan_param,
                       const ObTablet* tablet, sstable::ObSeqSSTableScanner& seq_scanner);
  int fill_scan_data(common::ObIterator& iterator, common::ObScanner& scanner);

  int internal_get(const common::ObGetParam& get_param, common::ObScanner& scanner);
  int acquire_tablet(const common::ObGetParam& get_param, ObMultiVersionTabletImage& image,
                     ObTablet* tablets[], int64_t& size, int64_t& tablet_version);
  int release_tablet(ObMultiVersionTabletImage& image, ObTablet* tablets[], int64_t size);
  int init_sstable_getter(const common::ObGetParam& get_param, ObTablet* tablets[],
                          const int64_t size, sstable::ObSSTableGetter& sstable_getter);
  int fill_get_data(common::ObIterator& iterator, common::ObScanner& scanner);

 public:

  int fill_tablet_info(const ObTablet& tablet, common::ObTabletReportInfo& tablet_info);
  int send_tablet_report(const common::ObTabletReportInfoList& tablets, bool has_more);

 public:
  // allocate new sstable file sequence, call after load_tablets();
  int64_t allocate_sstable_file_seq();
  // switch to new tablets, call by merge_tablets() after all new tablets loaded.
  int switch_cache();

 private:
  static const uint64_t TABLET_ARRAY_NUM = 2; // one for serving, the other for merging
  static const int64_t DEF_MAX_TABLETS_NUM  = 4000; // max tablets num
  static const int64_t MAX_RANGE_DUMP_SIZE = 256; // for log

 private:
  enum TabletMgrStatus {
    NORMAL = 0, // normal status
    MERGING,    // during daily merging process
    MERGED,     // merging complete, waiting to be switched
  };

  struct ObGetThreadContext {
    ObTablet* tablets_[common::OB_MAX_GET_ROW_NUMBER];
    int64_t tablets_count_;
    sstable::ObSSTableReader* readers_[common::OB_MAX_GET_ROW_NUMBER];
    int64_t readers_count_;
  };

 private:
  bool is_init_;
  volatile uint64_t cur_serving_idx_;
  volatile uint64_t mgr_status_;
  volatile uint64_t max_sstable_file_seq_;

  FileInfoCache fileinfo_cache_;
  sstable::ObBlockCache block_cache_[TABLET_ARRAY_NUM];
  sstable::ObBlockIndexCache block_index_cache_[TABLET_ARRAY_NUM];
  mergeserver::ObJoinCache join_cache_; //used for join phase of daily merge

  ObDiskManager disk_manager_;
  ObRegularRecycler regular_recycler_;
  ObScanRecycler scan_recycler_;
  ObMultiVersionTabletImage tablet_image_;
  ObSwitchCacheUtility switch_cache_utility_;

  ObChunkMerge chunk_merge_;
  const ObChunkServerParam* param_;
};
int reset_query_thread_local_buffer();
}
}


#endif //__OB_TABLET_MANAGER_H__
