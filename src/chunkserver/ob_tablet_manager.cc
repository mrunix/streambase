/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_tablet_manager.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     maoqi <maoqi@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *     rizhao <rizhao.ych@taobao.com>
 *
 */

#include "ob_tablet_manager.h"
#include "profiler.h"
#include "common/ob_malloc.h"
#include "common/ob_range.h"
#include "common/utility.h"
#include "common/ob_schema.h"
#include "common/ob_read_common_data.h"
#include "common/ob_tablet_info.h"
#include "common/ob_scanner.h"
#include "common/ob_atomic.h"
#include "sstable/ob_seq_sstable_scanner.h"
#include "sstable/ob_sstable_getter.h"
#include "sstable/ob_disk_path.h"
#include "ob_tablet.h"
#include "ob_root_server_rpc.h"
#include "ob_chunk_server.h"
#include "ob_chunk_server_main.h"
#include "ob_chunk_merge.h"
#include "ob_file_recycle.h"
#include "ob_chunk_server_stat.h"

#define LOG_CACHE_MEMORY_USAGE(header) \
  do  { \
    TBSYS_LOG(INFO, "%s cur_serving_idx_ =%ld, mgr_status_ =%ld," \
        "table memory usage=%ld," \
        "serving block index cache=%ld, block cache=%ld," \
        "unserving block index cache=%ld, block cache=%ld", \
        header, cur_serving_idx_, mgr_status_, \
        ob_get_mod_memory_usage(ObModIds::OB_CS_TABLET_IMAGE), \
        get_serving_block_index_cache().get_cache_mem_size(), \
        get_serving_block_cache().size(), \
        get_unserving_block_index_cache().get_cache_mem_size(), \
        get_unserving_block_cache().size()); \
  } while(0);

#define LOG_CACHE_MEMORY_USAGE_E(header, manager) \
  do  { \
    TBSYS_LOG(INFO, "%s cur_serving_idx_ =%ld, mgr_status_ =%ld," \
        "table memory usage=%ld," \
        "serving block index cache=%ld, block cache=%ld," \
        "unserving block index cache=%ld, block cache=%ld", \
        header, manager->cur_serving_idx_, manager->mgr_status_, \
        ob_get_mod_memory_usage(ObModIds::OB_CS_TABLET_IMAGE), \
        manager->get_serving_block_index_cache().get_cache_mem_size(), \
        manager->get_serving_block_cache().size(), \
        manager->get_unserving_block_index_cache().get_cache_mem_size(), \
        manager->get_unserving_block_cache().size()); \
  } while(0);

namespace sb {
namespace chunkserver {
using namespace sb::common;
using namespace sb::sstable;

/**
 * this function must be invoke before doing query request
 * like SCAN or GET
 *
 */
int reset_query_thread_local_buffer() {
  int err = OB_SUCCESS;

  static common::ModulePageAllocator mod_allocator(ObModIds::OB_SSTABLE_EGT_SCAN);
  static const int64_t QUERY_INTERNAL_PAGE_SIZE = 2L * 1024L * 1024L;

  common::ModuleArena* internal_buffer_arena = GET_TSI(common::ModuleArena);
  if (NULL == internal_buffer_arena) {
    TBSYS_LOG(ERROR, "cannot get tsi object of PageArena");
    err = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (internal_buffer_arena->total() > QUERY_INTERNAL_PAGE_SIZE * 2) {
      TBSYS_LOG(WARN, "thread local page arena hold memory over limited,"
                "total=%ld,used=%ld,pages=%ld", internal_buffer_arena->total(),
                internal_buffer_arena->used(), internal_buffer_arena->pages());
      internal_buffer_arena->partial_slow_free(0, 0, QUERY_INTERNAL_PAGE_SIZE);
    }
    internal_buffer_arena->set_page_size(QUERY_INTERNAL_PAGE_SIZE);
    internal_buffer_arena->set_page_alloctor(mod_allocator);
    internal_buffer_arena->reuse();
  }

  return err;
}


ObTabletManager::ObTabletManager()
  : is_init_(false),
    cur_serving_idx_(0),
    mgr_status_(NORMAL),
    max_sstable_file_seq_(0),
    regular_recycler_(*this),
    scan_recycler_(*this),
    tablet_image_(fileinfo_cache_),
    param_(NULL) {
}

ObTabletManager::~ObTabletManager() {
  destroy();
}

int ObTabletManager::init(const ObBlockCacheConf& bc_conf,
                          const ObBlockIndexCacheConf& bic_conf,
                          const int64_t max_tablets_num,
                          const char* data_dir,
                          const int64_t max_sstable_size) {
  int err = OB_SUCCESS;

  UNUSED(max_tablets_num);

  if (NULL == data_dir || max_sstable_size <= 0) {
    TBSYS_LOG(WARN, "invalid parameter, data_dir=%s, max_sstable_size=%ld",
              data_dir, max_sstable_size);
    err = OB_INVALID_ARGUMENT;
  } else if (!is_init_) {
    if (OB_SUCCESS == err) {
      err = fileinfo_cache_.init(bc_conf.ficache_max_num);
    }
    if (OB_SUCCESS == err) {
      block_cache_[cur_serving_idx_].set_fileinfo_cache(fileinfo_cache_);
      err = block_cache_[cur_serving_idx_].init(bc_conf);
    }
    if (OB_SUCCESS == err) {
      block_index_cache_[cur_serving_idx_].set_fileinfo_cache(fileinfo_cache_);
      err = block_index_cache_[cur_serving_idx_].init(bic_conf);
    }

    if (OB_SUCCESS == err) {
      err = disk_manager_.scan(data_dir, max_sstable_size);
    }


    if (OB_SUCCESS == err) {
      is_init_ = true;
    }
  }

  return err;
}

int ObTabletManager::init(const ObChunkServerParam* param) {
  int err = OB_SUCCESS;

  if (NULL == param) {
    TBSYS_LOG(ERROR, "invalid parameter, param is NULL");
    err = OB_INVALID_ARGUMENT;
  } else {
    err = init(param->get_block_cache_conf(), param->get_block_index_cache_conf(),
               param->get_max_tablets_num(), param->get_datadir_path(),
               param->get_max_sstable_size());
  }

  if (OB_SUCCESS == err) {
    param_ = param;
  }

  if (OB_SUCCESS == err) {
    if (param_->get_join_cache_conf().cache_mem_size > 0) { // <= 0 will disable the join cache
      if ((err = join_cache_.init(param_->get_join_cache_conf().cache_mem_size)) != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "init join cache failed");
      }
    }
  }
  return err;
}

int ObTabletManager::start_merge_thread() {
  return chunk_merge_.init(this);
}

ObChunkMerge& ObTabletManager::get_chunk_merge() {
  return chunk_merge_;
}

void ObTabletManager::destroy() {
  if (is_init_) {
    is_init_ = false;

    chunk_merge_.destroy();
    fileinfo_cache_.destroy();
    for (uint64_t i = 0; i < TABLET_ARRAY_NUM; ++i) {
      block_cache_[i].destroy();
      block_index_cache_[i].destroy();
    }
    join_cache_.destroy();
  }
}

int ObTabletManager::migrate_tablet(const common::ObRange& range,
                                    const common::ObServer& dest_server,
                                    char (*src_path)[OB_MAX_FILE_NAME_LENGTH],
                                    char (*dest_path)[OB_MAX_FILE_NAME_LENGTH],
                                    int64_t& num_file,
                                    int64_t& tablet_version,
                                    int32_t& dest_disk_no,
                                    uint64_t& crc_sum) {
  int rc = OB_SUCCESS;
  ObMultiVersionTabletImage& tablet_image = get_serving_tablet_image();
  ObTablet* tablet = NULL;
  char dest_dir_buf[OB_MAX_FILE_NAME_LENGTH];

  if (NULL == src_path || NULL == dest_path) {
    TBSYS_LOG(ERROR, "error parameters, src_path=%p, dest_path=%p", src_path, dest_path);
    rc = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == rc) {
    // TODO , get newest tablet.
    rc = tablet_image.acquire_tablet(range, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
    if (OB_SUCCESS != rc || NULL == tablet) {
      TBSYS_LOG(ERROR, "acquire tablet error.");
    } else if (range.compare_with_startkey2(tablet->get_range()) != 0
               || range.compare_with_endkey2(tablet->get_range()) != 0) {
      char range_buf[OB_RANGE_STR_BUFSIZ];
      range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(INFO, "migrate tablet range = <%s>", range_buf);
      tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(INFO, "not equal to local tablet range = <%s>", range_buf);
      rc = OB_ERROR;
    } else {
      TBSYS_LOG(INFO, "acquire tablet  success, tablet=%p", tablet);
    }
  }

  // get destination disk no and dest directory for store sstable files.
  dest_disk_no = 0;
  memset(dest_dir_buf, 0, OB_MAX_FILE_NAME_LENGTH);
  // buffer size set to OB_MAX_FILE_NAME_LENGTH,
  // make deserialize ObString copy to dest_dir_buf
  ObString dest_directory(OB_MAX_FILE_NAME_LENGTH, 0, dest_dir_buf);
  if (OB_SUCCESS == rc && NULL != tablet) {
    ObRootServerRpcStub cs_rpc_stub;
    rc = cs_rpc_stub.init(dest_server, &(THE_CHUNK_SERVER.get_client_manager()));
    if (OB_SUCCESS != rc) {
      TBSYS_LOG(ERROR, "init cs_rpc_stub error.");
    } else {
      rc = cs_rpc_stub.get_migrate_dest_location(
             tablet->get_occupy_size(), dest_disk_no, dest_directory);
      if (OB_SUCCESS == rc && dest_disk_no > 0) {
        TBSYS_LOG(INFO, "get_migrate_dest_location succeed, dest_disk_no=%d, %s",
                  dest_disk_no, dest_dir_buf);
      } else {
        TBSYS_LOG(ERROR, "get_migrate_dest_location failed, rc = %d, dest_disk_no=%d",
                  rc, dest_disk_no);
      }
    }
  }

  if (OB_SUCCESS == rc && NULL != tablet) {
    num_file = tablet->get_sstable_id_list().get_array_index();
    tablet_version = tablet->get_data_version();
    tablet->get_checksum(crc_sum);
    TBSYS_LOG(INFO, "migrate_tablet sstable file num =%ld , version=%ld, checksum=%lu",
              num_file, tablet_version, crc_sum);
  }

  // copy sstable files path & generate dest path
  if (OB_SUCCESS == rc && NULL != tablet && num_file > 0) {
    int64_t now = 0;
    now = tbsys::CTimeUtil::getTime();
    for (int64_t idx = 0; idx < num_file && OB_SUCCESS == rc; idx++) {
      ObSSTableId* sstable_id = tablet->get_sstable_id_list().at(idx);
      if (NULL != sstable_id) {
        rc = get_sstable_path(*sstable_id, src_path[idx], OB_MAX_FILE_NAME_LENGTH);
        if (OB_SUCCESS != rc) {
          TBSYS_LOG(WARN, "get sstable path error, rc=%d, sstable_id=%ld",
                    rc, sstable_id->sstable_file_id_);
        } else {
          // generate dest dir
          rc = snprintf(dest_path[idx],
                        OB_MAX_FILE_NAME_LENGTH,
                        "%s/%ld.%ld", dest_dir_buf,
                        sstable_id->sstable_file_id_, now);
          if (rc > 0) rc = OB_SUCCESS;
          else rc = OB_ERROR;
        }
      } else {
        TBSYS_LOG(ERROR, "sstable not exist, cannot happen.");
        rc = OB_ERROR;
      }
    }
  }

  // now we can release tablet no need wait to rsync complete.
  if (NULL != tablet) {
    int ret = tablet_image.release_tablet(tablet);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(WARN, "release tablet error.");
      rc = OB_ERROR;
    } else {
      tablet = NULL;
    }
  }

  if (OB_SUCCESS == rc && NULL != src_path && num_file > 0) {
    int64_t rsync_band_limit = THE_CHUNK_SERVER.get_param().get_rsync_band_limit();
    char cmd[MAX_COMMAND_LENGTH];
    char ip_addr[OB_IP_STR_BUFF];
    //send all sstable files of this tablet using scp
    for (int64_t idx = 0; idx < num_file && OB_SUCCESS == rc; idx++) {
      uint32_t ipv4 = dest_server.get_ipv4();

      rc = snprintf(ip_addr, OB_IP_STR_BUFF,
                    "%d.%d.%d.%d",
                    (ipv4 & 0xFF),
                    (ipv4 >> 8) & 0xFF,
                    (ipv4 >> 16) & 0xFF,
                    (ipv4 >> 24) & 0xFF);

      if (rc > 0) {
        rc = snprintf(cmd, MAX_COMMAND_LENGTH,
                      "scp -oStrictHostKeyChecking=no -c arcfour -l %ld %s %s:%s",
                      rsync_band_limit * 8, src_path[idx], ip_addr, dest_path[idx]);
      }

      if (rc > 0) {
        TBSYS_LOG(INFO, "copy sstable file, idx=%ld, cmd=%s", idx, cmd);
        rc = system(cmd);
        if (0 != rc) {
          TBSYS_LOG(ERROR, "transfer sstable file[%ld]=[%s] failed, rc=%d", idx, src_path, rc);
          rc = OB_ERROR;
        } else {
          TBSYS_LOG(DEBUG, "transfer sstable file[%ld]=[%s] success.", idx, src_path);
          rc = OB_SUCCESS;
        }
      }
    }//end of send all sstable file
  }

  return rc;
}

int ObTabletManager::dest_load_tablet(const common::ObRange& range,
                                      char (*dest_path)[OB_MAX_FILE_NAME_LENGTH],
                                      const int64_t num_file,
                                      const int64_t tablet_version,
                                      const int32_t dest_disk_no,
                                      const uint64_t crc_sum) {
  int rc = OB_SUCCESS;
  int idx = 0;
  //construct a new tablet and then add all sstable file to it
  ObMultiVersionTabletImage& tablet_image = get_serving_tablet_image();

  ObTablet* tablet = NULL;
  ObTablet* old_tablet = NULL;

  char path[OB_MAX_FILE_NAME_LENGTH];
  char input_range_buf[OB_RANGE_STR_BUFSIZ];
  range.to_string(input_range_buf, OB_RANGE_STR_BUFSIZ);

  int tablet_exist = OB_ENTRY_NOT_EXIST;

  if (chunk_merge_.is_pending_in_upgrade()) {
    TBSYS_LOG(WARN, "local merge in upgrade, cannot migrate "
              "new tablet = %s, version=%ld", input_range_buf, tablet_version);
    rc = OB_CS_EAGAIN;
  }
  if (tablet_version < get_serving_data_version()) {
    TBSYS_LOG(WARN, "migrate in tablet = %s version =%ld < local serving version=%ld",
              input_range_buf, tablet_version, get_serving_data_version());
    rc = OB_ERROR;
  } else if (OB_SUCCESS == (tablet_exist =
                              tablet_image.acquire_tablet_all_version(
                                range,  ObMultiVersionTabletImage::SCAN_FORWARD,
                                ObMultiVersionTabletImage::FROM_NEWEST_INDEX, 0, old_tablet))) {
    char exist_range_buf[OB_RANGE_STR_BUFSIZ];
    old_tablet->get_range().to_string(exist_range_buf, OB_RANGE_STR_BUFSIZ);
    TBSYS_LOG(INFO, "tablet intersect with exist, input <%s> and exist <%s>",
              input_range_buf, exist_range_buf);
    if (tablet_version < old_tablet->get_data_version()) {
      TBSYS_LOG(ERROR, "migrate in tablet's version =%ld < local tablet's version=%ld",
                tablet_version, old_tablet->get_data_version());
      rc = OB_ERROR;
    } else if (tablet_version == old_tablet->get_data_version()) {
      // equal to local tablet's version, verify checksum
      uint64_t exist_crc_sum = 0;
      old_tablet->get_checksum(exist_crc_sum);
      if (crc_sum == exist_crc_sum) {
        TBSYS_LOG(INFO, "migrate in tablet's version=%ld equal "
                  "local tablet's version=%ld, and in crc=%lu equal local crc=%lu",
                  tablet_version, old_tablet->get_data_version(),
                  crc_sum, exist_crc_sum);
        rc = OB_CS_MIGRATE_IN_EXIST;
      } else {
        TBSYS_LOG(ERROR, "migrate in tablet's version=%ld equal "
                  "local tablet's version=%ld, but in crc=%lu <> local crc=%lu",
                  tablet_version, old_tablet->get_data_version(),
                  crc_sum, exist_crc_sum);
        rc = OB_ERROR;
      }
    } else {
      // greate than local tablet's version, replace it;
      TBSYS_LOG(INFO, "migrate in tablet's version =%ld >"
                "local tablet's version=%ld, replace it.",
                tablet_version, old_tablet->get_data_version());
      rc = OB_SUCCESS;
    }
  }

  if (OB_SUCCESS == rc) {
    rc = tablet_image.alloc_tablet_object(range, tablet_version, tablet);
    if (OB_SUCCESS != rc) {
      TBSYS_LOG(ERROR, "alloc tablet failed.");
    } else {
      tablet->set_disk_no(dest_disk_no);
      tablet->set_data_version(tablet_version);
    }

    for (; idx < num_file && OB_SUCCESS == rc; idx++) {
      ObSSTableId sstable_id;
      sstable_id.sstable_file_id_     = allocate_sstable_file_seq();
      sstable_id.sstable_file_offset_ = 0;
      sstable_id.sstable_file_id_     = (sstable_id.sstable_file_id_ << 8) | (dest_disk_no & 0xff);
      rc = get_sstable_path(sstable_id, path, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS != rc) {
        TBSYS_LOG(ERROR, "get sstable path error, rc=%d", rc);
      } else {
        TBSYS_LOG(INFO, "dest_load_tablet, rename %s -> %s", dest_path[idx], path);
        rc = rename(dest_path[idx], path);
        if (OB_SUCCESS != rc) {
          TBSYS_LOG(ERROR, "rename %s -> %s failed, error: %d, %s",
                    dest_path[idx], path, errno, strerror(errno));
          rc = OB_IO_ERROR;
        }

        if (OB_SUCCESS == rc) {
          rc = tablet->add_sstable_by_id(sstable_id);
          if (OB_SUCCESS != rc) {
            TBSYS_LOG(ERROR, "add sstable file  error.");
          }
        }
      }
    }

    if (OB_SUCCESS == rc) {
      bool for_create = get_serving_data_version() == 0 ? true : false;
      rc = tablet_image.add_tablet(tablet, true, for_create);
      if (OB_SUCCESS != rc) {
        TBSYS_LOG(ERROR, "add tablet <%s> failed, rc=%d", input_range_buf, rc);
      } else {
        // save image file.
        rc = tablet_image.write(tablet->get_data_version(), dest_disk_no);
        disk_manager_.shrink_space(dest_disk_no, tablet->get_occupy_size());
      }
    }

    if (OB_SUCCESS == rc && OB_SUCCESS == tablet_exist && NULL != old_tablet) {
      // if we have old version tablet, no need to merge.
      old_tablet->set_merged();
      tablet_image.write(old_tablet->get_data_version(), old_tablet->get_disk_no());
    }

  }

  // cleanup, delete all migrated sstable files in /tmp;
  for (int64_t index = 0; index < num_file; index++) {
    unlink(dest_path[index]);
  }

  if (NULL != old_tablet) tablet_image.release_tablet(old_tablet);

  return rc;
}

void ObTabletManager::start_gc(const int64_t recycle_version) {
  TBSYS_LOG(INFO, "start gc");
  UNUSED(recycle_version);
  if (chunk_merge_.is_merge_stoped()) {
    scan_recycler_.recycle();
  }
  return;
}

int ObTabletManager::create_tablet(const ObRange& range, const int64_t data_version) {
  int err = OB_SUCCESS;
  ObTablet* tablet = NULL;

  if (range.empty() || 0 >= data_version) {
    TBSYS_LOG(ERROR, "create_tablet error, input range is empty "
              "or data_version=%ld", data_version);
    err = OB_INVALID_ARGUMENT;
  } else if ((OB_SUCCESS == (err =
                               tablet_image_.acquire_tablet(range,
                                                            ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet)))
             &&  NULL != tablet && tablet->get_data_version() >= data_version) {
    // find tablet if exist?
    TBSYS_LOG(ERROR, "tablet already exists! dump input and exist:");

    range.hex_dump(TBSYS_LOG_LEVEL_ERROR);
    tablet->get_range().hex_dump(TBSYS_LOG_LEVEL_ERROR);

    tablet_image_.release_tablet(tablet);
    tablet = NULL;
    err = OB_ERROR;
  } else if (OB_SUCCESS != (err =
                              tablet_image_.alloc_tablet_object(range, data_version, tablet))) {
    TBSYS_LOG(ERROR, "allocate tablet object failed, ret=%d, version=%ld",
              err, data_version);
  } else {
    // add empty tablet, there is no sstable files in it.
    // if scan or get query on this tablet, scan will return empty dataset.
    // TODO, create_tablet need send the %memtable_frozen_version
    // as first version of this new tablet.
    tablet->set_data_version(data_version);
    // assign a disk for new tablet.
    tablet->set_disk_no(get_disk_manager().get_dest_disk());
    // load empty sstable files on first time, and create new tablet.
    err = tablet_image_.add_tablet(tablet, true, true);
    // save new meta file in disk
    if (OB_SUCCESS == err) {
      err = tablet_image_.write(data_version, tablet->get_disk_no());
    }
  }

  return err;
}

int ObTabletManager::load_tablets(const int32_t* disk_no_array, const int32_t size) {
  int err = OB_SUCCESS;

  if (NULL == disk_no_array || size <= 0) {
    TBSYS_LOG(WARN, "invalid param, disk_no_array=%p, size=%d", disk_no_array, size);
    err = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (err =
                              get_serving_tablet_image().load_tablets(disk_no_array, size, true))) {
    TBSYS_LOG(ERROR, "read tablets from disk error, ret=%d", err);
  } else {
    //get_serving_tablet_image().dump(true);
    max_sstable_file_seq_ =
      get_serving_tablet_image().get_max_sstable_file_seq();
  }
  return err;
}

int ObTabletManager::get(const ObGetParam& get_param, ObScanner& scanner) {
  int err           = OB_SUCCESS;
  int64_t cell_size = get_param.get_cell_size();

  if (cell_size <= 0) {
    TBSYS_LOG(WARN, "invalid param, cell_size=%ld", cell_size);
    err = OB_INVALID_ARGUMENT;
  } else if (NULL == get_param.get_row_index() || get_param.get_row_size() <= 0) {
    TBSYS_LOG(WARN, "invalid get param, row_index=%p, row_size=%ld",
              get_param.get_row_index(), get_param.get_row_size());
    err = OB_INVALID_ARGUMENT;
  } else {
    err = internal_get(get_param, scanner);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to get_, err=%d", err);
    }
  }

  return err;
}

int ObTabletManager::internal_get(const common::ObGetParam& get_param,
                                  ObScanner& scanner) {
  int err                           = OB_SUCCESS;
  int64_t cell_size                 = get_param.get_cell_size();
  int64_t row_size                  = get_param.get_row_size();
  int64_t tablet_version            = 0;
  ObMultiVersionTabletImage& image  = get_serving_tablet_image();
  ObSSTableGetter* sstable_getter   = GET_TSI(ObSSTableGetter);
  ObGetThreadContext* get_context   = GET_TSI(ObTabletManager::ObGetThreadContext);

  if (NULL == sstable_getter || NULL == get_context) {
    TBSYS_LOG(WARN, "get thread local instance of sstable getter failed");
    err = OB_ERROR;
  } else if (row_size <= 0 || row_size > OB_MAX_GET_ROW_NUMBER) {
    TBSYS_LOG(WARN, "no cell or too many row to get, row_size=%ld", row_size);
    err = OB_INVALID_ARGUMENT;
  }

#ifdef OB_PROFILER
  PROFILER_START("profiler_get");
  PROFILER_BEGIN("internal_get");
#endif

  if (OB_SUCCESS == err) {
#ifdef OB_PROFILER
    PROFILER_BEGIN("acquire_tablet");
#endif
    get_context->tablets_count_ = OB_MAX_GET_ROW_NUMBER;
    err = acquire_tablet(get_param, image, &get_context->tablets_[0],
                         get_context->tablets_count_, tablet_version);
#ifdef OB_PROFILER
    PROFILER_END();
#endif
  }

  //if can't find the first row in all the tablets, just exit get
  if (OB_SUCCESS == err && 0 == get_context->tablets_count_) {
    scanner.set_data_version(tablet_version);
    scanner.set_is_req_fullfilled(true, 0);
    err = OB_CS_TABLET_NOT_EXIST;
  } else if (OB_SUCCESS != err || get_context->tablets_count_ <= 0
             || cell_size < get_context->tablets_count_) {
    TBSYS_LOG(WARN, "failed to acquire tablet, cell size=%ld, "
              "tablets size=%ld, err=%d",
              cell_size, get_context->tablets_count_, err);
    err = OB_ERROR;
  } else {
#ifdef OB_PROFILER
    PROFILER_BEGIN("init_sstable_getter");
#endif
    err = init_sstable_getter(get_param, &get_context->tablets_[0],
                              get_context->tablets_count_, *sstable_getter);
#ifdef OB_PROFILER
    PROFILER_END();
#endif
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to init sstable getter, err=%d", err);
    }
  }

  // fill result to result objects of ObScanner
  if (OB_SUCCESS == err) {
    scanner.set_data_version(tablet_version);
    err = fill_get_data(*sstable_getter, scanner);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to store cell array to scanner, err=%d", err);
    }
  }

#ifdef OB_PROFILER
  PROFILER_BEGIN("release_tablet");
#endif
  if (get_context->tablets_count_ > 0
      && OB_SUCCESS != release_tablet(image, &get_context->tablets_[0],
                                      get_context->tablets_count_)) {
    TBSYS_LOG(WARN, "failed to release tablets");
    err = OB_ERROR;
  }
#ifdef OB_PROFILER
  PROFILER_END();
#endif

#ifdef OB_PROFILER
  PROFILER_END();
  PROFILER_DUMP();
  PROFILER_STOP();
#endif

  return err;
}

int ObTabletManager::scan(const ObScanParam& scan_param, ObScanner& scanner) {
  int err = OB_SUCCESS;

  err = reset_query_thread_local_buffer();

  // suppose table_name, column_name already
  // translated to table_id, column_id by MergeServer
  if (OB_SUCCESS == err) {
    scanner.set_mem_size_limit(scan_param.get_scan_size());
    err = internal_scan(scan_param, scanner);
  }


  return err;
}

int64_t ObTabletManager::get_serving_data_version(void) const {
  return tablet_image_.get_serving_version();
}

int ObTabletManager::prepare_tablet_image(const int64_t memtable_frozen_version) {
  int ret = OB_SUCCESS;
  int64_t retry_times = THE_CHUNK_SERVER.get_param().get_retry_times();
  int64_t sleep_interval = THE_CHUNK_SERVER.get_param().get_network_time_out();
  int64_t i = 0;
  while (i++ < retry_times) {
    ret = tablet_image_.prepare_for_merge(memtable_frozen_version);
    if (OB_SUCCESS == ret) {
      break;
    } else if (OB_CS_EAGAIN == ret) {
      usleep(sleep_interval);
    } else {
      TBSYS_LOG(ERROR, "prepare image version = %ld error ret = %d",
                memtable_frozen_version, ret);
      break;
    }
  }

  return ret;
}

int ObTabletManager::prepare_merge_tablets(const int64_t memtable_frozen_version) {
  int err = OB_SUCCESS;

  if (OB_SUCCESS != (err = prepare_tablet_image(memtable_frozen_version))) {
    TBSYS_LOG(WARN, "prepare_for_merge version = %ld error, err = %d",
              memtable_frozen_version, err);
  } else if (OB_SUCCESS != (err = drop_unserving_cache())) {
    TBSYS_LOG(ERROR, "drop unserving cache for migrate cache failed,"
              "cannot launch new merge process, new version=%ld", memtable_frozen_version);
  } else {
    int64_t recycle_version = memtable_frozen_version
                              - ObMultiVersionTabletImage::MAX_RESERVE_VERSION_COUNT;
    TBSYS_LOG(INFO, "prepare_recycle forzen version=%ld, recycle version=%ld",
              memtable_frozen_version, recycle_version);
    if (recycle_version > 0) {
      TBSYS_LOG(INFO, "prepare recycle version = %ld", recycle_version);
      regular_recycler_.backup_meta_files(recycle_version);
      regular_recycler_.prepare_recycle(recycle_version);
    }
  }
  return err;
}

int ObTabletManager::merge_tablets(const int64_t memtable_frozen_version) {
  int ret = OB_SUCCESS;

  TBSYS_LOG(INFO, "start merge");

  if (OB_SUCCESS == ret) {
    //update schema,
    //new version coming,clear join cache
    join_cache_.destroy();
    if (param_->get_join_cache_conf().cache_mem_size > 0) { // <= 0 will disable the join cache
      if ((ret = join_cache_.init(param_->get_join_cache_conf().cache_mem_size)) != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "init join cache failed");
      }
    }
    disk_manager_.scan(param_->get_datadir_path(), param_->get_max_sstable_size());
    chunk_merge_.schedule(memtable_frozen_version);
  }
  return ret;
}

int ObTabletManager::report_capacity_info() {
  int err = OB_SUCCESS;
  if (!is_init_) {
    TBSYS_LOG(ERROR, "report_capacity_info not init");
    err = OB_NOT_INIT;
  } else if (cur_serving_idx_ >= TABLET_ARRAY_NUM) {
    TBSYS_LOG(ERROR, "report_capacity_info invalid status, cur_serving_idx=%ld", cur_serving_idx_);
    err = OB_ERROR;
  } else {
    const ObServer& root_server = THE_CHUNK_SERVER.get_root_server();
    ObRootServerRpcStub root_stub;
    err = root_stub.init(root_server, &(THE_CHUNK_SERVER.get_client_manager()));
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to init root stub, err=%d", err);
    } else {
      err = root_stub.report_capacity_info(THE_CHUNK_SERVER.get_self(),
                                           disk_manager_.get_total_capacity(), disk_manager_.get_total_used());
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to report capacity info, err=%d", err);
      }
    }
  }

  return err;
}

int ObTabletManager::report_tablets() {
  int err = OB_SUCCESS;
  ObTablet* tablet = NULL;
  ObTabletReportInfo tablet_info;
  // since report_tablets process never use block_uncompressed_buffer_
  // so borrow it to store ObTabletReportInfoList
  int64_t num = OB_MAX_TABLET_LIST_NUMBER;
  ObTabletReportInfoList* report_info_list =  GET_TSI(ObTabletReportInfoList);
  if (NULL == report_info_list) {
    err = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    report_info_list->reset();
  }

  ObMultiVersionTabletImage& image = get_serving_tablet_image();
  if (OB_SUCCESS == err) err = image.begin_scan_tablets();
  if (OB_ITER_END == err) {
    // iter ends
  } else if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "failed to scan tablets, tablet=%p, err=%d",
              tablet, err);
    err = OB_ERROR;
  } else {
    while (OB_SUCCESS == err) {
      while (OB_SUCCESS == err && num > 0) {
        err = image.get_next_tablet(tablet);
        if (OB_ITER_END == err) {
          // iter ends
        } else if (OB_SUCCESS != err || NULL == tablet) {
          TBSYS_LOG(WARN, "failed to get next tablet, err=%d, tablet=%p",
                    err, tablet);
          err = OB_ERROR;
        } else if (tablet->get_data_version() != image.get_serving_version()) {
          image.release_tablet(tablet);
          continue;
        } else {

          if (!tablet->get_range().border_flag_.is_left_open_right_closed()) {
            char range_buf[OB_RANGE_STR_BUFSIZ];
            tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
            TBSYS_LOG(WARN, "report illegal tablet range = <%s>", range_buf);
          }

          fill_tablet_info(*tablet, tablet_info);
          err = report_info_list->add_tablet(tablet_info);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "failed to add tablet info, num=%ld, err=%d", num, err);
          } else {
            --num;
          }

          if (OB_SUCCESS != image.release_tablet(tablet)) {
            TBSYS_LOG(WARN, "failed to release tablet, tablet=%p", tablet);
            err = OB_ERROR;
          }
        }
      }

      if (OB_SUCCESS == err) {
        err = send_tablet_report(*report_info_list, true);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "failed to send tablet info report, err=%d", err);
        }
      }

      if (OB_SUCCESS == err) {
        num = OB_MAX_TABLET_LIST_NUMBER;
        report_info_list->reset();
      }
    }
  }
  image.end_scan_tablets();
  if (OB_ITER_END == err) {
    err = send_tablet_report(*report_info_list, false);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "failed to send tablet report complete, ret=%d", err);
    }
  }


  return err;
}

int ObTabletManager::dump() {
  return OB_SUCCESS;
}

int ObTabletManager::init_seq_scanner(const ObScanParam& scan_param,
                                      const ObTablet* tablet, ObSeqSSTableScanner& seq_scanner) {
  int err = OB_SUCCESS;
  ObSSTableReader* sstable_reader_list[ObTablet::MAX_SSTABLE_PER_TABLET];
  int32_t size = ObTablet::MAX_SSTABLE_PER_TABLET;

  if (NULL == tablet) {
    TBSYS_LOG(ERROR, "invalid param, tablet=%p", tablet);
    err = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (
               err = tablet->find_sstable(
                       *scan_param.get_range(), sstable_reader_list, size))) {
    TBSYS_LOG(ERROR, "find_sstable err=%d, size=%d", err, size);
  } else {
    for (int32_t i = 0; i < size ; ++i) {
      err = seq_scanner.add_sstable_reader(sstable_reader_list[i]);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(ERROR, "add sstable reader object to seq scanner failed(%d,%p).",
                  i, sstable_reader_list[i]);
        break;
      } else {
        TBSYS_LOG(DEBUG, "add sstable reader %d, %p, file id:%ld",
                  i, sstable_reader_list[i], sstable_reader_list[i]->get_sstable_id().sstable_file_id_);
      }
    }
  }
  return err;
}

int ObTabletManager::internal_scan(const ObScanParam& scan_param, ObScanner& scanner) {
  int err = OB_SUCCESS;
  ObTablet* tablet = NULL;
  ObSeqSSTableScanner* seq_scanner = GET_TSI(ObSeqSSTableScanner);

  int64_t query_version = 0;
  ObMultiVersionTabletImage::ScanDirection scan_direction =
    scan_param.get_scan_direction() == ObScanParam::FORWARD ?
    ObMultiVersionTabletImage::SCAN_FORWARD : ObMultiVersionTabletImage::SCAN_BACKWARD;

  const ObVersionRange& version_range = scan_param.get_version_range();
  if (!version_range.border_flag_.is_max_value() && version_range.end_version_ != 0) {
    query_version = version_range.end_version_;
  }

  if (NULL == seq_scanner) {
    TBSYS_LOG(ERROR, "failed to get thread local sequence scaner, seq_scanner=%p",
              seq_scanner);
    err = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_SUCCESS != (err =
                              seq_scanner->set_scan_param(scan_param, get_serving_block_cache(),
                                                          get_serving_block_index_cache()))) {
    TBSYS_LOG(ERROR, "seq scanner set scan parameter error.");
  } else if (OB_SUCCESS != (err =
                              tablet_image_.acquire_tablet(*scan_param.get_range(),
                                                           scan_direction, query_version, tablet))) {
    TBSYS_LOG(WARN, "failed to acquire tablet, tablet=%p, version=%ld, err=%d",
              tablet, query_version, err);
  } else if (OB_SUCCESS != (err = init_seq_scanner(scan_param, tablet, *seq_scanner))) {
    TBSYS_LOG(ERROR, "init_seq_scanner error=%d", err);
  } else if (OB_SUCCESS != (err =  fill_scan_data(*seq_scanner, scanner))) {
    // fill result to result object of ObScanner
    TBSYS_LOG(ERROR, "failed to do fill scan data, err=%d", err);
  } else {
    scanner.set_data_version(tablet->get_data_version());
    //scanner.set_range(tablet->get_range());
    //
    ObRange copy_range;
    deep_copy_range(*GET_TSI(ModuleArena), tablet->get_range(), copy_range);
    scanner.set_range_shallow_copy(copy_range);

    if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG) {
      char range_buf[OB_RANGE_STR_BUFSIZ];
      tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(DEBUG, "scan result: tablet's data version=%ld, range=%s",
                tablet->get_data_version(), range_buf);

      common::ModuleArena* internal_buffer_arena = GET_TSI(common::ModuleArena);
      TBSYS_LOG(DEBUG, "thread local page arena hold memory usage,"
                "total=%ld,used=%ld,pages=%ld", internal_buffer_arena->total(),
                internal_buffer_arena->used(), internal_buffer_arena->pages());
    }
  }

  if (NULL != tablet && OB_SUCCESS != tablet_image_.release_tablet(tablet)) {
    TBSYS_LOG(ERROR, "failed to release tablet, tablet=%p", tablet);
    err = OB_ERROR;
  }

  if (NULL != seq_scanner) {
    seq_scanner->cleanup();
  }

  return err;
}

int ObTabletManager::acquire_tablet(const ObGetParam& get_param,
                                    ObMultiVersionTabletImage& image,
                                    ObTablet* tablets[], int64_t& size,
                                    int64_t& tablet_version) {
  int err                 = OB_SUCCESS;
  int64_t cell_size       = get_param.get_cell_size();
  const ObGetParam::ObRowIndex* row_index = NULL;
  int64_t row_size        = 0;
  int64_t i               = 0;
  int64_t cur_tablet_ver  = 0;
  int64_t tablets_count   = 0;
  ObRange range;

  if (cell_size <= 0) {
    TBSYS_LOG(WARN, "invalid param, cell_size=%ld", cell_size);
    err = OB_INVALID_ARGUMENT;
  } else if (NULL == tablets || size <= 0) {
    TBSYS_LOG(WARN, "invalid param, tablets=%p size=%ld", tablets, size);
    err = OB_INVALID_ARGUMENT;
  } else if (cur_serving_idx_ >= TABLET_ARRAY_NUM) {
    TBSYS_LOG(WARN, "invalid status, cur_serving_idx_=%ld", cur_serving_idx_);
    err = OB_ERROR;
  } else if (NULL == get_param.get_row_index() || get_param.get_row_size() <= 0) {
    TBSYS_LOG(WARN, "invalid get param");
    err = OB_ERROR;
  } else {
    row_index = get_param.get_row_index();
    row_size = get_param.get_row_size();
    for (i = 0; i < row_size && OB_SUCCESS == err; i++) {
      if (i >= size) {
        err = OB_SIZE_OVERFLOW;
        size = i - 1;
        break;
      }

      range.table_id_ = get_param[row_index[i].offset_]->table_id_;
      range.start_key_ = get_param[row_index[i].offset_]->row_key_;
      range.end_key_ = get_param[row_index[i].offset_]->row_key_;
      range.border_flag_.set_inclusive_start();
      range.border_flag_.set_inclusive_end();
      err = image.acquire_tablet(range, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablets[i]);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(DEBUG, "the tablet does not exist, rowkey: ");
        hex_dump(range.start_key_.ptr(), range.start_key_.length(),
                 true, TBSYS_LOG_LEVEL_DEBUG);
        tablets[i] = NULL;
        err = OB_SUCCESS;
        /**
         * don't get the next row after the first non-existent row
         * WARNING: please don't optimize this, mergeserver rely on this
         * feature, when chunkserver can't find tablet for this rowkey,
         * chunserver will return special error
         * code(OB_CS_TABLET_NOT_EXIST) for this case.
         */
        break;
      } else if (NULL != tablets[i]) {
        if (0 == cur_tablet_ver) {
          cur_tablet_ver = tablets[i]->get_data_version();
        } else if (cur_tablet_ver != tablets[i]->get_data_version()) {
          //release the tablet we don't need
          err = image.release_tablet(tablets[i]);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "failed to release tablet, tablet=%p, err=%d",
                      tablets[i], err);
          }
          //tablet version change, break acquire tablet, skip current tablet
          break;
        }
        tablets_count++;
      }
    }
  }

  if (OB_SUCCESS == err) {
    size = tablets_count;
  }

  //assign table_verion even though error happens
  if (0 == size || 0 == cur_tablet_ver) {
    /**
     * not found tablet or error happens, use the data verion of
     * current tablet image instead
     */
    cur_tablet_ver = get_serving_data_version();
  }
  tablet_version = cur_tablet_ver;

  return err;
}

int ObTabletManager::release_tablet(ObMultiVersionTabletImage& image,
                                    ObTablet* tablets[], const int64_t size) {
  int err = OB_SUCCESS;
  int ret = OB_SUCCESS;

  if (NULL == tablets || size <= 0) {
    TBSYS_LOG(WARN, "invalid param, tablets=%p size=%ld", tablets, size);
    ret = OB_INVALID_ARGUMENT;
  } else if (cur_serving_idx_ >= TABLET_ARRAY_NUM) {
    TBSYS_LOG(WARN, "invalid status, cur_serving_idx_=%ld", cur_serving_idx_);
    ret = OB_ERROR;
  } else {
    for (int64_t i = 0; i < size; ++i) {
      if (NULL != tablets[i]) {
        err = image.release_tablet(tablets[i]);
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "failed to release tablet, tablet=%p, err=%d",
                    tablets[i], err);
          ret = err;
        }
      }
    }
  }

  return ret;
}

int ObTabletManager::init_sstable_getter(const ObGetParam& get_param,
                                         ObTablet* tablets[],
                                         const int64_t size,
                                         ObSSTableGetter& sstable_getter) {
  int err                   = OB_SUCCESS;
  ObSSTableReader* reader   = NULL;
  int32_t reader_size       = 1;
  const ObGetParam::ObRowIndex* row_index = NULL;
  ObGetThreadContext* get_context         = GET_TSI(ObGetThreadContext);

  if (NULL == get_context) {
    TBSYS_LOG(WARN, "get thread local instance of sstable getter failed");
    err = OB_ERROR;
  } else if (NULL == tablets || size <= 0) {
    TBSYS_LOG(WARN, "invalid param, tablets=%p size=%ld", tablets, size);
    err = OB_INVALID_ARGUMENT;
  } else if (NULL == get_param.get_row_index() || get_param.get_row_size() <= 0) {
    TBSYS_LOG(WARN, "invalid get param");
    err = OB_ERROR;
  } else {
    row_index = get_param.get_row_index();

    for (int64_t i = 0; i < size && OB_SUCCESS == err; ++i) {
      if (NULL == tablets[i]) {
        get_context->readers_[i] = NULL;
        continue;
      }

      reader_size = 1;  //reset reader size, find_sstable will modify it
      /** FIXME: find sstable may return more than one reader */
      err = tablets[i]->find_sstable(get_param[row_index[i].offset_]->row_key_,
                                     &reader, reader_size);
      if (OB_SUCCESS == err && 1 == reader_size) {
        TBSYS_LOG(DEBUG, "find_sstable reader=%p, reader_size=%d",
                  reader, reader_size);
        get_context->readers_[i] = reader;

      } else if (OB_SIZE_OVERFLOW == err) {
        TBSYS_LOG(WARN, "find sstable reader by rowkey return more than"
                  "one reader, tablet=%p", tablets[i]);
        err = OB_ERROR;
        break;
      } else {
        TBSYS_LOG(DEBUG, "tablet find sstable reader failed, "
                  "tablet=%p, index=%ld, reader_size=%d",
                  tablets[i], i, reader_size);
        get_context->readers_[i] = NULL;
      }
    }

    if (OB_SUCCESS == err) {
      get_context->readers_count_ = size;
      err = sstable_getter.init(get_serving_block_cache() ,
                                get_serving_block_index_cache(),
                                get_param, &get_context->readers_[0],
                                get_context->readers_count_);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to set_get_param, err=%d", err);
      }
    }
  }

  return err;
}

int ObTabletManager::fill_scan_data(ObIterator& iterator, ObScanner& scanner) {
  int err = OB_SUCCESS;
  ObCellInfo* cell = NULL;
  ObCellInfo name_cell;

  while (OB_SUCCESS == (err = iterator.next_cell())) {
    err = iterator.get_cell(&cell);
    if (OB_SUCCESS != err || NULL == cell) {
      TBSYS_LOG(WARN, "failed to get cell, cell=%p, err=%d", cell, err);
      err = OB_ERROR;
    } else {
      err = scanner.add_cell(*cell);
      if (OB_SIZE_OVERFLOW == err) {
        TBSYS_LOG(INFO, "ObScanner size full, cannot add any cell.");
        scanner.rollback();
        scanner.set_is_req_fullfilled(false, 1);
        err = OB_SUCCESS;
        break;
      } else if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to add cell to scanner, err=%d", err);
        err = OB_ERROR;
      }
    }

    if (OB_SUCCESS != err) {
      break;
    }
  }

  if (OB_ITER_END == err) {
    scanner.set_is_req_fullfilled(true, 1);
    err = OB_SUCCESS;
  } else if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "error occurs while iterating, err=%d", err);
  }

  return err;
}

int ObTabletManager::fill_get_data(ObIterator& iterator, ObScanner& scanner) {
  int err = OB_SUCCESS;
  ObCellInfo* cell = NULL;
  ObCellInfo name_cell;
  int64_t cells_count = 0;

  while (OB_SUCCESS == (err = iterator.next_cell())) {
    err = iterator.get_cell(&cell);
    if (OB_SUCCESS != err || NULL == cell) {
      TBSYS_LOG(WARN, "failed to get cell, cell=%p, err=%d", cell, err);
      err = OB_ERROR;
    } else {
      err = scanner.add_cell(*cell);
      if (OB_SIZE_OVERFLOW == err) {
        TBSYS_LOG(INFO, "ObScanner size full, cannot add any cell.");
        scanner.rollback();
        cells_count = dynamic_cast<ObSSTableGetter&>(iterator).get_handled_cells_in_param();
        scanner.set_is_req_fullfilled(false, cells_count);
        err = OB_SUCCESS;
        break;
      } else if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "failed to add cell to scanner, err=%d", err);
        err = OB_ERROR;
      }
    }

    if (OB_SUCCESS != err) {
      break;
    }
  }

  if (OB_ITER_END == err) {
    cells_count = dynamic_cast<ObSSTableGetter&>(iterator).get_handled_cells_in_param();
    scanner.set_is_req_fullfilled(true, cells_count);
    err = OB_SUCCESS;
  } else if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "error occurs while iterating, err=%d", err);
  }
  return err;
}

int ObTabletManager::fill_tablet_info(const ObTablet& tablet,
                                      ObTabletReportInfo& report_tablet_info) {
  int err = OB_SUCCESS;

  report_tablet_info.tablet_info_.range_ = tablet.get_range();
  report_tablet_info.tablet_info_.occupy_size_ = tablet.get_occupy_size();
  report_tablet_info.tablet_info_.row_count_ = tablet.get_row_count();
  uint64_t tablet_checksum = 0;
  err = tablet.get_checksum(tablet_checksum);
  report_tablet_info.tablet_info_.crc_sum_ = tablet_checksum;

  const ObServer& self = THE_CHUNK_SERVER.get_self();
  report_tablet_info.tablet_location_.chunkserver_ = self;
  report_tablet_info.tablet_location_.tablet_version_ = tablet.get_data_version();

  return err;
}

int ObTabletManager::send_tablet_report(const ObTabletReportInfoList& tablets, bool has_more) {
  int err = OB_SUCCESS;

  const ObServer& root_server = THE_CHUNK_SERVER.get_root_server();
  ObRootServerRpcStub root_stub;

  if (OB_SUCCESS != (err =
                       root_stub.init(root_server, &(THE_CHUNK_SERVER.get_client_manager())))) {
    TBSYS_LOG(WARN, "failed to init root stub, err=%d", err);
  } else if (OB_SUCCESS != (err =
                              root_stub.report_tablets(tablets, 0 /*not used*/, has_more))) {
    TBSYS_LOG(WARN, "failed to send tablet report, has_more=%s, err=%d",
              has_more ? "true" : "false", err);
  }

  return err;
}

FileInfoCache&  ObTabletManager::get_fileinfo_cache() {
  return fileinfo_cache_;
}

ObBlockCache& ObTabletManager::get_serving_block_cache() {
  return block_cache_[cur_serving_idx_];
}

ObBlockCache& ObTabletManager::get_unserving_block_cache() {
  return block_cache_[(cur_serving_idx_ + 1) % TABLET_ARRAY_NUM];
}

ObBlockIndexCache& ObTabletManager::get_serving_block_index_cache() {
  return block_index_cache_[cur_serving_idx_];
}

ObBlockIndexCache& ObTabletManager::get_unserving_block_index_cache() {
  return block_index_cache_[(cur_serving_idx_ + 1) % TABLET_ARRAY_NUM];
}

ObMultiVersionTabletImage& ObTabletManager::get_serving_tablet_image() {
  return tablet_image_;
}

const ObMultiVersionTabletImage& ObTabletManager::get_serving_tablet_image() const {
  return tablet_image_;
}

ObDiskManager& ObTabletManager::get_disk_manager() {
  return disk_manager_;
}

ObRegularRecycler& ObTabletManager::get_regular_recycler() {
  return regular_recycler_;
}

ObScanRecycler& ObTabletManager::get_scan_recycler() {
  return scan_recycler_;
}

mergeserver::ObJoinCache& ObTabletManager::get_join_cache() {
  return join_cache_;
}

int ObTabletManager::build_unserving_cache() {
  int ret = OB_SUCCESS;

  if (NULL == param_) {
    TBSYS_LOG(WARN, "param is NULL");
    ret = OB_ERROR;
  } else {
    ret = build_unserving_cache(param_->get_block_cache_conf(),
                                param_->get_block_index_cache_conf());
  }

  return ret;
}

int ObTabletManager::build_unserving_cache(const ObBlockCacheConf& bc_conf,
                                           const ObBlockIndexCacheConf& bic_conf) {
  int ret                             = OB_SUCCESS;
  ObBlockCache& dst_block_cache       = get_unserving_block_cache();
  ObBlockCache& src_block_cache       = get_serving_block_cache();
  ObBlockIndexCache& dst_index_cache  = get_unserving_block_index_cache();
  int64_t src_tablet_version          = tablet_image_.get_eldest_version();
  int64_t dst_tablet_version          = tablet_image_.get_newest_version();

  if (OB_SUCCESS == ret) {
    //initialize unserving block cache
    dst_block_cache.set_fileinfo_cache(fileinfo_cache_);
    ret = dst_block_cache.init(bc_conf);
  }

  if (OB_SUCCESS == ret) {
    //initialize unserving block index cache
    dst_index_cache.set_fileinfo_cache(fileinfo_cache_);
    ret = dst_index_cache.init(bic_conf);
  }

  if (OB_SUCCESS == ret) {
    ret = switch_cache_utility_.switch_cache(tablet_image_, src_tablet_version,
                                             dst_tablet_version,
                                             src_block_cache, dst_block_cache,
                                             dst_index_cache);
  }

  return ret;
}

int ObTabletManager::drop_unserving_cache() {
  int ret                               = OB_SUCCESS;
  ObBlockCache& block_cache             = get_unserving_block_cache();
  ObBlockIndexCache& block_index_cache  = get_unserving_block_index_cache();

  ret = switch_cache_utility_.destroy_cache(block_cache, block_index_cache);

  return ret;
}

int64_t ObTabletManager::allocate_sstable_file_seq() {
  // TODO lock?
  return atomic_inc(&max_sstable_file_seq_);
}

/**
 * call by merge_tablets() after load all new tablets in Merging.
 * from this point, all (scan & get) request from client will be
 * serviced by new tablets.
 * now merge_tablets() can report tablets to RootServer.
 * then waiting RootServer for drop old tablets.
 */
int ObTabletManager::switch_cache() {
  // TODO lock?
  atomic_exchange(&cur_serving_idx_ , (cur_serving_idx_ + 1) % TABLET_ARRAY_NUM);
  return OB_SUCCESS;
}
}
}
