/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_chunk_merge.cc
 *
 * Authors:
 *     maoqi <maoqi@taobao.com>
 * Changes:
 *     qushan <qushan@taobao.com>
 *
 */

#include "ob_chunk_server_main.h"
#include "common/ob_read_common_data.h"
#include "ob_tablet_image.h"
#include "common/utility.h"
#include "ob_chunk_merge.h"
#include "sstable/ob_disk_path.h"
#include "common/ob_trace_log.h"
#include "ob_tablet_manager.h"
#include "common/ob_atomic.h"

namespace sb {
namespace chunkserver {
using namespace tbutil;
using namespace mergeserver;
using namespace common;
using namespace sstable;

/*-----------------------------------------------------------------------------
 *  ObChunkMerge
 *-----------------------------------------------------------------------------*/

ObChunkMerge::ObChunkMerge() : inited_(false), tablets_num_(0),
  tablet_index_(0), thread_num_(0),
  tablets_have_got_(0), active_thread_num_(0),
  frozen_version_(0), frozen_timestamp_(0), newest_frozen_version_(0),
  merge_start_time_(0), merge_last_end_time_(0),
  tablet_manager_(NULL),
  last_end_key_buffer_(common::OB_MAX_ROW_KEY_LENGTH),
  cur_row_key_buffer_(common::OB_MAX_ROW_KEY_LENGTH),
  round_start_(true), round_end_(false), pending_in_upgrade_(false),
  merge_load_high_(0), request_count_high_(0), merge_adjust_ratio_(0),
  merge_load_adjust_(0), merge_pause_row_count_(0), merge_pause_sleep_time_(0),
  merge_highload_sleep_time_(0), ups_stream_wrapper_(NULL)

{
  //memset(reinterpret_cast<void *>(&pending_merge_),0,sizeof(pending_merge_));
  for (uint32_t i = 0; i < sizeof(pending_merge_) / sizeof(pending_merge_[0]); ++i) {
    pending_merge_[i] = 0;
  }

}

int ObChunkMerge::fetch_update_server_list() {
  int ret = OB_SUCCESS;
  int32_t update_server_count = 0;
  if (NULL == ups_stream_wrapper_) {
    ret = OB_ERROR;
  } else {
    ret = ups_stream_wrapper_->get_ups_rpc_proxy()->fetch_update_server_list(update_server_count);
  }
  return ret;
}

int ObChunkMerge::fetch_update_server_for_merge() {
  int ret = OB_SUCCESS;
  ObServer ups_addr;
  char ups_addr_str[OB_MAX_SERVER_ADDR_SIZE];
  if (NULL == ups_stream_wrapper_) {
    ret = OB_ERROR;
  } else {
    int32_t retry_times = THE_CHUNK_SERVER.get_param().get_retry_times();
    rpc_retry_wait(inited_, retry_times,
                   ret, THE_CHUNK_SERVER.get_rs_rpc_stub().get_update_server(ups_addr, true));
    if (OB_SUCCESS == ret) {
      ups_addr.to_string(ups_addr_str, OB_MAX_SERVER_ADDR_SIZE);
      TBSYS_LOG(INFO, "fetch merge update server addr %s", ups_addr_str);
      ups_stream_wrapper_->set_update_server(ups_addr);
    }
  }
  return ret;
}

void ObChunkMerge::set_config_param() {
  ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
  merge_load_high_ = chunk_server.get_param().get_merge_threshold_load_high();
  request_count_high_ = chunk_server.get_param().get_merge_threshold_request_high();
  merge_adjust_ratio_ = chunk_server.get_param().get_merge_adjust_ratio();
  merge_load_adjust_ = (merge_load_high_ * merge_adjust_ratio_) / 100;
  merge_pause_row_count_ = chunk_server.get_param().get_merge_pause_row_count();
  merge_pause_sleep_time_ = chunk_server.get_param().get_merge_pause_sleep_time();
  merge_highload_sleep_time_ = chunk_server.get_param().get_merge_highload_sleep_time();

}

int ObChunkMerge::create_merge_threads(const int32_t max_merge_thread) {
  int ret = OB_SUCCESS;

  active_thread_num_ = max_merge_thread;
  if (0 == thread_num_) {
    for (int32_t i = 0; i < max_merge_thread; ++i) {
      if (0 != pthread_create(&tid_[thread_num_], NULL, ObChunkMerge::run, this)) {
        TBSYS_LOG(ERROR, "create merge thread failed, exit.");
        ret = OB_ERROR;
        break;
      }
      TBSYS_LOG(INFO, "create merge thread [%d] success.", thread_num_);
      ++thread_num_;
    }
  }

  if (max_merge_thread != thread_num_) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "merge : create merge thread failed, create(%d) != need(%d)",
              thread_num_, max_merge_thread);
  }

  return ret;
}

int ObChunkMerge::init(ObTabletManager* manager) {
  int ret = OB_SUCCESS;
  ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

  if (NULL == manager) {
    TBSYS_LOG(ERROR, "input error,manager is null");
    ret = OB_ERROR;
  } else if (NULL == (ups_stream_wrapper_ =
                        new(std::nothrow) ObMSUpsStreamWrapper(
    chunk_server.get_param().get_retry_times(),
    chunk_server.get_param().get_merge_timeout(),
    chunk_server.get_param().get_root_server()))) {
    TBSYS_LOG(ERROR, "cannot allocate ObMSUpsStreamWrapper.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_SUCCESS != (ret = ups_stream_wrapper_->init(
                                    chunk_server.get_thread_specific_rpc_buffer(),
                                    &chunk_server.get_client_manager()))) {
    TBSYS_LOG(ERROR, "init ups_stream_wrapper_ failed, ret = %d", ret);
  } else if (!inited_) {
    inited_ = true;

    tablet_manager_ = manager;
    frozen_version_ = manager->get_serving_data_version();
    newest_frozen_version_ = frozen_version_;

    pthread_mutex_init(&mutex_, NULL);
    pthread_cond_init(&cond_, NULL);

    int32_t max_merge_thread = chunk_server.get_param().get_max_merge_thread();
    if (max_merge_thread <= 0 || max_merge_thread > MAX_MERGE_THREAD)
      max_merge_thread = MAX_MERGE_THREAD;

    set_config_param();

    // try to fetch updateserver list, if not succeed, leave it to timer task.
    if (OB_SUCCESS != (fetch_update_server_list())) {
      TBSYS_LOG(WARN, "fetch update server list error, cannot merge.");
    }

    // if nameserver has no update server list, only got updateserver's vip
    // we still work correctly.
    if (OB_SUCCESS != (ret = fetch_update_server_for_merge())) {
      TBSYS_LOG(ERROR, "fetch_update_server_for_merge error, ret=%d", ret);
    } else {
      ret = create_merge_threads(max_merge_thread);
    }

  } else {
    TBSYS_LOG(WARN, "ObChunkMerge have been inited");
  }

  if (OB_SUCCESS != ret && inited_) {
    pthread_mutex_destroy(&mutex_);
    pthread_cond_destroy(&cond_);
    inited_ = false;
  }
  return ret;
}

void ObChunkMerge::destroy() {
  if (inited_) {
    inited_ = false;
    pthread_cond_broadcast(&cond_);
    usleep(50);

    for (int32_t i = 0; i < thread_num_; ++i) {
      pthread_join(tid_[i], NULL);
    }
    pthread_cond_destroy(&cond_);
    pthread_mutex_destroy(&mutex_);
  }

  if (NULL != ups_stream_wrapper_) {
    delete ups_stream_wrapper_;
    ups_stream_wrapper_ = NULL;
  }
}

bool ObChunkMerge::can_launch_next_round(const int64_t frozen_version) {
  bool ret = false;
  int64_t now = tbsys::CTimeUtil::getTime();

  if (inited_ && frozen_version > frozen_version_ && is_merge_stoped()
      && frozen_version > tablet_manager_->get_serving_data_version()
      && now - merge_last_end_time_ > THE_CHUNK_SERVER.get_param().get_min_merge_interval()) {
    ret = true;
  }

  return ret;
}

int ObChunkMerge::schedule(const int64_t frozen_version) {

  int ret = OB_SUCCESS;

  if (frozen_version > newest_frozen_version_) {
    newest_frozen_version_ = frozen_version;
  }

  // empty chunkserver, reset current frozen_version_ if
  // chunkserver got new tablet by migrate in or create new table;
  if (0 == frozen_version_) {
    frozen_version_ = tablet_manager_->get_serving_data_version();
  }

  if (1 >= frozen_version || (!can_launch_next_round(frozen_version))) {
    // do nothing
    TBSYS_LOG(INFO, "frozen_version=%ld, current frozen version = %ld, "
              "serving data version=%ld cannot launch next round.",
              frozen_version, frozen_version_, tablet_manager_->get_serving_data_version());
    ret = OB_CS_EAGAIN;
  } else if (0 == tablet_manager_->get_serving_data_version()) { //new chunkserver
    // empty chunkserver, no need to merge.
    TBSYS_LOG(INFO, "empty chunkserver , wait for migrate in.");
    ret = OB_CS_EAGAIN;
  } else {
    if (frozen_version - frozen_version_ > 1) {
      TBSYS_LOG(WARN, "merge is too slow,[%ld:%ld]", frozen_version_, frozen_version);
    }
    // only plus 1 in one merge process.
    frozen_version_ += 1;

    ret = start_round(frozen_version_);
    if (OB_SUCCESS != ret) {
      // start merge failed, maybe nameserver or updateserver not in serving.
      // restore old frozen_version_ for next merge process.
      frozen_version_ -= 1;
    }
  }

  if (inited_ && OB_SUCCESS == ret && thread_num_ > 0) {
    TBSYS_LOG(INFO, "wake up all merge threads, "
              "run new merge process with version=%ld", frozen_version_);
    merge_start_time_ = tbsys::CTimeUtil::getTime();
    round_start_ = true;
    round_end_ = false;
    pthread_cond_broadcast(&cond_);
  }

  return ret;
}

void* ObChunkMerge::run(void* arg) {
  if (NULL == arg) {
    TBSYS_LOG(ERROR, "internal merge : ObChunkServerMerge get a null arg");
  } else {
    ObChunkMerge* merge = reinterpret_cast<ObChunkMerge*>(arg);
    merge->merge_tablets();
  }
  return NULL;
}

void ObChunkMerge::merge_tablets() {
  int ret = OB_SUCCESS;
  const int64_t sleep_interval = 5000000;
  ObTablet* tablet = NULL;
  ObTabletMerger* merger = NULL;
  RowKeySwap  swap;

  ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

  if (OB_SUCCESS == ret) {
    merger = new(std::nothrow) ObTabletMerger(*this, *tablet_manager_);
  }

  if (NULL == merger) {
    TBSYS_LOG(ERROR, "alloc ObTabletMerger failed");
    ret = OB_ERROR;
  } else {
    swap.last_end_key_buf     = last_end_key_buffer_.get_buffer()->current();
    swap.last_end_key_buf_len = last_end_key_buffer_.get_buffer()->capacity();
    swap.start_key_buf        = cur_row_key_buffer_.get_buffer()->current();
    swap.start_key_buf_len    = cur_row_key_buffer_.get_buffer()->capacity();
    if ((ret = merger->init(&swap)) != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "init merger failed [%d]", ret);
    }
  }


  while (OB_SUCCESS == ret) {
    if (!inited_) {
      break;
    }

    if (!check_load()) {
      TBSYS_LOG(INFO, "load is too high, go to sleep");
      pthread_mutex_lock(&mutex_);

      if (1 == active_thread_num_) {
        pthread_mutex_unlock(&mutex_);
        usleep(sleep_interval); //5s
      } else {
        --active_thread_num_;
        pthread_cond_wait(&cond_, &mutex_);
        TBSYS_LOG(INFO, "to merge,active_thread_num_ :%ld", active_thread_num_);
        ++active_thread_num_;
        pthread_mutex_unlock(&mutex_);
      }

      if (!inited_) {
        break;
      }
    }

    pthread_mutex_lock(&mutex_);
    ret = get_tablets(tablet);
    while (true) {
      if (!inited_) {
        break;
      }
      if (OB_SUCCESS != ret) {
        pthread_mutex_unlock(&mutex_);
        usleep(sleep_interval);
        // retry to get tablet until got one or got nothing.
        pthread_mutex_lock(&mutex_);
      } else if (NULL == tablet) { // got nothing
        --active_thread_num_;
        TBSYS_LOG(INFO, "there is no tablet need merge, sleep wait for new merge proecess.");
        pthread_cond_wait(&cond_, &mutex_);
        TBSYS_LOG(INFO, "awake by signal, active_thread_num_:%ld", active_thread_num_);
        // retry to get new tablet for merge.
        ++active_thread_num_;
      } else { // got tablet for merge
        break;
      }
      ret = get_tablets(tablet);
    }
    pthread_mutex_unlock(&mutex_);

    int32_t retry_times = chunk_server.get_param().get_retry_times();

    // okay , we got a tablet for merge finally.
    if (NULL != tablet) {
      if (tablet->get_data_version() > frozen_version_) {
        //impossible
        TBSYS_LOG(ERROR, "local tablet version (%ld) > frozen_version_(%ld)", tablet->get_data_version(), frozen_version_);
        kill(getpid(), 2);
      } else if ((tablet->get_merge_count() > retry_times) && (have_new_version_in_othercs(tablet))) {
        TBSYS_LOG(WARN, "too many times,discard this tablet,wait for copy");
        tablet->set_merged();
      } else {
        if (((newest_frozen_version_ - tablet->get_data_version()) > chunk_server.get_param().get_max_version_gap())) {
          TBSYS_LOG(ERROR, "this tablet version (%ld : %ld) is too old,maybe don't need to merge",
                    tablet->get_data_version(), newest_frozen_version_);
        }

        volatile uint32_t* ref = &pending_merge_[ tablet->get_disk_no() ];
        if (*ref < MAX_MERGE_PER_DISK) {
          atomic_inc(ref);
          TBSYS_LOG(INFO, "get a tablet ,start merge");
          if (merger->merge(tablet, tablet->get_data_version() + 1) != OB_SUCCESS) {
            TBSYS_LOG(WARN, "merge tablet failed");
          }
          tablet->inc_merge_count();
          atomic_dec(ref);
        }
      }

      if (tablet_manager_->get_serving_tablet_image().release_tablet(tablet) != OB_SUCCESS) {
        TBSYS_LOG(WARN, "release tablet failed");
      }

      pthread_mutex_lock(&mutex_);
      ++tablets_have_got_;
      pthread_mutex_unlock(&mutex_);

    }

    if (tablet_manager_->is_stoped()) {
      TBSYS_LOG(WARN, "stop in merging");
      ret = OB_CS_MERGE_CANCELED;
    }
  }

  if (NULL != merger) {
    delete merger;
    merger = NULL;
  }
}

int ObChunkMerge::fetch_frozen_time_busy_wait(const int64_t frozen_version, int64_t& frozen_time) {
  int ret = OB_SUCCESS;
  if (0 == frozen_version) {
    TBSYS_LOG(ERROR, "invalid argument, frozen_version is 0");
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObServer ups_addr;
    int32_t retry_times  = THE_CHUNK_SERVER.get_param().get_retry_times();
    rpc_retry_wait(inited_, retry_times, ret,
                   THE_CHUNK_SERVER.get_rs_rpc_stub().get_update_server(ups_addr, false));

    if (OB_SUCCESS == ret) {
      ObRootServerRpcStub rpc_stub;
      rpc_stub.init(ups_addr, &THE_CHUNK_SERVER.get_client_manager());
      retry_times  = THE_CHUNK_SERVER.get_param().get_retry_times();
      rpc_retry_wait(inited_, retry_times, ret, rpc_stub.get_frozen_time(frozen_version, frozen_time));
    }
  }
  return ret;
}

/**
 * luanch next merge round, do something before doing actual
 * merge stuff .
 * 1. fetch new schema with current frozen version; must be
 * compatible with last schema;
 * 2. fetch new freeze timestamp with current frozen version,
 * for TTL (filter expired line);
 * 3. prepare for next merge, drop block cache used in pervoius
 * merge round, drop image slot used in prevoius version;
 */
int ObChunkMerge::start_round(const int64_t frozen_version) {
  int ret = OB_SUCCESS;
  // save schema used by last merge process .
  if (current_schema_.get_version() > 0) {
    last_schema_ = current_schema_;
  }

  int32_t retry_times = THE_CHUNK_SERVER.get_param().get_retry_times();

  // fetch current schema with frozen_version_;
  rpc_retry_wait(inited_, retry_times, ret,
                 THE_CHUNK_SERVER.get_rs_rpc_stub().fetch_schema(frozen_version, current_schema_));
  if (OB_SUCCESS == ret) {
    if (current_schema_.get_version() > 0 && last_schema_.get_version() > 0) {
      if (current_schema_.get_version() < last_schema_.get_version()) {
        TBSYS_LOG(ERROR, "the new schema old than last schema, current=%ld, last=%ld",
                  current_schema_.get_version(), last_schema_.get_version());
        ret = OB_CS_SCHEMA_INCOMPATIBLE;
      } else if (!last_schema_.is_compatible(current_schema_)) {
        TBSYS_LOG(ERROR, "the new schema and old schema is not compatible");
        ret = OB_CS_SCHEMA_INCOMPATIBLE;
      }
    }
  }

  if (OB_SUCCESS != ret) {
    TBSYS_LOG(WARN, "cannot luanch next merge round cause schema issue.");
  } else if (OB_SUCCESS != (ret = fetch_frozen_time_busy_wait(frozen_version, frozen_timestamp_))) {
    TBSYS_LOG(WARN, "cannot fetch frozen %ld timestamp from updateserver.", frozen_version);
  } else if (OB_SUCCESS != (ret = tablet_manager_->prepare_merge_tablets(frozen_version))) {
    TBSYS_LOG(WARN, "not prepared for new merge process, version=%ld", frozen_version);
  } else {
    TBSYS_LOG(INFO, "new merge process, version=%ld, frozen_timestamp_=%ld",
              frozen_version, frozen_timestamp_);
  }

  return ret;
}

int ObChunkMerge::finish_round(const int64_t frozen_version) {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = tablet_manager_->build_unserving_cache())) {
    TBSYS_LOG(WARN, "switch cache failed");
  } else if (OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().upgrade_service())) {
    TBSYS_LOG(ERROR, "upgrade_service to version = %ld failed", frozen_version);
  } else {
    TBSYS_LOG(INFO, "this version (%ld) is merge done,to switch cache and upgrade", frozen_version);
    // wait for other worker threads release old cache,
    // switch to use new cache.
    tablet_manager_->get_join_cache().destroy();
    tablet_manager_->switch_cache(); //switch cache
    tablet_manager_->get_disk_manager().scan(THE_CHUNK_SERVER.get_param().get_datadir_path(),
                                             THE_CHUNK_SERVER.get_param().get_max_sstable_size());
    // report tablets
    tablet_manager_->report_tablets();
    tablet_manager_->report_capacity_info();

    // upgrade complete, no need pending, for migrate in.
    pending_in_upgrade_ = false;

    ::usleep(THE_CHUNK_SERVER.get_param().get_min_drop_cache_wait_time());
    // now we suppose old cache not used by others,
    // drop it.
    tablet_manager_->drop_unserving_cache(); //ignore err
    // re scan all local disk to recycle sstable
    // left by RegularRecycler. e.g. migrated sstable.
    tablet_manager_->get_scan_recycler().recycle();

    round_end_ = true;
    merge_last_end_time_ = tbsys::CTimeUtil::getTime();
  }
  return ret;
}

/**
 * @brief get a tablet to merge,get lock first
 *
 * @return
 */
int ObChunkMerge::get_tablets(ObTablet*& tablet) {
  tablet = NULL;
  int err = OB_SUCCESS;
  pending_in_upgrade_ = true;
  if (tablets_num_ > 0 && tablet_index_ < tablets_num_) {
    TBSYS_LOG(INFO, "get tablet from local list,tablet_index_:%d,tablets_num_:%d", tablet_index_, tablets_num_);
    tablet = tablet_array_[tablet_index_++];
    if (NULL == tablet) {
      TBSYS_LOG(WARN, "tablet that get from tablet image is null");
    }
  } else if ((tablets_have_got_ == tablets_num_) &&
             (frozen_version_ > tablet_manager_->get_serving_data_version())) {

    while (OB_SUCCESS == err) {
      if (round_start_) {
        round_start_ = false;
      }

      tablets_num_ = sizeof(tablet_array_) / sizeof(tablet_array_[0]);
      tablet_index_ = 0;
      tablets_have_got_ = 0;

      TBSYS_LOG(INFO, "get tablet from tablet image, frozen_version_=%ld, tablets_num_=%d",
                frozen_version_, tablets_num_);
      err = tablet_manager_->get_serving_tablet_image().get_tablets_for_merge(
              frozen_version_, tablets_num_, tablet_array_);

      if (err != OB_SUCCESS) {
        TBSYS_LOG(WARN, "get tablets failed : [%d]", err);
      } else if (OB_SUCCESS == err && tablets_num_ > 0) {
        TBSYS_LOG(INFO, "get %d tablets from tablet image", tablets_num_);
        tablet = tablet_array_[tablet_index_++];
        break; //got it
      } else if (!round_end_) {
        if (OB_SUCCESS == (err = finish_round(frozen_version_))) {
          break;
        }
      } else {
        //impossible
        TBSYS_LOG(WARN, "can't get tablets and is not round end");
        break;
      }
    }
  } else {
    TBSYS_LOG(DEBUG, "tablets_num_:%d,tablets_have_got_:%d,"
              "frozen_version_:%ld,serving data version:%ld",
              tablets_num_, tablets_have_got_,
              frozen_version_, tablet_manager_->get_serving_data_version());
  }

  pending_in_upgrade_ = false;
  return err;
}

bool ObChunkMerge::have_new_version_in_othercs(const ObTablet* tablet) {
  bool ret = false;

  if (tablet != NULL) {
    ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
    ObTabletLocation list[OB_SAFE_COPY_COUNT];
    int32_t size = OB_SAFE_COPY_COUNT;
    int32_t new_copy = 0;
    int err = chunk_server.get_rs_rpc_stub().get_tablet_info(current_schema_, tablet->get_range().table_id_,
                                                             tablet->get_range(), list, size);
    if (OB_SUCCESS == err) {
      for (int32_t i = 0; i < size; ++i) {
        TBSYS_LOG(INFO, "version:%d", list[i].tablet_version_); //for test
        if (list[i].tablet_version_ > tablet->get_data_version())
          ++new_copy;
      }
    }
    if (OB_SAFE_COPY_COUNT - 1 == new_copy)
      ret = true;
  }
  return ret;
}

bool ObChunkMerge::check_load() {
  int err = OB_SUCCESS;
  bool ret = false;
  double loadavg[3];

  ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
  volatile int64_t current_request_count_ = 0;

  if (getloadavg(loadavg, sizeof(loadavg) / sizeof(loadavg[0])) < 0) {
    TBSYS_LOG(WARN, "getloadavg failed");
    err = OB_ERROR;
  }

  ObStat* stat = NULL;

  chunk_server.get_stat_manager().get_stat(ObChunkServerStatManager::META_TABLE_ID, stat);
  if (NULL == stat) {
    //TBSYS_LOG(WARN,"get stat failed");
    current_request_count_  = 0;
  } else {
    current_request_count_ = stat->get_value(ObChunkServerStatManager::INDEX_META_REQUEST_COUNT_PER_SECOND);
  }

  TBSYS_LOG(DEBUG, "loadavg[0] : %f,merge_load_high_:%ld,current_request_count_:%ld,request_count_high_:%ld",
            loadavg[0], merge_load_high_, current_request_count_, request_count_high_);

  if (OB_SUCCESS == err && (loadavg[0]  < merge_load_high_)
      && (current_request_count_ < request_count_high_)) {
    ret = true;
    int64_t sleep_thread = thread_num_ - active_thread_num_;
    int64_t remain_load = merge_load_high_ - static_cast<int64_t>(loadavg[0]) - 1; //loadavg[0] double to int
    int64_t remain_tablet = tablets_num_ - tablets_have_got_;
    if ((loadavg[0] < merge_load_adjust_) &&
        (remain_tablet > active_thread_num_) &&
        (sleep_thread > 0) && (remain_load > 0)) {
      TBSYS_LOG(INFO, "wake up %ld thread(s)", sleep_thread > remain_load ? remain_load : sleep_thread);
      pthread_mutex_lock(&mutex_);
      while (sleep_thread-- > 0 && remain_load-- > 0) {
        pthread_cond_signal(&cond_);
      }
      pthread_mutex_unlock(&mutex_);
    }
  }
  return ret;
}

/*-----------------------------------------------------------------------------
 *  ObTabletMerger
 *-----------------------------------------------------------------------------*/

ObTabletMerger::ObTabletMerger(ObChunkMerge& chunk_merge, ObTabletManager& manager) :
  chunk_merge_(chunk_merge),
  manager_(manager),
  row_key_swap_(NULL),
  cell_(NULL),
  old_tablet_(NULL),
  new_table_schema_(NULL),
  max_sstable_size_(0),
  frozen_version_(0),
  current_sstable_size_(0),
  row_num_(0),
  pre_column_group_row_num_(0),
  cs_proxy_(manager),
  ms_wrapper_(chunk_merge.get_ups_stream_wrapper()),
  merge_join_agent_(cs_proxy_)
{}

int ObTabletMerger::init(ObChunkMerge::RowKeySwap* swap) {
  int ret = OB_SUCCESS;
  ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

  if (NULL == swap) {
    TBSYS_LOG(ERROR, "interal error,swap is null");
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret) {
    row_key_swap_ = swap;
    max_sstable_size_ = chunk_server.get_param().get_max_sstable_size();
  }


  if (OB_SUCCESS == ret && chunk_server.get_param().get_join_cache_conf().cache_mem_size > 0) {
    ms_wrapper_.get_ups_get_cell_stream()->set_cache(manager_.get_join_cache());
  }

  if (OB_SUCCESS == ret) {
    int64_t dio_type = chunk_server.get_param().get_write_sstable_io_type();
    bool dio = false;
    if (dio_type > 0) {
      dio = true;
    }
    writer_.set_dio(dio);
  }

  return ret;
}

int ObTabletMerger::check_expire_column(const common::ObSchemaManagerV2& schema,
                                        const uint64_t table_id, ExpireColumnInfo& expire_column_info) {
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* col_schema = NULL;
  uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
  int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

  memset(&expire_column_info, 0, sizeof(ExpireColumnInfo));

  table_schema = schema.get_table_schema(table_id);
  if (NULL == table_schema) {
    TBSYS_LOG(ERROR, "table = %ld not exist.", table_id);
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = schema.get_column_groups(table_id,
                                                           column_group_ids, column_group_num))) {
    TBSYS_LOG(ERROR, "table = %ld get column groups error.", table_id);
  } else if (OB_SUCCESS != (ret =
                              table_schema->get_expire_condition(
                                expire_column_info.column_id_, expire_column_info.duration_))) {
    TBSYS_LOG(INFO, "table = %ld do not contain expire condition.", table_id);
  } else if (NULL == (col_schema = schema.get_column_schema(table_id, expire_column_info.column_id_))) {
    TBSYS_LOG(WARN, "define expire table=%ld, column id= %ld, not exist in table schema.",
              table_id, expire_column_info.column_id_);
    ret = OB_ERROR;
  } else if (ObDateTimeType != col_schema->get_type()
             && ObPreciseDateTimeType != col_schema->get_type()
             && ObCreateTimeType !=  col_schema->get_type()
             && ObModifyTimeType !=  col_schema->get_type()) {
    TBSYS_LOG(WARN, "define expire column type = %d, not a date time type.",
              col_schema->get_type());
    ret = OB_ERROR;
  } else if (column_group_num <= 0 || column_group_ids[0] != col_schema->get_column_group_id()) {
    TBSYS_LOG(WARN, "expire column in group = %ld, not the first column group=%ld, ignore.",
              col_schema->get_column_group_id(), column_group_num > 0 ? column_group_ids[0] : 0);
    ret = OB_ERROR;
  } else {
    expire_column_info.column_group_id_ = col_schema->get_column_group_id();
    expire_column_info.duration_ *= 1000000L; // trans to microseconds;
  }

  return ret;
}

bool ObTabletMerger::is_expired_cell(const common::ObCellInfo& cell,
                                     const int64_t expire_duration, const int64_t frozen_timestamp) {
  bool ret = false;
  int64_t timestamp = 0;
  int err = cell.value_.get_timestamp(timestamp);
  if (OB_SUCCESS != err) {
    ret = false;
  } else if (timestamp + expire_duration < frozen_timestamp) {
    ret = true;
  }
  return ret;
}

int ObTabletMerger::check_row_count_in_column_group() {
  int ret = OB_SUCCESS;
  if (pre_column_group_row_num_ != 0) {
    if (row_num_ != pre_column_group_row_num_) {
      TBSYS_LOG(ERROR, "the row num between two column groups is difference,[%ld,%ld]",
                row_num_, pre_column_group_row_num_);
      ret = OB_ERROR;
    }
  } else {
    pre_column_group_row_num_ = row_num_;
  }
  return ret;
}

void ObTabletMerger::reset_for_next_column_group() {
  row_.clear();
  merge_join_agent_.clear();
  scan_param_.reset();
  cs_proxy_.reset();
  reset_query_thread_local_buffer();
  row_num_ = 0;
}

int ObTabletMerger::save_current_row(const bool current_row_expired) {
  int ret = OB_SUCCESS;
  if (current_row_expired) {
    //TBSYS_LOG(DEBUG, "current row expired.");
    //hex_dump(row_.get_row_key().ptr(), row_.get_row_key().length(), false, TBSYS_LOG_LEVEL_DEBUG);
  } else if ((ret = writer_.append_row(row_, current_sstable_size_)) != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "Merge : append row failed [%d]", ret);
    TBSYS_LOG(ERROR, "this row_,obj count:%ld,table :%lu,group:%lu", row_.get_obj_count(),
              row_.get_table_id(), row_.get_column_group_id());
    TBSYS_LOG(ERROR, "rowkey:");
    hex_dump(row_.get_row_key().ptr(), row_.get_row_key().length(), false, TBSYS_LOG_LEVEL_ERROR);
    for (int64_t i = 0; i < row_.get_obj_count(); ++i) {
      row_.get_obj(i)->dump(TBSYS_LOG_LEVEL_ERROR);
    }
  }
  return ret;
}

int ObTabletMerger::wait_aio_buffer() const {
  int ret = OB_SUCCESS;
  int status = 0;

  ObThreadAIOBufferMgrArray* aio_buf_mgr_array = GET_TSI(ObThreadAIOBufferMgrArray);
  if (NULL == aio_buf_mgr_array) {
    ret = OB_ERROR;
  } else if (OB_AIO_TIMEOUT == (status =
                                  aio_buf_mgr_array->wait_all_aio_buf_mgr_free(10 * 1000000))) {
    TBSYS_LOG(WARN, "failed to wait all aio buffer manager free, stop current thread");
    ret = OB_ERROR;
  }

  return ret;

}

int ObTabletMerger::merge_column_group(
  const int64_t column_group_id,
  const int64_t column_group_num,
  const ExpireColumnInfo& expire_column_info,
  const int64_t split_row_pos,
  const int64_t tablet_after_merge,
  ExpireRowFilter& expire_row_filter,
  bool& is_tablet_splited) {
  int ret = OB_SUCCESS;
  RowStatus row_status = ROW_START;

  bool is_row_changed = false;
  bool expire_column_in_group = (expire_column_info.column_group_id_ == column_group_id);
  bool current_row_expired = false;
  int64_t expire_row_num = 0;
  int64_t merge_mem_limit = THE_CHUNK_SERVER.get_param().get_merge_mem_limit();

  is_tablet_splited = false;

  TBSYS_LOG(INFO, "column group %lu", column_group_id);

  if (OB_SUCCESS != (ret = wait_aio_buffer())) {
    TBSYS_LOG(ERROR, "wait aio buffer error, column_group_id = %ld", column_group_id);
  } else if (OB_SUCCESS != (ret = fill_scan_param(column_group_id))) {
    TBSYS_LOG(ERROR, "prepare scan param failed : [%d]", ret);
  } else if (OB_SUCCESS != (ret = merge_join_agent_.set_request_param(scan_param_,
                                  *ms_wrapper_.get_ups_scan_cell_stream(),
                                  *ms_wrapper_.get_ups_get_cell_stream(),
                                  chunk_merge_.current_schema_,
                                  merge_mem_limit))) {
    TBSYS_LOG(ERROR, "set request param for merge_join_agent failed [%d]", ret);
    merge_join_agent_.clear();
    scan_param_.reset();
  }

  while (OB_SUCCESS == ret) {
    cell_ = NULL; //in case
    if (manager_.is_stoped()) {
      TBSYS_LOG(WARN, "stop in merging column_group_id=%ld", column_group_id);
      ret = OB_CS_MERGE_CANCELED;
    } else if ((ret = merge_join_agent_.next_cell()) != OB_SUCCESS ||
               (ret = merge_join_agent_.get_cell(&cell_, &is_row_changed)) != OB_SUCCESS) {
      //end or error
      if (OB_ITER_END == ret) {
        TBSYS_LOG(DEBUG, "Merge : end of file");
        ret = OB_SUCCESS;
        if (ROW_GROWING == row_status
            && OB_SUCCESS == (ret = save_current_row(current_row_expired))) {
          ++row_num_;
          row_.clear();
          current_row_expired = false;
          row_status = ROW_END;
        }
      } else {
        TBSYS_LOG(ERROR, "Merge : get data error,ret : [%d]", ret);
      }

      // end of file
      break;
    } else if (cell_ != NULL) {
      /**
       * there are some data in scanner
       */
      if ((ROW_GROWING == row_status && is_row_changed)) {
        if (0 == (row_num_ % chunk_merge_.merge_pause_row_count_)) {
          if (chunk_merge_.merge_pause_sleep_time_ > 0) {
            if (0 == (row_num_ % (chunk_merge_.merge_pause_row_count_ * 20))) {
              // print log every sleep 20 times;
              TBSYS_LOG(INFO, "pause in merging,sleep %ld",
                        chunk_merge_.merge_pause_sleep_time_);
            }
            usleep(chunk_merge_.merge_pause_sleep_time_);
          }

          while (!chunk_merge_.check_load()) {
            TBSYS_LOG(INFO, "load is too high in merging,sleep %ld",
                      chunk_merge_.merge_highload_sleep_time_);
            usleep(chunk_merge_.merge_highload_sleep_time_);
          }
        }

        // we got new row, write last row we build.
        if (OB_SUCCESS == (ret = save_current_row(current_row_expired))) {
          ++row_num_;
          current_row_expired = false;
          // start new row.
          row_status = ROW_START;
        }

        // this split will use current %row_, so we clear row below.
        if (OB_SUCCESS == ret
            && tablet_after_merge > 1
            && row_num_ > split_row_pos
            && maybe_change_sstable()) {
          /**
           * record the end key
           */
          if ((ret = update_range_end_key()) != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "update end range error [%d]", ret);
          } else {
            is_tablet_splited = true;
          }
          // entercount split point, skip to merge next column group
          // we reset all status whenever start new column group, so
          // just break it, leave it to reset_for_next_column_group();
          break;
        }

        // before we build new row, must clear last row in use.
        row_.clear();
      }

      if (OB_SUCCESS == ret && ROW_START == row_status) {
        // first cell in current row, set rowkey and table info.
        row_.set_row_key(cell_->row_key_);
        row_.set_table_id(cell_->table_id_);
        row_.set_column_group_id(column_group_id);
        row_status = ROW_GROWING;
      }

      if (OB_SUCCESS == ret && ROW_GROWING == row_status) {
        // check current row if expired, and add current cell in %row_.
        // no need check if expired when only one column group.
        if (!current_row_expired && expire_column_info.column_id_ != 0) {
          if (!expire_column_in_group) {
            if (expire_row_filter.test(row_num_)) {
              current_row_expired = true;
              ++expire_row_num ;
            }
          } else if (cell_->column_id_ == expire_column_info.column_id_
                     && is_expired_cell(*cell_, expire_column_info.duration_,
                                        chunk_merge_.frozen_timestamp_)) {
            current_row_expired = true;
            ++expire_row_num ;
            if (column_group_num > 1 && OB_SUCCESS != (ret = expire_row_filter.set(row_num_, true))) {
              TBSYS_LOG(ERROR, "cannot insert expire row_num_ = %ld to bit set", row_num_);
            }
          }
        }

        // no need to add current cell to %row_ when current row expired.
        if ((!current_row_expired) && (ret = row_.add_obj(cell_->value_)) != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "Merge : add_cell_to_row failed [%d]", ret);
        }
      }
    } else {
      TBSYS_LOG(ERROR, "get cell return success but cell is null");
      ret = OB_ERROR;
    }
  }

  TBSYS_LOG(INFO, "column group %ld merge completed, ret = %d, "
            "total row count =%ld ,expire row count = %ld",
            column_group_id, ret, row_num_, expire_row_num);

  return ret;
}

int ObTabletMerger::merge(ObTablet* tablet, int64_t frozen_version) {
  int   ret = OB_SUCCESS;

  ExpireColumnInfo expire_column_info;

  int64_t split_row_pos = tablet->get_row_count();
  int64_t tablet_after_merge = 1;
  bool    is_tablet_splited = false;


  if (NULL == tablet || frozen_version <= 0) {
    TBSYS_LOG(ERROR, "merge : interal error ,param invalid frozen_version:[%ld]", frozen_version);
    ret = OB_ERROR;
  } else if (OB_SUCCESS != (ret = reset_query_thread_local_buffer())) {
    TBSYS_LOG(ERROR, "reset query thread local buffer error.");
  } else if (NULL == (new_table_schema_ =
                        chunk_merge_.current_schema_.get_table_schema(tablet->get_range().table_id_))) {
    //This table has been deleted
    TBSYS_LOG(ERROR, "table (%lu) has been deleted", tablet->get_range().table_id_);
    tablet->set_merged();
    ret = OB_CS_TABLE_HAS_DELETED;
  } else if (OB_SUCCESS != (ret = fill_sstable_schema(*new_table_schema_, sstable_schema_))) {
    TBSYS_LOG(ERROR, "convert table schema to sstable schema failed, table=%ld",
              tablet->get_range().table_id_);
  } else if (OB_SUCCESS != check_expire_column(chunk_merge_.current_schema_,
                                               tablet->get_range().table_id_, expire_column_info)) {
    // get expire info error, just ignore, do not abort merge process;
    memset(&expire_column_info, 0, sizeof(ExpireColumnInfo));
  } else {
    TBSYS_LOG(INFO, "table=%ld, expire column id = %ld, expire_duration=%ld, column group id=%ld",
              tablet->get_range().table_id_, expire_column_info.column_id_, expire_column_info.duration_,
              expire_column_info.column_group_id_);
  }


  if (OB_SUCCESS == ret) {
    // parse compress name from schema, may be NULL;
    const char* compressor_name = new_table_schema_->get_compress_func_name();
    if (NULL == compressor_name || '\0' == *compressor_name) {
      TBSYS_LOG(WARN, "no compressor with this sstable.");
    } else {
      TBSYS_LOG(INFO, "compressor is: %s", compressor_name);
    }
    compressor_string_.assign_ptr(const_cast<char*>(compressor_name), strlen(compressor_name));

    // according to sstable size,check if we need to split this sstable
    if (tablet->get_occupy_size() > (max_sstable_size_ + (max_sstable_size_ >> 2))) {
      TBSYS_LOG(INFO, "split this tablet, orig size:%ld orig row_count:%ld,max_sstable_size_:%ld",
                tablet->get_occupy_size(), tablet->get_row_count(), max_sstable_size_);
      split_row_pos = tablet->get_row_count() / 2;
      tablet_after_merge = 2;
    }
  }


  if (OB_SUCCESS == ret) {
    new_range_ = tablet->get_range();
    old_tablet_ = tablet;
    frozen_version_ = frozen_version;
    int64_t sstable_id = 0;
    char range_buf[OB_RANGE_STR_BUFSIZ];

    if ((tablet->get_sstable_id_list()).get_array_index() > 0)
      sstable_id = (tablet->get_sstable_id_list()).at(0)->sstable_file_id_;

    tablet->get_range().to_string(range_buf, sizeof(range_buf));
    TBSYS_LOG(INFO, " start merge sstable_id:%ld,range:%s", sstable_id , range_buf);

    if ((ret = create_new_sstable()) != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "create sstable failed.");
    }

    uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
    int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

    if (OB_SUCCESS != (ret = chunk_merge_.current_schema_.get_column_groups(
                               new_table_schema_->get_table_id(), column_group_ids, column_group_num))) {
      TBSYS_LOG(ERROR, "get column groups failed : [%d]", ret);
    }

    expire_filter_allocator_.reuse();
    ExpireRowFilter expire_row_filter(tablet->get_row_count() * 2, &expire_filter_allocator_);

    while (OB_SUCCESS == ret) {
      if (manager_.is_stoped()) {
        TBSYS_LOG(WARN, "stop in merging");
        ret = OB_CS_MERGE_CANCELED;
      }

      // clear all bits while start merge new tablet.
      // generally in table split.
      expire_row_filter.clear();
      pre_column_group_row_num_ = 0;

      for (int32_t group_index = 0; (group_index < column_group_num) && (OB_SUCCESS == ret); ++group_index) {
        if (manager_.is_stoped()) {
          TBSYS_LOG(WARN, "stop in merging");
          ret = OB_CS_MERGE_CANCELED;
        }
        // %expire_row_filter will be set in first column group,
        // and test in next merge column group.
        else if (OB_SUCCESS != (ret = merge_column_group(
                                        column_group_ids[group_index], column_group_num, expire_column_info,
                                        split_row_pos, tablet_after_merge, expire_row_filter, is_tablet_splited))) {
          TBSYS_LOG(ERROR, "merge column group[%ld] = %ld , group num = %ld,"
                    "split_row_pos = %ld tablet_after_merge=%ld error.",
                    group_index, column_group_ids[group_index],
                    column_group_num, split_row_pos, tablet_after_merge);
        } else if (OB_SUCCESS != (ret = check_row_count_in_column_group())) {
          TBSYS_LOG(ERROR, "check row count column group[%ld] = %ld",
                    group_index, column_group_ids[group_index]);
        }

        // tablet should split, but rows with same rowkey[0,split_pos-1] cannot store
        // in difference tablets, if this kind of row is the last row of current tablet
        // so give up split action, CAUTION, only first column group can split.
        if (group_index == 0 && (!is_tablet_splited) && tablet_after_merge > 1) {
          TBSYS_LOG(INFO, "this tablet should split to %d tablets, but split rule stop that.", tablet_after_merge);
          tablet_after_merge = 0;
        }

        // reset for next column group..
        // page arena reuse avoid memory explosion
        reset_for_next_column_group();
      }

      // all column group has been written,finish this sstable

      if (OB_SUCCESS == ret && (ret = finish_sstable()) != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "close sstable failed [%d]", ret);
      } else {
        --tablet_after_merge;
      }

      if (tablet_after_merge <= 0) {
        //this tablet has no split
        break;
      } else if (OB_SUCCESS == ret) {
        update_range_start_key();

        //prepare the next request param
        new_range_.end_key_ = tablet->get_range().end_key_;
        if (tablet->get_range().border_flag_.is_max_value()) {
          TBSYS_LOG(INFO, "this tablet has max flag,reset it");
          new_range_.border_flag_.set_max_value();
          new_range_.border_flag_.unset_inclusive_end();
        }
      }

      if ((OB_SUCCESS == ret) && (ret = create_new_sstable()) != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "create sstable failed [%d]", ret);
      }

    } // while (OB_SUCCESS == ret) //finish one tablet

    if (OB_SUCCESS == ret) {
      ret = update_meta();
    } else {
      TBSYS_LOG(ERROR, "merge failed,don't add these tablet");

      int64_t t_off = 0;
      int64_t s_size = 0;
      writer_.close_sstable(t_off, s_size);
      if (sstable_id_.sstable_file_id_ != 0) {
        unlink(path_); //delete
        TBSYS_LOG(WARN, "delete %s", path_);
        manager_.get_disk_manager().add_used_space((sstable_id_.sstable_file_id_ & DISK_NO_MASK), 0);
      }

      int64_t sstable_id = 0;

      for (ObVector<ObTablet*>::iterator it = tablet_array_.begin(); it != tablet_array_.end(); ++it) {
        if (((*it) != NULL) && ((*it)->get_sstable_id_list().get_array_index() > 0)) {
          sstable_id = (*it)->get_sstable_id_list().at(0)->sstable_file_id_;
          if (OB_SUCCESS == get_sstable_path(sstable_id, path_, sizeof(path_))) {
            unlink(path_);
          }
        }
      }
      tablet_array_.clear();
    }
  }
  return ret;
}

int ObTabletMerger::fill_sstable_schema(const ObTableSchema& common_schema, ObSSTableSchema& sstable_schema) {
  int ret = OB_SUCCESS;
  int32_t cols = 0;
  int32_t size = 0;
  ObSSTableSchemaColumnDef column_def;

  sstable_schema.reset();

  const ObColumnSchemaV2* col = chunk_merge_.current_schema_.get_table_schema(common_schema.get_table_id(), size);

  if (NULL == col || size <= 0) {
    TBSYS_LOG(ERROR, "cann't find this table:%lu", common_schema.get_table_id());
    ret = OB_ERROR;
  } else {
    for (int col_index = 0; col_index < size && OB_SUCCESS == ret; ++col_index) {
      memset(&column_def, 0, sizeof(column_def));
      column_def.table_id_ = common_schema.get_table_id();
      column_def.column_group_id_ = (col + col_index)->get_column_group_id();
      column_def.column_name_id_ = (col + col_index)->get_id();
      column_def.column_value_type_ = (col + col_index)->get_type();
      if ((ret = sstable_schema.add_column_def(column_def)) != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "add column_def(%lu,%lu,%lu) failed col_index : %d", column_def.table_id_,
                  column_def.column_group_id_, column_def.column_name_id_, col_index);
      }
      ++cols;
    }
  }

  if (0 == cols && OB_SUCCESS == ret) { //this table has moved to updateserver
    ret = OB_CS_TABLE_HAS_DELETED;
  }
  return ret;
}

int ObTabletMerger::create_new_sstable() {
  int ret                          = OB_SUCCESS;
  sstable_id_.sstable_file_id_     = manager_.allocate_sstable_file_seq();
  sstable_id_.sstable_file_offset_ = 0;
  int32_t disk_no                  = manager_.get_disk_manager().get_dest_disk();

  if (disk_no < 0) {
    TBSYS_LOG(ERROR, "does't have enough disk space");
    sstable_id_.sstable_file_id_ = 0;
    ret = OB_CS_OUTOF_DISK_SPACE;
  }

  if (OB_SUCCESS == ret) {
    sstable_id_.sstable_file_id_     = (sstable_id_.sstable_file_id_ << 8) | (disk_no & 0xff);

    if ((OB_SUCCESS == ret) && (ret = get_sstable_path(sstable_id_, path_, sizeof(path_))) != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "Merge : can't get the path of new sstable");
      ret = OB_ERROR;
    }

    if (OB_SUCCESS == ret) {
      TBSYS_LOG(INFO, "dest sstable_path:%s\n", path_);
      path_string_.assign_ptr(path_, strlen(path_) + 1);

      if ((ret = writer_.create_sstable(sstable_schema_, path_string_, compressor_string_, frozen_version_)) != OB_SUCCESS) {
        if (OB_IO_ERROR == ret)
          manager_.get_disk_manager().set_disk_status(disk_no, DISK_ERROR);
        TBSYS_LOG(ERROR, "Merge : create sstable failed : [%d]", ret);
      }
    }
  }
  return ret;
}

int ObTabletMerger::fill_scan_param(const uint64_t column_group_id) {
  int ret = OB_SUCCESS;

  ObString table_name_string;

  //scan_param_.reset();
  scan_param_.set(new_range_.table_id_, table_name_string, new_range_);
  /**
   * in merge,we do not use cache,
   * and just read frozen mem table.
   */
  scan_param_.set_is_result_cached(false);
  scan_param_.set_is_read_consistency(false);

  int64_t preread_mode = THE_CHUNK_SERVER.get_param().get_merge_scan_use_preread();
  if (0 == preread_mode) {
    scan_param_.set_read_mode(ObScanParam::SYNCREAD);
  } else {
    scan_param_.set_read_mode(ObScanParam::PREREAD);
  }

  ObVersionRange version_range;
  version_range.border_flag_.set_inclusive_end();

  version_range.start_version_ =  old_tablet_->get_data_version();
  version_range.end_version_   =  old_tablet_->get_data_version() + 1;

  scan_param_.set_version_range(version_range);
  char range_buf[OB_RANGE_STR_BUFSIZ];
  new_range_.to_string(range_buf, sizeof(range_buf));
  TBSYS_LOG(INFO, "SCAN PARAM: version range [%ld,%ld],range:%s", version_range.start_version_,
            version_range.end_version_, range_buf);

  int32_t size = 0;
  const ObColumnSchemaV2* col = chunk_merge_.current_schema_.get_group_schema(
                                  new_table_schema_->get_table_id(), column_group_id, size);

  if (NULL == col || size <= 0) {
    TBSYS_LOG(ERROR, "cann't find this column group:%lu", column_group_id);
    ret = OB_ERROR;
  } else {
    for (int32_t i = 0; i < size; ++i) {
      if ((ret = scan_param_.add_column((col + i)->get_id())) != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "add column id [%lu] to scan_param_ error[%d]", (col + i)->get_id(), ret);
        break;
      }
    }
  }
  return ret;
}

int ObTabletMerger::update_range_start_key() {
  std::swap(row_key_swap_->last_end_key_buf, row_key_swap_->start_key_buf);
  row_key_swap_->start_key_len = row_key_swap_->last_end_key_len;
  new_range_.start_key_.assign_ptr(row_key_swap_->start_key_buf, row_key_swap_->start_key_len);
  new_range_.border_flag_.unset_min_value();
  new_range_.border_flag_.unset_inclusive_start();
  return OB_SUCCESS;
}

int ObTabletMerger::update_range_end_key() {
  int ret = OB_SUCCESS;
  int32_t split_pos = new_table_schema_->get_split_pos();

  memcpy(row_key_swap_->last_end_key_buf, row_.get_row_key().ptr(), row_.get_row_key().length());
  row_key_swap_->last_end_key_len = row_.get_row_key().length();

  if (split_pos > 0) {
    if (row_key_swap_->last_end_key_len < split_pos) {
      TBSYS_LOG(ERROR, "rowkey is too short");
      ret = OB_ERROR;
    } else {
      memset(row_key_swap_->last_end_key_buf + split_pos, 0xFF, row_key_swap_->last_end_key_len - split_pos);
    }
  }

  if (OB_SUCCESS == ret) {
    new_range_.end_key_.assign_ptr(row_key_swap_->last_end_key_buf, row_key_swap_->last_end_key_len);
    new_range_.border_flag_.unset_max_value();
    new_range_.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObTabletMerger::finish_sstable() {
  int64_t trailer_offset = 0;
  int64_t sstable_size = 0;
  int ret = OB_ERROR;
  TBSYS_LOG(DEBUG, "Merge : finish_sstable");
  if ((ret = writer_.close_sstable(trailer_offset, sstable_size)) != OB_SUCCESS
      || sstable_size < 0) {
    TBSYS_LOG(ERROR, "Merge : close sstable failed.");
  } else {
    ObMultiVersionTabletImage& tablet_image = manager_.get_serving_tablet_image();
    ObTablet* tablet = NULL;
    char range_buf[OB_RANGE_STR_BUFSIZ];
    new_range_.to_string(range_buf, sizeof(range_buf));
    TBSYS_LOG(INFO, "this sstable range:%s\n", range_buf);
    row_num_ = 0;
    pre_column_group_row_num_ = 0;
    if ((ret = tablet_image.alloc_tablet_object(new_range_, frozen_version_, tablet)) != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "alloc_tablet_object failed.");
    } else {
      //set new data version
      tablet->set_data_version(frozen_version_);
      tablet->set_disk_no((sstable_id_.sstable_file_id_) & DISK_NO_MASK);
      if (sstable_size > 0) {
        if ((ret = tablet->add_sstable_by_id(sstable_id_)) != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "Merge : add sstable to tablet failed.");
        }
      }
    }

    if (OB_SUCCESS == ret) {
      ret = tablet_array_.push_back(tablet);
    }

    if (OB_SUCCESS == ret) {
      manager_.get_disk_manager().add_used_space((sstable_id_.sstable_file_id_ & DISK_NO_MASK), sstable_size);
    }
  }
  return ret;
}

int ObTabletMerger::update_meta() {
  int ret = OB_SUCCESS;
  ObMultiVersionTabletImage& tablet_image = manager_.get_serving_tablet_image();
  ObTablet* new_tablet_list[ tablet_array_.size() ];
  int32_t idx = 0;
  for (ObVector<ObTablet*>::iterator it = tablet_array_.begin(); it != tablet_array_.end(); ++it) {
    if (NULL == (*it)) { //in case
      ret = OB_ERROR;
      break;
    }
    new_tablet_list[idx++] = (*it);
  }

  if (OB_SUCCESS == ret) {
    // in case we have migrated tablets, discard current merge tablet
    if (!old_tablet_->is_merged()) {
      if (OB_SUCCESS != (ret = tablet_image.upgrade_tablet(
                                 old_tablet_, new_tablet_list, idx, true))) {
        TBSYS_LOG(WARN, "upgrade new merged tablets error [%d]", ret);
      }
      //else if (OB_SUCCESS != (ret = report_tablets(new_tablet_list,idx)))
      //{
      //  TBSYS_LOG(WARN,"report tablets to nameserver error.");
      //}
      else {
        // sync new tablet meta files;
        for (int32_t i = 0; i < idx; ++i) {
          if (OB_SUCCESS != (ret = tablet_image.write(
                                     new_tablet_list[i]->get_data_version(),
                                     new_tablet_list[i]->get_disk_no()))) {
            TBSYS_LOG(WARN, "write new meta failed i=%d, version=%ld, disk_no=%d", i ,
                      new_tablet_list[i]->get_data_version(), new_tablet_list[i]->get_disk_no());
          }
        }

        // sync old tablet meta files;
        if (OB_SUCCESS == ret
            && OB_SUCCESS != (ret = tablet_image.write(
                                      old_tablet_->get_data_version(), old_tablet_->get_disk_no()))) {
          TBSYS_LOG(WARN, "write old meta failed version=%ld, disk_no=%d",
                    old_tablet_->get_data_version(), old_tablet_->get_disk_no());
        }

        if (OB_SUCCESS == ret) {
          int64_t recycle_version = old_tablet_->get_data_version()
                                    - (ObMultiVersionTabletImage::MAX_RESERVE_VERSION_COUNT - 1);
          if (recycle_version > 0) {
            TBSYS_LOG(INFO, "recycle tablet version = %ld", recycle_version);
            manager_.get_regular_recycler().recycle_tablet(
              old_tablet_->get_range(), recycle_version);
          }
        }

      }
    } else {
      TBSYS_LOG(INFO, "current tablet covered by migrated tablets, discard.");
    }
  }

  tablet_array_.clear();
  return ret;
}

int ObTabletMerger::report_tablets(ObTablet* tablet_list[], int32_t tablet_size) {
  int  ret = OB_SUCCESS;
  int64_t num = OB_MAX_TABLET_LIST_NUMBER;
  ObTabletReportInfoList* report_info_list =  GET_TSI(ObTabletReportInfoList);

  if (tablet_size < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == report_info_list) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    report_info_list->reset();
    ObTabletReportInfo tablet_info;
    for (int32_t i = 0; i < tablet_size && num > 0 && OB_SUCCESS == ret; ++i) {
      manager_.fill_tablet_info(*tablet_list[i], tablet_info);
      if (OB_SUCCESS != (ret = report_info_list->add_tablet(tablet_info))) {
        TBSYS_LOG(WARN, "failed to add tablet info, num=%ld, err=%d", num, ret);
      } else {
        --num;
      }
    }
    if (OB_SUCCESS != (ret = manager_.send_tablet_report(*report_info_list, true))) { //always has more
      TBSYS_LOG(WARN, "failed to send request report to nameserver err = %d", ret);
    }
  }
  return ret;
}

bool ObTabletMerger::maybe_change_sstable() const {
  bool ret = false;
  int64_t rowkey_cmp_size = new_table_schema_->get_split_pos();

  if (0 != rowkey_cmp_size) {
    if (memcmp(row_.get_row_key().ptr(), cell_->row_key_.ptr(), rowkey_cmp_size) != 0) {
      TBSYS_LOG(DEBUG, "may be change sstable");
      ret = true;
    } else {
      TBSYS_LOG(DEBUG, "the same user,can't split,%d", new_table_schema_->get_split_pos());
      hex_dump(row_.get_row_key().ptr(), row_.get_row_key().length(), true);
      hex_dump(cell_->row_key_.ptr(), cell_->row_key_.length(), true);
    }
  } else {
    ret = true;
  }
  return ret;
}
} /* chunkserver */
} /* sb */
