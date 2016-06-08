/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_memtable.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_MEMTABLE_H_
#define  OCEANBASE_UPDATESERVER_MEMTABLE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <bitset>
#include <algorithm>
#include "ob_atomic.h"
#include "common/ob_define.h"
#include "common/ob_string_buf.h"
#include "common/ob_iterator.h"
#include "common/page_arena.h"
#include "common/ob_mutator.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_bloomfilter.h"
#include "ob_table_engine.h"
#include "ob_hash_engine.h"
#include "ob_btree_engine.h"
#include "ob_cellinfo_processor.h"
#include "ob_column_filter.h"
#include "ob_ups_mutator.h"

namespace sb {
namespace updateserver {
class MemTable;

#ifdef _BTREE_ENGINE_
typedef BtreeEngine TableEngine;
typedef BtreeEngineIterator TableEngineIterator;
typedef BtreeEngineTransHandle TableEngineTransHandle;
#else
// 使用hash实现
typedef HashEngine TableEngine;
typedef HashEngineIterator TableEngineIterator;
typedef HashEngineTransHandle TableEngineTransHandle;
#endif

typedef TETransType MemTableTransType;
typedef TableEngineTransHandle MemTableTransHandle;
class MemTableIterator : public common::ObIterator {
  friend class MemTable;
  class RowExtInfo {
    static const int64_t EXT_INFO_NUM = 2;
    static const int64_t EXT_CREATE_TIME = 0;
    static const int64_t EXT_MODIFY_TIME = 1;
   public:
    RowExtInfo();
    ~RowExtInfo();
   public:
    inline void set(const TEKey& te_key, const TEValue& te_value);
    inline bool have_ext_info() const;
    inline void get_ext_info(common::ObCellInfo& cell_info);
   private:
    TEKey te_key_;
    TEValue te_value_;
    std::bitset<EXT_INFO_NUM> got_map_;
    uint64_t create_time_column_id_;
    uint64_t modify_time_column_id_;
  };
 public:
  MemTableIterator();
  virtual ~MemTableIterator();
 public:
  virtual bool last_row_cell() const;
  virtual int next_cell();
  virtual int get_cell(sb::common::ObCellInfo** cell_info);
  virtual int get_cell(sb::common::ObCellInfo** cell_info, bool* is_row_changed);
  void reset();
 private:
  inline void set_(const TEKey& te_key,
                   const TEValue& te_value,
                   ColumnFilter* column_filter_);
  inline void set_(const TableEngineIterator& te_iter,
                   ColumnFilter* column_filter_,
                   const uint64_t table_id);
  inline int next_();
  inline void set_row_not_exist_(const uint64_t table_id, const common::ObString& row_key);
 private:
  bool row_has_expected_column_;
  bool row_has_returned_column_;
  bool last_row_cell_;
  bool new_row_cell_;
  bool first_next_;
  bool is_iter_end_;
  bool is_row_changed_;
  bool row_not_exist_;
  ObCellInfoNode* ci_iter_;
  ObCellInfoNode* ci_iter_end_;
  TableEngineIterator te_iter_;
  ColumnFilter* column_filter_;
  common::ObCellInfo cur_cell_info_;
  RowExtInfo row_ext_info_;
  uint64_t table_id_;
};

struct MemTableAttr {
  int64_t total_memlimit;
  //int64_t drop_page_num_once;
  //int64_t drop_sleep_interval_us;
  IExternMemTotal* extern_mem_total;
  MemTableAttr() : total_memlimit(0),
    //drop_page_num_once(0),
    //drop_sleep_interval_us(0),
    extern_mem_total(NULL) {
  };
};

class MemTable {
  static const char* MIN_STR;
  static const int64_t MAX_ROW_CELLINFO = 128;
  static const int64_t MAX_ROW_SIZE = (common::OB_MAX_PACKET_LENGTH - 512L * 1024L) / CELL_INFO_SIZE_UNIT;
  static const int64_t BLOOM_FILTER_NHASH = 1;
  static const int64_t BLOOM_FILTER_NBYTE = common::OB_MAX_PACKET_LENGTH - 1 * 1024;
 public:
  MemTable();
  ~MemTable();
 public:
#ifdef __UNIT_TEST__
  int init(int64_t hash_item_num, int64_t index_item_num);
#endif
  int init();
  int destroy();
 public:
  // 插入key-value对，如果已存在则覆盖
  // @param [in] key 待插入的key
  // @param [in] value 待插入的value
  int set(MemTableTransHandle& handle, ObUpsMutator& mutator, const bool check_checksum = false);
  int set(ObUpsMutator& mutator, const bool check_checksum);

  // 获取指定key的value
  // @param [in] key 要查询的key
  // @param [out] value 查询返回的value
  int get(MemTableTransHandle& handle,
          const uint64_t table_id, const common::ObString& row_key,
          MemTableIterator& iterator,
          bool& is_multi_update,
          ColumnFilter* column_filter = NULL);

  // 传入需要判断的get param
  // 传出用于请求chunkserver的get param
  bool need_query_sst(const uint64_t table_id, const common::ObString& row_key, bool dbsem);
  // 传入chunkserver返回的查询结果
  int set_row_exist(const uint64_t table_id, const common::ObString& row_key, bool exist);

  // 范围查询，返回一个iterator
  // @param [in] 查询范围的start key
  // @param [in] 查询范围是否包含start key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
  // @param [in] 查询范围的end key
  // @param [in] 查询范围是否包含end key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
  // @param [out] iter 查询结果的迭代器
  int scan(MemTableTransHandle& handle,
           const common::ObRange& range,
           const bool reverse,
           MemTableIterator& iter,
           ColumnFilter* column_filter = NULL);

  // 开始一次事务
  // 在btree实现时使用copy on write技术; 在hash实现时什么都不做 因此不保证事务
  // @param [in] type 事务类型, 读事务 or 写事务
  // @param [out] 事务handle
  int start_transaction(const MemTableTransType type,
                        MemTableTransHandle& handle);

  // 结束一次事务 对于写事务表示事务的提交
  // @param [in] 事务handle
  // @param [in] rollback 是否回滚事务
  int end_transaction(MemTableTransHandle& handle, bool rollback = false);

  // 开始一次事务性的更新
  // 一次mutation结束之前不能开启新的mutation
  int start_mutation(MemTableTransHandle& handle);

  // 结束一次mutation
  // @param[in] rollback 是否回滚
  int end_mutation(MemTableTransHandle& handle, bool rollback);

  // 建立索引
  int create_index();

  inline int64_t get_version() const {
    return version_;
  };
  inline void set_version(int64_t new_version) {
    atomic_exchange((uint64_t*)&version_, (uint64_t)new_version);
  };

  inline int64_t get_ref_cnt() const {
    return ref_cnt_;
  };
  inline int64_t inc_ref_cnt() {
    return atomic_inc((uint64_t*)&ref_cnt_);
  };
  inline int64_t dec_ref_cnt() {
    return atomic_dec((uint64_t*)&ref_cnt_);
  };

  int clear();

  inline int64_t total() const {
    return mem_tank_.total();
  };

  inline int64_t used() const {
    return mem_tank_.used();
  };

  inline void set_attr(const MemTableAttr& attr) {
    mem_tank_.set_total_limit(attr.total_memlimit);
    mem_tank_.set_extern_mem_total(attr.extern_mem_total);
  };

  inline void get_attr(MemTableAttr& attr) {
    attr.total_memlimit = mem_tank_.get_total_limit();
    attr.extern_mem_total = mem_tank_.get_extern_mem_total();
  };

  inline void log_memory_info() const {
    mem_tank_.log_info();
  };

  void dump2text(const common::ObString& dump_dir) {
    const int64_t BUFFER_SIZE = 1024;
    char buffer[BUFFER_SIZE];
    snprintf(buffer, BUFFER_SIZE, "%.*s/ups_memtable.ref_%ld.ver_%ld.pid_%d.tim_%ld",
             dump_dir.length(), dump_dir.ptr(),
             ref_cnt_, version_, getpid(), tbsys::CTimeUtil::getTime());
    table_engine_.dump2text(buffer);
  };

  int64_t size() {
    return table_engine_.size();
  };

  int get_bloomfilter(common::TableBloomFilter& table_bf) const;

  int scan_all_start(TableEngineTransHandle& trans_handle, TableEngineIterator& iter);
  int scan_all_end(TableEngineTransHandle& trans_handle);

 private:
  inline ObCellInfoNode* copy_cell_(const common::ObCellInfo& cell_info);
  //int copy_row_key_(const ObCellInfo &cell_info, TEKey &key);
  inline int db_sem_handler_(MemTableTransHandle& handle,
                             ObCellInfoNode* cell_info_node,
                             TEKey& cur_key,
                             TEValue& cur_value,
                             const TEKey& prev_key,
                             const TEValue& prev_value,
                             const int64_t op_type,
                             const int64_t mutate_timestamp);
  inline int ob_sem_handler_(MemTableTransHandle& handle,
                             ObCellInfoNode* cell_info_node,
                             TEKey& cur_key,
                             TEValue& cur_value,
                             const TEKey& prev_key,
                             const TEValue& prev_value,
                             const int64_t mutate_timestamp);
  inline int get_cur_value_(MemTableTransHandle& handle,
                            TEKey& cur_key,
                            const TEKey& prev_key,
                            const TEValue& prev_value,
                            TEValue& cur_value);
  inline int get_cur_value_(TEKey& cur_key,
                            const TEKey& prev_key,
                            const TEValue& prev_value,
                            TEValue& cur_value);

  inline static bool is_delete_row_(const common::ObObj& value) {
    return (value.get_ext() == common::ObActionFlag::OP_DEL_ROW);
  };
  inline static bool is_insert_(const int64_t op_type) {
    return (op_type == common::ObActionFlag::OP_INSERT);
  };
  inline static bool is_update_(const int64_t op_type) {
    return (op_type == common::ObActionFlag::OP_UPDATE);
  };

  inline static const common::ObString& get_start_key(const common::ObRange& range) {
    return range.start_key_;
  };
  inline static const common::ObString& get_end_key(const common::ObRange& range) {
    return range.end_key_;
  };
  inline static uint64_t get_table_id(const common::ObRange& range) {
    return range.table_id_;
  };
  inline static int get_start_exclude(const common::ObRange& range) {
    return range.border_flag_.inclusive_start() ? 0 : 1;
  };
  inline static int get_end_exclude(const common::ObRange& range) {
    return range.border_flag_.inclusive_end() ? 0 : 1;
  };
  inline static int get_min_key(const common::ObRange& range) {
    return range.border_flag_.is_min_value() ? 1 : 0;
  };
  inline static int get_max_key(const common::ObRange& range) {
    return range.border_flag_.is_max_value() ? 1 : 0;
  };

  inline void update_value_(const TEKey& key, ObCellInfoNode* cell_info_node, TEValue& value, const int64_t mutate_timestamp);

  void handle_checksum_error(ObUpsMutator& mutator);
 private:
  bool inited_;
  MemTank mem_tank_;
  TableEngine table_engine_;
  common::TableBloomFilter table_bf_;

  int64_t version_;
  int64_t ref_cnt_;

  int64_t checksum_before_mutate_;
  int64_t checksum_after_mutate_;
};
}
}

#endif //OCEANBASE_UPDATESERVER_MEMTABLE_H_



