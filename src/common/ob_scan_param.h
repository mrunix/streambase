/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scan_param.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_SCAN_PARAM_H_
#define OCEANBASE_COMMON_SCAN_PARAM_H_

#include "ob_define.h"
#include "ob_array_helper.h"
#include "ob_object.h"
#include "ob_range.h"
#include "ob_simple_filter.h"
#include "ob_common_param.h"
#include "ob_groupby.h"

namespace sb {
namespace common {
class ObScanParam : public ObReadParam {
 public:
  enum Order {
    ASC = 1,
    DESC
  };

  enum Direction {
    FORWARD = 0,
    BACKWARD = 1,
  };

  enum ReadMode {
    SYNCREAD = 0,
    PREREAD = 1,
  };

  struct ScanFlag {
    ScanFlag()
      : scan_direction_(FORWARD), read_mode_(PREREAD), reserved_(0) {}
    ScanFlag(const Direction dir, const ReadMode mode)
      : scan_direction_(dir), read_mode_(mode), reserved_(0) {}
    int64_t scan_direction_ : 4;
    int64_t read_mode_ : 4;
    int64_t reserved_ : 56;
  };

  ObScanParam();
  virtual ~ObScanParam();

  void set(const uint64_t& table_id, const ObString& table_name, const ObRange& range);

  int add_column(const ObString& column_name);
  int add_column(const uint64_t& column_id);
  void clear_column(void);

  inline void set_scan_size(const int64_t scan_size) { scan_size_ = scan_size; }
  inline void set_scan_direction(const Direction scan_dir) { scan_flag_.scan_direction_ = scan_dir; }
  inline void set_read_mode(const ReadMode mode) { scan_flag_.read_mode_ = mode; }
  inline void set_scan_flag(const ScanFlag flag) { scan_flag_ = flag; }

  inline uint64_t get_table_id() const { return table_id_; }
  inline const ObString get_table_name() const { return table_name_; }
  inline const ObRange* get_range() const { return &range_; }
  inline ScanFlag get_scan_flag()const {return scan_flag_; }
  inline int64_t get_scan_size() const { return scan_size_; }
  inline Direction get_scan_direction() const {return (Direction)scan_flag_.scan_direction_; }
  inline ReadMode get_read_mode() const { return (ReadMode)scan_flag_.read_mode_; }
  inline int64_t get_column_name_size() const { return column_list_.get_array_index(); }
  inline int64_t get_column_id_size() const { return column_id_list_.get_array_index(); }
  inline const ObString* get_column_name() const { return column_names_; }
  inline const uint64_t* get_column_id() const { return column_ids_; }

  /// member functions about groupby
  /// groupby columns were used to divide groups, if no group by columns,
  /// means all result is a single group, e.x. count(*)
  int add_groupby_column(const sb::common::ObString& column_name);
  int add_groupby_column(const int64_t column_idx);

  /// the return columns' values of a group were only decided by first row fetched belong to this group
  int add_return_column(const sb::common::ObString& column_name);
  int add_return_column(const int64_t column_idx);

  /// add an aggregate column
  int add_aggregate_column(const sb::common::ObString& org_column_name,
                           const sb::common::ObString& as_column_name,
                           const sb::common::ObAggregateFuncType  aggregate_func);
  int add_aggregate_column(const int64_t org_column_idx, const sb::common::ObAggregateFuncType  aggregate_func);

  const ObGroupByParam& get_group_by_param()const;
  ObGroupByParam& get_group_by_param();

  /// set and get condition filter
  int add_cond(const ObString& column_name, const ObLogicOperator& cond_op, const ObObj& cond_value);
  const ObSimpleFilter& get_filter_info(void)const;
  ObSimpleFilter& get_filter_info(void);

  /// set and get order by information
  int add_orderby_column(const ObString& column_name, Order order = ASC);
  int add_orderby_column(const int64_t column_idx, Order order = ASC);
  int64_t get_orderby_column_size()const;
  void get_orderby_column(ObString const*& names, uint8_t  const*& orders,
                          int64_t& column_size)const;
  void get_orderby_column(int64_t const*& column_idxs, uint8_t const*& orders,
                          int64_t& column_size)const;

  /// set and get limit information
  int set_limit_info(const int64_t offset, const int64_t count);
  void get_limit_info(int64_t& offset, int64_t& count) const;

  void reset(void);

  /// safe copy the array data and init the pointer to itself data
  /// warning: only include some basic info and column info
  /// not ensure the advanced info copy safely
  void safe_copy(const ObScanParam& other);

  NEED_SERIALIZE_AND_DESERIALIZE;

 private:
  // BASIC_PARAM_FIELD
  int serialize_basic_param(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_basic_param_serialize_size(void) const;

  // COLUMN_PARAM_FIELD
  int serialize_column_param(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_column_param(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_column_param_serialize_size(void) const;

  // FILTER_PARAM_FILED
  int serialize_filter_param(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_filter_param(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_filter_param_serialize_size(void) const;

  // GROUPBY_PARAM_FILED
  int serialize_groupby_param(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_groupby_param(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_groupby_param_serialize_size(void) const;

  // SORT_PARAM_FIELD
  int serialize_sort_param(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_sort_param(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_sort_param_serialize_size(void) const;

  // LIMIT_PARAM_FIELD
  int serialize_limit_param(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_limit_param(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_limit_param_serialize_size(void) const;

  // END_PARAM_FIELD
  int serialize_end_param(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_end_param(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_end_param_serialize_size(void) const;

 private:
  uint64_t table_id_;
  ObString table_name_;
  ObRange range_;
  int64_t scan_size_;
  ScanFlag scan_flag_;
  ObString column_names_[OB_MAX_COLUMN_NUMBER];
  ObArrayHelper<ObString> column_list_;
  /// 接口中所有add_...idx的时候，其中的idx是引用column_ids_或者groupby_param_中id数组的下标
  uint64_t column_ids_[OB_MAX_COLUMN_NUMBER];
  ObArrayHelper<uint64_t> column_id_list_;

  int64_t limit_offset_;
  /// @property 0 means not limit
  int64_t limit_count_;

  /// advanced property
  // orderby and filter and groupby
  ObString orderby_column_names_[OB_MAX_COLUMN_NUMBER];
  ObArrayHelper<ObString> orderby_column_name_list_;
  int64_t orderby_column_ids_[OB_MAX_COLUMN_NUMBER];
  ObArrayHelper<int64_t> orderby_column_id_list_;
  uint8_t orderby_orders_[OB_MAX_COLUMN_NUMBER];
  ObArrayHelper<uint8_t> orderby_order_list_;
  ObSimpleFilter condition_filter_;
  ObGroupByParam group_by_param_;
};

} /* common */
} /* sb */

#endif /* end of include guard: OCEANBASE_COMMON_SCAN_PARAM_H_ */


