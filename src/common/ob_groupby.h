/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_groupby.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_GROUPBY_H_
#define OCEANBASE_COMMON_OB_GROUPBY_H_
#include "ob_string.h"
#include "ob_object.h"
#include "ob_string_buf.h"
#include "ob_cell_array.h"
#include "ob_array_helper.h"
#include <vector>
namespace sb {
namespace common {
/// we do not implement avg, because avg can be calculated by client using SUM and COUNT
typedef enum {
  AGG_FUNC_MIN,
  SUM,
  COUNT,
  MAX,
  MIN,
  AGG_FUNC_END
} ObAggregateFuncType;
/// aggregate column
class ObAggregateColumn {
 public:
  ObAggregateColumn();
  ObAggregateColumn(sb::common::ObString& org_column_name, sb::common::ObString& as_column_name,
                    const int64_t as_column_idx, const ObAggregateFuncType& func_type);
  ObAggregateColumn(const int64_t org_column_idx, const int64_t as_column_idx, const ObAggregateFuncType& func_type);
  ~ObAggregateColumn();

  /// accessor members
  inline int64_t get_org_column_idx()const {
    return org_column_idx_;
  }
  inline int64_t get_as_column_idx()const {
    return as_column_idx_;
  }
  inline ObString get_org_column_name()const {
    return org_column_name_;
  }
  inline ObString get_as_column_name()const {
    return as_column_name_;
  }
  inline ObAggregateFuncType get_func_type()const {
    return func_type_;
  }

  /// calculage aggregate value
  int calc_aggregate_val(sb::common::ObObj& aggregated_val, const sb::common::ObObj& new_val)const;

  bool operator==(const ObAggregateColumn& other)const;

  int get_first_aggregate_obj(const sb::common::ObObj& first_obj,
                              sb::common::ObObj& agg_obj)const;
  int init_aggregate_obj(sb::common::ObObj& agg_obj)const;
 private:
  sb::common::ObString org_column_name_;
  sb::common::ObString as_column_name_;
  int64_t                     org_column_idx_;
  int64_t                     as_column_idx_;
  ObAggregateFuncType         func_type_;
};

class ObGroupByParam;
/// key of group decided by groupby columns
class ObGroupKey {
 public:
  enum {
    INVALID_KEY_TYPE,
    ORG_KEY,
    AGG_KEY
  };
  ObGroupKey();
  ~ObGroupKey();
  /// init key
  int init(const ObCellArray& cell_array, const ObGroupByParam& param,
           const int64_t row_beg, const int64_t row_end, const int32_t type);
  /// get hash value of the key
  inline uint64_t hash() const {
    return static_cast<uint64_t>(hash_val_);
  }
  /// check if two key is equal
  bool operator ==(const ObGroupKey& other)const;

  inline int get_key_type()const {
    return key_type_;
  }
  inline int64_t get_row_beg() const {
    return row_beg_;
  }
  inline int64_t get_row_end() const {
    return row_end_;
  }
  inline const ObGroupByParam* get_groupby_param() const {
    return group_by_param_;
  }
  inline const sb::common::ObCellArray* get_cell_array() const {
    return cell_array_;
  }

  inline uint32_t get_hash_val() const {
    return hash_val_;
  }

 private:
  void initialize();
  uint32_t hash_val_;
  int32_t  key_type_;
  const ObGroupByParam* group_by_param_;
  const sb::common::ObCellArray* cell_array_;
  int64_t row_beg_;
  int64_t row_end_;
};

/// groupby parameter
class ObGroupByParam {
 public:
  ObGroupByParam();
  ~ObGroupByParam();
  void clear();

  /// groupby columns were used to divide groups, if column_name is NULL, means all result is a single group, e.x. count(*)
  int add_groupby_column(const sb::common::ObString& column_name);
  /// column_idx == -1 means all result is a single group, e.x. count(*)
  int add_groupby_column(const int64_t column_idx);

  /// the return columns' values of a group were only decided by first row fetched belong to this group
  int add_return_column(const sb::common::ObString& column_name);
  int add_return_column(const int64_t column_idx);

  /// add an aggregate column
  static sb::common::ObString COUNT_ROWS_COLUMN_NAME;
  int add_aggregate_column(const sb::common::ObString& org_column_name,
                           const sb::common::ObString& as_column_name, const ObAggregateFuncType  aggregate_func);
  int add_aggregate_column(const int64_t org_column_idx, const ObAggregateFuncType  aggregate_func);

  /// calculate the group key of a row, each group decided by groupby columns has a uniq group key,
  /// group key was composited by all column values of grouby columns,
  /// if groupby columns indicate that this param is something like count(*), this function will give same result for all rows
  static int calc_group_key_hash_val(const ObGroupKey& key, uint32_t& val);
  static bool is_equal(const ObGroupKey& left, const ObGroupKey& right);

  /// calculate aggregate result
  /// org_cells [in] result after merge & join  [org_row_beg, org_row_end]
  /// aggregate_cells [in/out] aggregate result row of current group [aggregate_row_beg,aggregate_row_end]
  int aggregate(const sb::common::ObCellArray& org_cells,  const int64_t org_row_beg,
                const int64_t org_row_end, sb::common::ObCellArray& aggregate_cells,
                const int64_t aggregate_row_beg, const int64_t aggregate_row_end)const;

  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size(void) const;

  bool operator == (const ObGroupByParam& other)const;

  inline int64_t get_aggregate_row_width()const {
    return column_num_;
  }

  struct ColumnInfo {
    sb::common::ObString column_name_;
    int64_t                     org_column_idx_;
    int64_t                     as_column_idx_;
    ColumnInfo() {
      org_column_idx_ = -1;
      as_column_idx_ = -1;
    }
    bool operator == (const ColumnInfo& other) const {
      return (column_name_ == other.column_name_
              && org_column_idx_ == other.org_column_idx_
              && as_column_idx_ == other.as_column_idx_);
    }
  };

  inline const sb::common::ObArrayHelper<ColumnInfo>& get_groupby_columns(void)const {
    return group_by_columns_;
  }

  inline const sb::common::ObArrayHelper<ColumnInfo>& get_return_columns(void)const {
    return return_columns_;
  }

  inline const sb::common::ObArrayHelper<ObAggregateColumn>& get_aggregate_columns(void)const {
    return aggregate_columns_;
  }

  int64_t find_column(const sb::common::ObString& column_name)const;

  int  get_aggregate_column_name(const int64_t column_idx, ObString& column_name)const;
 private:
  int64_t serialize_helper(char* buf, const int64_t buf_len, int64_t& pos) const;
  int64_t groupby_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t& pos) const;
  int64_t return_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t& pos) const;
  int64_t aggregate_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_groupby_columns(const char* buf, const int64_t buf_len, int64_t& pos);
  int deserialize_return_columns(const char* buf, const int64_t buf_len, int64_t& pos);
  int deserialize_aggregate_columns(const char* buf, const int64_t buf_len, int64_t& pos);

  int calc_org_group_key_hash_val(const ObCellArray& cells, const int64_t row_beg, const int64_t row_end, uint32_t& val)const;
  int calc_agg_group_key_hash_val(const ObCellArray& cells, const int64_t row_beg, const int64_t row_end, uint32_t& val)const;

  int64_t get_target_cell_idx(const ObGroupKey& key, const int64_t groupby_idx)const;


  mutable bool using_id_;
  mutable bool using_name_;
  sb::common::ObStringBuf  buffer_;
  int64_t                         column_num_;
  ///由于当前ObVector不支持重用分配的内存，代码里先使用stl::vector，郁白同学会实现支持内存重复使用的vector
  /// @todo (wushi wushi.ly@taobao.com) replace stl vector with yubai's vector
  ColumnInfo                      group_by_columns_buf_[OB_MAX_COLUMN_NUMBER];
  ObArrayHelper<ColumnInfo>       group_by_columns_;
  ColumnInfo                      return_columns_buf_[OB_MAX_COLUMN_NUMBER];
  ObArrayHelper<ColumnInfo>       return_columns_;
  ObAggregateColumn               aggregate_columns_buf_[OB_MAX_COLUMN_NUMBER];
  ObArrayHelper<ObAggregateColumn> aggregate_columns_;
};
}
}
#endif /* OCEANBASE_COMMON_OB_GROUPBY_H_ */

