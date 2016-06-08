/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cell_array.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OB_CELL_ARRAY_H_
#define OB_CELL_ARRAY_H_

#include "ob_link.h"
#include "ob_memory_pool.h"
#include "page_arena.h"
#include "ob_iterator.h"
#include "ob_vector.h"
#include "ob_string.h"

namespace sb {
namespace common {
template <>
struct ob_vector_traits<int64_t> {
 public:
  typedef int64_t& pointee_type;
  typedef int64_t value_type;
  typedef const int64_t const_value_type;
  typedef value_type* iterator;
  typedef const value_type* const_iterator;
  typedef int32_t difference_type;
};

/// @class  ObCellInfo vector, can access by offset
/// @author wushi(wushi.ly@taobao.com)  (9/16/2010)
class ObCellArray : public ObIterator {
 public:
  friend class iterator;
  /// @fn constructor
  ObCellArray();
  /// @fn destructor
  virtual ~ObCellArray();
  /// @fn append a cell into the array, the whole cell whill be copied
  int append(const ObCellInfo& cell, ObCellInfo*& cell_out);
  /// @fn expand the array
  int expand(int32_t expanding_size);
  /// @fn apply changes to a given cell
  int apply(const ObCellInfo& cell, const int64_t offset, ObCellInfo*& cell_out);
  int apply(const ObCellInfo& src_cell, ObCellInfo*& affected_cell,
            const int64_t pre_cell_offset = -1);

  /// @fn get a specific cell in this array, there will be no reference number
  int get_cell(const int64_t offset, ObCellInfo*& cell) const;
  /// @fn get cell according to operator []
  ObCellInfo& operator[](int64_t offset);
  const ObCellInfo& operator[](int64_t offset)const;
  /// @fn get number of cell in the array
  int64_t get_cell_size()const;
  /// @fn clear all infomation and cell stored in the cell
  void clear();
  void reset();
  /// @fn get memory size used by this array
  int64_t get_memory_size_used();

 public:
  class iterator {
   public:
    iterator();
    ~iterator();

    iterator& operator ++();
    iterator operator ++(int);
    iterator operator +(int64_t inc_num);
    ObCellInfo& operator*();
    ObCellInfo* operator->();
    bool operator !=(const ObCellArray::iterator& other);
   private:
    friend class ObCellArray;
    void set_args(ObCellArray& cell_array, int64_t offset);
    ObCellArray* array_;
    int64_t cur_offset_;
    ObCellInfo cell_ugly_used_for_empty_iterator_;
  };
  iterator begin();
  iterator end();

 public:
  /// @struct  describe each cell order
  struct OrderDesc {
    int32_t cell_idx_;
    int32_t order_;
  };
  /// @fn order all rows, number of cell in each row is identified by row_width,
  /// @note only affect output of member functions of ObIterator
  /// orderby and reverse_rows should be only called once
  int orderby(int64_t row_width, OrderDesc* order_desc, int64_t desc_size);
  /// reverse rows, meanse orderby rowkey in "desc order"
  int reverse_rows(const int64_t row_width_in);
  /// @fn limit the output
  /// @param offset only out put row in range [offset, offset+count), offset begin with 0
  /// @param count the row count, 0 means unlimited
  /// @note if need order the output, one should call orderby first, and row_width
  ///   must be consistent, this class's implementation will not check these rules
  /// @note only affect output of member functions of ObIterator
  int limit(int64_t offset, int64_t count, int32_t row_width);

 public:
  void reset_iterator();
  virtual int next_cell();
  virtual int get_cell(ObCellInfo** cell);
  virtual int get_cell(ObCellInfo** cell, bool* is_row_changed);
  void consume_all_cell();
  int  unget_cell();
  inline int64_t get_consumed_cell_num()const {
    return (consumed_row_num_ * row_width_ + cur_row_consumed_cell_num_ + 1);
  }

 private:
  class RowComp {
   public:
    /// @fn constructor
    RowComp(OrderDesc* order_desc, int32_t order_column_num,
            const ObCellArray& cell_array);
    virtual ~RowComp();
    /// @fn compare tow cell
    bool operator()(int64_t off1, int64_t off2);
   private:
    OrderDesc* desc_;
    int32_t   desc_size_;
    const ObCellArray* cell_array_;
  };
  /// @fn copy a obj
  int copy_obj_(ObObj& dst, const ObObj& src);
  /// @fn copy a cell
  int copy_cell_(ObCellInfo& dst, const ObCellInfo& src,
                 const int64_t prev_cell_idx);
  /// @fn initialize all properties
  void initialize_();
  /// @property ObVarMemPool block size;
  static const int64_t VAR_MEMPOOL_BLOCK_SIZE = 64 * 1024;
  /// @property allocate CELL_BLOCK_CAPACITY
  static const int64_t CELL_BLOCK_CAPACITY = 1024;
  static const int64_t CELL_BLOCK_SHIF_BITS = 10;
  static const int64_t CELL_IN_BLOCK_OFFSET_AND_VAL = 1023;
  /// @property cell block array size
  static const int64_t CELL_BLOCK_ARRAY_SIZE = 1025;
  /// @property number of cell can be accessed in O(1)
  static const int64_t O1_ACCESS_CELL_NUM = CELL_BLOCK_CAPACITY * CELL_BLOCK_ARRAY_SIZE;
  /// @struct  every time ObCellArray allocated a CellBlock from memory pool
  struct CellBlock {
    ObDLink      cell_block_link_;
    ObCellInfo   cell_array_[CELL_BLOCK_CAPACITY];
  };
  /// @property we first put CellBlock into this array, only when this array is full,
  ///   we use CellBlock::cell_block_link_
  ///   CELL_BLOCK_CAPACITY*CELL_BLOCK_ARRAY_SIZE = 1M, which can satisfy most query
  CellBlock*   cell_block_array_[CELL_BLOCK_ARRAY_SIZE];
  /// @property where to put the new allocated CellBlock
  int32_t     next_block_array_slot_;
  /// @property list of all CellBlock
  ObDLink     cell_block_list_;
  /// @property the number of cell in this ObCellArray
  int64_t     cell_num_;
  /// @property current cell block
  CellBlock*   current_block_;
  /// @property cell number in the current cell block
  int64_t     current_block_cell_num_;
  /// @property allocate memory for vary sized object, like table name
  PageArena<char, ModulePageAllocator> page_arena_;
  /// @property the number of cell consumed by iterator
  int64_t     consumed_row_num_;
  int32_t     cur_row_consumed_cell_num_;
  ObString prev_key_;
  uint64_t    prev_tableid_;
  bool cur_cell_row_changed_;

  /// order by and limit
  /// @property number of columns need concern in orderby
  int32_t orderby_column_num_;
  /// @property order by description
  OrderDesc orderby_columns_[OB_MAX_COLUMN_NUMBER];
  /// @property begin offsets of each row
  ObVector<int64_t> sorted_row_offsets_;
  /// @property number of row
  int64_t row_num_;
  /// @property width of each row
  int64_t row_width_;
  /// @property limited cell number
  int64_t limit_cell_num_;
  /// @property ugly usage, because no exception can be used
  ObCellInfo cell_ugly_used_for_array_random_access_;
  ObCellInfo empty_cell_;
  /// @property memory size allocated
  int64_t allocated_memory_size_;
};
}
}

#endif /* OB_CELL_ARRAY_H_ */


