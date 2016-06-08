/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_get_param.h for ...
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_GET_PARAM_H_
#define OCEANBASE_COMMON_GET_PARAM_H_

#include "ob_common_param.h"

namespace sb {
namespace common {
class ObGetParam : public ObReadParam {
  static const int64_t DEFAULT_CELLS_BUF_SIZE = OB_MAX_PACKET_LENGTH;

 public:
  struct ObRowIndex {
    int32_t offset_;  //index of the cell in cells list
    int32_t size_;    //cells count of this row
  };

 public:
  ObGetParam();

  /**
   * this constructor is only used for ups to get one row data
   * from transfer sstable. the cell info must be with column id
   * 0, table id and row key. user must ensure the cell info is
   * legal, otherwise it will fail to use this get param to get
   * row data from transfer sstable.
   */
  explicit ObGetParam(const ObCellInfo& cell_info);
  virtual ~ObGetParam();

  /**
   * add one cell into get param
   *
   * @param cell_info the cell to add
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR or OB_INVALID_ARGUMENT
   */
  int add_cell(const ObCellInfo& cell_info);

  /**
   * add only one cell in get parameter, this function is only be
   * used for ups to get one row
   *
   * @param cell_info the cell to add
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR or OB_INVALID_ARGUMENT
   */
  int add_only_one_cell(const ObCellInfo& cell_info);

  /**
   * get the number of cells in get parameter
   *
   * @return int64_t return the cells count
   */
  inline int64_t get_cell_size() const {
    return cell_size_;
  }

  /**
   * random access the cell in get parameter with index
   *
   * @param index the index of cell to access
   *
   * @return ObCellInfo* the current cell at this index
   */
  inline ObCellInfo* operator[](int64_t index) const {
    ObCellInfo* cell = NULL;
    if (index >= 0 && index < get_cell_size()) {
      cell = &cell_list_[index];
    }
    return cell;
  }

  /**
   * rollback the last row, this function is used by merge server
   * to ensure there is not part row in get parameter
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR
   */
  int rollback();

  /**
   * rollback the rows with cells count, this function will skip
   * the last "count" cells
   *
   * @param count cells count to skip at the end of get parameter
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR
   */
  int rollback(const int64_t count);

  /**
   * reset get parameter, easier to reuse it.
   */
  void reset();

  /**
   * get how mang rows in get parameter
   *
   * @return int64_t the rows count in get parameter
   */
  inline int64_t get_row_size() const {
    return row_size_;
  }

  /**
   * get the row index array in get parameter, just use for
   * chunkserver to optimize get by row
   *
   * @return const ObRowIndex* the row index array in get
   *         parameter
   */
  inline const ObRowIndex* get_row_index() const {
    return row_index_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

 private:
  /**
   * allocate memory for cell_list_ and row_index_ if necessary
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR
   */
  int init();

  /**
   * accorrding to the cell offset in cell list, find which row
   * the cell belong to, return the index in row index array, we
   * can ensure the row index array is in ascending order by cell
   * offset, so we use binary search
   *
   * @param cell_offset cell offset in cell list
   *
   * @return int64_t the row index which the cell belong to, if
   *         not found, return -1
   */
  int64_t find_row_index(const int64_t cell_offset) const;

  /**
   * check whether the table name, table id, column name and
   * column id of the specified cell are legal, and this function
   * will set the "id_only_" flag
   *
   * @param cell_info cell to check
   * @param is_first if it's the first cell in cell list
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR
   */
  int check_name_and_id(const ObCellInfo& cell_info, bool is_first);

  //common type serialize and deserialzie helper
  int serialize_flag(char* buf, const int64_t buf_len, int64_t& pos,
                     const int64_t flag) const;
  int serialize_int(char* buf, const int64_t buf_len, int64_t& pos,
                    const int64_t value) const;
  int deserialize_int(const char* buf, const int64_t buf_len, int64_t& pos,
                      int64_t& value) const;
  int64_t get_obj_serialize_size(const int64_t value, bool is_ext) const;
  int64_t get_obj_serialize_size(const ObString& str) const;

  //name or id serialize, deserialize helper
  int serialize_name_or_id(char* buf, const int64_t buf_len,
                           int64_t& pos, const ObString name,
                           const uint64_t id, bool is_field) const;
  int deserialize_name_or_id(const char* buf, const int64_t data_len,
                             int64_t& pos, ObString& name,
                             uint64_t& id);
  int64_t get_serialize_name_or_id_size(const ObString name,
                                        const uint64_t id,
                                        bool is_field) const;

  //serialize and deserialize table info(table field, table name or table id)
  int serialize_table_info(char* buf, const int64_t buf_len,
                           int64_t& pos, const ObString table_name,
                           const uint64_t table_id) const;
  int deserialize_table_info(const char* buf, const int64_t buf_len,
                             int64_t& pos, ObString& table_name,
                             uint64_t& table_id);
  int64_t get_serialize_table_info_size(const ObString table_name,
                                        const uint64_t table_id) const;

  //serialize column info(column name or column id)
  int serialize_column_info(char* buf, const int64_t buf_len,
                            int64_t& pos, const ObString column_name,
                            const uint64_t column_id) const;
  int64_t get_serialize_column_info_size(const ObString column_name,
                                         const uint64_t column_id) const;

  //serialize, deserialize row key
  int serialize_rowkey(char* buf, const int64_t buf_len,
                       int64_t& pos, const ObString row_key) const;
  int deserialize_rowkey(const char* buf, const int64_t data_len,
                         int64_t& pos, ObString& row_key);
  int64_t get_serialize_rowkey_size(const ObString row_key) const;

  //basic parameter serialize and deserialize
  int serialize_basic_field(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_basic_field(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_basic_field_serialize_size(void) const;

  //cells serialize and deserialize
  bool is_table_change(ObCellInfo& cell, ObString& table_name,
                       uint64_t& table_id) const;
  bool is_rowkey_change(ObCellInfo& cell, ObString& row_key) const;
  int serialize_cells(char* buf, const int64_t buf_len, int64_t& pos) const;
  int64_t get_cells_serialize_size(void) const;

 private:
  DISALLOW_COPY_AND_ASSIGN(ObGetParam);

  bool single_cell_;
  ObCellInfo fixed_cell_;
  ObRowIndex fixed_row_index_;

  ObCellInfo* cell_list_;   //cell array to store all cells
  int64_t cell_size_;       //cell count in cell list
  int64_t cell_capacity_;   //cell list capacity

  ObString prev_table_name_;//previous table name, for check if row key change
  uint64_t prev_table_id_;  //previous table id, for check if row key change
  ObString prev_rowkey_;    //previous row key, for check if row key change
  ObRowIndex* row_index_;   //row index, for index cell list by row
  int32_t row_size_;        //how many row item in row index
  bool id_only_;            //whether only id(table id, column id) in cell
};
} /* common */
} /* sb */

#endif /* end of include guard: OCEANBASE_COMMON_GET_PARAM_H_ */

