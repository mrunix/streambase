/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scanner.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_SCANNER_H_
#define OCEANBASE_COMMON_OB_SCANNER_H_

#include "ob_define.h"
#include "ob_read_common_data.h"
#include "ob_iterator.h"
#include "ob_string_buf.h"
#include "ob_object.h"

namespace sb {
namespace common {
class ObScanner : public ObIterator {
  static const int64_t DEFAULT_MAX_SERIALIZE_SIZE = OB_MAX_PACKET_LENGTH - 1024;
  static const int64_t DEFAULT_MEMBLOCK_SIZE = 512 * 1024;

  enum ID_NAME_STATUS {
    UNKNOWN = 0,
    ID = 1,
    NAME = 2
  };

  struct MemBlock {
    int64_t memory_size;
    int64_t rollback_pos;
    int64_t cur_pos;
    MemBlock* next;
    char memory[0];
    explicit MemBlock(const int64_t size) : memory_size(size), next(NULL) {
      reset();
    };
    void reset() {
      rollback_pos = 0;
      cur_pos = 0;
    };
  };
 public:
  /// only column name/id and value is stored, when table name/id or row key are the same.
  /// when iterating through stored table, current table name/id and row key should be recorded.
  class TableReader {
   public:
    TableReader();
    virtual ~TableReader();

    /// @brief get a complete ObCellInfo from the buffer
    /// iterate a series of cells, store the current table and row
    /// reconstruct a complete ObCellInfo with the current column and the table and row info
    /// @param [out] cell_info output complete ObCellInfo
    /// @param [out] last_obj the last read ObObj
    int read_cell(const char* buf, const int64_t data_len, int64_t& pos,
                  ObCellInfo& cell_info, ObObj& last_obj);

    /// @brief get current table name
    /// namely the last read table name
    inline const ObString& get_cur_table_name() const {
      return cur_table_name_;
    }

    /// @brief get current table id
    /// namely the last read table id
    inline uint64_t get_cur_table_id() const {
      return cur_table_id_;
    }

    /// @brief get current row key
    /// namely the last read row key
    inline const ObString& get_cur_row_key() const {
      return cur_row_key_;
    }

    inline bool is_row_changed() const {
      return is_row_changed_;
    }

   private:
    ObString cur_table_name_;
    uint64_t cur_table_id_;
    ObString cur_row_key_;
    int64_t id_name_type_;
    bool is_row_changed_;
  };

  class Iterator {
   public:
    Iterator();
    ~Iterator();
    Iterator(const Iterator& other);
    Iterator(const ObScanner* scanner, const ObScanner::MemBlock* mem_block,
             int64_t size_counter = 0, int64_t cur_pos = 0);
   public:
    int get_cell(ObCellInfo& cell_info);
    int get_cell(ObCellInfo** cell_info, bool* is_row_changed = NULL);
    Iterator& operator ++ ();
    Iterator operator ++ (int);
    bool operator == (const Iterator& other) const;
    bool operator != (const Iterator& other) const;
    Iterator& operator= (const Iterator& other);
   private:
    bool is_iterator_end_();
    void update_cur_mem_block_();
    int get_cell_(ObCellInfo& cell_info, int64_t& next_pos);
    int get_cell_(const char* data, const int64_t data_len, const int64_t cur_pos,
                  ObCellInfo& cell_info, int64_t& next_pos);
   private:
    friend class ObScanner;
    const ObScanner* scanner_;
    const ObScanner::MemBlock* cur_mem_block_;
    int64_t cur_pos_;
    int64_t next_pos_;
    int64_t iter_size_counter_;
    TableReader reader_;
    ObCellInfo cur_cell_info_;
    bool new_row_cell_;
    ObObj last_obj_;
  };

 public:
  ObScanner();
  virtual ~ObScanner();

  void reset();
  void clear();

  int64_t set_mem_size_limit(const int64_t limit);

  /// @brief add a cell in the ObScanner
  /// @retval OB_SUCCESS successful
  /// @retval OB_SIZE_OVERFLOW memory limit is exceeded if add this cell
  /// @retval otherwise error
  int add_cell(const ObCellInfo& cell_info);

  int rollback();

  NEED_SERIALIZE_AND_DESERIALIZE;

 private:
  /// append a new cell, the cell is serialized into inner memory buffer
  /// after serialize this cell, check mem_size_limit, return OB_SIZE_OVERFLOW when exceeded
  int append_serialize_(const ObCellInfo& cell_info);

  int serialize_table_name_or_id_(char* buf, const int64_t buf_len, int64_t& pos,
                                  const ObCellInfo& cell_info);

  int serialize_row_key_(char* buf, const int64_t buf_len, int64_t& pos,
                         const ObCellInfo& cell_info);

  int serialize_column_(char* buf, const int64_t buf_len, int64_t& pos,
                        const ObCellInfo& cell_info);

  /// alocate a new memblock to store ObCellInfo
  int get_memblock_(const int64_t size);

  /// when deserialization, iterate all ObCellInfo and get the size
  int64_t get_valid_data_size_(const char* data, const int64_t data_len, int64_t& pos, ObObj& last_obj);

  int deserialize_basic_(const char* buf, const int64_t data_len, int64_t& pos, ObObj& last_obj);

  int deserialize_table_(const char* buf, const int64_t data_len, int64_t& pos, ObObj& last_obj);

  static int deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
                              int64_t& value, ObObj& last_obj);

  static int deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                  ObString& value, ObObj& last_obj);

  static int deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                         int64_t& int_value, ObString& varchar_value, ObObj& last_obj);

 public:
  Iterator begin() const;
  Iterator end() const;
 public:
  int reset_iter();

  /// @retval OB_ITER_END iterate end
  /// @retval OB_SUCCESS go to the next cell succ
  int next_cell();

  int get_cell(sb::common::ObCellInfo** cell);
  int get_cell(sb::common::ObCellInfo** cell, bool* is_row_changed);

  bool is_empty() const;

  /// after deserialization, get_last_row_key can retreive the last row key of this scanner
  /// @param [out] row_key the last row key
  int get_last_row_key(ObString& row_key) const;

 public:

  /// indicating the request was fullfiled, must be setted by cs and ups
  /// @param [in] is_fullfilled  缓冲区不足导致scanner中只包含部分结果的时候设置为false
  /// @param [in] fullfilled_item_num -# when getting, this means processed position in GetParam
  ///                                 -# when scanning, this means row number fullfilled
  int set_is_req_fullfilled(const bool& is_fullfilled, const int64_t fullfilled_item_num);
  int get_is_req_fullfilled(bool& is_fullfilled, int64_t& fullfilled_item_num) const;

  /// when response scan request, range must be setted
  /// the range is the seviced range of this tablet or (min, max) in updateserver
  /// @param [in] range
  int set_range(const ObRange& range);
  /// same as above but do not deep copy keys of %range, just set the reference.
  /// caller ensures keys of %range remains reachable until ObScanner serialized.
  int set_range_shallow_copy(const ObRange& range);
  int get_range(ObRange& range) const;

  inline void set_data_version(const int64_t version) {
    data_version_ = version;
  }

  inline int64_t get_data_version() const {
    return data_version_;
  }

  inline int64_t get_size() const {
    return cur_size_counter_;
  }
 private:
  MemBlock* head_memblock_;
  MemBlock* cur_memblock_;
  MemBlock* rollback_memblock_;
  int64_t cur_size_counter_;
  int64_t rollback_size_counter_;
  ObString cur_table_name_;
  uint64_t cur_table_id_;
  ObString cur_row_key_;
  ObString tmp_table_name_;
  uint64_t tmp_table_id_;
  ObString tmp_row_key_;
  bool has_row_key_flag_;
  int64_t mem_size_limit_;
  Iterator iter_;
  bool first_next_;
  ObStringBuf string_buf_;
  ObRange range_;
  int64_t data_version_;
  bool has_range_;
  bool is_request_fullfilled_;
  int64_t  fullfilled_item_num_;
  int64_t id_name_type_;

  ObString last_row_key_;
};
typedef ObScanner::Iterator ObScannerIterator;
} /* common */
} /* sb */

#endif /* end of include guard: OCEANBASE_COMMON_OB_SCANNER_H_ */

