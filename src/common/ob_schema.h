/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema.h for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *   maoqi <maoqi@taobao.com>
 *   fangji <fangji.hcm@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_SCHEMA_H_
#define OCEANBASE_COMMON_OB_SCHEMA_H_
#include <stdint.h>

#include <tbsys.h>

#include "ob_define.h"
#include "ob_object.h"
#include "ob_string.h"
#include "hash/ob_hashutils.h"
#include "hash/ob_hashmap.h"

namespace tbsys {
class CConfig;
}

namespace sb {
namespace common {
//these classes are so close in logical, so I put them together to make client have a easy life
typedef ObObjType ColumnType;

class BaseInited {
 public:
  BaseInited();
  virtual ~BaseInited();
  void set_flag();
  bool have_inited() const;
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
 private:
  bool inited_;
};

class ObColumnSchema : public BaseInited {
 public:

  ObColumnSchema();
  bool init(const bool maintained, const uint64_t id, const char* name, const ColumnType& type, const int64_t size);
  virtual ~ObColumnSchema();

  uint64_t get_id() const;
  const char* get_name() const;
  ColumnType get_type() const;
  int64_t get_size() const;
  bool is_maintained() const;
  //this is for test
  void print_info() const;

  static ColumnType convert_str_to_column_type(const char* str);
  bool operator < (const ObColumnSchema& rv) const;
  bool operator < (const char* name) const;

  NEED_SERIALIZE_AND_DESERIALIZE;
 private:
  bool maintained_;
  uint64_t id_;
  int64_t size_;  //only used when type is char or varchar
  ColumnType type_;
  char name_[OB_MAX_COLUMN_NAME_LENGTH];
};

class ObJoinInfo : public BaseInited {
 public:
  ObJoinInfo();
  bool init(const uint64_t left_column, const int32_t start_pos, const int32_t end_pos, const uint64_t table_id_joined);
  virtual ~ObJoinInfo();

  //if you got start_pos = end_pos = -1, that means you should use the whole rowkey
  void get_rowkey_join_range(int32_t& out_start_pos, int32_t& out_end_pos) const;

  void add_correlated_column(const uint64_t left_column_id, const uint64_t right_column_id);

  uint64_t get_table_id_joined() const;

  uint64_t find_left_column_id(const uint64_t rid) const;
  uint64_t find_right_column_id(const uint64_t lid) const;
  NEED_SERIALIZE_AND_DESERIALIZE;

  //this is for test
  void print_info() const;

 private:
  uint64_t left_column_id_; //for the first version, this one always is 1, means rowkey only
  int32_t  start_pos_;
  int32_t  end_pos_;  //this means which part of left_column_ to be used make rowkey
  uint64_t table_id_joined_;

  uint64_t correlated_left_columns[OB_OLD_MAX_COLUMN_NUMBER];
  uint64_t correlated_right_columns[OB_OLD_MAX_COLUMN_NUMBER];
  // 3,4 means this table column 3 will be
  //corrected by table_id_joined_ column 4
  int32_t correlated_info_size_;
};

class ObOperator;
class ObSchemaManager;
class ObSchemaManagerV2;

int schema_1_to_2(ObSchemaManager& v1, ObSchemaManagerV2& v2);

class ObSchema : public BaseInited {
 public:
  enum TableType {
    INVALID = 0,
    SSTABLE_IN_DISK,
    SSTABLE_IN_RAM,
  };

  ObSchema();
  bool init(const uint64_t table_id, const uint64_t max_column_id, const char* name,
            const TableType table_type, const int32_t rowkey_split, const int32_t rowkye_max_length,
            const char* compress_func_name, const int32_t block_size, const int32_t is_use_bloomfilter,
            const int32_t is_row_key_fixed_len);

  virtual ~ObSchema();

  uint64_t get_table_id() const;
  TableType get_table_type() const;

  const char* get_table_name() const;
  int32_t get_split_pos() const;
  int32_t get_rowkey_max_length() const;

  bool is_available(/*const ObOperator& ob_ot*/) const;
  const ObColumnSchema* find_column_info(const uint64_t column_id) const;
  const ObColumnSchema* find_column_info(const char* column_name) const;
  const ObColumnSchema* find_column_info(const ObString& column_name) const;
  const ObColumnSchema* column_begin() const;
  const ObColumnSchema* column_end() const;

  void add_join_info(const ObJoinInfo& join_info);
  bool add_column(const uint64_t column_id, const ObColumnSchema& column);

  const ObJoinInfo* find_join_info(const uint64_t lcolumn_id) const;
  const ObJoinInfo* get_join_info(const int64_t index) const;
  bool is_pure_update_table() const;
  bool is_use_bloomfilter() const;
  bool is_row_key_fixed_len() const;
  uint64_t get_create_time_column_id() const;
  uint64_t get_modify_time_column_id() const;
  int32_t get_block_size() const;
  const char* get_compress_func_name() const;



  NEED_SERIALIZE_AND_DESERIALIZE;
  //this is for test
  void print_info() const;

  friend int schema_1_to_2(ObSchemaManager& v1, ObSchemaManagerV2& v2);

 private:
  void sort_column();
  const ObColumnSchema* find_column_info_inner(const char* column_name) const;
  friend class ObSchemaManager;
 private:
  uint64_t table_id_;
  uint64_t max_column_id_;
  int32_t rowkey_split_;
  int32_t rowkey_max_length_;
  int64_t join_info_num_;
  int64_t column_info_num_;
  int16_t columns_sort_helper[OB_OLD_MAX_COLUMN_NUMBER];
  ObColumnSchema columns_[OB_OLD_MAX_COLUMN_NUMBER];
  ObJoinInfo join_infos_[OB_MAX_JOIN_INFO_NUMBER];
  char name_[OB_MAX_TABLE_NAME_LENGTH];
  char compress_func_name_[OB_MAX_TABLE_NAME_LENGTH];
  int32_t block_size_; //KB
  TableType table_type_;
  char have_been_sorted_;
  char is_pure_update_table_;
  char is_use_bloomfilter_;
  char is_row_key_fixed_len_;
  uint64_t create_time_column_id_;
  uint64_t modify_time_column_id_;

};

class ObSchemaManager : public BaseInited {
 public:
  ObSchemaManager();
  explicit ObSchemaManager(const int64_t time_stamp);
  virtual ~ObSchemaManager();

  int64_t get_version() const;
  bool parse_from_file(const char* file_name, tbsys::CConfig& config);

  uint64_t get_table_id(const char* name) const;
  uint64_t get_table_id(const ObString& name) const;
  const char* get_table_name(const uint64_t table_id) const;

  const ObSchema* get_table_schema(const uint64_t table_id) const;

  uint64_t get_column_id(const uint64_t table_id, const char* col_name) const;
  uint64_t get_column_id(const uint64_t table_id, const ObString& col_name) const;
  const char* get_column_name(const uint64_t table_id, const uint64_t col_id) const;

  const ObColumnSchema* get_column_schema(const uint64_t table_id, const uint64_t col_id) const;

  const char* get_app_name() const;

  // if a update server table has the same name with a chunk server table
  // that means, the chunk one must merge the update one to get the final
  // result. so one is the other's buddy.
  //const ObSchema* find_buddy_table(const ObSchema* table) const;

  // if this func return false; then we can not use these schemas
  // to replace the old one
  bool is_compatible(const ObSchemaManager& schema_manager) const;

  const ObSchema* begin() const;
  const ObSchema* end() const;
  NEED_SERIALIZE_AND_DESERIALIZE;

  //this is for test
  void print_info() const;


 private:
  bool parse_one_table(const char* section_name, tbsys::CConfig& config, ObSchema& schema);
  bool parse_column_info(const char* section_name, tbsys::CConfig& config, ObSchema& schema);
  bool parse_join_info(const char* section_name, tbsys::CConfig& config, ObSchema& schema);
  //static bool is_update_server_table(const char* table_name);

 private:
  friend int schema_1_to_2(ObSchemaManager& v1, ObSchemaManagerV2& v2);
 private:
  int64_t version_;
  uint64_t max_table_id_;
  int64_t schema_size_;
  ObSchema schemas_[OB_MAX_TABLE_NUMBER];
  char app_name_[OB_MAX_APP_NAME_LENGTH];
};


class ObColumnSchemaV2 {
 public:

  struct ObJoinInfo {
    ObJoinInfo() : join_table_(OB_INVALID_ID) {}
    uint64_t join_table_;
    uint64_t left_column_id_;//for the first version, this one always is 1, means rowkey only
    uint64_t correlated_column_;  //this means which part of left_column_ to be used make rowkey
    int32_t start_pos_;
    int32_t end_pos_;
  };

  ObColumnSchemaV2();
  ~ObColumnSchemaV2() {}

  uint64_t    get_id()   const;
  const char* get_name() const;
  ColumnType  get_type() const;
  int64_t     get_size() const;
  uint64_t    get_table_id()        const;
  bool        is_maintained()       const;
  uint64_t    get_column_group_id() const;


  void set_table_id(const uint64_t id);
  void set_column_id(const uint64_t id);
  void set_column_name(const char* name);
  void set_column_type(const ColumnType type);
  void set_column_size(const int64_t size); //only used when type is varchar
  void set_column_group_id(const uint64_t id);
  void set_maintained(bool maintained);

  void set_join_info(const uint64_t join_table, const uint64_t left_column_id,
                     const uint64_t correlated_column, const int32_t start_pos, const int32_t end_pos);

  const ObJoinInfo* get_join_info() const;

  bool operator==(const ObColumnSchemaV2& r) const;

  static ColumnType convert_str_to_column_type(const char* str);

  //this is for test
  void print_info() const;

  NEED_SERIALIZE_AND_DESERIALIZE;
 private:
  friend class ObSchemaManagerV2;
 private:
  bool maintained_;

  uint64_t table_id_;
  uint64_t column_group_id_;
  uint64_t column_id_;

  int64_t size_;  //only used when type is char or varchar
  ColumnType type_;
  char name_[OB_MAX_COLUMN_NAME_LENGTH];

  //join info
  ObJoinInfo join_info_;

  //in mem
  ObColumnSchemaV2* column_group_next_;
};

struct ObColumnSchemaV2Compare {
  bool operator()(const ObColumnSchemaV2& lhs, const ObColumnSchemaV2& rhs) {
    bool ret = false;
    if ((lhs.get_table_id() < rhs.get_table_id()) ||
        (lhs.get_table_id() == rhs.get_table_id() && lhs.get_column_group_id() < rhs.get_column_group_id()) ||
        (lhs.get_table_id() == rhs.get_table_id() &&
         (lhs.get_column_group_id() == rhs.get_column_group_id()) && lhs.get_id() < rhs.get_id())) {
      ret = true;
    }
    //bool ret = true;
    //if ( (lhs.get_table_id() > rhs.get_table_id()) ||
    //     (lhs.get_table_id() == rhs.get_table_id() && lhs.get_column_group_id() > rhs.get_column_group_id()) ||
    //     (lhs.get_table_id() == rhs.get_table_id() &&
    //      (lhs.get_column_group_id() == rhs.get_column_group_id()) && lhs.get_id() > rhs.get_id()) )
    //{
    //  ret = false;
    //}
    return ret;
  }
};


class ObTableSchema {
 public:
  ObTableSchema();
  ~ObTableSchema() {}
  enum TableType {
    INVALID = 0,
    SSTABLE_IN_DISK,
    SSTABLE_IN_RAM,
  };

  struct ExpireInfo {
    uint64_t column_id_;
    int64_t duration_;
  };

  uint64_t    get_table_id()   const;
  TableType   get_table_type() const;
  const char* get_table_name() const;
  const char* get_compress_func_name() const;
  uint64_t    get_max_column_id() const;
  int         get_expire_condition(uint64_t& column_id, int64_t& duration) const;
  int32_t     get_version() const;

  int32_t get_split_pos() const;
  int32_t get_rowkey_max_length() const;

  bool is_pure_update_table() const;
  bool is_use_bloomfilter()   const;
  bool is_row_key_fixed_len() const;
  int32_t get_block_size()    const;

  uint64_t get_create_time_column_id() const;
  uint64_t get_modify_time_column_id() const;

  void set_table_id(const uint64_t id);
  void set_max_column_id(const uint64_t id);
  void set_version(const int32_t version);

  void set_table_type(TableType type);
  void set_split_pos(const int64_t split_pos);

  void set_rowkey_max_length(const int64_t len);
  void set_block_size(const int64_t block_size);

  void set_table_name(const char* name);
  void set_compressor_name(const char* compressor);

  void set_pure_update_table(bool is_pure);
  void set_use_bloomfilter(bool use_bloomfilter);
  void set_rowkey_fixed_len(bool fixed_len);

  void set_expire_info(ExpireInfo& expire_info);

  void set_create_time_column(uint64_t id);
  void set_modify_time_column(uint64_t id);

  bool operator ==(const ObTableSchema& r) const;
  bool operator ==(const ObString& table_name) const;
  bool operator ==(const uint64_t table_id) const;

  NEED_SERIALIZE_AND_DESERIALIZE;
  //this is for test
  void print_info() const;

 private:
  uint64_t table_id_;
  uint64_t max_column_id_;
  int64_t rowkey_split_;
  int64_t rowkey_max_length_;

  int32_t block_size_; //KB
  TableType table_type_;

  char name_[OB_MAX_TABLE_NAME_LENGTH];
  char compress_func_name_[OB_MAX_TABLE_NAME_LENGTH];
  bool is_pure_update_table_;
  bool is_use_bloomfilter_;
  bool is_row_key_fixed_len_;
  ExpireInfo expire_info_;
  int32_t version_;

  //in mem
  uint64_t create_time_column_id_;
  uint64_t modify_time_column_id_;
};

class ObSchemaManagerV2 {
 public:
  ObSchemaManagerV2();
  explicit ObSchemaManagerV2(const int64_t timestamp);
  ~ObSchemaManagerV2();
 public:
  const ObColumnSchemaV2* column_begin() const;
  const ObColumnSchemaV2* column_end() const;
  const char* get_app_name() const;

  int64_t get_column_count() const;
  int64_t get_table_count() const;

  /**
   * @brief timestap is the version of schema,version_ is used for serialize syntax
   *
   * @return
   */
  int64_t get_version() const;

  /**
   * @brife return code version not schema version
   *
   * @return int32_t
   */
  int32_t get_code_version() const;

  const ObColumnSchemaV2* get_column_schema(const int32_t index) const;

  /**
   * @brief get a column accroding to table_id/column_group_id/column_id
   *
   * @param table_id
   * @param column_group_id
   * @param column_id
   *
   * @return column or null
   */
  const ObColumnSchemaV2* get_column_schema(const uint64_t table_id,
                                            const uint64_t column_group_id,
                                            const uint64_t column_id) const;

  /**
   * @brief get a column accroding to table_id/column_id, if this column belongs to
   * more than one column group,then return the column in first column group
   *
   * @param table_id the id of table
   * @param column_id the column id
   * @param idx[out] the index in table of this column
   *
   * @return column or null
   */
  const ObColumnSchemaV2* get_column_schema(const uint64_t table_id,
                                            const uint64_t column_id,
                                            int32_t* idx = NULL) const;


  const ObColumnSchemaV2* get_column_schema(const char* table_name,
                                            const char* column_name,
                                            int32_t* idx = NULL) const;

  const ObColumnSchemaV2* get_column_schema(const ObString& table_name,
                                            const ObString& column_name,
                                            int32_t* idx = NULL) const;


  const ObColumnSchemaV2* get_table_schema(const uint64_t table_id, int32_t& size) const;

  const ObColumnSchemaV2* get_group_schema(const uint64_t table_id,
                                           const uint64_t column_group_id,
                                           int32_t& size) const;

  const ObTableSchema* table_begin() const;
  const ObTableSchema* table_end() const;

  const ObTableSchema* get_table_schema(const char* table_name) const;
  const ObTableSchema* get_table_schema(const ObString& table_name) const;
  const ObTableSchema* get_table_schema(const uint64_t table_id) const;
  ObTableSchema* get_table_schema(const uint64_t table_id);

  uint64_t get_create_time_column_id(const uint64_t table_id) const;
  uint64_t get_modify_time_column_id(const uint64_t table_id) const;

  int get_column_index(const char* table_name, const char* column_name, int32_t index_array[], int32_t& size) const;
  int get_column_index(const uint64_t table_id, const uint64_t column_id, int32_t index_array[], int32_t& size) const;

  int get_column_schema(const uint64_t table_id, const uint64_t column_id,
                        ObColumnSchemaV2* columns[], int32_t& size) const;

  int get_column_schema(const char* table_name, const char* column_name,
                        ObColumnSchemaV2* columns[], int32_t& size) const;

  int get_column_schema(const ObString& table_name,
                        const ObString& column_name,
                        ObColumnSchemaV2* columns[], int32_t& size) const;


  int get_column_groups(uint64_t table_id, uint64_t column_groups[], int32_t& size) const;

  bool is_compatible(const ObSchemaManagerV2& schema_manager) const;

  int add_column(ObColumnSchemaV2& column);

  /**
   * @brief if you don't want to use column group,set drop_group to true and call this
   *        method before deserialize
   *
   * @param drop_group true - don't use column group,otherwise not
   *
   */
  void set_drop_column_group(bool drop_group = true);

 public:
  bool parse_from_file(const char* file_name, tbsys::CConfig& config);
  bool parse_one_table(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
  bool parse_column_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
  bool parse_join_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
  bool parse_expire_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
 public:
  ObSchemaManagerV2& operator=(const ObSchemaManagerV2& schema); //ugly,for hashMap
  ObSchemaManagerV2(const ObSchemaManagerV2& schema);

 public:
  void print_info() const;

 public:
  struct ObColumnNameKey {
    int64_t hash() const;
    bool operator==(const ObColumnNameKey& key) const;
    ObString table_name_;
    ObString column_name_;
  };

  struct ObColumnIdKey {
    int64_t hash() const;
    bool operator==(const ObColumnIdKey& key) const;
    uint64_t table_id_;
    uint64_t column_id_;
  };

  struct ObColumnInfo {
    ObColumnSchemaV2* head_;
    int32_t table_begin_index_;
    ObColumnInfo() : head_(NULL), table_begin_index_(-1) {}
  };

  struct ObColumnGroupHelper {
    uint64_t table_id_;
    uint64_t column_group_id_;
  };

  struct ObColumnGroupHelperCompare {
    bool operator()(const ObColumnGroupHelper& l, const ObColumnGroupHelper& r) const;
  };

 public:
  NEED_SERIALIZE_AND_DESERIALIZE;
  friend int schema_1_to_2(ObSchemaManager& v1, ObSchemaManagerV2& v2);

 private:
  int sort_column();

  //static const int64_t DEFAULT_MAX_COLUMNS = OB_MAX_TABLE_NUMBER * OB_MAX_COLUMN_NUMBER;

 private:
  int32_t   schema_magic_;
  int32_t   version_;
  int64_t   timestamp_;
  uint64_t  max_table_id_;
  int64_t   column_nums_;
  int64_t   table_nums_;

  char app_name_[OB_MAX_APP_NAME_LENGTH];

  ObTableSchema    table_infos_[OB_MAX_TABLE_NUMBER];
  //ObColumnSchemaV2 columns_[DEFAULT_MAX_COLUMNS];
  ObColumnSchemaV2 columns_[OB_MAX_COLUMN_NUMBER * OB_MAX_TABLE_NUMBER];

  //just in mem
  bool drop_column_group_; //
  bool hash_sorted_;       //after deserialize,will rebuild the hash maps
  hash::ObHashMap<ObColumnNameKey, ObColumnInfo, hash::NoPthreadDefendMode> column_hash_map_;
  hash::ObHashMap<ObColumnIdKey, ObColumnInfo, hash::NoPthreadDefendMode> id_hash_map_;

  int64_t column_group_nums_;
  ObColumnGroupHelper column_groups_[OB_MAX_COLUMN_GROUP_NUMBER];
};
}
}
#endif

