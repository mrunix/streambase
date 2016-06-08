/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema.cc for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *   maoqi <maoqi@taobao.com>
 *   fangji <fangji.hcm@taobao.com>
 *
 */
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <algorithm>

#include <config.h>
#include <tblog.h>

#include "common/utility.h"
#include "ob_schema.h"
namespace {
const char* STR_TABLE_ID = "table_id";
const char* STR_TABLE_TYPE = "table_type";
const char* STR_ROWKEY_SPLIT = "rowkey_split";
const char* STR_ROWKEY_LENGTH = "rowkey_max_length";
const char* STR_COLUMN_INFO = "column_info";
const char* STR_JOIN_RELATION = "join";
const char* STR_COMPRESS_FUNC_NAME = "compress_func_name";
const char* STR_BLOCK_SIZE = "block_size";
const char* STR_USE_BLOOMFILTER = "use_bloomfilter";
const char* STR_ROWKEY_IS_FIXED_LENGTH = "rowkey_is_fixed_length";

const char* STR_MAX_COLUMN_ID = "max_column_id";
const char* STR_COLUMN_TYPE_INT = "int";
const char* STR_COLUMN_TYPE_FLOAT = "float";
const char* STR_COLUMN_TYPE_DOUBLE = "double";
const char* STR_COLUMN_TYPE_VCHAR = "varchar";
const char* STR_COLUMN_TYPE_DATETIME = "datetime";
const char* STR_COLUMN_TYPE_PRECISE_DATETIME = "precise_datetime";
const char* STR_COLUMN_TYPE_SEQ = "seq";
const char* STR_COLUMN_TYPE_C_TIME = "create_time";
const char* STR_COLUMN_TYPE_M_TIME = "modify_time";

const char* STR_SECTION_APP_NAME = "app_name";
const char* STR_KEY_APP_NAME = "name";
const char* STR_MAX_TABLE_ID = "max_table_id";

const char* STR_COLUMN_GROUP_INFO = "column_group_info";
const char* STR_EXPIRE_INFO = "expire_info";
const int EXPIRE_ITEM = 2;

const unsigned int COLUMN_ID_RESERVED = 2;

const int64_t OB_SCHEMA_VERSION = 1;
const int64_t OB_SCHEMA_VERSION_TWO = 2;

const int POS_COLUM_MANTAINED = 0;
const int POS_COLUM_ID = 1;
const int POS_COLUM_NAME = 2;
const int POS_COLUM_TYPE = 3;
const int POS_COLUM_SIZE = 4;

const uint64_t ROW_KEY_COLUMN_ID = 1;
const uint64_t MAX_ID_USED = 65535;//we limit our id in 2 byte, so update server can take advantage of this

const int POS_COLUMN_GROUP_ID = 0;
const uint64_t OB_DEFAULT_COLUMN_GROUP_ID = 0;

const int32_t OB_SCHEMA_MAGIC_NUMBER = 0x4353; //SC
}


namespace sb {
namespace common {
using namespace std;

BaseInited::BaseInited(): inited_(false) {
}

BaseInited::~BaseInited() {
}

void BaseInited::set_flag() {
  inited_ = true;
}

bool BaseInited::have_inited() const {
  return inited_;
}
DEFINE_SERIALIZE(BaseInited) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ret = serialization::encode_bool(buf, buf_len, tmp_pos, inited_);
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_DESERIALIZE(BaseInited) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ret = serialization::decode_bool(buf, data_len, tmp_pos, &inited_);
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_GET_SERIALIZE_SIZE(BaseInited) {
  int64_t len = serialization::encoded_length_bool(inited_);
  return len;

}

ObColumnSchema::ObColumnSchema()
  : maintained_(false), id_(OB_INVALID_ID), size_(0), type_(ObNullType) {
  name_[0] = '\0';
}

bool ObColumnSchema::init(const bool maitained, const uint64_t id, const char* name,
                          const ColumnType& type, const int64_t size) {
  bool res = false;
  int name_len = 0;
  if (name != NULL) name_len = strlen(name);
  if (!have_inited() && name_len < OB_MAX_COLUMN_NAME_LENGTH && name_len > 0) {
    set_flag();
    maintained_ = maitained;
    id_ = id;
    size_ = size;
    type_ = type;
    if (name != NULL) {
      strncpy(name_, name, OB_MAX_COLUMN_NAME_LENGTH);
      name_[OB_MAX_COLUMN_NAME_LENGTH - 1] = '\0';
    }
    res = true;
  }
  return res;
}

ObColumnSchema::~ObColumnSchema() {
}

uint64_t ObColumnSchema::get_id() const {
  return id_;
}

const char* ObColumnSchema::get_name() const {
  return name_;
}

ColumnType ObColumnSchema::get_type() const {
  return type_;
}

int64_t ObColumnSchema::get_size() const {
  return size_;
}
bool ObColumnSchema::is_maintained() const {
  return maintained_;
}

ColumnType ObColumnSchema::convert_str_to_column_type(const char* str) {
  ColumnType type = ObNullType;
  if (strcmp(str, STR_COLUMN_TYPE_INT) == 0) {
    type = ObIntType;
  } else if (strcmp(str, STR_COLUMN_TYPE_FLOAT) == 0) {
    type = ObFloatType;
  } else if (strcmp(str, STR_COLUMN_TYPE_DOUBLE) == 0) {
    type = ObDoubleType;
  } else if (strcmp(str, STR_COLUMN_TYPE_VCHAR) == 0) {
    type = ObVarcharType;
  } else if (strcmp(str, STR_COLUMN_TYPE_DATETIME) == 0) {
    type = ObDateTimeType;
  } else if (strcmp(str, STR_COLUMN_TYPE_PRECISE_DATETIME) == 0) {
    type = ObPreciseDateTimeType;
  } else if (strcmp(str, STR_COLUMN_TYPE_SEQ) == 0) {
    type = ObSeqType;
  } else if (strcmp(str, STR_COLUMN_TYPE_C_TIME) == 0) {
    type = ObCreateTimeType;
  } else if (strcmp(str, STR_COLUMN_TYPE_M_TIME) == 0) {
    type = ObModifyTimeType;
  } else {
    TBSYS_LOG(ERROR, "column type %s not be supported", str);
  }
  return type;
}
bool ObColumnSchema::operator < (const ObColumnSchema& rv) const {
  return strncmp(get_name(), rv.get_name(), OB_MAX_COLUMN_NAME_LENGTH) < 0;
}

bool ObColumnSchema::operator < (const char* name) const {
  return strncmp(get_name(), name, OB_MAX_COLUMN_NAME_LENGTH) < 0;
}

void ObColumnSchema::print_info() const {
  TBSYS_LOG(INFO, "column: name =%s id = %lu size = %ld type = %d maintaied = %d",
            name_, id_, size_, type_, maintained_);
}

DEFINE_SERIALIZE(ObColumnSchema) {
  int ret = 0;
  int64_t tmp_pos = pos;
  ret = BaseInited::serialize(buf, buf_len, tmp_pos);
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_bool(buf, buf_len, tmp_pos, maintained_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, id_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, size_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, type_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vstr(buf, buf_len, tmp_pos, name_);
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_DESERIALIZE(ObColumnSchema) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ret = BaseInited::deserialize(buf, data_len, tmp_pos);
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_bool(buf, data_len, tmp_pos, &maintained_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&id_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &size_);
  }
  if (OB_SUCCESS == ret) {
    int32_t type = 0;
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &type);
    if (OB_SUCCESS == ret) {
      type_ = static_cast<ColumnType>(type);
    }
  }
  if (OB_SUCCESS == ret) {
    int64_t len = 0;
    serialization::decode_vstr(buf, data_len, tmp_pos,
                               name_, OB_MAX_COLUMN_NAME_LENGTH, &len);
    if (-1 == len) {
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_GET_SERIALIZE_SIZE(ObColumnSchema) {
  int64_t len = BaseInited::get_serialize_size();
  len += serialization::encoded_length_bool(maintained_);
  len += serialization::encoded_length_vi64(id_);
  len += serialization::encoded_length_vi64(size_);
  len += serialization::encoded_length_vi32(type_);
  len += serialization::encoded_length_vstr(name_);
  return len;
}


ObJoinInfo::ObJoinInfo()
  : left_column_id_(1), start_pos_(-1), end_pos_(-1),
    table_id_joined_(0), correlated_info_size_(0) {
  memset(correlated_left_columns, 0, sizeof(correlated_left_columns));
  memset(correlated_right_columns, 0, sizeof(correlated_right_columns));
}

bool ObJoinInfo::init(const uint64_t left_column, const int32_t start_pos, const int32_t end_pos, const uint64_t table_id_joined) {
  bool res = false;
  if (!have_inited()) {
    set_flag();
    left_column_id_ = left_column;
    start_pos_ = start_pos;
    end_pos_ = end_pos;
    table_id_joined_ = table_id_joined;

    left_column_id_ = ROW_KEY_COLUMN_ID; // first version, this always 1 means rowkey
    res = true;
  }
  return res;
}
ObJoinInfo::~ObJoinInfo() {
}

void ObJoinInfo::get_rowkey_join_range(int32_t& out_start_pos, int32_t& out_end_pos) const {
  out_start_pos = out_end_pos = -1;
  if (0 != left_column_id_) {
    if (0 <= start_pos_ && 0 <= end_pos_) {
      if (start_pos_ <= end_pos_) {
        out_start_pos = start_pos_;
        out_end_pos = end_pos_;
      } else {
        out_end_pos = start_pos_;
        out_start_pos = end_pos_;
      }
    }
  }
}

void ObJoinInfo::add_correlated_column(const uint64_t left_column_id, const uint64_t right_column_id) {
  if (correlated_info_size_ < OB_OLD_MAX_COLUMN_NUMBER) {
    correlated_left_columns[correlated_info_size_] = left_column_id;
    correlated_right_columns[correlated_info_size_++] = right_column_id;
  } else {
    TBSYS_LOG(ERROR, "correlated number must less than %ld, "
              "infor will be abandoned", OB_OLD_MAX_COLUMN_NUMBER);
  }
}

uint64_t ObJoinInfo::get_table_id_joined() const {
  return table_id_joined_;
}

uint64_t ObJoinInfo::find_left_column_id(const uint64_t rid) const {
  uint64_t result = OB_INVALID_ID;
  for (int32_t i = 0; i < correlated_info_size_; ++i) {
    if (correlated_right_columns[i] == rid) {
      result = correlated_left_columns[i];
      break;
    }
  }
  return result;
}

uint64_t ObJoinInfo::find_right_column_id(const uint64_t lid) const {
  uint64_t result = OB_INVALID_ID;
  for (int32_t i = 0; i < correlated_info_size_; ++i) {
    if (correlated_left_columns[i] == lid) {
      result = correlated_right_columns[i];
      break;
    }
  }
  return result;
}

void ObJoinInfo::print_info() const {
  TBSYS_LOG(INFO, "ObJoinInfo: left_column = %lu start_pod = %d end_pos = %d "
            "table_id_joined = %lu correlated_info_size_ = %d", left_column_id_,
            start_pos_, end_pos_, table_id_joined_, correlated_info_size_);
  for (int32_t i = 0; i < correlated_info_size_; ++i) {
    TBSYS_LOG(INFO, "lid = %lu rid = %lu",
              correlated_left_columns[i],
              correlated_right_columns[i]);
  }

}
DEFINE_SERIALIZE(ObJoinInfo) {
  int ret = 0;
  int64_t tmp_pos = pos;
  ret = BaseInited::serialize(buf, buf_len, tmp_pos);
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, left_column_id_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, start_pos_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, end_pos_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_id_joined_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, correlated_info_size_);
  }
  if (OB_SUCCESS == ret) {
    if (correlated_info_size_ > OB_OLD_MAX_COLUMN_NUMBER) {
      TBSYS_LOG(ERROR, "bugs, correlated_info_size_ is %d max is %ld",
                correlated_info_size_, OB_OLD_MAX_COLUMN_NUMBER);
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    for (int i = 0; i < correlated_info_size_; ++i) {
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, correlated_left_columns[i]);
      if (OB_SUCCESS != ret) {
        break;
      }
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, correlated_right_columns[i]);
      if (OB_SUCCESS != ret) {
        break;
      }
    }
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_DESERIALIZE(ObJoinInfo) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ret = BaseInited::deserialize(buf, data_len, tmp_pos);
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                     reinterpret_cast<int64_t*>(&left_column_id_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &start_pos_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &end_pos_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                     reinterpret_cast<int64_t*>(&table_id_joined_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &correlated_info_size_);
  }
  if (OB_SUCCESS == ret) {
    if (correlated_info_size_ < 0 || correlated_info_size_ > OB_OLD_MAX_COLUMN_NUMBER) {
      TBSYS_LOG(ERROR, "bugs, correlated_info_size_ (%d) is wrong", correlated_info_size_);
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    for (int i = 0; i < correlated_info_size_; i++) {
      ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                       reinterpret_cast<int64_t*>(&correlated_left_columns[i]));
      if (OB_SUCCESS != ret) break;
      ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                       reinterpret_cast<int64_t*>(&correlated_right_columns[i]));
      if (OB_SUCCESS != ret) break;
    }
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_GET_SERIALIZE_SIZE(ObJoinInfo) {
  int64_t len = BaseInited::get_serialize_size();
  len += serialization::encoded_length_vi64(left_column_id_);
  len += serialization::encoded_length_vi32(start_pos_);
  len += serialization::encoded_length_vi32(end_pos_);
  len += serialization::encoded_length_vi64(table_id_joined_);
  len += serialization::encoded_length_vi32(end_pos_); //bug
  len += serialization::encoded_length_vi32(correlated_info_size_);
  if (correlated_info_size_ < 0 || correlated_info_size_ > OB_OLD_MAX_COLUMN_NUMBER) {
    TBSYS_LOG(ERROR, "bugs correlated_info_size_ %d error", correlated_info_size_);
  } else {
    for (int i = 0; i < correlated_info_size_; i++) {
      len += serialization::encoded_length_vi64(correlated_left_columns[i]);
      len += serialization::encoded_length_vi64(correlated_right_columns[i]);
    }
  }
  return len;
}

ObSchema::ObSchema()
  : table_id_(OB_INVALID_ID), max_column_id_(0), rowkey_split_(0), rowkey_max_length_(0),
    join_info_num_(0), column_info_num_(0), block_size_(0),
    table_type_(INVALID), have_been_sorted_(0), is_pure_update_table_(1), is_use_bloomfilter_(0), is_row_key_fixed_len_(1),
    create_time_column_id_(OB_INVALID_ID), modify_time_column_id_(OB_INVALID_ID) {
  compress_func_name_[0] = '\0';
  name_[0] = '\0';
  for (int i = 0; i < OB_OLD_MAX_COLUMN_NUMBER; ++i) {
    columns_sort_helper[i] = i;
  }
}

bool ObSchema::init(const uint64_t table_id, const uint64_t max_column_id, const char* name,
                    const TableType table_type, const int32_t rowkey_split, const int32_t rowkey_max_length,
                    const char* compress_func_name, const int32_t block_size, const int32_t is_use_bloomfilter,
                    const int32_t is_row_key_fixed_len) {
  bool res = false;
  if (!have_inited()) {
    set_flag();
    for (int i = 0; i < OB_OLD_MAX_COLUMN_NUMBER; ++i) {
      columns_sort_helper[i] = i;
    }
    table_id_ = table_id;
    max_column_id_ = max_column_id;
    if (name != NULL) {
      strncpy(name_, name, OB_MAX_TABLE_NAME_LENGTH);
      name_[OB_MAX_TABLE_NAME_LENGTH - 1] = '\0';
    }
    rowkey_split_ = rowkey_split;
    rowkey_max_length_ = rowkey_max_length;
    table_type_ = table_type;
    if (compress_func_name != NULL) {
      snprintf(compress_func_name_, OB_MAX_TABLE_NAME_LENGTH, "%s", compress_func_name);
    }
    block_size_ = block_size;
    is_use_bloomfilter_ = is_use_bloomfilter;
    is_row_key_fixed_len_ = is_row_key_fixed_len;
    create_time_column_id_ = OB_INVALID_ID;
    modify_time_column_id_ = OB_INVALID_ID;
    is_pure_update_table_ = 1;
    res = true;
  }
  return res;
}

ObSchema::~ObSchema() {

}

uint64_t ObSchema::get_table_id() const {
  return table_id_;
}

ObSchema::TableType ObSchema::get_table_type() const {
  return table_type_;
}

const char* ObSchema::get_table_name() const {
  return name_;
}

int32_t ObSchema::get_split_pos() const {
  return rowkey_split_;
}

int32_t ObSchema::get_rowkey_max_length() const {
  return rowkey_max_length_;
}

bool ObSchema::is_available(/*const ObOperator& ot*/) const {
  //TODO check name will hanve been done, so we need check type and
  //tobe sure, this operator can ben done in this tyep
  return true;
}

const ObColumnSchema* ObSchema::find_column_info(const uint64_t column_id) const {
  const ObColumnSchema* out = NULL;
  if (column_id >= COLUMN_ID_RESERVED) {
    for (int i = 0; i < column_info_num_; ++i) {
      if (columns_[i].have_inited() && columns_[i].get_id() == column_id) {
        out = &columns_[i];
        break;
      }
    }
  }
  return out;
}
const ObColumnSchema* ObSchema::column_begin() const {
  return columns_;
}
const ObColumnSchema* ObSchema::column_end() const {
  return columns_ + column_info_num_;
}
const ObColumnSchema* ObSchema::find_column_info_inner(const char* column_name) const {
  const ObColumnSchema* out = NULL;
  for (int32_t i = 0; i < OB_OLD_MAX_COLUMN_NUMBER; ++i) {
    if (columns_[i].have_inited()) {
      if (strncmp(column_name, columns_[i].get_name(), OB_MAX_COLUMN_NAME_LENGTH) == 0) {
        out = &columns_[i];
        break;
      }
    }
  }
  return out;
}

const ObColumnSchema* ObSchema::find_column_info(const char* column_name) const {
  const ObColumnSchema* out = NULL;
  if (have_been_sorted_ == 0) {
    out = find_column_info_inner(column_name);
  } else {
    int low = 0;
    int high = OB_OLD_MAX_COLUMN_NUMBER - 1;
    int mid = 0;
    int mid_index = 0;
    int com = 0;
    while (low <= high) {
      mid = low + ((high - low) / 2);
      mid_index = columns_sort_helper[mid];

      com = strncmp(columns_[mid_index].get_name(), column_name, OB_MAX_COLUMN_NAME_LENGTH);

      if (com == 0) {
        out = &columns_[mid_index];
        break;
      }
      if (com < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
  }
  return out;
}
const ObColumnSchema* ObSchema::find_column_info(const ObString& column_name) const {
  const ObColumnSchema* out = NULL;
  for (int32_t i = 0; i < OB_OLD_MAX_COLUMN_NUMBER; ++i) {
    if (columns_[i].have_inited()) {
      if (column_name.compare(columns_[i].get_name()) == 0) {
        out = &columns_[i];
        break;
      }
    }
  }
  return out;
}

void ObSchema::add_join_info(const ObJoinInfo& join_info) {
  if (join_info_num_ < OB_MAX_JOIN_INFO_NUMBER) {
    join_infos_[join_info_num_++] = join_info;
  }
}

void ObSchema::sort_column() {
  for (int i = 0; i < OB_OLD_MAX_COLUMN_NUMBER - 1; ++i) {
    for (int j = i + 1; j < OB_OLD_MAX_COLUMN_NUMBER; j++) {
      if (!(columns_[columns_sort_helper[i]] < columns_[columns_sort_helper[j]])) {
        int t = columns_sort_helper[i];
        columns_sort_helper[i] = columns_sort_helper[j];
        columns_sort_helper[j] = t;
      }
    }
  }
  have_been_sorted_ = 1;
}

bool ObSchema::add_column(const uint64_t column_id, const ObColumnSchema& column) {
  bool add_result = false;
  if (column_info_num_ < OB_OLD_MAX_COLUMN_NUMBER) {
    if (column_id < COLUMN_ID_RESERVED) {
      TBSYS_LOG(ERROR, "we reserved id 0 for nothing id 1 for rowkey");
    } else if (column_id > max_column_id_) {
      TBSYS_LOG(ERROR, "column id %lu greater thean max_column_id %lu", column_id, max_column_id_);
    } else {
      columns_[column_info_num_++] = column;
      add_result = true;
    }
  } else {
    TBSYS_LOG(ERROR, "too many columns abandon this info");
  }
  return add_result;
}

const ObJoinInfo* ObSchema::find_join_info(const uint64_t lcolumn_id) const {
  const ObJoinInfo* result = NULL;
  for (int32_t i = 0; i < join_info_num_; ++i) {
    if (join_infos_[i].have_inited()) {
      if (join_infos_[i].find_right_column_id(lcolumn_id) != OB_INVALID_ID) {
        result = &join_infos_[i];
        break;
      }
    }
  }
  return result;
}

const ObJoinInfo* ObSchema::get_join_info(const int64_t index) const {
  const ObJoinInfo* result = NULL;
  if (index < join_info_num_ && join_infos_[index].have_inited()) {
    result = &join_infos_[index];
  }
  return result;
}

void ObSchema::print_info() const {
  TBSYS_LOG(INFO, "ObSchema: name = %s table_id = %lu max_column_id = %lu rowkey_split = %d "
            "rowkey_max_length = %d table_type_ = %d join_info_size = %ld "
            "compress_func_name_ = %s block_size_ = %d is_pure_update_table_=%d is_use_bloomfilter_ = %d is_row_key_fixed_len_ = %d "
            "create_time_column_id_ = %ld modify_time_column_id_ = %ld",
            name_, table_id_, max_column_id_, rowkey_split_, rowkey_max_length_, table_type_, join_info_num_,
            compress_func_name_, block_size_, is_pure_update_table_, is_use_bloomfilter_, is_row_key_fixed_len_,
            create_time_column_id_, modify_time_column_id_);
  for (int32_t i = 0; i < column_info_num_; ++i) {
    if (columns_[i].have_inited()) {
      columns_[i].print_info();
    }
  }
  for (int32_t i = 0; i < join_info_num_; ++i) {
    if (join_infos_[i].have_inited()) {
      join_infos_[i].print_info();
    }
  }
  TBSYS_LOG(INFO, "----------------after sorted---------------");
  int index = 0;
  for (int32_t i = 0; i < OB_OLD_MAX_COLUMN_NUMBER; ++i) {
    index = columns_sort_helper[i];
    if (columns_[index].have_inited()) {
      columns_[index].print_info();
    }
  }
  TBSYS_LOG(INFO, "-----------------------------------------");
  TBSYS_LOG(INFO, "serialized_size=%ld", get_serialize_size());
}
bool ObSchema::is_pure_update_table() const {
  return is_pure_update_table_ != 0;
}
bool ObSchema::is_use_bloomfilter() const {
  return is_use_bloomfilter_ != 0;
}
bool ObSchema::is_row_key_fixed_len() const {
  return is_row_key_fixed_len_ != 0;
}
uint64_t ObSchema::get_create_time_column_id() const {
  return create_time_column_id_ ;
}
uint64_t ObSchema::get_modify_time_column_id() const {
  return modify_time_column_id_ ;
}
int32_t ObSchema::get_block_size() const {
  return block_size_;
}
const char* ObSchema::get_compress_func_name() const {
  return compress_func_name_;
}
DEFINE_SERIALIZE(ObSchema) {
  int ret = 0;
  int64_t tmp_pos = pos;
  ret = BaseInited::serialize(buf, buf_len, tmp_pos);
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_id_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, max_column_id_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, rowkey_split_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, rowkey_max_length_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, join_info_num_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, column_info_num_);
  }
  if (OB_SUCCESS == ret) {
    for (int i = 0; i < OB_OLD_MAX_COLUMN_NUMBER; ++i) {
      ret = serialization::encode_i16(buf, buf_len, tmp_pos,
                                      columns_sort_helper[i]);
      if (OB_SUCCESS != ret) break;
      ret = columns_[i].serialize(buf, buf_len, tmp_pos);
      if (OB_SUCCESS != ret) break;
    }
  }
  if (OB_SUCCESS == ret) {
    for (int i = 0; i < OB_MAX_JOIN_INFO_NUMBER; ++i) {
      join_infos_[i].serialize(buf, buf_len, tmp_pos);
      if (OB_SUCCESS != ret) break;
    }
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, table_type_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vstr(buf, buf_len, tmp_pos, name_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vstr(buf, buf_len, tmp_pos, compress_func_name_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, block_size_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_i8(buf, buf_len, tmp_pos, have_been_sorted_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_i8(buf, buf_len, tmp_pos, is_pure_update_table_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_i8(buf, buf_len, tmp_pos, is_use_bloomfilter_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_i8(buf, buf_len, tmp_pos, is_row_key_fixed_len_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, create_time_column_id_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, modify_time_column_id_);
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_DESERIALIZE(ObSchema) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ret = BaseInited::deserialize(buf, data_len, tmp_pos);
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                     reinterpret_cast<int64_t*>(&table_id_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                     reinterpret_cast<int64_t*>(&max_column_id_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &rowkey_split_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &rowkey_max_length_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &join_info_num_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &column_info_num_);
  }
  if (OB_SUCCESS == ret) {
    for (int i = 0; i < OB_OLD_MAX_COLUMN_NUMBER; ++i) {
      ret = serialization::decode_i16(buf, data_len, tmp_pos, &columns_sort_helper[i]);
      if (OB_SUCCESS != ret) break;
      ret = columns_[i].deserialize(buf, data_len, tmp_pos);
      if (OB_SUCCESS != ret) break;
    }
  }
  if (OB_SUCCESS == ret) {
    for (int i = 0; i < OB_MAX_JOIN_INFO_NUMBER; ++i) {
      ret = join_infos_[i].deserialize(buf, data_len, tmp_pos);
      if (OB_SUCCESS != ret) break;
    }
  }
  if (OB_SUCCESS == ret) {
    int32_t table_type = 0;
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &table_type);
    table_type_ = static_cast<TableType>(table_type);
  }
  if (OB_SUCCESS == ret) {
    int64_t len = 0;
    serialization::decode_vstr(buf, data_len, tmp_pos,
                               name_, OB_MAX_TABLE_NAME_LENGTH, &len);
    if (-1 == len) {
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    int64_t len = 0;
    serialization::decode_vstr(buf, data_len, tmp_pos,
                               compress_func_name_, OB_MAX_TABLE_NAME_LENGTH, &len);
    if (-1 == len) {
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &block_size_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_i8(buf, data_len, tmp_pos,
                                   reinterpret_cast<int8_t*>(&have_been_sorted_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_i8(buf, data_len, tmp_pos,
                                   reinterpret_cast<int8_t*>(&is_pure_update_table_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_i8(buf, data_len, tmp_pos,
                                   reinterpret_cast<int8_t*>(&is_use_bloomfilter_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_i8(buf, data_len, tmp_pos,
                                   reinterpret_cast<int8_t*>(&is_row_key_fixed_len_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                     reinterpret_cast<int64_t*>(&create_time_column_id_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                     reinterpret_cast<int64_t*>(&modify_time_column_id_));
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_GET_SERIALIZE_SIZE(ObSchema) {
  int64_t len = BaseInited::get_serialize_size();
  len += serialization::encoded_length_vi64(table_id_);
  len += serialization::encoded_length_vi64(max_column_id_);
  len += serialization::encoded_length_vi32(rowkey_split_);
  len += serialization::encoded_length_vi32(rowkey_max_length_);
  len += serialization::encoded_length_vi64(join_info_num_);
  len += serialization::encoded_length_vi64(column_info_num_);
  for (int i = 0; i < OB_OLD_MAX_COLUMN_NUMBER; ++i) {
    len += serialization::encoded_length_i16(columns_sort_helper[i]);
    len += columns_[i].get_serialize_size();
  }
  for (int i = 0; i < OB_MAX_JOIN_INFO_NUMBER; ++i) {
    len += join_infos_[i].get_serialize_size();
  }
  len += serialization::encoded_length_vi32(table_type_);
  len += serialization::encoded_length_vstr(name_);
  len += serialization::encoded_length_vstr(compress_func_name_);
  len += serialization::encoded_length_vi32(block_size_);
  len += serialization::encoded_length_i8(have_been_sorted_);
  len += serialization::encoded_length_i8(is_pure_update_table_);
  len += serialization::encoded_length_i8(is_use_bloomfilter_);
  len += serialization::encoded_length_i8(is_row_key_fixed_len_);
  len += serialization::encoded_length_vi64(create_time_column_id_);
  len += serialization::encoded_length_vi64(modify_time_column_id_);
  return len;
}

ObSchemaManager::ObSchemaManager()
  : version_(0), max_table_id_(0), schema_size_(0) {
  app_name_[0] = '\0';
}

ObSchemaManager::ObSchemaManager(const int64_t version)
  : version_(version), max_table_id_(0), schema_size_(0) {
  app_name_[0] = '\0';
}

int64_t ObSchemaManager::get_version() const {
  return version_;
}

bool ObSchemaManager::parse_from_file(const char* file_name, tbsys::CConfig& config) {
  bool parse_ok = true;
  if (have_inited()) {
    parse_ok = false;
  }

  if (parse_ok && file_name != NULL && config.load(file_name) != EXIT_SUCCESS) {
    TBSYS_LOG(ERROR, "can not open config file, file name is %s", file_name);
    parse_ok = false;
  }
  if (parse_ok) {
    const char* p = config.getString(STR_SECTION_APP_NAME, STR_KEY_APP_NAME, NULL);

    int length = 0;
    if (p != NULL) length = strlen(p);
    if (p == NULL || length >= OB_MAX_APP_NAME_LENGTH) {
      TBSYS_LOG(ERROR, "parse [%s]  %s error", STR_SECTION_APP_NAME, STR_KEY_APP_NAME);
      parse_ok = false;
    } else {
      strncpy(app_name_, p, OB_MAX_APP_NAME_LENGTH);
      app_name_[OB_MAX_APP_NAME_LENGTH - 1] = '\0';
      max_table_id_ = config.getInt(STR_SECTION_APP_NAME, STR_MAX_TABLE_ID, 0);
      if (max_table_id_ > MAX_ID_USED) {
        TBSYS_LOG(ERROR, "we limit our table id less than %lu", MAX_ID_USED);
        parse_ok = false;
      }
      if (max_table_id_ < 1) {
        TBSYS_LOG(ERROR, "max_table_id is %lu", max_table_id_);
        parse_ok = false;
      }

      vector<string> sections;
      config.getSectionName(sections);
      schema_size_ = sections.size() - 1;
      if (schema_size_ > OB_MAX_TABLE_NUMBER || schema_size_ < 0) {
        TBSYS_LOG(ERROR, "%ld error table number", schema_size_);
        parse_ok = false;
        schema_size_ = 0;
      }
      // app section and sections.size() - 1 tables
      uint32_t index = 0;
      for (vector<string>::iterator it = sections.begin();
           it != sections.end() && parse_ok; ++it) {
        if (strcmp(it->c_str(), STR_SECTION_APP_NAME) == 0) {
          continue;
        }
        if (!parse_one_table(it->c_str(), config, schemas_[index])) {
          parse_ok = false;
          schema_size_ = 0;
          break;
        }
        ++index;
      }

      // we must get all tables, so we can check join info;
      index = 0;
      for (vector<string>::iterator it = sections.begin();
           it != sections.end() && parse_ok; ++it) {
        if (strcmp(it->c_str(), STR_SECTION_APP_NAME) == 0) {
          continue;
        }
        if (!parse_join_info(it->c_str(), config, schemas_[index])) {
          parse_ok = false;
          schema_size_ = 0;
          break;
        }
        ++index;
      }
    }

  }
  if (parse_ok) set_flag();

  return parse_ok;
}

bool ObSchemaManager::parse_one_table(const char* section_name, tbsys::CConfig& config, ObSchema& schema) {
  uint64_t table_id = OB_INVALID_ID;
  int type = 0;
  ObSchema::TableType table_type = ObSchema::INVALID;
  int32_t rowkey_split = 0;
  int32_t rowkey_max_length = 0;
  uint64_t max_column_id = 0;

  bool parse_ok = true;
  const char* name = section_name;
  int name_len = 0;
  if (name != NULL) name_len = strlen(name);
  if (name == NULL || name_len >= OB_MAX_TABLE_NAME_LENGTH) {
    TBSYS_LOG(ERROR, "table_name is missing");
    parse_ok = false;
  }
  if (parse_ok) {
    table_id = config.getInt(section_name, STR_TABLE_ID, 0);
    if (table_id == 0) {
      TBSYS_LOG(ERROR, "table id can not be 0");
      parse_ok = false;
    }
    if (table_id > max_table_id_) {
      TBSYS_LOG(ERROR, "table id %lu greater than max_table_id %lu", table_id, max_table_id_);
      parse_ok = false;
    }
  }
  if (parse_ok && get_table_name(table_id) != NULL) {
    TBSYS_LOG(ERROR, "table id %lu have been used", table_id);
    parse_ok = false;
  }

  if (parse_ok) {
    type = config.getInt(section_name, STR_TABLE_TYPE, 0);
    //if (type == ObSchema::MEM_TABLE)
    //{
    //  table_type = ObSchema::MEM_TABLE;
    //}
    //else
    if (type == ObSchema::SSTABLE_IN_DISK) {
      table_type = ObSchema::SSTABLE_IN_DISK;
    } else if (type == ObSchema::SSTABLE_IN_RAM) {
      table_type = ObSchema::SSTABLE_IN_RAM;
    } else {
      TBSYS_LOG(ERROR, "%d is a invalid table type", type);
      parse_ok = false;
    }
  }

  if (parse_ok) {
    //if (table_type != ObSchema::MEM_TABLE)
    //{
    //  if (is_update_server_table(name))
    //  {
    //    TBSYS_LOG(WARN, "table name have prefix %s so change table type to %d",
    //        STR_UPDATE_TABLE_PREFIX, ObSchema::MEM_TABLE);
    //    table_type = ObSchema::MEM_TABLE;
    //  }
    //}

    rowkey_split = config.getInt(section_name, STR_ROWKEY_SPLIT, 0);
    rowkey_max_length = config.getInt(section_name, STR_ROWKEY_LENGTH, 0);
    max_column_id = config.getInt(section_name, STR_MAX_COLUMN_ID, 0);
    if (max_column_id < 1) {
      TBSYS_LOG(ERROR, "max_column_id is %lu", max_column_id);
      parse_ok = false;
    }
    if (max_column_id > MAX_ID_USED) {
      TBSYS_LOG(ERROR, "we limit our column id less than %lu", MAX_ID_USED);
      parse_ok = false;
    }
    if (rowkey_max_length <= 0 || rowkey_split < 0) {
      TBSYS_LOG(ERROR, "rowkey length must greater than 0");
      parse_ok = false;
    }
  }
  if (parse_ok && rowkey_max_length < rowkey_split) {
    TBSYS_LOG(ERROR, "rowkey length must greater than rowkey split");
    parse_ok = false;
  }
  const char* compress_func_name = NULL;
  int32_t block_size = 0;
  int32_t is_use_bloomfilter = 0;
  int32_t is_row_key_fixed_len = 1;
  if (parse_ok) {
    compress_func_name = config.getString(section_name, STR_COMPRESS_FUNC_NAME, "none");
    block_size = config.getInt(section_name, STR_BLOCK_SIZE, 64);
    is_use_bloomfilter = config.getInt(section_name, STR_USE_BLOOMFILTER, 0);
    is_row_key_fixed_len = config.getInt(section_name, STR_ROWKEY_IS_FIXED_LENGTH, 1);
  }
  if (parse_ok && !schema.init(table_id, max_column_id, name, table_type, rowkey_split, rowkey_max_length,
                               compress_func_name, block_size, is_use_bloomfilter, is_row_key_fixed_len)) {
    parse_ok = false;
  }
  if (parse_ok) {
    parse_ok = parse_column_info(section_name, config, schema);
    if (parse_ok) schema.sort_column();
  }

  return parse_ok;
}
bool ObSchemaManager::parse_column_info(const char* section_name, tbsys::CConfig& config, ObSchema& schema) {
  bool parse_ok = true;
  char* str = NULL;
  uint64_t id = OB_INVALID_ID;
  uint64_t maintained = 0;
  ColumnType type = ObNullType;
  vector<const char*> column_info_strs;
  if (section_name != NULL) {
    column_info_strs = config.getStringList(section_name, STR_COLUMN_INFO);
  }
  if (column_info_strs.empty()) {
    parse_ok = false;
  }
  int size = 0;
  for (vector<const char*>::const_iterator it = column_info_strs.begin();
       it != column_info_strs.end() && parse_ok; ++it) {
    size = 0;
    str = strdup(*it);
    vector<char*> node_list;
    str = str_trim(str);
    tbsys::CStringUtil::split(str, ",", node_list);
    if (node_list.size() < POS_COLUM_TYPE + 1) {
      TBSYS_LOG(ERROR, "parse column |%s| error", str);
      parse_ok = false;
    }
    if (parse_ok) {
      maintained = strtoll(node_list[POS_COLUM_MANTAINED], NULL, 10);
      if (maintained != 0 && maintained != 1) {
        TBSYS_LOG(ERROR, "maintained %ld is unacceptable", maintained);
        parse_ok = false;
      }
      if (maintained == 1) {
        schema.is_pure_update_table_ = 0;
      }
    }
    if (parse_ok) {
      id = strtoll(node_list[POS_COLUM_ID], NULL, 10);
      if (id == 0) {
        TBSYS_LOG(ERROR, "column id error id = %ld", id);
        parse_ok = false;
      }
    }

    if (parse_ok) {
      type = ObColumnSchema::convert_str_to_column_type(node_list[POS_COLUM_TYPE]);
      if (type == ObNullType) {
        TBSYS_LOG(ERROR, "column type error |%s|", node_list[POS_COLUM_TYPE]);
        parse_ok = false;
      }
    }

    if (parse_ok && type == ObVarcharType) {
      if (node_list.size() < POS_COLUM_SIZE + 1) {
        TBSYS_LOG(ERROR, "column type need size |%s|", node_list[POS_COLUM_TYPE]);
        parse_ok = false;
      }
      if (parse_ok) {
        size = strtoll(node_list[POS_COLUM_SIZE], NULL, 10);
        if (size <= 0) {
          TBSYS_LOG(ERROR, "column type size error |%s|", node_list[POS_COLUM_SIZE]);
          parse_ok = false;
        }
      }
    }
    if (parse_ok && type == ObCreateTimeType) {
      if (schema.get_create_time_column_id() != OB_INVALID_ID) {
        TBSYS_LOG(ERROR, "more than one column have create time type");
        parse_ok = false;
      } else {
        schema.create_time_column_id_ = id;
      }
    }
    if (parse_ok && type == ObModifyTimeType) {
      if (schema.get_modify_time_column_id() != OB_INVALID_ID) {
        TBSYS_LOG(ERROR, "more than one column have modify time type");
        parse_ok = false;
      } else {
        schema.modify_time_column_id_ = id;
      }
    }
    if (parse_ok) {
      ObColumnSchema column;
      if (!column.init(maintained, id, node_list[POS_COLUM_NAME], type, size)) {
        parse_ok = false;
      }

      if (parse_ok && !schema.add_column(id, column)) {
        parse_ok = false;
      }
    }
    free(str);
    str = NULL;
  }
  return parse_ok;
}

bool ObSchemaManager::parse_join_info(const char* section_name, tbsys::CConfig& config, ObSchema& schema) {
  uint64_t left_column = 1;
  int32_t start_pos = -1;
  int32_t end_pos = -1;
  int64_t table_id_joined = OB_INVALID_ID;

  bool parse_ok = true;
  char* str = NULL;
  vector<const char*> join_info_strs;
  if (section_name != NULL) {
    join_info_strs = config.getStringList(section_name, STR_JOIN_RELATION);
  }
  if (!join_info_strs.empty()) {
    char* s = NULL;
    int len = 0;
    char* p = NULL;
    for (vector<const char*>::iterator it = join_info_strs.begin();
         parse_ok && it != join_info_strs.end(); ++it) {
      str = strdup(*it);
      s = str;
      len = strlen(s);

      p = NULL;

      if (len < 7) {
        TBSYS_LOG(ERROR, "error format  in join info |%s|", str);
        parse_ok = false;
      }
      if (parse_ok && strncmp(s, "rowkey", 6) != 0) {
        TBSYS_LOG(ERROR, "%s is not acceptable, must be rowkey for now", str);
        parse_ok = false;
      }
      if (parse_ok) {
        s += 6;
        if (*s == '[') {
          p =  strchr(s, ',');
          if (p == NULL) {
            TBSYS_LOG(ERROR, "%s rowkey range error", str);
            parse_ok = false;
          }
          if (parse_ok) {
            s++;
            *p = '\0';
            start_pos = atol(s);
            s = p + 1;
            p = strchr(s, ']');
            if (p == NULL) {
              TBSYS_LOG(ERROR, "%s rowkey range error", str);
              parse_ok = false;
            }
          }
          if (parse_ok) {
            *p = '\0';
            end_pos = atol(s);
            s = p + 1;
          }
        }
      }

      if (parse_ok && *s != '%') {
        TBSYS_LOG(ERROR, "%s format error, should be rowkey%%", str);
        parse_ok = false;
      }
      if (parse_ok) {
        s++;
        p = strchr(s, ':');
        if (p == NULL) {
          TBSYS_LOG(ERROR, "%s format error, could not find ':'", str);
          parse_ok = false;
        }
      }
      if (parse_ok) {
        *p = '\0';
        table_id_joined = get_table_id(s);
        if (table_id_joined == 0) {
          TBSYS_LOG(ERROR, "%s table not exist ", s);
          parse_ok = false;
        }
      }
      const ObSchema* schema_table_joined = NULL;
      vector<char*> node_list;
      if (parse_ok) {
        s = p + 1;

        schema_table_joined = get_table_schema(table_id_joined);
        if (schema_table_joined == NULL) {
          TBSYS_LOG(ERROR, "can not found joined table");
          parse_ok = false;
        } else {
          s = str_trim(s);
          tbsys::CStringUtil::split(s, ",", node_list);
          if (node_list.empty()) {
            TBSYS_LOG(ERROR, "%s can not find correct info", str);
            parse_ok = false;
          }
        }
      }
      ObJoinInfo join_info;

      if (parse_ok && !join_info.init(left_column, start_pos, end_pos, table_id_joined)) {
        parse_ok = false;
      }
      uint64_t ltable_id = OB_INVALID_ID;
      if (parse_ok) {
        ltable_id = schema.get_table_id();
        uint64_t lid = OB_INVALID_ID;
        uint64_t rid = OB_INVALID_ID;
        char* p;
        for (uint32_t i = 0; parse_ok && i < node_list.size(); ++i) {
          p = NULL;
          lid = OB_INVALID_ID;
          rid = OB_INVALID_ID;
          p = strchr(node_list[i], '$');
          if (p == NULL) {
            TBSYS_LOG(ERROR, "error can not find '$' %s ", node_list[i]);
            parse_ok = false;
            break;
          }
          *p = '\0';
          p++;
          lid = get_column_id(ltable_id, node_list[i]);
          rid = get_column_id(table_id_joined, p);

          if (lid <= ROW_KEY_COLUMN_ID || rid <= ROW_KEY_COLUMN_ID || lid == OB_INVALID_ID || rid == OB_INVALID_ID) {
            TBSYS_LOG(ERROR, "error column name %s %s ", node_list[i], p);
            parse_ok = false;
            break;
          }
          const ObColumnSchema* lcs = schema.find_column_info(lid);
          const ObColumnSchema* rcs = get_column_schema(table_id_joined, rid);
          if (lcs == NULL || rcs == NULL) {
            TBSYS_LOG(ERROR, "we should not reach this  column name %s %s ", node_list[i], p);
            parse_ok = false;
            break;
          }
          if (lcs->get_type() != rcs->get_type()) {
            //the join should be happen between too columns have the same type
            if (lcs->get_type() == ObPreciseDateTimeType &&
                (rcs->get_type() == ObCreateTimeType || rcs->get_type() == ObModifyTimeType)) {
              //except that ObPreciseDateTimeType join with ObCreateTimeType or ObModifyTimeType
            } else {
              TBSYS_LOG(ERROR, "join column have different types %s %s ", node_list[i], p);
              parse_ok = false;
              break;
            }
          }
          if (lcs->get_type() == ObCreateTimeType || lcs->get_type() == ObModifyTimeType) {
            TBSYS_LOG(ERROR, "column %s can not be jonined, it hase type ObCreateTimeType or ObModifyTimeType", node_list[i]);
            parse_ok = false;
            break;
          }
          join_info.add_correlated_column(lid, rid);
        }
        if (parse_ok) {
          schema.add_join_info(join_info);
        }
      }
      free(str);
      str = NULL;
    }
  }
  if (str) free(str);
  return parse_ok;

}

uint64_t ObSchemaManager::get_table_id(const char* name) const {
  uint64_t table_id = OB_INVALID_ID;
  if (name != NULL) {
    for (int32_t i = 0; i < schema_size_; ++i) {
      if (schemas_[i].have_inited() && strncmp(schemas_[i].get_table_name(), name, OB_MAX_TABLE_NAME_LENGTH) == 0) {
        table_id = schemas_[i].get_table_id();
        break;
      }
    }
  }
  return table_id;
}
uint64_t ObSchemaManager::get_table_id(const ObString& name) const {
  uint64_t table_id = OB_INVALID_ID;
  {
    for (int32_t i = 0; i < schema_size_; ++i) {
      if (schemas_[i].have_inited() && name.compare(schemas_[i].get_table_name()) == 0) {
        table_id = schemas_[i].get_table_id();
        break;
      }
    }
  }
  return table_id;
}

const char* ObSchemaManager::get_table_name(const uint64_t table_id) const {
  const char*  table_name = NULL;
  if (table_id != OB_INVALID_ID) {
    for (int32_t i = 0; i < schema_size_; ++i) {
      if (schemas_[i].have_inited() && schemas_[i].get_table_id() == table_id) {
        table_name = schemas_[i].get_table_name();
        break;
      }
    }
  }
  return table_name;
}

const ObSchema* ObSchemaManager::get_table_schema(const uint64_t table_id) const {
  const ObSchema* table = NULL;
  if (table_id != OB_INVALID_ID) {
    for (int32_t i = 0; i < schema_size_; ++i) {
      if (schemas_[i].have_inited() && schemas_[i].get_table_id() == table_id) {
        table = &schemas_[i];
        break;
      }
    }
  }
  return table;
}

uint64_t ObSchemaManager::get_column_id(const uint64_t table_id, const char* col_name) const {
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchema* column = NULL;
  const ObSchema* table = get_table_schema(table_id);
  if (table != NULL) {
    column = table->find_column_info(col_name);
  }
  if (column != NULL) {
    column_id = column->get_id();
  }
  return column_id;
}
uint64_t ObSchemaManager::get_column_id(const uint64_t table_id, const ObString& col_name) const {
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchema* column = NULL;
  const ObSchema* table = get_table_schema(table_id);
  if (table != NULL) {
    column = table->find_column_info(col_name);
  }
  if (column != NULL) {
    column_id = column->get_id();
  }
  return column_id;
}

const char* ObSchemaManager::get_column_name(const uint64_t table_id, const uint64_t col_id) const {
  const char* column_name = NULL;
  const ObColumnSchema* column = get_column_schema(table_id, col_id);
  if (column != NULL) {
    column_name = column->get_name();
  }
  return column_name;
}

const ObColumnSchema* ObSchemaManager::get_column_schema(const uint64_t table_id, const uint64_t col_id) const {
  const ObColumnSchema* column = NULL;
  const ObSchema* table = get_table_schema(table_id);
  if (table != NULL) {
    column = table->find_column_info(col_id);
  }
  return column;
}

const char* ObSchemaManager::get_app_name() const {
  return app_name_;
}

// if a update server table has the same name with a chunk server table
// that means, the chunk one must merge the update one to get the final
// result. so one is the other's buddy.
//const ObSchema* ObSchemaManager::find_buddy_table(const ObSchema* table) const
//{
//  const ObSchema* buddy_table = NULL;
//  if (table != NULL)
//  {
//    uint64_t buddy_table_id = 0;
//    char buddy_table_name[OB_MAX_TABLE_NAME_LENGTH];
//    if (is_update_server_table(table->get_table_name()))
//    {
//      strncpy(buddy_table_name,
//          table->get_table_name() + UPDATE_TABLE_PREFIX_LENGTH,
//          OB_MAX_TABLE_NAME_LENGTH);
//    }
//    else
//    {
//      strncpy(buddy_table_name, STR_UPDATE_TABLE_PREFIX, UPDATE_TABLE_PREFIX_LENGTH);
//      strncpy(buddy_table_name + UPDATE_TABLE_PREFIX_LENGTH,
//          table->get_table_name(),
//          OB_MAX_TABLE_NAME_LENGTH - UPDATE_TABLE_PREFIX_LENGTH);
//    }
//    buddy_table_name[OB_MAX_TABLE_NAME_LENGTH - 1] = '\0';

//    buddy_table_id = get_table_id(buddy_table_name);

//    if (buddy_table_id != OB_INVALID_ID)
//    {
//      buddy_table = get_table_schema(buddy_table_id);
//      if (buddy_table != NULL)
//      {
//        if (table->get_table_type() == ObSchema::MEM_TABLE)
//        {
//          if (buddy_table->get_table_type() != ObSchema::SSTABLE_IN_DISK &&
//              buddy_table->get_table_type() != ObSchema::SSTABLE_IN_RAM)
//          {
//            TBSYS_LOG(WARN, "table %s and his buddy %s have the same type",
//                table->get_table_name(), buddy_table->get_table_name());
//            buddy_table = NULL;
//          }
//        }
//        else
//        {
//          if (buddy_table->get_table_type() != ObSchema::MEM_TABLE)
//          {
//            TBSYS_LOG(WARN, "table %s and his buddy %s have the same type",
//                table->get_table_name(), buddy_table->get_table_name());
//            buddy_table = NULL;
//          }
//        }
//      }
//    }
//  }
//  return buddy_table;
//}

bool ObSchemaManager::is_compatible(const ObSchemaManager& schema_manager) const {
  bool compatible = true;
  if (strncmp(app_name_, schema_manager.get_app_name(), OB_MAX_APP_NAME_LENGTH) != 0) {
    compatible = false;
  }
  for (int32_t i = 0; compatible && i < schema_size_; ++i) {
    assert(schemas_[i].have_inited());
    const ObSchema* opp_table = schema_manager.get_table_schema(schemas_[i].get_table_id());
    if (opp_table == NULL) {
      continue;
    }
    //if (opp_table->get_table_type() == ObSchema::MEM_TABLE)
    //{
    //  if (schemas_[i].get_table_type() != ObSchema::MEM_TABLE)
    //  {
    //    compatible = false;
    //    break;
    //  }
    //}
    //else
    //{
    //  if (schemas_[i].get_table_type() == ObSchema::MEM_TABLE)
    //  {
    //    compatible = false;
    //    break;
    //  }
    //}
    for (int j = 0; j < schemas_[i].column_info_num_ ; ++j) {
      const ObColumnSchema* column_schema = &schemas_[i].columns_[j];
      const ObColumnSchema* opp_column_schema = opp_table->find_column_info(column_schema->get_id());
      if (opp_column_schema == NULL) continue;
      if (column_schema->get_type() != opp_column_schema->get_type()) {
        compatible = false;
        break;
      }
      if (column_schema->get_type() == ObVarcharType) {
        if (column_schema->get_size() != opp_column_schema->get_size()) {
          compatible = false;
          break;
        }
      }

    }//end every column
  } //end every schemas
  return compatible;
}
const ObSchema* ObSchemaManager::begin() const {
  return schemas_;
}
const ObSchema* ObSchemaManager::end() const {
  return schemas_ + schema_size_;
}

ObSchemaManager::~ObSchemaManager() {
}
void ObSchemaManager::print_info() const {
  TBSYS_LOG(INFO, "app_name = %s table_count = %ld", app_name_, schema_size_);
  for (int64_t i = 0; i < schema_size_; ++i) {
    if (schemas_[i].have_inited()) {
      schemas_[i].print_info();
    }
  }
}

//bool ObSchemaManager::is_update_server_table(const char* table_name)
//{
//  return strncmp(STR_UPDATE_TABLE_PREFIX, table_name, UPDATE_TABLE_PREFIX_LENGTH) == 0;
//}

DEFINE_SERIALIZE(ObSchemaManager) {
  int ret = 0;
  int64_t tmp_pos = pos;
  ret = BaseInited::serialize(buf, buf_len, tmp_pos);
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, version_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, max_table_id_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, schema_size_);
    if (schema_size_ > OB_MAX_TABLE_NUMBER) {
      TBSYS_LOG(ERROR, "bugs schema_size_ %ld error", schema_size_);
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    for (int i = 0; i < schema_size_; i++) {
      ret = schemas_[i].serialize(buf, buf_len, tmp_pos);
      if (OB_SUCCESS != ret) break;
    }
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vstr(buf, buf_len, tmp_pos, app_name_);
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_DESERIALIZE(ObSchemaManager) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ret = BaseInited::deserialize(buf, data_len, tmp_pos);
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &version_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&max_table_id_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &schema_size_);
  }
  if (OB_SUCCESS == ret) {
    if (schema_size_ < 0 || schema_size_ > OB_MAX_TABLE_NUMBER) {
      TBSYS_LOG(ERROR, "bugs, schema_size_ %ld error", schema_size_);
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    for (int i = 0; i < schema_size_; ++i) {
      ret = schemas_[i].deserialize(buf, data_len, tmp_pos);
      if (OB_SUCCESS != ret) break;
    }
  }
  if (OB_SUCCESS == ret) {
    int64_t len = 0;
    serialization::decode_vstr(buf, data_len, tmp_pos,
                               app_name_, OB_MAX_APP_NAME_LENGTH, &len);
    if (-1 == len) {
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_GET_SERIALIZE_SIZE(ObSchemaManager) {
  int64_t len = BaseInited::get_serialize_size();
  len += serialization::encoded_length_vi64(version_);
  len += serialization::encoded_length_vi64(max_table_id_);
  len += serialization::encoded_length_vi64(schema_size_);
  if (schema_size_ < 0 || schema_size_ > OB_MAX_TABLE_NUMBER) {
    TBSYS_LOG(ERROR, "bugs schema_size_ %ld error", schema_size_);
  } else {
    for (int i = 0; i < schema_size_; ++i) {
      len += schemas_[i].get_serialize_size();
    }
    len += serialization::encoded_length_vstr(app_name_);
  }
  return len;
}

/*-----------------------------------------------------------------------------
 * ObColumnSchemaV2
 *-----------------------------------------------------------------------------*/

ObColumnSchemaV2::ObColumnSchemaV2() : maintained_(false),
  table_id_(OB_INVALID_ID), column_group_id_(OB_INVALID_ID),
  column_id_(OB_INVALID_ID), size_(0), type_(ObNullType), column_group_next_(NULL)
{}

uint64_t    ObColumnSchemaV2::get_id()   const {
  return column_id_;
}

const char* ObColumnSchemaV2::get_name() const {
  return name_;
}

ColumnType  ObColumnSchemaV2::get_type() const {
  return type_;
}

int64_t  ObColumnSchemaV2::get_size() const {
  return size_;
}

uint64_t ObColumnSchemaV2::get_table_id() const {
  return table_id_;
}

bool  ObColumnSchemaV2::is_maintained() const {
  return maintained_;
}

uint64_t ObColumnSchemaV2::get_column_group_id() const {
  return column_group_id_;
}

void ObColumnSchemaV2::set_table_id(const uint64_t id) {
  table_id_ = id;
}

void ObColumnSchemaV2::set_column_id(const uint64_t id) {
  column_id_ = id;
}

void ObColumnSchemaV2::set_column_name(const char* name) {
  if (NULL != name && '\0' != *name) {
    snprintf(name_, sizeof(name_), "%s", name);
  }
}

void ObColumnSchemaV2::set_column_type(const ColumnType type) {
  type_ = type;
}

void ObColumnSchemaV2::set_column_size(int64_t size) { //only used when type is varchar
  size_ = size;
}

void ObColumnSchemaV2::set_column_group_id(const uint64_t id) {
  column_group_id_ = id;
}

void ObColumnSchemaV2::set_maintained(const bool maintained) {
  maintained_ = maintained;
}

void ObColumnSchemaV2::set_join_info(const uint64_t join_table, const uint64_t left_column_id,
                                     const uint64_t correlated_column,
                                     const int32_t start_pos, const int32_t end_pos) {
  join_info_.join_table_ = join_table;
  join_info_.left_column_id_ = left_column_id;
  join_info_.correlated_column_ = correlated_column;
  join_info_.start_pos_ = start_pos;
  join_info_.end_pos_ = end_pos;
}

const ObColumnSchemaV2::ObJoinInfo* ObColumnSchemaV2::get_join_info() const {
  const ObJoinInfo* info = NULL;
  if (join_info_.join_table_ != OB_INVALID_ID) {
    info = &join_info_;
  }
  return info;
}

bool ObColumnSchemaV2::operator==(const ObColumnSchemaV2& r) const {
  bool ret = false;
  if (table_id_ == r.table_id_ && column_group_id_ == r.column_group_id_ &&
      column_id_ == r.column_id_) {
    ret = true;
  }
  return ret;
}

void ObColumnSchemaV2::print_info() const {
  TBSYS_LOG(DEBUG, "COLUMN:(%lu,%lu,%lu)", table_id_, column_group_id_, column_id_);
  TBSYS_LOG(DEBUG, "JOIN  :(%lu,%lu,%lu)", join_info_.join_table_, join_info_.left_column_id_, join_info_.correlated_column_);
}


ColumnType ObColumnSchemaV2::convert_str_to_column_type(const char* str) {
  ColumnType type = ObNullType;
  if (strcmp(str, STR_COLUMN_TYPE_INT) == 0) {
    type = ObIntType;
  } else if (strcmp(str, STR_COLUMN_TYPE_FLOAT) == 0) {
    type = ObFloatType;
  } else if (strcmp(str, STR_COLUMN_TYPE_DOUBLE) == 0) {
    type = ObDoubleType;
  } else if (strcmp(str, STR_COLUMN_TYPE_VCHAR) == 0) {
    type = ObVarcharType;
  } else if (strcmp(str, STR_COLUMN_TYPE_DATETIME) == 0) {
    type = ObDateTimeType;
  } else if (strcmp(str, STR_COLUMN_TYPE_PRECISE_DATETIME) == 0) {
    type = ObPreciseDateTimeType;
  } else if (strcmp(str, STR_COLUMN_TYPE_SEQ) == 0) {
    type = ObSeqType;
  } else if (strcmp(str, STR_COLUMN_TYPE_C_TIME) == 0) {
    type = ObCreateTimeType;
  } else if (strcmp(str, STR_COLUMN_TYPE_M_TIME) == 0) {
    type = ObModifyTimeType;
  } else {
    TBSYS_LOG(ERROR, "column type %s not be supported", str);
  }
  return type;
}

ObColumnSchemaV2* find_column_info_tmp(ObColumnSchemaV2* columns_info, const int64_t array_size, const char* column_name) {
  ObColumnSchemaV2* info = NULL;
  for (int32_t i = 0; i < array_size; ++i) {
    if (NULL != columns_info[i].get_name()) {
      if (strlen(columns_info[i].get_name()) == strlen(column_name) &&
          0 == strncmp(columns_info[i].get_name(), column_name, strlen(column_name))) {
        info = &columns_info[i];
        break;
      }
    }
  }
  return info;
}

DEFINE_SERIALIZE(ObColumnSchemaV2) {
  int ret = 0;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_bool(buf, buf_len, tmp_pos, maintained_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, static_cast<int64_t>(table_id_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, static_cast<int64_t>(column_group_id_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, static_cast<int64_t>(column_id_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, size_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, type_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vstr(buf, buf_len, tmp_pos, name_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, static_cast<int64_t>(join_info_.join_table_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, static_cast<int64_t>(join_info_.left_column_id_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, join_info_.start_pos_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, join_info_.end_pos_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, static_cast<int64_t>(join_info_.correlated_column_));
  }

  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObColumnSchemaV2) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_bool(buf, data_len, tmp_pos, &maintained_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&table_id_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&column_group_id_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&column_id_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &size_);
  }
  if (OB_SUCCESS == ret) {
    int32_t type = 0;
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &type);
    if (OB_SUCCESS == ret) {
      type_ = static_cast<ColumnType>(type);
    }
  }
  if (OB_SUCCESS == ret) {
    int64_t len = 0;
    serialization::decode_vstr(buf, data_len, tmp_pos,
                               name_, OB_MAX_COLUMN_NAME_LENGTH, &len);
    if (-1 == len) {
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&join_info_.join_table_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&join_info_.left_column_id_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &join_info_.start_pos_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &join_info_.end_pos_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&join_info_.correlated_column_));
  }

  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObColumnSchemaV2) {
  int64_t len = serialization::encoded_length_bool(maintained_);
  len += serialization::encoded_length_vi64(table_id_);
  len += serialization::encoded_length_vi64(column_group_id_);
  len += serialization::encoded_length_vi64(column_id_);
  len += serialization::encoded_length_vi64(size_);
  len += serialization::encoded_length_vi32(type_);
  len += serialization::encoded_length_vstr(name_);
  len += serialization::encoded_length_vi64(join_info_.join_table_);
  len += serialization::encoded_length_vi64(join_info_.left_column_id_);
  len += serialization::encoded_length_vi32(join_info_.start_pos_);
  len += serialization::encoded_length_vi32(join_info_.end_pos_);
  len += serialization::encoded_length_vi64(join_info_.correlated_column_);
  return len;
}


/*-----------------------------------------------------------------------------
 *  ObTableSchema
 *-----------------------------------------------------------------------------*/

ObTableSchema::ObTableSchema() : table_id_(OB_INVALID_ID), version_(OB_SCHEMA_VERSION_TWO),
  create_time_column_id_(OB_INVALID_ID),
  modify_time_column_id_(OB_INVALID_ID) {
  name_[0] = '\0';
}
uint64_t    ObTableSchema::get_table_id()   const {
  return table_id_;
}

ObTableSchema::TableType   ObTableSchema::get_table_type() const {
  return table_type_;
}

const char* ObTableSchema::get_table_name() const {
  return name_;
}

const char* ObTableSchema::get_compress_func_name() const {
  return compress_func_name_;
}

uint64_t ObTableSchema::get_max_column_id() const {
  return max_column_id_;
}

int32_t ObTableSchema::get_version() const {
  return version_;
}

int32_t ObTableSchema::get_split_pos() const {
  return rowkey_split_;
}

int32_t ObTableSchema::get_rowkey_max_length() const {
  return rowkey_max_length_;
}

bool ObTableSchema::is_pure_update_table() const {
  return is_pure_update_table_;
}

bool ObTableSchema::is_use_bloomfilter()   const {
  return is_use_bloomfilter_;
}

bool ObTableSchema::is_row_key_fixed_len() const {
  return is_row_key_fixed_len_;
}

int32_t ObTableSchema::get_block_size()    const {
  return block_size_;
}

int ObTableSchema::get_expire_condition(uint64_t& column_id, int64_t& duration) const {
  int ret = OB_SUCCESS;
  if (0 == expire_info_.column_id_ || -1 == expire_info_.duration_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    column_id = expire_info_.column_id_;
    duration  = expire_info_.duration_;
  }
  return ret;
}

uint64_t ObTableSchema::get_create_time_column_id() const {
  return create_time_column_id_;
}

uint64_t ObTableSchema::get_modify_time_column_id() const {
  return modify_time_column_id_;
}

void ObTableSchema::set_table_id(const uint64_t id) {
  table_id_ = id;
}

void ObTableSchema::set_version(const int32_t version) {
  version_ = version;
}

void ObTableSchema::set_max_column_id(const uint64_t id) {
  max_column_id_ = id;
}

void ObTableSchema::set_table_type(TableType type) {
  table_type_ = type;
}

void ObTableSchema::set_split_pos(const int64_t split_pos) {
  rowkey_split_ = split_pos;
}

void ObTableSchema::set_rowkey_max_length(const int64_t len) {
  rowkey_max_length_ = len;
}

void ObTableSchema::set_block_size(const int64_t block_size) {
  block_size_ = block_size;
}

void ObTableSchema::set_table_name(const char* name) {
  if (name != NULL && *name != '\0') {
    snprintf(name_, sizeof(name_), "%s", name);
  }
}

void ObTableSchema::set_compressor_name(const char* compressor) {
  if (compressor != NULL && *compressor != '\0') {
    snprintf(compress_func_name_, sizeof(compress_func_name_), "%s", compressor);
  }
}

void ObTableSchema::set_pure_update_table(bool is_pure) {
  is_pure_update_table_ = is_pure;
}

void ObTableSchema::set_use_bloomfilter(bool use_bloomfilter) {
  is_use_bloomfilter_ = use_bloomfilter;
}

void ObTableSchema::set_rowkey_fixed_len(bool fixed_len) {
  is_row_key_fixed_len_ = fixed_len;
}

void ObTableSchema::set_create_time_column(uint64_t id) {
  create_time_column_id_ = id;
}

void ObTableSchema::set_expire_info(ExpireInfo& expire_info) {
  expire_info_.column_id_ = expire_info.column_id_;
  expire_info_.duration_  = expire_info.duration_;
}

void ObTableSchema::set_modify_time_column(uint64_t id) {
  modify_time_column_id_ = id;
}

bool ObTableSchema::operator ==(const ObTableSchema& r) const {
  bool ret = false;

  if ((table_id_ != OB_INVALID_ID && table_id_ == r.table_id_) ||
      (('\0' != *name_ && '\0' != *r.name_) && (strlen(name_) == strlen(r.name_))
       && (0 == strncmp(name_, r.name_, strlen(name_))))) {
    ret = true;
  }
  return ret;
}

bool ObTableSchema::operator ==(const uint64_t table_id) const {
  bool ret = false;

  if (table_id != OB_INVALID_ID && table_id == table_id_) {
    ret = true;
  }
  return ret;
}

bool ObTableSchema::operator ==(const ObString& table_name) const {
  bool ret = false;
  if (0 == table_name.compare(name_)) {
    ret = true;
  }
  return ret;
}

DEFINE_SERIALIZE(ObTableSchema) {
  int ret = 0;
  int64_t tmp_pos = pos;

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_id_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, max_column_id_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, rowkey_split_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, rowkey_max_length_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, block_size_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, table_type_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vstr(buf, buf_len, tmp_pos, name_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vstr(buf, buf_len, tmp_pos, compress_func_name_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_bool(buf, buf_len, tmp_pos, is_pure_update_table_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_bool(buf, buf_len, tmp_pos, is_use_bloomfilter_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_bool(buf, buf_len, tmp_pos, is_row_key_fixed_len_);
  }
  if (OB_SCHEMA_VERSION < get_version()) {
    if (OB_SUCCESS == ret) {
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, expire_info_.column_id_);
    }
    if (OB_SUCCESS == ret) {
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, expire_info_.duration_);
    }
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTableSchema) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                     reinterpret_cast<int64_t*>(&table_id_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos,
                                     reinterpret_cast<int64_t*>(&max_column_id_));
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &rowkey_split_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &rowkey_max_length_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &block_size_);
  }

  if (OB_SUCCESS == ret) {
    int32_t table_type = 0;
    ret = serialization::decode_vi32(buf, data_len, tmp_pos, &table_type);
    table_type_ = static_cast<TableType>(table_type);
  }
  if (OB_SUCCESS == ret) {
    int64_t len = 0;
    serialization::decode_vstr(buf, data_len, tmp_pos,
                               name_, OB_MAX_TABLE_NAME_LENGTH, &len);
    if (-1 == len) {
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    int64_t len = 0;
    serialization::decode_vstr(buf, data_len, tmp_pos,
                               compress_func_name_, OB_MAX_TABLE_NAME_LENGTH, &len);
    if (-1 == len) {
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_pure_update_table_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_use_bloomfilter_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_row_key_fixed_len_);
  }
  if (OB_SCHEMA_VERSION < get_version()) {
    if (OB_SUCCESS == ret) {
      ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&expire_info_.column_id_));
    }
    if (OB_SUCCESS == ret) {
      ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&expire_info_.duration_));
    }
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTableSchema) {
  int64_t len =  serialization::encoded_length_vi64(table_id_);
  len += serialization::encoded_length_vi64(max_column_id_);
  len += serialization::encoded_length_vi64(rowkey_split_);
  len += serialization::encoded_length_vi64(rowkey_max_length_);
  len += serialization::encoded_length_vi32(block_size_);
  len += serialization::encoded_length_vi32(table_type_);
  len += serialization::encoded_length_vstr(name_);
  len += serialization::encoded_length_vstr(compress_func_name_);
  len += serialization::encoded_length_bool(is_pure_update_table_);
  len += serialization::encoded_length_bool(is_use_bloomfilter_);
  len += serialization::encoded_length_bool(is_row_key_fixed_len_);
  if (OB_SCHEMA_VERSION < get_version()) {
    len += serialization::encoded_length_vi64(expire_info_.column_id_);
    len += serialization::encoded_length_vi64(expire_info_.duration_);
  }
  return len;
}



/*-----------------------------------------------------------------------------
 *  ObSchemaManagerV2
 *-----------------------------------------------------------------------------*/
ObSchemaManagerV2::ObSchemaManagerV2(): schema_magic_(OB_SCHEMA_MAGIC_NUMBER), version_(OB_SCHEMA_VERSION_TWO),
  timestamp_(0), max_table_id_(OB_INVALID_ID), column_nums_(0),
  table_nums_(0), drop_column_group_(false), hash_sorted_(false),
  column_group_nums_(0) {
  app_name_[0] = '\0';
}

ObSchemaManagerV2::ObSchemaManagerV2(const int64_t timestamp): schema_magic_(OB_SCHEMA_MAGIC_NUMBER),
  version_(OB_SCHEMA_VERSION_TWO), timestamp_(timestamp),
  max_table_id_(OB_INVALID_ID), column_nums_(0),
  table_nums_(0), drop_column_group_(false), hash_sorted_(false),
  column_group_nums_(0) {
  app_name_[0] = '\0';
}

ObSchemaManagerV2::~ObSchemaManagerV2() {
  if (hash_sorted_) {
    column_hash_map_.destroy();
    id_hash_map_.destroy();
  }
  TBSYS_LOG(DEBUG, "de ObSchemaManagerV2");
}

const ObTableSchema* ObSchemaManagerV2::table_begin() const {
  return table_infos_;
}

const ObTableSchema* ObSchemaManagerV2::table_end() const {
  return table_infos_ + table_nums_;
}

const ObTableSchema* ObSchemaManagerV2::get_table_schema(const char* table_name) const {
  const ObTableSchema* table = NULL;

  if (table_name != NULL && *table_name != '\0' && table_nums_ > 0) {
    ObTableSchema tmp;
    tmp.set_table_name(table_name);
    table = std::find(table_infos_, table_infos_ + table_nums_, tmp);
    if (table == (table_infos_ + table_nums_)) {
      table = NULL;
    }
  }
  return table;
}

const ObTableSchema* ObSchemaManagerV2::get_table_schema(const ObString& table_name) const {
  const ObTableSchema* table = NULL;

  if (table_name.ptr() != NULL && table_name.length() > 0 && table_nums_ > 0) {
    table = std::find(table_infos_, table_infos_ + table_nums_, table_name);
    if (table == (table_infos_ + table_nums_)) {
      table = NULL;
    }
  }
  return table;
}


const ObTableSchema* ObSchemaManagerV2::get_table_schema(const uint64_t table_id) const {
  const ObTableSchema* table = NULL;

  if (table_nums_ > 0) {
    ObTableSchema tmp;
    tmp.set_table_id(table_id);
    table = std::find(table_infos_, table_infos_ + table_nums_, tmp);
    if (table == (table_infos_ + table_nums_)) {
      table = NULL;
    }
  }
  return table;
}

ObTableSchema* ObSchemaManagerV2::get_table_schema(const uint64_t table_id) {
  ObTableSchema* table = NULL;

  if (table_nums_ > 0) {
    ObTableSchema tmp;
    tmp.set_table_id(table_id);
    table = std::find(table_infos_, table_infos_ + table_nums_, tmp);
    if (table == (table_infos_ + table_nums_)) {
      table = NULL;
    }
  }
  return table;
}

uint64_t ObSchemaManagerV2::get_create_time_column_id(const uint64_t table_id) const {
  uint64_t id = OB_INVALID_ID;
  const ObTableSchema* table = get_table_schema(table_id);
  if (table != NULL) {
    id = table->get_create_time_column_id();
  }
  return id;
}

uint64_t ObSchemaManagerV2::get_modify_time_column_id(const uint64_t table_id) const {
  uint64_t id = OB_INVALID_ID;
  const ObTableSchema* table = get_table_schema(table_id);
  if (table != NULL) {
    id = table->get_modify_time_column_id();
  }
  return id;
}

struct __table_sort {
  __table_sort(tbsys::CConfig& config): config_(config) {}
  bool operator()(const std::string& l, const std::string& r) {
    uint64_t l_table_id = config_.getInt(l.c_str(), STR_TABLE_ID, 0);
    uint64_t r_table_id = config_.getInt(r.c_str(), STR_TABLE_ID, 0);
    return l_table_id < r_table_id;
  }
  tbsys::CConfig& config_;
};


bool ObSchemaManagerV2::parse_from_file(const char* file_name, tbsys::CConfig& config) {
  bool parse_ok = true;

  if (parse_ok && file_name != NULL && config.load(file_name) != EXIT_SUCCESS) {
    TBSYS_LOG(ERROR, "can not open config file, file name is %s", file_name);
    parse_ok = false;
  }

  TBSYS_LOG(DEBUG, "config:%p", &config);

  if (parse_ok) {
    const char* p = config.getString(STR_SECTION_APP_NAME, STR_KEY_APP_NAME, NULL);

    int length = 0;
    if (p != NULL) length = strlen(p);
    if (p == NULL || length >= OB_MAX_APP_NAME_LENGTH) {
      TBSYS_LOG(ERROR, "parse [%s]  %s error", STR_SECTION_APP_NAME, STR_KEY_APP_NAME);
      parse_ok = false;
    } else {
      strncpy(app_name_, p, OB_MAX_APP_NAME_LENGTH);
      app_name_[OB_MAX_APP_NAME_LENGTH - 1] = '\0';
      max_table_id_ = config.getInt(STR_SECTION_APP_NAME, STR_MAX_TABLE_ID, 0);
      if (max_table_id_ > MAX_ID_USED) {
        TBSYS_LOG(ERROR, "we limit our table id less than %lu", MAX_ID_USED);
        parse_ok = false;
      }
      if (max_table_id_ < 1) {
        TBSYS_LOG(ERROR, "max_table_id is %lu", max_table_id_);
        parse_ok = false;
      }

      vector<string> sections;
      config.getSectionName(sections);
      table_nums_ = sections.size() - 1;
      if (table_nums_ > OB_MAX_TABLE_NUMBER || table_nums_ < 1) {
        TBSYS_LOG(ERROR, "%ld error table number", table_nums_);
        parse_ok = false;
        table_nums_ = 0;
      }
      // app section and sections.size() - 1 tables

      //sort the table
      std::sort(sections.begin(), sections.end(), __table_sort(config));

      uint32_t index = 0;
      for (vector<string>::iterator it = sections.begin();
           it != sections.end() && parse_ok; ++it) {
        if (strcmp(it->c_str(), STR_SECTION_APP_NAME) == 0) {
          continue;
        }
        TBSYS_LOG(DEBUG, "table name :%s ", it->c_str());

        if (!parse_one_table(it->c_str(), config, table_infos_[index])) {
          parse_ok = false;
          table_nums_ = 0;
          break;
        }
        ++index;
      }

      if (parse_ok && sort_column() != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "sort column failed");
        parse_ok = false;
      }

      TBSYS_LOG(DEBUG, "config:%p", &config);
      // we must get all tables, so we can check join info;
      index = 0;
      for (vector<string>::iterator it = sections.begin();
           it != sections.end() && parse_ok; ++it) {
        if (strcmp(it->c_str(), STR_SECTION_APP_NAME) == 0) {
          continue;
        }
        if (!parse_join_info(it->c_str(), config, table_infos_[index])) {
          parse_ok = false;
          table_nums_ = 0;
          break;
        }
        ++index;
      }
    }

  }
  return parse_ok;
}

bool ObSchemaManagerV2::parse_one_table(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema) {
  uint64_t table_id = OB_INVALID_ID;
  int type = 0;
  //ObSchema::TableType table_type = ObSchema::INVALID;
  ObTableSchema::TableType table_type = ObTableSchema::INVALID;
  int32_t rowkey_split = 0;
  int32_t rowkey_max_length = 0;
  uint64_t max_column_id = 0;

  bool parse_ok = true;
  const char* name = section_name;
  int name_len = 0;
  if (name != NULL) name_len = strlen(name);
  if (name == NULL || name_len >= OB_MAX_TABLE_NAME_LENGTH) {
    TBSYS_LOG(ERROR, "table_name is missing");
    parse_ok = false;
  }
  if (parse_ok) {
    table_id = config.getInt(section_name, STR_TABLE_ID, 0);
    if (table_id == 0) {
      TBSYS_LOG(ERROR, "table id can not be 0");
      parse_ok = false;
    }
    if (table_id > max_table_id_) {
      TBSYS_LOG(ERROR, "table id %lu greater than max_table_id %lu", table_id, max_table_id_);
      parse_ok = false;
    }
  }
  if (parse_ok && get_table_schema(table_id) != NULL) {
    TBSYS_LOG(ERROR, "table id %lu have been used", table_id);
    parse_ok = false;
  }

  if (parse_ok) {
    type = config.getInt(section_name, STR_TABLE_TYPE, 0);

    if (type == ObTableSchema::SSTABLE_IN_DISK) {
      table_type = ObTableSchema::SSTABLE_IN_DISK;
    } else if (type == ObTableSchema::SSTABLE_IN_RAM) {
      table_type = ObTableSchema::SSTABLE_IN_RAM;
    } else {
      TBSYS_LOG(ERROR, "%d is a invalid table type", type);
      parse_ok = false;
    }
  }

  if (parse_ok) {
    rowkey_split = config.getInt(section_name, STR_ROWKEY_SPLIT, 0);
    rowkey_max_length = config.getInt(section_name, STR_ROWKEY_LENGTH, 0);
    max_column_id = config.getInt(section_name, STR_MAX_COLUMN_ID, 0);
    if (max_column_id < 1) {
      TBSYS_LOG(ERROR, "max_column_id is %lu", max_column_id);
      parse_ok = false;
    }
    if (max_column_id > MAX_ID_USED) {
      TBSYS_LOG(ERROR, "we limit our column id less than %lu", MAX_ID_USED);
      parse_ok = false;
    }
    if (rowkey_max_length <= 0 || rowkey_split < 0) {
      TBSYS_LOG(ERROR, "rowkey length must greater than 0");
      parse_ok = false;
    }
  }
  if (parse_ok && rowkey_max_length < rowkey_split) {
    TBSYS_LOG(ERROR, "rowkey length must greater than rowkey split");
    parse_ok = false;
  }
  const char* compress_func_name = NULL;
  int32_t block_size = 0;
  int32_t is_use_bloomfilter = 0;
  int32_t is_row_key_fixed_len = 1;
  if (parse_ok) {
    compress_func_name = config.getString(section_name, STR_COMPRESS_FUNC_NAME, "none");
    block_size = config.getInt(section_name, STR_BLOCK_SIZE, 64);
    is_use_bloomfilter = config.getInt(section_name, STR_USE_BLOOMFILTER, 0);
    is_row_key_fixed_len = config.getInt(section_name, STR_ROWKEY_IS_FIXED_LENGTH, 1);
  }

  if (parse_ok) {
    schema.set_table_id(table_id);
    schema.set_max_column_id(max_column_id);
    schema.set_table_name(name);
    schema.set_table_type(table_type);
    schema.set_split_pos(rowkey_split);
    schema.set_rowkey_max_length(rowkey_max_length);
    schema.set_compressor_name(compress_func_name);
    schema.set_block_size(block_size);
    schema.set_use_bloomfilter(is_use_bloomfilter);
    schema.set_rowkey_fixed_len(is_row_key_fixed_len);
  }

  if (parse_ok) {
    parse_ok = parse_expire_info(section_name, config, schema);
  }

  if (parse_ok) {
    parse_ok = parse_column_info(section_name, config, schema);
    //if (parse_ok &&  schema.sort_column() != OB_SUCCESS)
    //{
    //  TBSYS_LOG(ERROR,"sort column failed");
    //  parse_ok = false;
    //}
  }
  return parse_ok;
}

bool ObSchemaManagerV2::parse_column_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema) {
  bool parse_ok = true;
  char* str = NULL;
  uint64_t id = OB_INVALID_ID;
  uint64_t maintained = 0;
  ColumnType type = ObNullType;
  vector<const char*> column_info_strs;

  bool has_create_time_column = false;
  bool has_modify_time_column = false;

  if (section_name != NULL) {
    column_info_strs = config.getStringList(section_name, STR_COLUMN_INFO);
  }
  if (column_info_strs.empty()) {
    parse_ok = false;
  }
  int size = 0;

  ObColumnSchemaV2* columns_info = new(std::nothrow) ObColumnSchemaV2 [OB_MAX_COLUMN_NUMBER];
  int32_t column_index = 0;

  if (NULL == columns_info) {
    TBSYS_LOG(ERROR, "alloc columns_info failed");
    parse_ok = false;
  }

  for (vector<const char*>::const_iterator it = column_info_strs.begin();
       it != column_info_strs.end() && parse_ok; ++it) {
    size = 0;
    str = strdup(*it);
    vector<char*> node_list;
    str = str_trim(str);
    tbsys::CStringUtil::split(str, ",", node_list);
    if (node_list.size() < POS_COLUM_TYPE + 1) {
      TBSYS_LOG(ERROR, "parse column |%s| error", str);
      parse_ok = false;
    }
    if (parse_ok) {
      maintained = strtoll(node_list[POS_COLUM_MANTAINED], NULL, 10);
      if (maintained != 0 && maintained != 1) {
        TBSYS_LOG(ERROR, "maintained %lu is unacceptable", maintained);
        parse_ok = false;
      }
      if (maintained == 1) {
        schema.set_pure_update_table(false);
      }
    }
    if (parse_ok) {
      id = strtoll(node_list[POS_COLUM_ID], NULL, 10);
      if (id == 0) {
        TBSYS_LOG(ERROR, "column id error id = %lu", id);
        parse_ok = false;
      }
    }

    if (parse_ok) {
      type = ObColumnSchemaV2::convert_str_to_column_type(node_list[POS_COLUM_TYPE]);
      if (type == ObNullType) {
        TBSYS_LOG(ERROR, "column type error |%s|", node_list[POS_COLUM_TYPE]);
        parse_ok = false;
      }
    }

    if (parse_ok && type == ObVarcharType) {
      if (node_list.size() < POS_COLUM_SIZE + 1) {
        TBSYS_LOG(ERROR, "column type need size |%s|", node_list[POS_COLUM_TYPE]);
        parse_ok = false;
      }
      if (parse_ok) {
        size = strtoll(node_list[POS_COLUM_SIZE], NULL, 10);
        if (size <= 0) {
          TBSYS_LOG(ERROR, "column type size error |%s|", node_list[POS_COLUM_SIZE]);
          parse_ok = false;
        }
      }
    }

    if (parse_ok && type == ObCreateTimeType) {
      if (has_create_time_column) {
        TBSYS_LOG(ERROR, "more than one column have create time type");
        parse_ok = false;
      } else {
        has_create_time_column = true;
        schema.set_create_time_column(id);
      }
    }

    if (parse_ok && type == ObModifyTimeType) {
      if (has_modify_time_column) {
        TBSYS_LOG(ERROR, "more than one column have modify time type");
        parse_ok = false;
      } else {
        has_modify_time_column = true;
        schema.set_modify_time_column(id);
      }
    }

    if (parse_ok) {
      ObColumnSchemaV2 column;

      column.set_table_id(schema.get_table_id());
      column.set_column_id(id);
      column.set_column_name(node_list[POS_COLUM_NAME]);
      column.set_column_type(type);
      column.set_column_size(size);
      column.set_maintained(1 == maintained ? true : false);

      if (parse_ok && column_index < OB_MAX_COLUMN_NUMBER) {
        columns_info[column_index++] = column;
      } else {
        parse_ok = false;
      }
    }
    free(str);
    str = NULL;
  }

  //parse column group info
  vector<const char*> column_group_infos;
  char* group_str = NULL;
  uint32_t column_group_id = 0;

  if (parse_ok) {
    column_group_infos = config.getStringList(section_name, STR_COLUMN_GROUP_INFO);
  }


  if (parse_ok && !column_group_infos.empty()) {
    int64_t total_column_in_group = 0;

    //set column who are not in any group in schema to default group
    int8_t exist_in_column_group_info[OB_MAX_COLUMN_NUMBER];
    memset(exist_in_column_group_info, 0, OB_MAX_COLUMN_NUMBER);
    for (vector<const char*>::const_iterator it = column_group_infos.begin();
         it != column_group_infos.end() && parse_ok; ++it) {
      group_str = strdup(*it);
      vector<char*> column_list;
      group_str = str_trim(group_str);
      tbsys::CStringUtil::split(group_str, ",", column_list);
      if (column_list.size() < 3) {
        TBSYS_LOG(ERROR, "parse column group |%s| error", group_str);
        parse_ok = false;
      }

      if (parse_ok) {
        for (uint32_t i = 1; i < column_list.size() && parse_ok; ++i) {
          ObColumnSchemaV2* col  = find_column_info_tmp(columns_info, column_index, column_list[i]);
          if (NULL == col) {
            TBSYS_LOG(ERROR, "can't find column |%s|", column_list[i]);
            parse_ok = false;
          } else {
            exist_in_column_group_info[col - columns_info] = 1;
          }
        }
      }
      free(group_str);
      group_str = NULL;
    }

    for (int32_t index = 0; index < column_index; ++index) {
      if (0 == exist_in_column_group_info[index]) {
        ObColumnSchemaV2* col = columns_info + index;
        col->set_column_group_id(OB_DEFAULT_COLUMN_GROUP_ID);
        if (add_column(*col) != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't add column |%s|", col->get_name());
          parse_ok = false;
        } else {
          ++total_column_in_group;
        }
      }
    }

    for (vector<const char*>::const_iterator it = column_group_infos.begin();
         it != column_group_infos.end() && parse_ok; ++it) {
      group_str = strdup(*it);
      vector<char*> column_list;
      group_str = str_trim(group_str);
      tbsys::CStringUtil::split(group_str, ",", column_list);
      if (column_list.size() < 3) {
        TBSYS_LOG(ERROR, "parse column group |%s| error", group_str);
        parse_ok = false;
      }

      if (parse_ok) {
        column_group_id = static_cast<uint32_t>(strtoul(column_list[POS_COLUMN_GROUP_ID], NULL, 10));
      }

      if (parse_ok) {
        for (uint32_t i = 1; i < column_list.size() && parse_ok; ++i) {
          ObColumnSchemaV2* col  = find_column_info_tmp(columns_info, column_index, column_list[i]);
          if (NULL == col) {
            TBSYS_LOG(ERROR, "can't find column |%s|", column_list[i]);
            parse_ok = false;
          } else {
            col->set_column_group_id(column_group_id);
            //schema.add_column(col->get_id(),*col);
            if (add_column(*col) != OB_SUCCESS) {
              TBSYS_LOG(ERROR, "can't add column |%s|", column_list[i]);
              parse_ok = false;
            } else {
              ++total_column_in_group;
            }
          }
        }
      }
      free(group_str);
      group_str = NULL;
    }

    if (parse_ok && total_column_in_group < column_index) {
      TBSYS_LOG(ERROR, "there is a column not belongs any column group");
      parse_ok = false;
    }
  } else if (parse_ok && column_group_infos.empty()) {
    for (int32_t i = 0; i < column_index && parse_ok; ++i) {
      columns_info[i].set_column_group_id(OB_DEFAULT_COLUMN_GROUP_ID);
      if (add_column(columns_info[i]) != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "add column |%s| failed", columns_info[i].get_name());
        parse_ok = false;
      }
    }
  }

  if (columns_info != NULL) {
    delete [] columns_info;
    columns_info = NULL;
  }

  return parse_ok;
}

bool ObSchemaManagerV2::parse_expire_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema) {
  bool parse_ok = true;
  vector<const char*> expire_info_strs;
  ObTableSchema::ExpireInfo expire_info;
  expire_info.column_id_ = 0;
  expire_info.duration_  = -1;
  if (section_name != NULL) {
    expire_info_strs = config.getStringList(section_name, STR_EXPIRE_INFO);
  }
  if (!expire_info_strs.empty()) {
    char* expire_str = NULL;
    expire_str = strdup(*expire_info_strs.begin());
    vector<char*> column_list;
    expire_str = str_trim(expire_str);
    tbsys::CStringUtil::split(expire_str, ",", column_list);
    if (EXPIRE_ITEM == static_cast<int64_t>(column_list.size())) {
      uint64_t column_id = strtoll(column_list[0], NULL, 10);
      int64_t duration   = strtoll(column_list[1], NULL, 10);
      if (0 >= column_id || column_id > schema.get_max_column_id()) {
        TBSYS_LOG(WARN, "column id(%lu) is illegal, max schema clumn id is %lu",
                  column_id, schema.get_max_column_id());
      } else if (0 > duration) {
        TBSYS_LOG(WARN, "duration is illegal, duration = %ld", duration);
      } else {
        expire_info.column_id_ = column_id;
        expire_info.duration_  = duration;
      }
    } else {
      TBSYS_LOG(WARN, "Expire info format error, set no expire info");
    }
    free(expire_str);
  }
  schema.set_expire_info(expire_info);
  return parse_ok;
}

const ObColumnSchemaV2* ObSchemaManagerV2::column_begin() const {
  return columns_;
}
const ObColumnSchemaV2* ObSchemaManagerV2::column_end() const {
  return columns_ + column_nums_;
}

const char* ObSchemaManagerV2::get_app_name() const {
  return app_name_;
}

int64_t ObSchemaManagerV2::get_column_count() const {
  return column_nums_;
}

int64_t ObSchemaManagerV2::get_table_count() const {
  return table_nums_;
}

int64_t ObSchemaManagerV2::get_version() const {
  return timestamp_;
}

int32_t ObSchemaManagerV2::get_code_version() const {
  return version_;
}

const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const int32_t index) const {
  const ObColumnSchemaV2* column = NULL;
  if (index >= 0 && index < column_nums_) {
    column = &columns_[index];
  }
  return column;
}

const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const uint64_t table_id,
    const uint64_t column_group_id,
    const uint64_t column_id) const {
  const ObColumnSchemaV2* column = NULL;

  if (OB_INVALID_ID != table_id && OB_INVALID_ID != column_group_id && OB_INVALID_ID != column_id) {
    ObColumnIdKey k;
    ObColumnInfo v;
    int err = OB_SUCCESS;

    k.table_id_ = table_id;
    k.column_id_ = column_id;

    err = id_hash_map_.get(k, v);
    if (-1 == err || hash::HASH_NOT_EXIST == err) {
      TBSYS_LOG(WARN, "get (%lu,%lu) failed", table_id, column_id);
    } else {
      ObColumnSchemaV2* tmp = v.head_;
      for (; tmp != NULL; tmp = tmp->column_group_next_) {
        if (tmp->get_column_group_id() == column_group_id) {
          column = tmp;
          break;
        }
      }
    }
  }
  return column;
}

const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const uint64_t table_id,
    const uint64_t column_id,
    int32_t* idx /*=NULL*/) const {
  const ObColumnSchemaV2* column = NULL;

  if (OB_INVALID_ID != table_id && OB_INVALID_ID != column_id) {
    ObColumnIdKey k;
    ObColumnInfo v;
    int err = OB_SUCCESS;

    k.table_id_ = table_id;
    k.column_id_ = column_id;

    err = id_hash_map_.get(k, v);
    if (-1 == err || hash::HASH_NOT_EXIST == err) {
      TBSYS_LOG(WARN, "get (%lu,%lu) failed", table_id, column_id);
    } else if (v.head_ != NULL) {
      column = v.head_;
      if (idx != NULL) {
        *idx = column - column_begin() - v.table_begin_index_;
      }
    } else {
      TBSYS_LOG(ERROR, "found column but v.head_ is NULL");
    }
  }
  return column;
}

const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const char* table_name,
    const char* column_name,
    int32_t* idx /*=NULL*/) const {
  const ObColumnSchemaV2* column = NULL;

  if (NULL != table_name && '\0' != *table_name && NULL != column_name && '\0' != column_name) {
    ObColumnNameKey k;
    k.table_name_.assign_ptr(const_cast<char*>(table_name), strlen(table_name));
    k.column_name_.assign_ptr(const_cast<char*>(column_name), strlen(column_name));
    column = get_column_schema(k.table_name_, k.column_name_, idx);
  }
  return column;
}

const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const ObString& table_name,
    const ObString& column_name,
    int32_t* idx /*=NULL*/) const {
  const ObColumnSchemaV2* column = NULL;

  ObColumnNameKey k;
  k.table_name_ = table_name;
  k.column_name_ = column_name;
  int err = OB_SUCCESS;

  ObColumnInfo v;
  err = column_hash_map_.get(k, v);
  if (-1 == err || hash::HASH_NOT_EXIST  == err) {
    TBSYS_LOG(WARN, "%s:%s not fould [%d]", table_name.ptr(), column_name.ptr(), err);
  } else if (v.head_  != NULL) {
    column = v.head_;
    //column = &columns_[v.index_[0]];
    if (idx != NULL) {
      *idx = column - column_begin() - v.table_begin_index_;
      //*idx = v.index_[0] - v.table_begin_index_;
    }
  } else {
    TBSYS_LOG(ERROR, "found column but v.head_ is null");
  }
  return column;
}

const ObColumnSchemaV2* ObSchemaManagerV2::get_table_schema(const uint64_t table_id, int32_t& size) const {
  int err = OB_SUCCESS;
  const ObColumnSchemaV2* begin = NULL;
  const ObColumnSchemaV2* end = NULL;
  if (OB_INVALID_ID == table_id) {
    TBSYS_LOG(ERROR, "table id invalid [%lu]", table_id);
    err = OB_ERROR;
  }

  ObColumnSchemaV2 target;
  size = 0;

  if (OB_SUCCESS == err) {
    target.set_table_id(table_id);
    target.set_column_group_id(0);
    target.set_column_id(0);
    begin = std::lower_bound(column_begin(), column_end(), target, ObColumnSchemaV2Compare());
  }

  if (OB_SUCCESS == err && begin != NULL && begin != column_end()
      && begin->get_table_id() == table_id) {
    target.set_column_group_id(OB_INVALID_ID);
    target.set_column_id(OB_INVALID_ID);
    end = std::upper_bound(begin, column_end(), target, ObColumnSchemaV2Compare());
    size = end - begin;
  }
  return begin != column_end() ? begin : NULL;
}

const ObColumnSchemaV2* ObSchemaManagerV2::get_group_schema(const uint64_t table_id,
                                                            const uint64_t column_group_id, int32_t& size) const {
  int err = OB_SUCCESS;

  const ObColumnSchemaV2* group_begin = NULL;
  const ObColumnSchemaV2* group_end = NULL;
  if (OB_INVALID_ID == column_group_id) {
    TBSYS_LOG(ERROR, "column group id invalid [%lu]", column_group_id);
    err = OB_ERROR;
  }

  ObColumnSchemaV2 target;
  size = 0;

  if (OB_SUCCESS == err) {
    target.set_table_id(table_id);
    target.set_column_group_id(column_group_id);
    target.set_column_id(0);
    group_begin = std::lower_bound(column_begin(), column_end(), target, ObColumnSchemaV2Compare());
  }

  if (OB_SUCCESS == err && group_begin != NULL && group_begin != column_end()
      && group_begin->get_table_id() == table_id
      && group_begin->get_column_group_id() == column_group_id) {
    target.set_column_id(OB_INVALID_ID);
    group_end = std::upper_bound(group_begin, column_end(), target, ObColumnSchemaV2Compare());
    size = group_end - group_begin;
  }
  return group_begin != column_end() ? group_begin : NULL;
}

int ObSchemaManagerV2::add_column(ObColumnSchemaV2& column) {
  int ret = OB_ERROR;
  const ObTableSchema* table = get_table_schema(column.get_table_id());

  if (NULL == table) {
    TBSYS_LOG(ERROR, "can't find this table:%lu", column.get_table_id());
    ret = OB_ERROR;
  } else if (column_nums_ < OB_MAX_COLUMN_NUMBER * OB_MAX_TABLE_NUMBER) {
    if (column.get_id() < COLUMN_ID_RESERVED) {
      TBSYS_LOG(ERROR, "we reserved id 0 for nothing id 1 for rowkey");
    } else if (column.get_id() > table->get_max_column_id()) {
      TBSYS_LOG(ERROR, "column id %lu greater thean max_column_id %lu", column.get_id(), table->get_max_column_id());
    } else if (column_nums_ > 0 &&
               ((column.get_table_id() < columns_[column_nums_ - 1].get_table_id()) ||
                (column.get_table_id() == columns_[column_nums_ - 1].get_table_id() &&
                 column.get_column_group_id() < columns_[column_nums_ - 1].get_column_group_id()))
              ) {
      TBSYS_LOG(ERROR, "table id,column group id must in order,prev:(%lu,%lu),"
                "i:(%lu,%lu)", columns_[column_nums_ - 1].get_table_id(),
                columns_[column_nums_ - 1].get_column_group_id(),
                column.get_table_id(),
                column.get_column_group_id());
    } else {
      columns_[column_nums_++] = column;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

void ObSchemaManagerV2::set_drop_column_group(bool drop_group /* = true*/) {
  drop_column_group_ = drop_group;
}

int ObSchemaManagerV2::get_column_index(const char* table_name,
                                        const char* column_name,
                                        int32_t index_array[], int32_t& size) const {
  int ret = OB_SUCCESS;

  if (NULL == table_name || NULL == column_name || '\0' == *table_name || '\0' == column_name || size <= 0) {
    TBSYS_LOG(ERROR, "invalid argument");
    ret = OB_ERROR;
  } else {
    ObColumnNameKey key;
    key.table_name_.assign_ptr(const_cast<char*>(table_name), strlen(table_name));
    key.column_name_.assign_ptr(const_cast<char*>(column_name), strlen(column_name));

    ObColumnInfo info;
    ret = column_hash_map_.get(key, info);
    if (-1 == ret || hash::HASH_NOT_EXIST  == ret) {
      TBSYS_LOG(WARN, "%s:%s not fould [%d]", table_name, column_name, ret);
    } else {
      int32_t i = 0;
      ObColumnSchemaV2* tmp = info.head_;
      for (; i < size && tmp != NULL; ++i) {
        index_array[i] = tmp - columns_;
        tmp = tmp->column_group_next_;
      }
      size = i;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSchemaManagerV2::get_column_index(const uint64_t table_id,
                                        const uint64_t column_id,
                                        int32_t index_array[], int32_t& size) const {
  int ret = OB_SUCCESS;

  if (OB_INVALID_ID == table_id || OB_INVALID_ID == column_id || size <= 0) {
    TBSYS_LOG(ERROR, "invalid argument");
  } else {
    ObColumnIdKey key;
    key.table_id_ = table_id;
    key.column_id_ = column_id;

    ObColumnInfo info;
    ret = id_hash_map_.get(key, info);
    if (-1 == ret || hash::HASH_NOT_EXIST  == ret) {
      TBSYS_LOG(WARN, "%lu:%lu not fould [%d]", table_id, column_id, ret);
    } else {
      ObColumnSchemaV2* tmp = info.head_;
      int32_t i = 0;
      for (; i < size && tmp != NULL; ++i) {
        index_array[i] = tmp - columns_;
        tmp = tmp->column_group_next_;
      }
      size = i;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSchemaManagerV2::get_column_schema(const uint64_t table_id, const uint64_t column_id,
                                         ObColumnSchemaV2* columns[], int32_t& size) const {
  int ret = OB_SUCCESS;

  if (OB_INVALID_ID == table_id || OB_INVALID_ID == column_id || size <= 0) {
    TBSYS_LOG(ERROR, "invalid argument");
  } else {
    ObColumnIdKey key;
    key.table_id_ = table_id;
    key.column_id_ = column_id;

    ObColumnInfo info;
    ret = id_hash_map_.get(key, info);
    if (-1 == ret || hash::HASH_NOT_EXIST  == ret) {
      TBSYS_LOG(WARN, "%lu:%lu not fould [%d]", table_id, column_id, ret);
    } else {
      ObColumnSchemaV2* tmp = info.head_;
      int32_t i = 0;
      for (; i < size && tmp != NULL; ++i) {
        columns[i] = tmp;
        tmp = tmp->column_group_next_;
      }
      size = i;
      ret = OB_SUCCESS;
    }
  }
  return ret;

}

int ObSchemaManagerV2::get_column_schema(const char* table_name, const char* column_name,
                                         ObColumnSchemaV2* columns[], int32_t& size) const {
  int ret = OB_SUCCESS;

  if (NULL == table_name || NULL == column_name || '\0' == *table_name || '\0' == column_name || size <= 0) {
    TBSYS_LOG(ERROR, "invalid argument");
    ret = OB_ERROR;
  } else {
    ObColumnNameKey key;
    key.table_name_.assign_ptr(const_cast<char*>(table_name), strlen(table_name));
    key.column_name_.assign_ptr(const_cast<char*>(column_name), strlen(column_name));
    ret = get_column_schema(key.table_name_, key.column_name_, columns, size);
  }
  return ret;
}

int ObSchemaManagerV2::get_column_schema(const ObString& table_name,
                                         const ObString& column_name,
                                         ObColumnSchemaV2* columns[], int32_t& size) const {
  int ret = OB_SUCCESS;

  if (size <= 0) {
    TBSYS_LOG(ERROR, "invalid argument");
    ret = OB_ERROR;
  } else {
    ObColumnNameKey key;
    key.table_name_ = table_name;
    key.column_name_ = column_name;

    ObColumnInfo info;
    ret = column_hash_map_.get(key, info);
    if (-1 == ret || hash::HASH_NOT_EXIST  == ret) {
      TBSYS_LOG(WARN, "%s:%s not fould [%d]", table_name.ptr(), column_name.ptr(), ret);
    } else {
      ObColumnSchemaV2* tmp = info.head_;
      int32_t i = 0;
      for (; i < size && tmp != NULL; ++i) {
        columns[i] = tmp;
        tmp = tmp->column_group_next_;
      }
      size = i;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

bool ObSchemaManagerV2::is_compatible(const ObSchemaManagerV2& schema_manager) const {
  bool compatible = true;
  if (strncmp(app_name_, schema_manager.get_app_name(), OB_MAX_APP_NAME_LENGTH) != 0) {
    compatible = false;
  }
  for (int64_t i = 0; compatible && i < column_nums_; ++i) {
    const ObColumnSchemaV2* column_left = get_column_schema(i);
    const ObColumnSchemaV2* column_right = get_column_schema(column_left->get_table_id(),
                                                             column_left->get_column_group_id(), column_left->get_id());
    if (NULL == column_right) {
      continue;
    } else {
      if (column_left->get_type() != column_left->get_type()) {
        compatible = false;
      }

      if (column_left->get_type() == ObVarcharType) {
        if (column_left->get_size() != column_right->get_size())
          compatible = false;
      }
    }
  }
  return compatible;
}

bool ObSchemaManagerV2::parse_join_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema) {
  uint64_t left_column = 1;
  int32_t start_pos = -1;
  int32_t end_pos = -1;
  uint64_t table_id_joined = OB_INVALID_ID;
  const ObTableSchema* table_joined = NULL;

  bool parse_ok = true;
  char* str = NULL;
  vector<const char*> join_info_strs;
  if (section_name != NULL) {
    join_info_strs = config.getStringList(section_name, STR_JOIN_RELATION);
  }
  if (!join_info_strs.empty()) {
    char* s = NULL;
    int len = 0;
    char* p = NULL;
    for (vector<const char*>::iterator it = join_info_strs.begin();
         parse_ok && it != join_info_strs.end(); ++it) {
      str = strdup(*it);
      s = str;
      len = strlen(s);

      p = NULL;

      if (len < 7) {
        TBSYS_LOG(ERROR, "error format  in join info |%s|", str);
        parse_ok = false;
      }
      if (parse_ok && strncmp(s, "rowkey", 6) != 0) {
        TBSYS_LOG(ERROR, "%s is not acceptable, must be rowkey for now", str);
        parse_ok = false;
      }
      if (parse_ok) {
        s += 6;
        if (*s == '[') {
          p =  strchr(s, ',');
          if (p == NULL) {
            TBSYS_LOG(ERROR, "%s rowkey range error", str);
            parse_ok = false;
          }
          if (parse_ok) {
            s++;
            *p = '\0';
            start_pos = atol(s);
            s = p + 1;
            p = strchr(s, ']');
            if (p == NULL) {
              TBSYS_LOG(ERROR, "%s rowkey range error", str);
              parse_ok = false;
            }
          }
          if (parse_ok) {
            *p = '\0';
            end_pos = atol(s);
            s = p + 1;
          }
        }
      }

      if (parse_ok && *s != '%') {
        TBSYS_LOG(ERROR, "%s format error, should be rowkey", str);
        parse_ok = false;
      }
      if (parse_ok) {
        s++;
        p = strchr(s, ':');
        if (p == NULL) {
          TBSYS_LOG(ERROR, "%s format error, could not find ':'", str);
          parse_ok = false;
        }
      }
      if (parse_ok) {
        *p = '\0';
        table_joined = get_table_schema(s);
        if (NULL != table_joined) {
          table_id_joined = table_joined->get_table_id();
        }

        if (NULL == table_joined || table_id_joined == OB_INVALID_ID) {
          TBSYS_LOG(ERROR, "%s table not exist ", s);
          parse_ok = false;
        }
      }

      vector<char*> node_list;
      if (parse_ok) {
        s = p + 1;
        s = str_trim(s);
        tbsys::CStringUtil::split(s, ",", node_list);
        if (node_list.empty()) {
          TBSYS_LOG(ERROR, "%s can not find correct info", str);
          parse_ok = false;
        }
      }

      uint64_t ltable_id = OB_INVALID_ID;

      if (parse_ok) {
        ltable_id = schema.get_table_id();
        uint64_t lid = OB_INVALID_ID;
        uint64_t rid = OB_INVALID_ID;
        char* p;
        for (uint32_t i = 0; parse_ok && i < node_list.size(); ++i) {
          p = NULL;
          lid = OB_INVALID_ID;
          rid = OB_INVALID_ID;
          p = strchr(node_list[i], '$');
          if (p == NULL) {
            TBSYS_LOG(ERROR, "error can not find '$' %s ", node_list[i]);
            parse_ok = false;
            break;
          }
          *p = '\0';
          p++;


          int32_t l_column_index[OB_MAX_COLUMN_GROUP_NUMBER];
          int32_t r_column_index[OB_MAX_COLUMN_GROUP_NUMBER];

          int32_t l_column_size = sizeof(l_column_index) / sizeof(l_column_index[0]);
          int32_t r_column_size = sizeof(r_column_index) / sizeof(r_column_index[0]);

          get_column_index(schema.get_table_name(), node_list[i], l_column_index, l_column_size);
          get_column_index(table_joined->get_table_name(), p, r_column_index, r_column_size);

          if (l_column_size <= 0 || r_column_size <= 0) {
            TBSYS_LOG(ERROR, "error column name %s %s ", node_list[i], p);
            parse_ok = false;
            break;
          }

          for (int32_t l_index = 0; l_index < l_column_size && parse_ok; ++l_index) {
            //TODO check column
            ObColumnSchemaV2* lcs = &columns_[ l_column_index[l_index] ];
            ObColumnSchemaV2* rcs = &columns_[ r_column_index[0] ];  //just need the id of the right column

            if (lcs->get_type() != rcs->get_type()) {
              //the join should be happen between too columns have the same type
              if (lcs->get_type() == ObPreciseDateTimeType &&
                  (rcs->get_type() == ObCreateTimeType || rcs->get_type() == ObModifyTimeType)) {
                //except that ObPreciseDateTimeType join with ObCreateTimeType or ObModifyTimeType
              } else {
                TBSYS_LOG(ERROR, "join column have different types %s %s ", node_list[i], p);
                parse_ok = false;
                break;
              }
            }
            if (lcs->get_type() == ObCreateTimeType || lcs->get_type() == ObModifyTimeType) {
              TBSYS_LOG(ERROR, "column %s can not be jonined, it hase type ObCreateTimeType or ObModifyTimeType", node_list[i]);
              parse_ok = false;
              break;
            }
            lcs->set_join_info(table_id_joined, left_column, rcs->get_id(), start_pos, end_pos);
          }
        }
      }
      free(str);
      str = NULL;
    }
  }
  if (str) free(str);
  return parse_ok;
}

void ObSchemaManagerV2::print_info() const {
  for (int64_t i = 0; i < column_nums_; ++i) {
    columns_[i].print_info();
  }
}

int ObSchemaManagerV2::sort_column() {
  int ret = OB_SUCCESS;

  std::sort(columns_, columns_ + column_nums_, ObColumnSchemaV2Compare());
  if (!hash_sorted_) {
    TBSYS_LOG(INFO, "create hash table");
    if ((ret = column_hash_map_.create(hash::cal_next_prime(512))) != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "create column_hash_map_ failed");
    } else if ((ret = id_hash_map_.create(hash::cal_next_prime(512))) != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "create id_hash_map_ failed");
      column_hash_map_.destroy();
    }
  } else {
    TBSYS_LOG(INFO, "rebuild hash table,clear first");
    column_hash_map_.clear();
    id_hash_map_.clear();
  }

  column_group_nums_ = 0;

  if (OB_SUCCESS == ret && drop_column_group_) {
    TBSYS_LOG(INFO, "drop all column group info");
    for (int64_t i = 0; i < column_nums_; ++i) {
      columns_[i].set_column_group_id(0); //drop all column group
    }

    TBSYS_LOG(DEBUG, "old column nums:%ld", column_nums_);
    //get rid of column that belongs to more one column group
    sort(columns_, columns_ + column_nums_, ObColumnSchemaV2Compare());
    ObColumnSchemaV2* new_end = unique(columns_, columns_ + column_nums_);
    column_nums_ = new_end - column_begin();
    TBSYS_LOG(DEBUG, "new column nums:%ld", column_nums_);
  }

  if (OB_SUCCESS == ret) {
    ObColumnNameKey name_key;
    ObColumnIdKey   id_key;
    ObColumnInfo info;

    int err = OB_SUCCESS;
    int32_t table_begin_offset = 0;
    const char* table_name = NULL;

    for (int i = 0; i < column_nums_ && OB_SUCCESS == ret; ++i) {
      columns_[i].column_group_next_ = NULL; //rebuild the list

      table_name = get_table_schema(columns_[i].get_table_id())->get_table_name();

      name_key.table_name_.assign_ptr(const_cast<char*>(table_name), strlen(table_name));
      name_key.column_name_.assign_ptr(const_cast<char*>(columns_[i].get_name()), strlen(columns_[i].get_name()));

      id_key.table_id_ = columns_[i].get_table_id();
      id_key.column_id_ = columns_[i].get_id();

      info.head_ = &columns_[i];

      if (i > 0 && columns_[i].get_table_id() != columns_[i - 1].get_table_id()) {
        table_begin_offset = i;
      }

      info.table_begin_index_ = table_begin_offset;

      ret = column_hash_map_.set(name_key, info);
      err = id_hash_map_.set(id_key, info);

      if (-1 == ret || -1 == err) {
        TBSYS_LOG(ERROR, "insert column into hash set failed");
        ret = OB_ERROR;
      } else if (hash::HASH_EXIST == ret && hash::HASH_EXIST == err) {
        ret = column_hash_map_.get(name_key, info);
        err = id_hash_map_.get(id_key, info);
        if (-1 == ret || -1 == err) {
          TBSYS_LOG(ERROR, "get (%s,%s) failed", name_key.table_name_.ptr(), name_key.column_name_.ptr());
          ret = OB_ERROR;
        } else {
          ObColumnSchemaV2* tmp = info.head_;

          while (tmp->column_group_next_ != NULL) {
            tmp = tmp->column_group_next_;
          }
          tmp->column_group_next_ = &columns_[i];

          ret = column_hash_map_.set(name_key, info, 1); //overwrite
          err = id_hash_map_.set(id_key, info, 1);
        }
      } else if (ret != err) {
        TBSYS_LOG(ERROR, "name->id & id->name no match: [ret:%d,err:%d]", ret, err);
        ret = OB_ERROR;
      } else {
        //TBSYS_LOG(DEBUG,"insert (%s,%s),(%lu,%lu) into hash succ",name_key.table_name_,name_key.column_name_,
        //    id_key.table_id_,id_key.column_id_);
        //succ
      }

      if (-1 == ret || -1 == err) {
        TBSYS_LOG(ERROR, "error happend in sort column ");
        ret = OB_ERROR;
      } else {
        ret = OB_SUCCESS;
      }

      if (column_group_nums_ > 0) {
        if (columns_[i].get_table_id() > column_groups_[column_group_nums_ - 1].table_id_ ||
            columns_[i].get_column_group_id() > column_groups_[column_group_nums_ - 1].column_group_id_) {
          column_groups_[column_group_nums_].table_id_ = columns_[i].get_table_id();
          column_groups_[column_group_nums_].column_group_id_ = columns_[i].get_column_group_id();
          ++column_group_nums_;
        }
      } else {
        column_groups_[column_group_nums_].table_id_ = columns_[i].get_table_id();
        column_groups_[column_group_nums_].column_group_id_ = columns_[i].get_column_group_id();
        ++column_group_nums_;
      }


      if (columns_[i].get_type() == ObCreateTimeType || columns_[i].get_type() == ObModifyTimeType) {
        ObTableSchema* table = get_table_schema(columns_[i].get_table_id());
        if (NULL != table) {
          if (columns_[i].get_type() == ObCreateTimeType)
            table->set_create_time_column(columns_[i].get_id());
          else
            table->set_modify_time_column(columns_[i].get_id());
        }
      }
    } //end for
  }

  if (OB_SUCCESS == ret) {
    hash_sorted_ = true;
  }

  return ret;
}

ObSchemaManagerV2& ObSchemaManagerV2::operator=(const ObSchemaManagerV2& schema) { //ugly,for hashMap
  if (this != &schema) {
    version_ = schema.version_;
    timestamp_ = schema.timestamp_;
    max_table_id_ = schema.max_table_id_;
    column_nums_ = schema.column_nums_;
    table_nums_ = schema.table_nums_;

    snprintf(app_name_, sizeof(app_name_), "%s", schema.app_name_);

    memcpy(&table_infos_, &schema.table_infos_, sizeof(table_infos_));
    memcpy(&columns_, &schema.columns_, sizeof(columns_));
    sort_column();
  }
  return *this;
}

ObSchemaManagerV2::ObSchemaManagerV2(const ObSchemaManagerV2& schema) {
  *this = schema;
}



bool ObSchemaManagerV2::ObColumnGroupHelperCompare::operator()(const ObColumnGroupHelper& l,
    const ObColumnGroupHelper& r) const {
  bool ret = false;
  if (l.table_id_ < r.table_id_ ||
      (l.table_id_ == r.table_id_ && l.column_group_id_ < r.column_group_id_)) {
    ret = true;
  }
  //bool ret = true;
  //if (l.table_id_ > r.table_id_ ||
  //    (l.table_id_ == r.table_id_ && l.column_group_id_ > r.column_group_id_) )
  //{
  //  ret = false;
  //}
  return ret;
}

int ObSchemaManagerV2::get_column_groups(uint64_t table_id, uint64_t column_groups[], int32_t& size) const {
  int ret = OB_SUCCESS;
  const ObColumnGroupHelper* begin = NULL;

  if (OB_INVALID_ID == table_id || size < 0) {
    TBSYS_LOG(ERROR, "invalid argument");
    ret = OB_ERROR;
  } else {
    const ObColumnGroupHelper* end = NULL;

    ObColumnGroupHelper target;
    target.table_id_ = table_id;
    target.column_group_id_ = 0;

    begin = std::lower_bound(column_groups_, column_groups_ + column_group_nums_, target, ObColumnGroupHelperCompare());

    if (begin != NULL && begin != column_groups_ + column_group_nums_
        && begin->table_id_ == table_id) {
      target.column_group_id_ = OB_INVALID_ID;
      end = std::upper_bound(begin, column_groups_ + column_group_nums_, target, ObColumnGroupHelperCompare());
      size = size > end - begin ? end - begin : size;
      for (int32_t i = 0; i < size; ++i) {
        column_groups[i] = (begin + i)->column_group_id_;
      }
    } else {
      size = 0;
      TBSYS_LOG(WARN, "not found column group in table %lu,column_group_nums_:%ld", table_id, column_group_nums_);
      ret = OB_ERROR;
    }
  }
  return ret;
}

int64_t ObSchemaManagerV2::ObColumnNameKey::hash() const {
  return table_name_.hash() + column_name_.hash();
}

bool ObSchemaManagerV2::ObColumnNameKey::operator==(const ObColumnNameKey& key) const {
  bool ret = false;
  if (table_name_ == key.table_name_ && column_name_ == key.column_name_) {
    ret = true;
  }
  return ret;
}

int64_t ObSchemaManagerV2::ObColumnIdKey::hash() const {
  hash::hash_func<uint64_t> h;
  return h(table_id_) + h(column_id_);
}

bool ObSchemaManagerV2::ObColumnIdKey::operator==(const ObColumnIdKey& key) const {
  bool ret = false;
  if (table_id_ == key.table_id_ && column_id_ == key.column_id_) {
    ret = true;
  }
  return ret;
}

DEFINE_SERIALIZE(ObSchemaManagerV2) {
  int ret = 0;
  int64_t tmp_pos = pos;

  if (schema_magic_ != OB_SCHEMA_MAGIC_NUMBER) {
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_i32(buf, buf_len, tmp_pos, schema_magic_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi32(buf, buf_len, tmp_pos, version_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, timestamp_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, static_cast<int64_t>(max_table_id_));
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, column_nums_);
  }

  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_nums_);
  }


  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vstr(buf, buf_len, tmp_pos, app_name_);
  }

  if (OB_SUCCESS == ret) {
    for (int64_t i = 0; i < table_nums_; i++) {
      ret = table_infos_[i].serialize(buf, buf_len, tmp_pos);
      if (OB_SUCCESS != ret) {
        break;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    for (int64_t i = 0; i < column_nums_; i++) {
      ret = columns_[i].serialize(buf, buf_len, tmp_pos);
      if (OB_SUCCESS != ret) {
        break;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSchemaManagerV2) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (OB_SUCCESS == ret) {
    ret = serialization::decode_i32(buf, data_len, tmp_pos, &schema_magic_);
  }

  if (OB_SUCCESS == ret) {
    if (schema_magic_ != OB_SCHEMA_MAGIC_NUMBER) { //old schema
      TBSYS_LOG(INFO, "schema magic numer is wrong,try to deserialize schema with old syntax");
      ObSchemaManager* old_schema = new(std::nothrow) ObSchemaManager();
      if (old_schema != NULL) {
        tmp_pos -= 4; //magic_
        if ((ret = old_schema->deserialize(buf, data_len, tmp_pos)) != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "deserialize old schema failed [%d]", ret);
        } else {
          ret = schema_1_to_2(*old_schema, *this);
        }

        delete old_schema;
        old_schema = NULL;
      } else {
        ret = OB_ERROR;
      }
    } else {
      if (OB_SUCCESS == ret) {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &version_);
      }

      if (OB_SUCCESS == ret) {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &timestamp_);
      }

      if (OB_SUCCESS == ret) {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&max_table_id_));
      }
      if (OB_SUCCESS == ret) {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &column_nums_);
      }

      if (OB_SUCCESS == ret) {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &table_nums_);
      }

      if (OB_SUCCESS == ret) {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos,
                                   app_name_, OB_MAX_APP_NAME_LENGTH, &len);
        if (-1 == len) {
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret) {
        if (table_nums_ < 0 || table_nums_ > OB_MAX_TABLE_NUMBER) {
          TBSYS_LOG(ERROR, "bugs, table_nums_ %ld error", table_nums_);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret) {
        for (int64_t i = 0; i < table_nums_; ++i) {
          table_infos_[i].set_version(version_);
          ret = table_infos_[i].deserialize(buf, data_len, tmp_pos);
          if (OB_SUCCESS != ret)
            break;
        }
      }

      if (OB_SUCCESS == ret) {
        for (int64_t i = 0; i < column_nums_; ++i) {
          ret = columns_[i].deserialize(buf, data_len, tmp_pos);
          if (OB_SUCCESS != ret)
            break;
        }
      }

      if (OB_SUCCESS == ret) {
        pos = tmp_pos;
      }

      if (OB_SUCCESS == ret) {
        sort_column();
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSchemaManagerV2) {
  int64_t len = serialization::encoded_length_i32(schema_magic_); //magic number use fixed length
  len += serialization::encoded_length_vi32(version_);
  len += serialization::encoded_length_vi64(timestamp_);
  len += serialization::encoded_length_vi64(static_cast<int64_t>(max_table_id_));
  len += serialization::encoded_length_vi64(column_nums_);
  len += serialization::encoded_length_vi64(table_nums_);
  len += serialization::encoded_length_vstr(app_name_);

  for (int64_t i = 0; i < table_nums_; ++i) {
    len += table_infos_[i].get_serialize_size();
  }

  for (int64_t i = 0; i < column_nums_; ++i) {
    len += columns_[i].get_serialize_size();
  }

  return len;
}

int schema_1_to_2(ObSchemaManager& v1, ObSchemaManagerV2& v2) {
  int ret = OB_SUCCESS;
  v2.timestamp_ = v1.version_;
  v2.max_table_id_ = v1.max_table_id_;
  v2.table_nums_ = v1.schema_size_;

  snprintf(v2.app_name_, sizeof(v2.app_name_), "%s", v1.app_name_);

  uint64_t table_sort_helper[ v1.schema_size_ ];

  for (int64_t i = 0; i < v1.schema_size_; ++i) {
    table_sort_helper[i] = v1.schemas_[i].get_table_id();
  }

  std::sort(table_sort_helper, table_sort_helper + v1.schema_size_);

  for (int64_t i = 0; i < v1.schema_size_; ++i) {
    const ObSchema* table = v1.get_table_schema(table_sort_helper[i]);
    if (NULL == table) {
      TBSYS_LOG(WARN, "cant get table %lu", table_sort_helper[i]);
    } else {
      v2.table_infos_[i].set_table_id(table->get_table_id());
      v2.table_infos_[i].set_table_type(static_cast<ObTableSchema::TableType>(table->get_table_type()));
      v2.table_infos_[i].set_max_column_id(table->max_column_id_);
      v2.table_infos_[i].set_split_pos(table->get_split_pos());
      v2.table_infos_[i].set_block_size(table->get_block_size());
      v2.table_infos_[i].set_rowkey_max_length(table->get_rowkey_max_length());
      v2.table_infos_[i].set_pure_update_table(table->is_pure_update_table());
      v2.table_infos_[i].set_use_bloomfilter(table->is_use_bloomfilter());
      v2.table_infos_[i].set_rowkey_fixed_len(table->is_row_key_fixed_len());
      v2.table_infos_[i].set_table_name(table->get_table_name());
      v2.table_infos_[i].set_compressor_name(table->get_compress_func_name());

      const ObJoinInfo* join_info = NULL;
      int32_t start_pos = -1;
      int32_t end_pos = -1;
      for (const ObColumnSchema* col = table->column_begin(); col != table->column_end(); ++col) {
        ObColumnSchemaV2 column;
        column.set_table_id(table->get_table_id());
        column.set_column_id(col->get_id());
        column.set_column_group_id(0); //v1 has no column group
        column.set_column_name(col->get_name());
        column.set_column_type(col->get_type());
        column.set_column_size(col->get_size());
        column.set_maintained(col->is_maintained());

        join_info = table->find_join_info(col->get_id());
        if (join_info != NULL) {
          join_info->get_rowkey_join_range(start_pos, end_pos);
          column.set_join_info(join_info->get_table_id_joined(), 1, join_info->find_right_column_id(col->get_id()),
                               start_pos, end_pos);
        }
        v2.add_column(column);
      } //end every column
    }
  } //end table

  v2.sort_column();
  return ret;
} //end schema_1_to_2
}
}


