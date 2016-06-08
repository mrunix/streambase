/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_object.h for ...
 *
 * Authors:
 *   maoqi <maoqi@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_OBJECT_H_
#define OCEANBASE_COMMON_OB_OBJECT_H_

#include "ob_define.h"
#include "ob_string.h"
#include "tbsys.h"

namespace sb {
namespace common {
enum ObObjType {
  ObMinType = -1,
  ObNullType,   // 空类型
  ObIntType,
  ObFloatType,
  ObDoubleType,
  ObDateTimeType,
  ObPreciseDateTimeType,
  ObVarcharType,
  ObSeqType,
  ObCreateTimeType,
  ObModifyTimeType,
  ObExtendType,
  ObMaxType,
};

class ObObj {
 public:
  ObObj() {
    meta_.type_ = ObNullType;
    meta_.op_flag_ = INVALID_OP_FLAG;
    meta_.reserved_ = 0;
    varchar_len_ = 0;
    memset(&value_, 0, sizeof(value_));
  }

  /// set add flag
  int set_add(const bool is_add = false);
  bool get_add()const;

  /*
   *   设置列值
   */
  void set_int(const int64_t value, const bool is_add = false);
  void set_float(const float value, const bool is_add = false);
  void set_double(const double value, const bool is_add = false);
  void set_ext(const int64_t value);
  /*
   *   设置日期类型，如果date_time为OB_SYS_DATE，表示设置为服务器当前时间
   */
  void set_datetime(const ObDateTime& value, const bool is_add = false);
  void set_precise_datetime(const ObPreciseDateTime& value, const bool is_add = false);
  void set_varchar(const ObString& value);
  void set_seq();
  void set_modifytime(const ObModifyTime& value);
  void set_createtime(const ObCreateTime& value);
  /*
   *   设置列值为空
   */
  void set_null();

  /// @fn apply mutation to this obj
  int apply(const ObObj& mutation);

  bool operator<(const ObObj& that_obj) const;
  bool operator>(const ObObj& that_obj) const;
  bool operator<=(const ObObj& that_obj) const;
  bool operator>=(const ObObj& that_obj) const;
  bool operator==(const ObObj& that_obj) const;
  bool operator!=(const ObObj& that_obj) const;

  void reset();
  bool is_valid_type() const;

  void dump(const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;
  //
  NEED_SERIALIZE_AND_DESERIALIZE;
 public:
  /*
   *   获取列值，用户根据已知的数据类型调用相应函数，如果类型不符则返回失败
   */
  int get_int(int64_t& value, bool& is_add) const;
  int get_int(int64_t& value) const;

  int get_float(float& value, bool& is_add) const;
  int get_float(float& value) const;

  int get_double(double& value, bool& is_add) const;
  int get_double(double& value) const;

  int get_ext(int64_t& value) const;
  int64_t get_ext() const;

  int get_datetime(ObDateTime& value, bool& is_add) const;
  int get_datetime(ObDateTime& value) const;

  int get_precise_datetime(ObPreciseDateTime& value, bool& is_add) const;
  int get_precise_datetime(ObPreciseDateTime& value) const;

  int get_modifytime(ObModifyTime& value) const;
  int get_createtime(ObCreateTime& value) const;

  /// get timestamp using microsecond
  int get_timestamp(int64_t& timestamp) const;
  bool can_compare(const ObObj& other) const;
  bool is_datetime(void) const;

  /*
   *   获取varchar类型数据，直接使用ObObj内部缓冲区
   */
  int get_varchar(ObString& value) const;
  /*
   *   获取数据类型
   */
  ObObjType get_type(void) const;
  /*
   *   计算obj内数据的校验和
   */
  int64_t checksum(const int64_t current) const;

  uint32_t murmurhash2(const uint32_t hash) const;
 private:
  static const int8_t INVALID_OP_FLAG = 0x0;
  static const int8_t ADD = 0x1;
  struct ObObjMeta {
    int32_t type_: 8;
    int32_t op_flag_: 8;
    int32_t reserved_: 16;
  };
  ObObjMeta meta_;
  int32_t varchar_len_;
  union {        // value实际内容
    int64_t int_val;
    int64_t ext_val;
    float float_val;
    double double_val;
    ObDateTime time_val;
    ObPreciseDateTime precisetime_val;
    ObModifyTime modifytime_val;
    ObCreateTime createtime_val;
    const char* varchar_val;
  } value_;
};

inline void ObObj::reset() {
  meta_.type_ = ObNullType;
  meta_.op_flag_ = INVALID_OP_FLAG;
  meta_.reserved_ = 0;
}



inline bool ObObj::get_add()const {
  bool ret = false;
  switch (meta_.type_) {
  case ObIntType:
  case ObDoubleType:
  case ObFloatType:
  case ObDateTimeType:
  case ObPreciseDateTimeType:
    ret = (meta_.op_flag_ == ADD);
    break;
  default:
    ;/// do nothing
  }
  return ret;
}

inline int ObObj::set_add(const bool is_add /*=false*/) {
  int ret = OB_SUCCESS;
  switch (meta_.type_) {
  case ObIntType:
  case ObDoubleType:
  case ObFloatType:
  case ObDateTimeType:
  case ObPreciseDateTimeType:
    meta_.op_flag_ = is_add ? ADD : INVALID_OP_FLAG;
    break;
  default:
    TBSYS_LOG(ERROR, "check obj type failed:type[%d]", meta_.type_);
    ret = OB_ERROR;
  }
  return ret;
}

inline void ObObj::set_int(const int64_t value, const bool is_add /*=false*/) {
  meta_.type_ = ObIntType;
  meta_.op_flag_ = is_add ? ADD : INVALID_OP_FLAG;
  value_.int_val = value;
}

inline void ObObj::set_float(const float value, const bool is_add /*=false*/) {
  meta_.type_ = ObFloatType;
  meta_.op_flag_ = is_add ? ADD : INVALID_OP_FLAG;
  value_.float_val = value;
}

inline void ObObj::set_double(const double value, const bool is_add /*=false*/) {
  meta_.type_ = ObDoubleType;
  meta_.op_flag_ = is_add ? ADD : INVALID_OP_FLAG;
  value_.double_val = value;
}

inline void ObObj::set_datetime(const ObDateTime& value, const bool is_add /*=false*/) {
  meta_.type_ = ObDateTimeType;
  meta_.op_flag_ = is_add ? ADD : INVALID_OP_FLAG;
  value_.time_val = value;
}

inline void ObObj::set_precise_datetime(const ObPreciseDateTime& value, const bool is_add /*=false*/) {
  meta_.type_ = ObPreciseDateTimeType;
  meta_.op_flag_ = is_add ? ADD : INVALID_OP_FLAG;
  value_.precisetime_val = value;
}

inline void ObObj::set_varchar(const ObString& value) {
  meta_.type_ = ObVarcharType;
  meta_.op_flag_ = INVALID_OP_FLAG;
  value_.varchar_val = value.ptr();
  varchar_len_ = value.length();
}

inline void ObObj::set_seq() {
  meta_.type_ = ObSeqType;
  meta_.op_flag_ = INVALID_OP_FLAG;
}

inline void ObObj::set_modifytime(const ObModifyTime& value) {
  meta_.type_ = ObModifyTimeType;
  meta_.op_flag_ = INVALID_OP_FLAG;
  value_.modifytime_val = value;
}

inline void ObObj::set_createtime(const ObCreateTime& value) {
  meta_.type_ = ObCreateTimeType;
  meta_.op_flag_ = INVALID_OP_FLAG;
  value_.createtime_val = value;
}

inline void ObObj::set_null() {
  meta_.type_ = ObNullType;
  meta_.op_flag_ = INVALID_OP_FLAG;
}

inline void ObObj::set_ext(const int64_t value) {
  meta_.type_ = ObExtendType;
  meta_.op_flag_ = INVALID_OP_FLAG;
  value_.ext_val = value;
}

inline bool ObObj::is_valid_type() const {
  ObObjType type = get_type();
  bool ret = true;
  if (type <= ObMinType || type >= ObMaxType) {
    ret = false;
  }
  return ret;
}

inline int ObObj::get_int(int64_t& value, bool& is_add) const {
  int res = OB_OBJ_TYPE_ERROR;
  if (meta_.type_ == ObIntType) {
    is_add = (ADD == meta_.op_flag_);
    value = value_.int_val;
    res = OB_SUCCESS;
  }
  return res;
}

inline int ObObj::get_int(int64_t& value) const {
  bool add = false;
  return get_int(value, add);
}

inline int ObObj::get_float(float& value, bool& is_add) const {
  int res = OB_OBJ_TYPE_ERROR;
  if (meta_.type_ == ObFloatType) {
    value = value_.float_val;
    is_add = (meta_.op_flag_ == ADD);
    res = OB_SUCCESS;
  }
  return res;
}

inline int ObObj::get_float(float& value) const {
  bool add = false;
  return get_float(value, add);
}

inline int ObObj::get_double(double& value, bool& is_add) const {
  int res = OB_OBJ_TYPE_ERROR;
  if (meta_.type_ == ObDoubleType) {
    value = value_.double_val;
    is_add = (meta_.op_flag_ == ADD);
    res = OB_SUCCESS;
  }
  return res;
}

inline int ObObj::get_double(double& value) const {
  bool add = false;
  return get_double(value, add);
}

inline bool ObObj::is_datetime(void) const {
  return ((meta_.type_ == ObDateTimeType)
          || (meta_.type_ == ObPreciseDateTimeType)
          || (meta_.type_ == ObCreateTimeType)
          || (meta_.type_ == ObModifyTimeType));
}

inline bool ObObj::can_compare(const ObObj& other) const {
  bool ret = false;
  if ((get_type() == ObNullType) || (other.get_type() == ObNullType)
      || (get_type() == other.get_type()) || (is_datetime() && other.is_datetime())) {
    ret = true;
  }
  return ret;
}

inline int ObObj::get_timestamp(int64_t& timestamp) const {
  int ret = OB_SUCCESS;
  if (ObDateTimeType == meta_.type_) {
    // second to us
    timestamp = value_.time_val * 1000 * 1000L;
  } else if (ObPreciseDateTimeType == meta_.type_) {
    timestamp = value_.precisetime_val;
  } else if (ObModifyTimeType == meta_.type_) {
    timestamp = value_.modifytime_val;
  } else if (ObCreateTimeType == meta_.type_) {
    timestamp = value_.createtime_val;
  } else {
    ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

inline int ObObj::get_datetime(ObDateTime& value, bool& is_add) const {
  int ret = OB_OBJ_TYPE_ERROR;
  if (ObDateTimeType == meta_.type_) {
    value = value_.time_val;
    is_add = (meta_.op_flag_ == ADD);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_datetime(ObDateTime& value) const {
  bool add = false;
  return get_datetime(value, add);
}

inline int ObObj::get_precise_datetime(ObPreciseDateTime& value, bool& is_add) const {
  int ret = OB_OBJ_TYPE_ERROR;
  if (ObPreciseDateTimeType == meta_.type_) {
    value = value_.precisetime_val;
    is_add = (meta_.op_flag_ == ADD);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_precise_datetime(ObPreciseDateTime& value) const {
  bool add = false;
  return get_precise_datetime(value, add);
}

inline int ObObj::get_varchar(ObString& value) const {
  int res = OB_OBJ_TYPE_ERROR;
  if (meta_.type_ == ObVarcharType) {
    value.assign_ptr(const_cast<char*>(value_.varchar_val), varchar_len_);
    res = OB_SUCCESS;
  }
  return res;
}

inline int ObObj::get_modifytime(ObModifyTime& value) const {
  int res = OB_OBJ_TYPE_ERROR;
  if (ObModifyTimeType == meta_.type_) {
    value = value_.modifytime_val;
    res = OB_SUCCESS;
  }
  return res;
}

inline int ObObj::get_createtime(ObCreateTime& value) const {
  int res = OB_OBJ_TYPE_ERROR;
  if (ObCreateTimeType == meta_.type_) {
    value = value_.createtime_val;
    res = OB_SUCCESS;
  }
  return res;
}

inline int ObObj::get_ext(int64_t& value) const {
  int res = OB_OBJ_TYPE_ERROR;
  if (ObExtendType == meta_.type_) {
    value = value_.ext_val;
    res = OB_SUCCESS;
  }
  return res;
}

inline int64_t ObObj::get_ext() const {
  int64_t res = 0;
  if (ObExtendType == meta_.type_) {
    res = value_.ext_val;
  }
  return res;
}

inline ObObjType ObObj::get_type(void) const {
  return static_cast<ObObjType>(meta_.type_);
}
}
}

#endif //


