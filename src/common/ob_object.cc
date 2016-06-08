/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_object.cc for ...
 *
 * Authors:
 *   maoqi <maoqi@taobao.com>
 *
 */
#include <string.h>
#include <algorithm>
#include <math.h>

#include "ob_object.h"
#include "serialization.h"
#include "tbsys.h"
#include "ob_action_flag.h"
#include "utility.h"
#include "ob_crc64.h"
#include "murmur_hash.h"

using namespace sb;
using namespace sb::common;
namespace {
struct _ObjValue {
  int64_t int_val;
  float float_val;
  double double_val;
  ObDateTime second_val;
  ObPreciseDateTime microsecond_val;
  ObCreateTime      create_time_val;
  ObModifyTime      modify_time_val;
  ObString      varchar_val;
};
}

namespace sb {
namespace common {
const int8_t ObObj::INVALID_OP_FLAG;
const int8_t ObObj::ADD;

const float FLOAT_EPSINON = 1e-6;
const double DOUBLE_EPSINON = 1e-14;

bool ObObj::operator < (const ObObj& that_obj) const {
  bool result = false;
  int err = OB_SUCCESS;
  if (get_type() == ObNullType && that_obj.get_type() != ObNullType) {
    result = true;
  } else if (that_obj.get_type() == ObNullType) {
    result = false;
  } else if (!can_compare(that_obj)) {
    result = false;
    TBSYS_LOG(ERROR, "logic error, cann't compare two obj with different type [this.type:%d,"
              "that.type:%d]", get_type(), that_obj.get_type());
  } else {
    _ObjValue this_value;
    _ObjValue that_value;
    int64_t this_timestamp = 0;
    int64_t that_timestamp = 0;
    ObObjType type = get_type();
    switch (type) {
    case ObIntType:
      err = get_int(this_value.int_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_int(that_value.int_val);
        if (OB_SUCCESS == err) {
          result = this_value.int_val < that_value.int_val;
        }
      }
      break;
    case ObVarcharType:
      err = get_varchar(this_value.varchar_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_varchar(that_value.varchar_val);
        if (OB_SUCCESS == err) {
          result = this_value.varchar_val < that_value.varchar_val;
        }
      }
      break;
    case ObFloatType:
      err = get_float(this_value.float_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_float(that_value.float_val);
        if (OB_SUCCESS == err) {
          result = this_value.float_val < that_value.float_val;
        }
      }
      break;
    case ObDoubleType:
      err = get_double(this_value.double_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_double(that_value.double_val);
        if (OB_SUCCESS == err) {
          result = this_value.double_val < that_value.double_val;
        }
      }
      break;
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
      err = get_timestamp(this_timestamp);
      if (OB_SUCCESS == err) {
        err = that_obj.get_timestamp(that_timestamp);
        if (OB_SUCCESS == err) {
          result = this_timestamp < that_timestamp;
        }
      }
      break;
    /*case ObSeqType:
      /// @todo (wushi wushi.ly@taobao.com) sequence
      break;*/
    default:
      result = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected obj type [obj.type:%d]", type);
      break;
    }
  }
  return result;
}

bool ObObj::operator>(const ObObj& that_obj) const {
  int err = OB_SUCCESS;
  bool result = false;
  if (get_type() != ObNullType && that_obj.get_type() == ObNullType) {
    result = true;
  } else if (get_type() == ObNullType) {
    result = false;
  } else if (!can_compare(that_obj)) {
    result = false;
    TBSYS_LOG(ERROR, "logic error, cann't compare two obj with different type [this.type:%d,"
              "that.type:%d]", get_type(), that_obj.get_type());
  } else {
    _ObjValue this_value;
    _ObjValue that_value;
    int64_t this_timestamp = 0;
    int64_t that_timestamp = 0;
    ObObjType type = get_type();
    switch (type) {
    case ObIntType:
      err = get_int(this_value.int_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_int(that_value.int_val);
        if (OB_SUCCESS == err) {
          result = this_value.int_val > that_value.int_val;
        }
      }
      break;
    case ObVarcharType:
      err = get_varchar(this_value.varchar_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_varchar(that_value.varchar_val);
        if (OB_SUCCESS == err) {
          result = this_value.varchar_val > that_value.varchar_val;
        }
      }
      break;
    case ObFloatType:
      err = get_float(this_value.float_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_float(that_value.float_val);
        if (OB_SUCCESS == err) {
          result = this_value.float_val > that_value.float_val;
        }
      }
      break;
    case ObDoubleType:
      err = get_double(this_value.double_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_double(that_value.double_val);
        if (OB_SUCCESS == err) {
          result = this_value.double_val > that_value.double_val;
        }
      }
      break;
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
      err = get_timestamp(this_timestamp);
      if (OB_SUCCESS == err) {
        err = that_obj.get_timestamp(that_timestamp);
        if (OB_SUCCESS == err) {
          result = this_timestamp > that_timestamp;
        }
      }
      break;
    /*case ObSeqType:
      /// @todo (wushi wushi.ly@taobao.com) sequence
      break;*/
    default:
      result = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected obj type [obj.type:%d]", type);
      break;
    }
  }
  return result;
}

bool ObObj::operator>=(const ObObj& that_obj) const {
  bool result = false;
  int err = OB_SUCCESS;
  if (that_obj.get_type() == ObNullType) {
    result = true;
  } else if (get_type() == ObNullType) {
    result = false;
  } else if (!can_compare(that_obj)) {
    result = false;
    TBSYS_LOG(ERROR, "logic error, cann't compare two obj with different type [this.type:%d,"
              "that.type:%d]", get_type(), that_obj.get_type());
  } else {
    _ObjValue this_value;
    _ObjValue that_value;
    int64_t this_timestamp = 0;
    int64_t that_timestamp = 0;
    ObObjType type = get_type();
    switch (type) {
    case ObIntType:
      err = get_int(this_value.int_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_int(that_value.int_val);
        if (OB_SUCCESS == err) {
          result = this_value.int_val >= that_value.int_val;
        }
      }
      break;
    case ObVarcharType:
      err = get_varchar(this_value.varchar_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_varchar(that_value.varchar_val);
        if (OB_SUCCESS == err) {
          result = this_value.varchar_val >= that_value.varchar_val;
        }
      }
      break;
    case ObFloatType:
      err = get_float(this_value.float_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_float(that_value.float_val);
        if (OB_SUCCESS == err) {
          result = ((this_value.float_val > that_value.float_val)
                    || (fabsf(this_value.float_val - that_value.float_val) < FLOAT_EPSINON));
        }
      }
      break;
    case ObDoubleType:
      err = get_double(this_value.double_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_double(that_value.double_val);
        if (OB_SUCCESS == err) {
          result = ((this_value.double_val > that_value.double_val)
                    || (fabsf(this_value.double_val - that_value.double_val) < DOUBLE_EPSINON));
        }
      }
      break;
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
      err = get_timestamp(this_timestamp);
      if (OB_SUCCESS == err) {
        err = that_obj.get_timestamp(that_timestamp);
        if (OB_SUCCESS == err) {
          result = this_timestamp >= that_timestamp;
        }
      }
      break;
    /*case ObSeqType:
      /// @todo (wushi wushi.ly@taobao.com) sequence
      break;*/
    default:
      result = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected obj type [obj.type:%d]", type);
      break;
    }
  }
  return result;
}

bool ObObj::operator<=(const ObObj& that_obj) const {
  bool result = false;
  int err = OB_SUCCESS;
  if (get_type() == ObNullType) {
    result = true;
  } else if (that_obj.get_type() == ObNullType) {
    result = false;
  } else if (!can_compare(that_obj)) {
    result = false;
    TBSYS_LOG(ERROR, "logic error, cann't compare two obj with different type [this.type:%d,"
              "that.type:%d]", get_type(), that_obj.get_type());
  } else {
    _ObjValue this_value;
    _ObjValue that_value;
    int64_t this_timestamp = 0;
    int64_t that_timestamp = 0;
    ObObjType type = get_type();
    switch (type) {
    case ObIntType:
      err = get_int(this_value.int_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_int(that_value.int_val);
        if (OB_SUCCESS == err) {
          result = this_value.int_val <= that_value.int_val;
        }
      }
      break;
    case ObVarcharType:
      err = get_varchar(this_value.varchar_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_varchar(that_value.varchar_val);
        if (OB_SUCCESS == err) {
          result = this_value.varchar_val <= that_value.varchar_val;
        }
      }
      break;
    case ObFloatType:
      err = get_float(this_value.float_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_float(that_value.float_val);
        if (OB_SUCCESS == err) {
          result = ((this_value.float_val < that_value.float_val)
                    || (fabsf(this_value.float_val - that_value.float_val) < FLOAT_EPSINON));
        }
      }
      break;
    case ObDoubleType:
      err = get_double(this_value.double_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_double(that_value.double_val);
        if (OB_SUCCESS == err) {
          result = ((this_value.double_val < that_value.double_val)
                    || (fabsf(this_value.double_val - that_value.double_val) < DOUBLE_EPSINON));
        }
      }
      break;
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
      err = get_timestamp(this_timestamp);
      if (OB_SUCCESS == err) {
        err = that_obj.get_timestamp(that_timestamp);
        if (OB_SUCCESS == err) {
          result = this_timestamp <= that_timestamp;
        }
      }
      break;
    /*case ObSeqType:
      /// @todo (wushi wushi.ly@taobao.com) sequence
      break;*/
    default:
      result = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected obj type [obj.type:%d]", type);
      break;
    }
  }
  return result;
}

bool ObObj::operator==(const ObObj& that_obj) const {
  bool result = false;
  int err = OB_SUCCESS;
  if ((get_type() == ObNullType) && (that_obj.get_type() == ObNullType)) {
    result = true;
  } else if ((get_type() == ObNullType) || (that_obj.get_type() == ObNullType)) {
    result = false;
  } else if (!can_compare(that_obj)) {
    result = false;
    TBSYS_LOG(ERROR, "logic error, cann't compare two obj with different type [this.type:%d,"
              "that.type:%d]", get_type(), that_obj.get_type());
  } else {
    _ObjValue this_value;
    _ObjValue that_value;
    int64_t this_timestamp = 0;
    int64_t that_timestamp = 0;
    ObObjType type = get_type();
    switch (type) {
    case ObIntType:
      err = get_int(this_value.int_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_int(that_value.int_val);
        if (OB_SUCCESS == err) {
          result = this_value.int_val == that_value.int_val;
        }
      }
      break;
    case ObVarcharType:
      err = get_varchar(this_value.varchar_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_varchar(that_value.varchar_val);
        if (OB_SUCCESS == err) {
          result = this_value.varchar_val == that_value.varchar_val;
        }
      }
      break;
    case ObFloatType:
      err = get_float(this_value.float_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_float(that_value.float_val);
        if (OB_SUCCESS == err) {
          result = fabsf(this_value.float_val - that_value.float_val) < FLOAT_EPSINON;
        }
      }
      break;
    case ObDoubleType:
      err = get_double(this_value.double_val);
      if (OB_SUCCESS == err) {
        err = that_obj.get_double(that_value.double_val);
        if (OB_SUCCESS == err) {
          result = fabs(this_value.double_val - that_value.double_val) < DOUBLE_EPSINON;
        }
      }
      break;
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
      err = get_timestamp(this_timestamp);
      if (OB_SUCCESS == err) {
        err = that_obj.get_timestamp(that_timestamp);
        if (OB_SUCCESS == err) {
          result = this_timestamp == that_timestamp;
        }
      }
      break;
    /*case ObSeqType:
      /// @todo (wushi wushi.ly@taobao.com) sequence
      break;*/
    default:
      result = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected obj type [obj.type:%d]", type);
      break;
    }
  }
  return result;
}

bool ObObj::operator!=(const ObObj& that_obj) const {
  return !(*this == that_obj);
}

int ObObj::apply(const ObObj& mutation) {
  int64_t org_type = get_type();
  int64_t org_ext = get_ext();
  int err = OB_SUCCESS;
  ObCreateTime create_time = 0;
  ObModifyTime modify_time = 0;
  bool is_add = false;
  bool org_is_add = false;
  if (ObSeqType == mutation.get_type()
      || ObMinType >= mutation.get_type()
      || ObMaxType <= mutation.get_type()) {
    TBSYS_LOG(WARN, "unsupported type [type:%d]", static_cast<int32_t>(mutation.get_type()));
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err
      && ObExtendType != get_type()
      && ObNullType != get_type()
      && ObExtendType != mutation.get_type()
      && ObNullType != mutation.get_type()
      && get_type() != mutation.get_type()) {
    TBSYS_LOG(WARN, "type not coincident [this->type:%d,mutation.type:%d]",
              static_cast<int>(get_type()), static_cast<int>(mutation.get_type()));
    err = OB_INVALID_ARGUMENT;
  }
  _ObjValue value, mutation_value;
  if (OB_SUCCESS == err) {
    bool ext_val_can_change = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == get_ext())  || (ObNullType == get_type());
    bool org_is_nop = (ObActionFlag::OP_NOP == get_ext());
    switch (mutation.get_type()) {
    case ObNullType:
      set_null();
      break;
    case ObVarcharType:
      *this = mutation;
      break;
    case ObIntType:
      if (ext_val_can_change || org_is_nop) {
        set_int(0);
      }
      err = get_int(value.int_val, org_is_add);
      if (OB_SUCCESS == err) {
        err = mutation.get_int(mutation_value.int_val, is_add);
      }
      if (OB_SUCCESS == err) {
        if (is_add) {
          value.int_val += mutation_value.int_val;
        } else {
          value.int_val = mutation_value.int_val;
        }
        set_int(value.int_val, (org_is_add || org_is_nop) && is_add);
      }
      break;
    case ObFloatType:
      if (ext_val_can_change || org_is_nop) {
        set_float(0);
      }
      err = get_float(value.float_val, org_is_add);
      if (OB_SUCCESS == err) {
        err = mutation.get_float(mutation_value.float_val, is_add);
      }
      if (OB_SUCCESS == err) {
        if (is_add) {
          value.float_val += mutation_value.float_val;
        } else {
          value.float_val = mutation_value.float_val;
        }
        set_float(value.float_val, is_add && (org_is_add || org_is_nop));
      }
      break;
    case ObDoubleType:
      if (ext_val_can_change || org_is_nop) {
        set_double(0);
      }
      err = get_double(value.double_val, org_is_add);
      if (OB_SUCCESS == err) {
        err = mutation.get_double(mutation_value.double_val, is_add);
      }
      if (OB_SUCCESS == err) {
        if (is_add) {
          value.double_val += mutation_value.double_val;
        } else {
          value.double_val = mutation_value.double_val;
        }
        set_double(value.double_val, (org_is_add || org_is_nop) && is_add);
      }
      break;
    case ObDateTimeType:
      if (ext_val_can_change || org_is_nop) {
        set_datetime(0);
      }
      err = get_datetime(value.second_val, org_is_add);
      if (OB_SUCCESS == err) {
        err = mutation.get_datetime(mutation_value.second_val, is_add);
      }
      if (OB_SUCCESS == err) {
        if (is_add) {
          value.second_val += mutation_value.second_val;
        } else {
          value.second_val = mutation_value.second_val;
        }
        set_datetime(value.second_val, is_add && (org_is_add || org_is_nop));
      }
      break;
    case ObPreciseDateTimeType:
      if (ext_val_can_change || org_is_nop) {
        set_precise_datetime(0);
      }
      err = get_precise_datetime(value.microsecond_val, org_is_add);
      if (OB_SUCCESS == err) {
        err = mutation.get_precise_datetime(mutation_value.microsecond_val, is_add);
      }
      if (OB_SUCCESS == err) {
        if (is_add) {
          value.microsecond_val += mutation_value.microsecond_val;
        } else {
          value.microsecond_val = mutation_value.microsecond_val;
        }
        set_precise_datetime(value.microsecond_val, is_add && (org_is_add || org_is_nop));
      }
      break;
    case ObExtendType:
      switch (mutation.get_ext()) {
      case ObActionFlag::OP_DEL_ROW:
      case ObActionFlag::OP_DEL_TABLE:
        /// used for join, if right row was deleted, set the cell to null
        set_null();
        break;
      case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
        /// do nothing
        break;
      case ObActionFlag::OP_NOP:
        if (get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST
            || get_ext() == ObActionFlag::OP_DEL_ROW) {
          set_null();
        }
        break;
      default:
        TBSYS_LOG(ERROR, "unsupported ext value [value:%ld]", mutation.get_ext());
        err = OB_INVALID_ARGUMENT;
        break;
      }

      break;
    case ObCreateTimeType:
      err = mutation.get_createtime(create_time);
      if (OB_SUCCESS == err) {
        if (ext_val_can_change || org_is_nop) {
          set_createtime(create_time);
        }
      }
      if (OB_SUCCESS == err) {
        err = get_createtime(value.create_time_val);
      }
      if (OB_SUCCESS == err) {
        err = mutation.get_createtime(mutation_value.create_time_val);
      }
      if (OB_SUCCESS == err) {
        set_createtime(std::min<ObCreateTime>(value.create_time_val, mutation_value.create_time_val));
      }
      break;
    case ObModifyTimeType:
      err = mutation.get_modifytime(modify_time);
      if (OB_SUCCESS == err) {
        if (ext_val_can_change || org_is_nop) {
          set_modifytime(modify_time);
        }
      }
      if (OB_SUCCESS == err) {
        err = get_modifytime(value.modify_time_val);
      }
      if (OB_SUCCESS == err) {
        err = mutation.get_modifytime(mutation_value.modify_time_val);
      }
      if (OB_SUCCESS == err) {
        set_modifytime(std::max<ObCreateTime>(value.modify_time_val, mutation_value.modify_time_val));
      }
      break;
    default:
      /* case ObSeqType: */
      TBSYS_LOG(ERROR, "unsupported type [type:%d]", static_cast<int32_t>(mutation.get_type()));
      err = OB_INVALID_ARGUMENT;
      break;
    }
  }
  if (OB_SUCCESS != err) {
    TBSYS_LOG(WARN, "fail to apply [this->type:%ld,this->ext:%ld,mutation->type:%d,mutation->ext:%ld, err:%d]",
              org_type, org_ext, mutation.get_type(), mutation.get_ext(), err);
  }
  return err;
}

void ObObj::dump(const int32_t log_level /*=TBSYS_LOG_LEVEL_DEBUG*/) const {
  int64_t int_val = 0;
  float float_val = 0.0;
  double double_val = 0.0;
  bool is_add = false;
  ObString str_val;
  switch (get_type()) {
  case ObNullType:
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%ld] type:ObNull", pthread_self());
    break;
  case ObIntType:
    get_int(int_val, is_add);
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                            "[%ld] type:ObInt, val:%ld,is_add:%s", pthread_self(), int_val, is_add ? "true" : "false");
    break;
  case ObVarcharType:
    get_varchar(str_val);
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                            "[%ld] type:ObVarChar,len :%d,val:", pthread_self(), str_val.length());
    common::hex_dump(str_val.ptr(), str_val.length(), true, log_level);
    break;
  case ObFloatType:
    get_float(float_val, is_add);
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                            "[%ld] type:ObFloat, val:%f,is_add:%s", pthread_self(), float_val, is_add ? "true" : "false");
    break;
  case ObDoubleType:
    get_double(double_val, is_add);
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                            "[%ld] type:ObDouble, val:%f,is_add:%s", pthread_self(), double_val, is_add ? "true" : "false");
    break;
  case ObDateTimeType:
    get_datetime(int_val, is_add);
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                            "[%ld] type:ObDateTime(seconds), val:%ld,is_add:%s", pthread_self(), int_val, is_add ? "true" : "false");
    break;
  case ObPreciseDateTimeType:
    get_precise_datetime(int_val, is_add);
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                            "[%ld] type:ObPreciseDateTime(microseconds), val:%ld,is_add:%s", pthread_self(), int_val, is_add ? "true" : "false");
    break;
  case ObSeqType:
    //TODO
    break;
  case ObCreateTimeType:
    get_createtime(int_val);
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                            "[%ld] type:ObCreateTime, val:%ld", pthread_self(), int_val);
    break;
  case ObModifyTimeType:
    get_modifytime(int_val);
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                            "[%ld] type:ObModifyTime, val:%ld", pthread_self(), int_val);
    break;
  case ObExtendType:
    get_ext(int_val);
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                            "[%ld] type:ObExt, val:%ld", pthread_self(), int_val);
    break;
  default:
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), " [%ld] unexpected type (%d)", pthread_self(), get_type());
    break;
  }
}

DEFINE_SERIALIZE(ObObj) {
  ObObjType type = get_type();
  int ret = 0;
  int64_t tmp_pos = pos;
  int8_t obj_op_flag = meta_.op_flag_;

  if (OB_SUCCESS == ret) {
    switch (type) {
    case ObNullType:
      ret = serialization::encode_null(buf, buf_len, tmp_pos);
      break;
    case ObIntType:
      ret = serialization::encode_int(buf, buf_len, tmp_pos, value_.int_val, obj_op_flag == ADD);
      break;
    case ObVarcharType:
      ret = serialization::encode_str(buf, buf_len, tmp_pos, value_.varchar_val, varchar_len_);
      break;
    case ObFloatType:
      ret = serialization::encode_float_type(buf, buf_len, tmp_pos, value_.float_val, obj_op_flag == ADD);
      break;
    case ObDoubleType:
      ret = serialization::encode_double_type(buf, buf_len, tmp_pos, value_.double_val, obj_op_flag == ADD);
      break;
    case ObDateTimeType:
      ret = serialization::encode_datetime_type(buf, buf_len, tmp_pos, value_.time_val, obj_op_flag == ADD);
      break;
    case ObPreciseDateTimeType:
      ret = serialization::encode_precise_datetime_type(buf, buf_len, tmp_pos, value_.precisetime_val, obj_op_flag == ADD);
      break;
    case ObModifyTimeType:
      ret = serialization::encode_modifytime_type(buf, buf_len, tmp_pos, value_.modifytime_val);
      break;
    case ObCreateTimeType:
      ret = serialization::encode_createtime_type(buf, buf_len, tmp_pos, value_.createtime_val);
      break;
    case ObSeqType:
      //TODO
      break;
    case ObExtendType:
      ret = serialization::encode_extend_type(buf, buf_len, tmp_pos, value_.ext_val);
      break;
    default:
      TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  if (OB_SUCCESS == ret)
    pos = tmp_pos;
  return ret;
}

DEFINE_DESERIALIZE(ObObj) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int8_t first_byte = 0;
  bool is_add = false;

  if (OB_SUCCESS == (ret = serialization::decode_i8(buf, data_len, tmp_pos, &first_byte))) {
    if (serialization::OB_EXTEND_TYPE == first_byte) {  // is extend type
      meta_.type_ = ObExtendType;
      ret = serialization::decode_vi64(buf, data_len, tmp_pos, &value_.ext_val);
    } else {
      int8_t type = (first_byte & 0xc0) >> 6;
      switch (type) {
      case 0:
      case 1: //int
        meta_.type_ = ObIntType;
        ret = serialization::decode_int(buf, data_len, first_byte, tmp_pos, value_.int_val, is_add);
        break;
      case 2: //str
        meta_.type_ = ObVarcharType;
        value_.varchar_val = serialization::decode_str(buf, data_len, first_byte, tmp_pos, varchar_len_);
        if (NULL == value_.varchar_val) {
          ret = OB_ERROR;
        }
        break;
      case 3: { //other
        int8_t  sub_type = (first_byte & 0x30) >> 4; //00 11 00 00
        switch (sub_type) {
        case 0: //TODO seq & reserved
          break;
        case 1: //ObDatetime
          meta_.type_ = ObDateTimeType;
          ret = serialization::decode_datetime_type(buf, data_len, first_byte, tmp_pos, value_.time_val, is_add);
          break;
        case 2: //ObPreciseDateTime
          meta_.type_ = ObPreciseDateTimeType;
          ret = serialization::decode_precise_datetime_type(buf, data_len, first_byte, tmp_pos, value_.precisetime_val, is_add);
          break;
        case 3: { //other
          int8_t sub_sub_type = (first_byte & 0x0c) >> 2; // 00 00 11 00
          switch (sub_sub_type) {
          case 0: //ObModifyTime
            meta_.type_ = ObModifyTimeType;
            ret = serialization::decode_modifytime_type(buf, data_len, first_byte, tmp_pos, value_.modifytime_val);
            break;
          case 1: //ObCreateTime
            meta_.type_ = ObCreateTimeType;
            ret = serialization::decode_createtime_type(buf, data_len, first_byte, tmp_pos, value_.createtime_val);
            break;
          case 2:
            if (first_byte & 0x02) { //ObDouble
              meta_.type_ = ObDoubleType;
              ret = serialization::decode_double_type(buf, data_len, first_byte, tmp_pos, value_.double_val, is_add);
            } else { //ObFloat
              meta_.type_ = ObFloatType;
              ret = serialization::decode_float_type(buf, data_len, first_byte, tmp_pos, value_.float_val, is_add);
            }
            break;
          case 3: //ObNull
            meta_.type_ = ObNullType;
            break;
          default:
            TBSYS_LOG(ERROR, "invalid obj_type=%d", sub_sub_type);
            ret = OB_ERR_UNEXPECTED;
            break;
          }
        }
        break;
        default:
          TBSYS_LOG(ERROR, "invalid obj_type=%d", sub_type);
          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }
      break;
      default:
        TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      //
      if (is_add) {
        meta_.op_flag_ = ADD;
      } else {
        meta_.op_flag_ = INVALID_OP_FLAG;
      }
    }
    if (OB_SUCCESS == ret)
      pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObObj) {
  ObObjType type = get_type();
  int64_t len = 0;

  switch (type) {
  case ObNullType:
    len += serialization::encoded_length_null();
    break;
  case ObIntType:
    len += serialization::encoded_length_int(value_.int_val);
    break;
  case ObVarcharType:
    len += serialization::encoded_length_str(varchar_len_);
    break;
  case ObFloatType:
    len += serialization::encoded_length_float_type();
    break;
  case ObDoubleType:
    len += serialization::encoded_length_double_type();
    break;
  case ObDateTimeType:
    len += serialization::encoded_length_datetime(value_.time_val);
    break;
  case ObPreciseDateTimeType:
    len += serialization::encoded_length_precise_datetime(value_.precisetime_val);
    break;
  case ObModifyTimeType:
    len += serialization::encoded_length_modifytime(value_.modifytime_val);
    break;
  case ObCreateTimeType:
    len += serialization::encoded_length_createtime(value_.createtime_val);
    break;
  case ObSeqType:
    //TODO (maoqi)
    break;
  case ObExtendType:
    len += serialization::encoded_length_extend(value_.ext_val);
    break;
  default:
    TBSYS_LOG(ERROR, "unexpected obj type [obj.type:%d]", type);
    break;
  }
  return len;
}

uint32_t ObObj::murmurhash2(const uint32_t hash) const {
  uint32_t result = hash;
  ObObjType type = get_type();

  result = ::murmurhash2(&meta_, sizeof(meta_), result);
  switch (type) {
  case ObNullType:
    break;
  case ObIntType:
    result = ::murmurhash2(&value_.int_val, sizeof(value_.int_val), result);
    break;
  case ObVarcharType:
    result = ::murmurhash2(value_.varchar_val, varchar_len_, result);
    break;
  case ObFloatType:
    result = ::murmurhash2(&value_.float_val, sizeof(value_.float_val), result);
    break;
  case ObDoubleType:
    result = ::murmurhash2(&value_.double_val, sizeof(value_.double_val), result);
    break;
  case ObDateTimeType:
    result = ::murmurhash2(&value_.time_val, sizeof(value_.time_val), result);
    break;
  case ObPreciseDateTimeType:
    result = ::murmurhash2(&value_.precisetime_val, sizeof(value_.precisetime_val), result);
    break;
  case ObModifyTimeType:
    result = ::murmurhash2(&value_.modifytime_val, sizeof(value_.modifytime_val), result);
    break;
  case ObCreateTimeType:
    result = ::murmurhash2(&value_.createtime_val, sizeof(value_.createtime_val), result);
    break;
  case ObSeqType:
    //TODO (maoqi)
    break;
  case ObExtendType:
    result = ::murmurhash2(&value_.ext_val, sizeof(value_.ext_val), result);
    break;
  default:
    TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
    result = 0;
    break;
  }
  return result;
}



int64_t ObObj::checksum(const int64_t current) const {
  int64_t ret = current;
  ObObjType type = get_type();

  ret = ob_crc64(ret, &meta_, sizeof(meta_));
  switch (type) {
  case ObNullType:
    break;
  case ObIntType:
    ret = ob_crc64(ret, &value_.int_val, sizeof(value_.int_val));
    break;
  case ObVarcharType:
    ret = ob_crc64(ret, value_.varchar_val, varchar_len_);
    break;
  case ObFloatType:
    ret = ob_crc64(ret, &value_.float_val, sizeof(value_.float_val));
    break;
  case ObDoubleType:
    ret = ob_crc64(ret, &value_.double_val, sizeof(value_.double_val));
    break;
  case ObDateTimeType:
    ret = ob_crc64(ret, &value_.time_val, sizeof(value_.time_val));
    break;
  case ObPreciseDateTimeType:
    ret = ob_crc64(ret, &value_.precisetime_val, sizeof(value_.precisetime_val));
    break;
  case ObModifyTimeType:
    ret = ob_crc64(ret, &value_.modifytime_val, sizeof(value_.modifytime_val));
    break;
  case ObCreateTimeType:
    ret = ob_crc64(ret, &value_.createtime_val, sizeof(value_.createtime_val));
    break;
  case ObSeqType:
    //TODO (maoqi)
    break;
  case ObExtendType:
    ret = ob_crc64(ret, &value_.ext_val, sizeof(value_.ext_val));
    break;
  default:
    TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
    ret = 0;
    break;
  }
  return ret;
}
}
}

