/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_range.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include "ob_range.h"
#include <tbsys.h>
#include <string.h>
#include "utility.h"

namespace sb {
namespace common {

// --------------------------------------------------------
// class ObRange implements
// --------------------------------------------------------

bool ObRange::check(void) const {
  bool ret = true;
  if ((start_key_ > end_key_) && (!border_flag_.is_max_value()) && (!border_flag_.is_min_value())) {
    TBSYS_LOG(WARN, "%s", "check start key gt than end key");
    ret = false;
  } else if (start_key_ == end_key_) {
    if (!border_flag_.is_min_value() && !border_flag_.is_max_value()) {
      if (start_key_.length() == 0 || !border_flag_.inclusive_start() || !border_flag_.inclusive_end()) {
        TBSYS_LOG(WARN, "check border flag or length failed:length[%d], flag[%u]",
                  start_key_.length(), border_flag_.get_data());
        ret = false;
      }
    }
  }
  return ret;
}

bool ObRange::equal(const ObRange& r) const {
  return (compare_with_startkey(r) == 0) && (compare_with_endkey(r) == 0);
}

bool ObRange::intersect(const ObRange& r) const {
  // suppose range.start_key_ <= range.end_key_
  if (table_id_ != r.table_id_) return false;
  if (empty() || r.empty()) return false;
  if (is_whole_range() || r.is_whole_range()) return true;

  ObString lkey, rkey;
  bool ret = false;
  int8_t include_lborder = 0;
  int8_t include_rborder = 0;
  bool min_value = false;
  int cmp = compare_with_endkey(r);
  if (cmp < 0) {
    lkey = end_key_;
    rkey = r.start_key_;
    include_lborder = (border_flag_.inclusive_end());
    include_rborder = (r.border_flag_.inclusive_start());
    min_value = (r.border_flag_.is_min_value());
  } else if (cmp > 0) {
    lkey = r.end_key_;
    rkey = start_key_;
    include_lborder = (r.border_flag_.inclusive_end());
    include_rborder = (border_flag_.inclusive_start());
    min_value = (border_flag_.is_min_value());
  } else {
    ret = true;
  }

  if (cmp != 0) {
    if (min_value) ret = true;
    else if (lkey < rkey) ret = false;
    else if (lkey > rkey) ret = true;
    else ret = (include_lborder != 0 && include_rborder != 0);
  }

  return ret;
}

void ObRange::dump() const {
  // TODO, used in ObString is a c string end with '\0'
  // just for test.
  const int32_t MAX_LEN = OB_MAX_ROW_KEY_LENGTH;
  char sbuf[MAX_LEN];
  char ebuf[MAX_LEN];
  memcpy(sbuf, start_key_.ptr(), start_key_.length());
  sbuf[start_key_.length()] = 0;
  memcpy(ebuf, end_key_.ptr(), end_key_.length());
  ebuf[end_key_.length()] = 0;
  TBSYS_LOG(DEBUG,
            "table:%ld, border flag:%d, start key:%s(%d), end key:%s(%d)\n",
            table_id_, border_flag_.get_data(), sbuf,  start_key_.length(), ebuf, end_key_.length());
}

void ObRange::hex_dump(const int32_t log_level) const {
  TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                          "table:%ld, border flag:%d\n", table_id_, border_flag_.get_data());
  TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                          "dump start key with hex format, length(%d)", start_key_.length());
  common::hex_dump(start_key_.ptr(), start_key_.length(), true, log_level);
  TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                          "dump end   key with hex format, length(%d)", end_key_.length());
  common::hex_dump(end_key_.ptr(), end_key_.length(), true, log_level);
}

int ObRange::to_string(char* buffer, const int32_t length) const {
  int ret = OB_SUCCESS;
  if (NULL == buffer || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }
  const int32_t min_header = 20;
  const int32_t key_length = 2 * start_key_.length() + 2 * end_key_.length() + 3 ;
  const int32_t min_body = 9; // (MIN,MAX)
  const int32_t min_length = min_header + ((key_length > min_body) ? key_length : min_body);
  if (length < min_length) {
    ret = OB_SIZE_OVERFLOW;
  }
  int32_t pos = 0;
  if (OB_SUCCESS == ret) {

    const char* lb = 0;
    if (border_flag_.inclusive_start()) lb = "[";
    else lb = "(";
    if (border_flag_.is_min_value()) lb = "(MIN";

    snprintf(buffer, length, "table:%ld,%s", table_id_, lb);
    pos += strlen(buffer);

    // add start_key_
    if (!border_flag_.is_min_value()) {
      int byte_len = hex_to_str(start_key_.ptr(), start_key_.length(), buffer + pos, length - pos);
      pos += byte_len * 2;
    }

    // add ,
    buffer[pos++] = ',';

    // add end_key_
    if (!border_flag_.is_max_value()) {
      int byte_len = hex_to_str(end_key_.ptr(), end_key_.length(), buffer + pos, length - pos);
      pos += byte_len * 2;
    }

    const char* rb = 0;
    if (border_flag_.inclusive_end()) rb = "]";
    else rb = ")";
    if (border_flag_.is_max_value()) rb = "MAX)";
    snprintf(buffer + pos, length - pos, "%s", rb);

  }

  return ret;
}

DEFINE_SERIALIZE(ObRange) {
  int ret = OB_ERROR;
  ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(table_id_));

  if (ret == OB_SUCCESS)
    ret = serialization::encode_i8(buf, buf_len, pos, border_flag_.get_data());

  if (ret == OB_SUCCESS)
    ret = start_key_.serialize(buf, buf_len, pos);

  if (ret == OB_SUCCESS)
    ret = end_key_.serialize(buf, buf_len, pos);

  return ret;
}

DEFINE_DESERIALIZE(ObRange) {
  int ret = OB_ERROR;
  ret = serialization::decode_vi64(buf, data_len, pos, (int64_t*)&table_id_);

  if (ret == OB_SUCCESS) {
    int8_t flag = 0;
    ret = serialization::decode_i8(buf, data_len, pos, &flag);
    if (ret == OB_SUCCESS)
      border_flag_.set_data(flag);
  }

  if (ret == OB_SUCCESS)
    ret = start_key_.deserialize(buf, data_len, pos);

  if (ret == OB_SUCCESS)
    ret = end_key_.deserialize(buf, data_len, pos);

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRange) {
  int64_t total_size = 0;

  total_size += serialization::encoded_length_vi64(table_id_);
  total_size += serialization::encoded_length_i8(border_flag_.get_data());

  total_size += start_key_.get_serialize_size();
  total_size += end_key_.get_serialize_size();

  return total_size;
}
} // end namespace common
} // end namespace sb


