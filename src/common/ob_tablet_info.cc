/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_info.cc for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */
#include "tbsys.h"
#include "ob_tablet_info.h"

namespace sb {
namespace common {

// --------------------------------------------------------
// class ObTabletLocation implements
// --------------------------------------------------------

void ObTabletLocation::dump(const ObTabletLocation& location) {
  const int32_t MAX_SERVER_ADDR_SIZE = 128;
  char server_addr[MAX_SERVER_ADDR_SIZE];
  location.chunkserver_.to_string(server_addr, MAX_SERVER_ADDR_SIZE);
  TBSYS_LOG(INFO, "tablet_version :%ld, location:%s\n",
            location.tablet_version_, server_addr);
}

DEFINE_SERIALIZE(ObTabletLocation) {
  int ret = OB_ERROR;
  ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(tablet_version_));

  if (ret == OB_SUCCESS)
    ret = chunkserver_.serialize(buf, buf_len, pos);

  return ret;
}

DEFINE_DESERIALIZE(ObTabletLocation) {
  int ret = OB_ERROR;
  ret = serialization::decode_vi64(buf, data_len, pos, (int64_t*)&tablet_version_);

  if (ret == OB_SUCCESS)
    ret = chunkserver_.deserialize(buf, data_len, pos);

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTabletLocation) {
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(tablet_version_);
  total_size += chunkserver_.get_serialize_size();
  return total_size;
}

DEFINE_SERIALIZE(ObTabletInfo) {
  int ret = OB_ERROR;
  ret = serialization::encode_vi64(buf, buf_len, pos, row_count_);

  if (ret == OB_SUCCESS)
    ret = serialization::encode_vi64(buf, buf_len, pos, occupy_size_);

  if (ret == OB_SUCCESS)
    ret = serialization::encode_vi64(buf, buf_len, pos, crc_sum_);

  if (ret == OB_SUCCESS)
    ret = range_.serialize(buf, buf_len, pos);

  return ret;
}

DEFINE_DESERIALIZE(ObTabletInfo) {
  int ret = OB_ERROR;
  ret = serialization::decode_vi64(buf, data_len, pos, &row_count_);

  if (ret == OB_SUCCESS)
    ret = serialization::decode_vi64(buf, data_len, pos, &occupy_size_);

  if (ret == OB_SUCCESS)
    ret = serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t*>(&crc_sum_));

  if (ret == OB_SUCCESS)
    ret = range_.deserialize(buf, data_len, pos);

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTabletInfo) {
  int64_t total_size = 0;

  total_size += serialization::encoded_length_vi64(row_count_);
  total_size += serialization::encoded_length_vi64(occupy_size_);
  total_size += serialization::encoded_length_vi64(crc_sum_);
  total_size += range_.get_serialize_size();

  return total_size;
}

// ObTabletReportInfo
DEFINE_SERIALIZE(ObTabletReportInfo) {
  int ret = OB_SUCCESS;
  ret = tablet_info_.serialize(buf, buf_len, pos);
  if (ret == OB_SUCCESS)
    ret = tablet_location_.serialize(buf, buf_len, pos);
  return ret;
}

DEFINE_DESERIALIZE(ObTabletReportInfo) {
  int ret = OB_SUCCESS;
  ret = tablet_info_.deserialize(buf, data_len, pos);
  if (ret == OB_SUCCESS)
    ret = tablet_location_.deserialize(buf, data_len, pos);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTabletReportInfo) {
  int64_t total_size = 0;

  total_size += tablet_info_.get_serialize_size();
  total_size += tablet_location_.get_serialize_size();

  return total_size;
}

// ObTabletReportInfoList
DEFINE_SERIALIZE(ObTabletReportInfoList) {
  int ret = OB_ERROR;

  int64_t size = tablet_list_.get_array_index();
  ret = serialization::encode_vi64(buf, buf_len, pos, size);

  if (ret == OB_SUCCESS) {
    for (int64_t i = 0; i < size; ++i) {
      ret = tablets_[i].serialize(buf, buf_len, pos);
      if (ret != OB_SUCCESS)
        break;
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObTabletReportInfoList) {
  int ret = OB_ERROR;

  int64_t size = 0;
  ret = serialization::decode_vi64(buf, data_len, pos, &size);

  if (ret == OB_SUCCESS && size > 0) {
    for (int64_t i = 0; i < size; ++i) {
      ObTabletReportInfo tablet;
      ret = tablet.deserialize(buf, data_len, pos);
      if (ret != OB_SUCCESS)
        break;

      tablet_list_.push_back(tablet);
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTabletReportInfoList) {
  int total_size = 0;

  int64_t size = tablet_list_.get_array_index();
  total_size += serialization::encoded_length_vi64(size);

  if (size > 0) {
    for (int64_t i = 0; i < size; ++i)
      total_size += tablets_[i].get_serialize_size();
  }

  return total_size;
}


// ObTabletInfoList
DEFINE_SERIALIZE(ObTabletInfoList) {
  int ret = OB_ERROR;

  int64_t size = tablet_list.get_array_index();
  ret = serialization::encode_vi64(buf, buf_len, pos, size);

  if (ret == OB_SUCCESS) {
    for (int64_t i = 0; i < size; ++i) {
      ret = tablets[i].serialize(buf, buf_len, pos);
      if (ret != OB_SUCCESS)
        break;
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObTabletInfoList) {
  int ret = OB_ERROR;

  int64_t size = 0;
  ret = serialization::decode_vi64(buf, data_len, pos, &size);

  if (ret == OB_SUCCESS && size > 0) {
    ObTabletInfo tablet;
    for (int64_t i = 0; i < size; ++i) {
      ret = tablet.deserialize(buf, data_len, pos);
      if (ret != OB_SUCCESS)
        break;

      tablet_list.push_back(tablet);
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTabletInfoList) {
  int total_size = 0;

  int64_t size = tablet_list.get_array_index();
  total_size += serialization::encoded_length_vi64(size);

  if (size > 0) {
    for (int64_t i = 0; i < size; ++i)
      total_size += tablets[i].get_serialize_size();
  }

  return total_size;
}

} // end namespace common
} // end namespace sb


