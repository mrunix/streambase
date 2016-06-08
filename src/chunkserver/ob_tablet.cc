/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_tablet.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     huating <huating.zmq@taobao.com>
 *
 */

#include "ob_tablet.h"
#include "common/ob_crc64.h"
#include "common/utility.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_tablet_image.h"

using namespace sb::common;
using namespace sb::sstable;

namespace sb {
namespace chunkserver {
//----------------------------------------
// struct ObTabletRangeInfo
//----------------------------------------
DEFINE_SERIALIZE(ObTabletRangeInfo) {
  int ret = OB_SUCCESS;
  int64_t serialize_size = get_serialize_size();

  if ((NULL == buf) || (serialize_size + pos > buf_len)) {
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret
      && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, start_key_size_)
      && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, end_key_size_)
      && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, is_merged_)
      && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, border_flag_)
      && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, table_id_)) {
    ret = OB_SUCCESS;
  } else {
    ret = OB_ERROR;
  }

  return ret;
}

DEFINE_DESERIALIZE(ObTabletRangeInfo) {
  int ret = OB_SUCCESS;
  int64_t serialize_size = get_serialize_size();

  if (NULL == buf || serialize_size > data_len) {
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret
      && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &start_key_size_)
      && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &end_key_size_)
      && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &is_merged_)
      && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &border_flag_)
      && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &table_id_)) {
    ret = OB_SUCCESS;
  } else {
    ret = OB_ERROR;
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTabletRangeInfo) {
  int64_t total_size = 0;

  total_size += serialization::encoded_length_i16(start_key_size_);
  total_size += serialization::encoded_length_i16(end_key_size_);
  total_size += serialization::encoded_length_i16(is_merged_);
  total_size += serialization::encoded_length_i32(border_flag_);
  total_size += serialization::encoded_length_i64(table_id_);

  return total_size;
}




//----------------------------------------
// class ObTablet
//----------------------------------------
ObTablet::ObTablet() {
  reset();
}

ObTablet::~ObTablet() {
  destroy();
}

void ObTablet::destroy() {
  // ObTabletImage will free reader's memory on destory.
  int64_t reader_count = sstable_reader_list_.get_array_index();
  for (int64_t i = 0 ; i < reader_count; ++i) {
    ObSSTableReader* reader = *sstable_reader_list_.at(i);
    reader->~ObSSTableReader();
  }
  reset();
}

void ObTablet::reset() {
  sstable_loaded_ = OB_NOT_INIT;
  merged_ = 0;
  merge_count_ = 0;
  disk_no_ = 0;
  data_version_ = 0;
  memset(sstable_id_inventory_, 0, sizeof(sstable_id_inventory_));
  sstable_id_list_.init(MAX_SSTABLE_PER_TABLET, sstable_id_inventory_, 0);
  memset(sstable_reader_inventory_, 0, sizeof(sstable_reader_inventory_));
  sstable_reader_list_.init(MAX_SSTABLE_PER_TABLET, sstable_reader_inventory_, 0);
}

int ObTablet::add_sstable_by_id(const ObSSTableId& sstable_id) {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == sstable_loaded_) {
    TBSYS_LOG(ERROR, "sstable already loaded, cannot add.");
    ret = OB_INIT_TWICE;
  }

  // check disk no?
  if (OB_SUCCESS == ret) {
    if (0 != disk_no_ && (uint32_t)disk_no_ != get_sstable_disk_no(sstable_id.sstable_file_id_)) {
      TBSYS_LOG(ERROR, "add file :%ld not in same disk:%d",
                sstable_id.sstable_file_id_, disk_no_);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret) {
    if (!sstable_id_list_.push_back(sstable_id))
      ret = OB_SIZE_OVERFLOW;
  }

  return ret;
}

int ObTablet::set_range_by_info(const ObTabletRangeInfo& info,
                                char* row_key_stream_ptr, const int64_t row_key_stream_size) {
  int ret = OB_SUCCESS;
  if (info.start_key_size_ + info.end_key_size_ > row_key_stream_size) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    range_.start_key_.assign_ptr(row_key_stream_ptr, info.start_key_size_);
    range_.end_key_.assign_ptr(row_key_stream_ptr + info.start_key_size_,
                               info.end_key_size_);
    range_.table_id_ = info.table_id_;
    range_.border_flag_.set_data(static_cast<int8_t>(info.border_flag_));
    merged_ = info.is_merged_;
  }
  return ret;
}

void ObTablet::get_range_info(ObTabletRangeInfo& info) const {
  info.start_key_size_ = range_.start_key_.length();
  info.end_key_size_ = range_.end_key_.length();
  info.table_id_ = range_.table_id_;
  info.border_flag_ = range_.border_flag_.get_data();
  info.is_merged_ = merged_;
}

DEFINE_SERIALIZE(ObTablet) {
  int ret = OB_ERROR;
  ret = serialization::encode_vi64(buf, buf_len, pos, data_version_);

  int64_t size = sstable_id_list_.get_array_index();
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, pos, size);
  }

  if (OB_SUCCESS == ret) {
    for (int64_t i = 0; i < size; ++i) {
      ObSSTableId* sstable_id = sstable_id_list_.at(i);
      ret = sstable_id->serialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret)
        break;
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObTablet) {
  int ret = OB_ERROR;

  ret = serialization::decode_vi64(buf, data_len, pos, &data_version_);

  int64_t size = 0;
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, pos, &size);
  }

  if (OB_SUCCESS == ret && size > 0) {
    for (int64_t i = 0; i < size; ++i) {
      ObSSTableId sstable_id;
      ret = sstable_id.deserialize(buf, data_len, pos);
      if (ret != OB_SUCCESS)
        break;

      sstable_id_list_.push_back(sstable_id);
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTablet) {
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(data_version_);

  int64_t size = sstable_id_list_.get_array_index();
  total_size += serialization::encoded_length_vi64(size);

  if (size > 0) {
    for (int64_t i = 0; i < size; ++i)
      total_size += sstable_id_list_.at(i)->get_serialize_size();
  }

  return total_size;
}

int ObTablet::find_sstable(const common::ObRange& range,
                           ObSSTableReader* sstable[], int32_t& size) const {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != sstable_loaded_)
    ret = sstable_loaded_;

  UNUSED(range);
  int index = 0;
  if (OB_SUCCESS == ret) {
    int64_t sstable_size = sstable_reader_list_.get_array_index();
    for (int64_t i = 0; i < sstable_size; ++i) {
      ObSSTableReader* reader = *sstable_reader_list_.at(i);
      if (index >= size) { ret = OB_SIZE_OVERFLOW; break; }
      sstable[index++] = reader;
    }
    if (OB_SUCCESS == ret) size = index;
  }
  return ret;
}

int ObTablet::find_sstable(const common::ObString& key,
                           ObSSTableReader* sstable[], int32_t& size) const {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != sstable_loaded_)
    ret = sstable_loaded_;

  int index = 0;
  if (OB_SUCCESS == ret) {
    int64_t sstable_size = sstable_reader_list_.get_array_index();
    for (int64_t i = 0; i < sstable_size; ++i) {
      ObSSTableReader* reader = *sstable_reader_list_.at(i);
      if (!reader->may_contain(key)) continue;
      if (index >= size) { ret = OB_SIZE_OVERFLOW; break; }
      sstable[index++] = reader;
    }
    if (OB_SUCCESS == ret) size = index;
  }
  return ret;
}

int ObTablet::find_sstable(const ObSSTableId& sstable_id,
                           ObSSTableReader*& reader) const {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != sstable_loaded_)
    ret = sstable_loaded_;

  int64_t i = 0;
  int64_t size = sstable_reader_list_.get_array_index();
  if (OB_SUCCESS == ret) {
    for (; i < size; ++i) {
      reader = *sstable_reader_list_.at(i);
      if (reader->get_sstable_id() == sstable_id)
        break;
    }

    if (i >= size) {
      ret = OB_ENTRY_NOT_EXIST;
      reader = NULL;
    }
  }

  return ret;
}

/*
 * @return OB_SUCCESS if sstable_id exists in tablet
 * or OB_CS
 */
int ObTablet::include_sstable(const ObSSTableId& sstable_id) const {
  int ret = OB_SUCCESS;

  int64_t i = 0;
  int64_t size = sstable_id_list_.get_array_index();
  if (OB_SUCCESS == ret) {
    for (; i < size; ++i) {
      if (*sstable_id_list_.at(i) == sstable_id)
        break;
    }

    if (i >= size) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

int ObTablet::load_sstable(ObTabletImage* tablet_image) {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == sstable_loaded_) {
    ret = OB_INIT_TWICE;
  } else {
    int64_t size = sstable_id_list_.get_array_index();
    for (int64_t i = 0; i < size; ++i) {
      ObSSTableReader* reader = NULL;
      ret = tablet_image->load_sstable(*sstable_id_list_.at(i), reader);
      if (OB_SUCCESS != ret || NULL == reader) {
        TBSYS_LOG(ERROR, "load sstable failed, sstable id=%ld, ret =%d",
                  sstable_id_list_.at(i)->sstable_file_id_, ret);
        ret = OB_ERROR;
        break;
      }
      if (!sstable_reader_list_.push_back(reader)) {
        ret = OB_SIZE_OVERFLOW;
        break;
      }
    }
  }
  sstable_loaded_ = ret;
  return ret;
}

int64_t ObTablet::get_max_sstable_file_seq() const {
  int64_t size = sstable_id_list_.get_array_index();
  int64_t max_sstable_file_seq = 0;
  for (int64_t i = 0; i < size; ++i) {
    int64_t cur_file_seq = sstable_id_list_.at(i)->sstable_file_id_ >> 8;
    if (cur_file_seq > max_sstable_file_seq)
      max_sstable_file_seq = cur_file_seq;
  }
  return max_sstable_file_seq;
}

/*
 * get row count of all sstables in this tablet
 */
int64_t ObTablet::get_row_count() const {
  int64_t row_count = 0;
  if (OB_SUCCESS == sstable_loaded_) {
    int64_t size = sstable_reader_list_.get_array_index();
    for (int64_t i = 0; i < size; ++i) {
      ObSSTableReader* reader = *sstable_reader_list_.at(i);
      if (NULL != reader) {
        row_count += reader->get_row_count();
      }
    }
  }
  return row_count;
}

/*
 * get approximate occupy size of all sstables in this tablet
 */
int64_t ObTablet::get_occupy_size() const {
  int64_t occupy_size = 0;
  if (OB_SUCCESS == sstable_loaded_) {
    int64_t size = sstable_reader_list_.get_array_index();
    for (int64_t i = 0; i < size; ++i) {
      ObSSTableReader* reader = *sstable_reader_list_.at(i);
      if (NULL != reader) {
        occupy_size += reader->get_sstable_size();
      }
    }
  }
  return occupy_size;
}

/*
 * get checksum of all sstable files
 */
int ObTablet::get_checksum(uint64_t& tablet_checksum) const {
  int64_t checksum_len = sizeof(uint64_t);
  char checksum_buf[checksum_len];
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  tablet_checksum = 0;
  if (OB_SUCCESS == sstable_loaded_) {
    int64_t size = sstable_reader_list_.get_array_index();
    for (int64_t i = 0; i < size; ++i) {
      ObSSTableReader* reader = *sstable_reader_list_.at(i);
      if (NULL != reader) {
        uint64_t sstable_checksum = reader->get_trailer().get_sstable_checksum();
        pos = 0;
        ret = serialization::encode_i64(checksum_buf,
                                        checksum_len, pos, sstable_checksum);
        if (OB_SUCCESS == ret) {
          tablet_checksum = ob_crc64(tablet_checksum, checksum_buf, checksum_len);
        } else {
          ret = OB_ERROR;
          break;
        }
      } else {
        ret = OB_ERROR;
        break;
      }
    }
  } else {
    ret = OB_ERROR;
  }
  return ret;
}

int ObTablet::dump(const bool dump_sstable) const {
  char range_buf[OB_RANGE_STR_BUFSIZ];
  range_.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
  uint64_t tablet_checksum = 0;
  get_checksum(tablet_checksum);
  TBSYS_LOG(INFO, "range=%s, data version=%ld, disk_no=%d, "
            "row count=%ld, occupy size = %ld crc sum=%lu, merged=%d",
            range_buf, data_version_, disk_no_,
            get_row_count(), get_occupy_size(), tablet_checksum, merged_);
  if (dump_sstable && OB_SUCCESS == sstable_loaded_) {
    //do nothing
    int64_t size = sstable_reader_list_.get_array_index();
    for (int64_t i = 0; i < size; ++i) {
      ObSSTableReader* reader = *sstable_reader_list_.at(i);
      if (NULL != reader) {
        TBSYS_LOG(INFO, "sstable [%ld]: id=%ld, "
                  "size = %ld, row count =%ld,block count=%ld",
                  i, sstable_id_list_.at(i)->sstable_file_id_,
                  reader->get_trailer().get_size(),
                  reader->get_trailer().get_row_count(),
                  reader->get_trailer().get_block_count()) ;
      }
    }
  } else {
    int64_t size = sstable_id_list_.get_array_index();
    for (int64_t i = 0; i < size; ++i) {
      TBSYS_LOG(INFO, "sstable [%ld]: id=%ld",
                i, sstable_id_list_.at(i)->sstable_file_id_) ;
    }
  }
  return OB_SUCCESS;
}

} // end namespace chunkserver
} // end namespace sb



