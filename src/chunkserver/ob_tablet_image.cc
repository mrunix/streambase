/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_tablet_image.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     huating <huating.zmq@taobao.com>
 *     maoqi <maoqi@taobao.com>
 *
 */

#include "ob_tablet_image.h"
#include <dirent.h>
#include "common/ob_record_header.h"
#include "common/file_directory_utils.h"
#include "common/ob_file.h"
#include "common/ob_mod_define.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "ob_tablet.h"

using namespace sb::common;
using namespace sb::common::serialization;
using namespace sb::sstable;


namespace sb {
namespace chunkserver {

//----------------------------------------
// struct ObTabletMetaHeader
//----------------------------------------
DEFINE_SERIALIZE(ObTabletMetaHeader) {
  int ret = OB_SUCCESS;
  int64_t serialize_size = get_serialize_size();

  if ((NULL == buf) || (serialize_size + pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret
      && (OB_SUCCESS == encode_i64(buf, buf_len, pos, tablet_count_))
      && (OB_SUCCESS == encode_i64(buf, buf_len, pos, data_version_))
      && (OB_SUCCESS == encode_i32(buf, buf_len, pos, row_key_stream_offset_))
      && (OB_SUCCESS == encode_i32(buf, buf_len, pos, row_key_stream_size_))
      && (OB_SUCCESS == encode_i64(buf, buf_len, pos, reserved64_))) {
    ret = OB_SUCCESS;
  } else {
    ret = OB_SERIALIZE_ERROR;
  }

  return ret;
}

DEFINE_DESERIALIZE(ObTabletMetaHeader) {
  int ret = OB_SUCCESS;
  int64_t serialize_size = get_serialize_size();

  if (NULL == buf || serialize_size > data_len) {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret
      && (OB_SUCCESS == decode_i64(buf, data_len, pos, &tablet_count_))
      && (OB_SUCCESS == decode_i64(buf, data_len, pos, &data_version_))
      && (OB_SUCCESS == decode_i32(buf, data_len, pos, &row_key_stream_offset_))
      && (OB_SUCCESS == decode_i32(buf, data_len, pos, &row_key_stream_size_))
      && (OB_SUCCESS == decode_i64(buf, data_len, pos, &reserved64_))) {
    ret = OB_SUCCESS;
  } else {
    ret = OB_DESERIALIZE_ERROR;
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTabletMetaHeader) {
  return (encoded_length_i64(tablet_count_)
          + encoded_length_i64(data_version_)
          + encoded_length_i32(row_key_stream_offset_)
          + encoded_length_i32(row_key_stream_size_)
          + encoded_length_i64(reserved64_));
}

//----------------------------------------
// struct ObTabletRowkeyBuilder
//----------------------------------------
ObTabletRowkeyBuilder::ObTabletRowkeyBuilder(int64_t max_table_num)
  : row_key_data_size_(0) {
  row_key_buf_size_ = max_table_num * 2 * OB_MAX_ROW_KEY_LENGTH;
  row_key_buf_ = static_cast<char*>(ob_malloc(row_key_buf_size_));
}

ObTabletRowkeyBuilder::~ObTabletRowkeyBuilder() {
  if (NULL != row_key_buf_) {
    ob_free(row_key_buf_);
  }
  row_key_buf_ = NULL;
}

int ObTabletRowkeyBuilder::ensure_space(const common::ObRange& range) {
  int ret = OB_SUCCESS;
  if (NULL == row_key_buf_)
    ret = OB_ALLOCATE_MEMORY_FAILED;

  if (OB_SUCCESS == ret) {
    int64_t row_key_size =
      range.start_key_.length() + range.end_key_.length();
    if (row_key_data_size_ + row_key_size >= row_key_buf_size_)
      ret = OB_SIZE_OVERFLOW;
  }

  return ret;
}

int ObTabletRowkeyBuilder::append_range(const common::ObRange& range) {
  int ret = ensure_space(range);
  if (OB_SUCCESS == ret) {
    // copy start_key & end_key to row_key_buf_;
    memcpy(row_key_buf_ + row_key_data_size_,
           range.start_key_.ptr(), range.start_key_.length());
    row_key_data_size_ += range.start_key_.length();
    memcpy(row_key_buf_ + row_key_data_size_,
           range.end_key_.ptr(), range.end_key_.length());
    row_key_data_size_ += range.end_key_.length();
  }
  return ret;
}

//----------------------------------------
// class ObTabletImage
//----------------------------------------
ObTabletImage::ObTabletImage()
  : data_version_(0),
    max_sstable_file_seq_(0),
    ref_count_(0), cur_iter_idx_(INVALID_ITER_INDEX),
    mod_(ObModIds::OB_CS_TABLET_IMAGE),
    allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
    fileinfo_cache_(NULL) {
  // TODO reserve vector size.
}

ObTabletImage::~ObTabletImage() {
  destroy();
}

int ObTabletImage::destroy() {
  int ret = OB_SUCCESS;
  if (atomic_read((atomic_t*)&ref_count_) != 0) {
    TBSYS_LOG(WARN, "ObTabletImage still been used ref=%d, cannot destory..", ref_count_);
    ret = OB_CS_EAGAIN;
  }
  if (OB_SUCCESS == ret) {
    int64_t tablet_count = tablet_list_.size();
    for (int64_t i = 0 ; i < tablet_count; ++i) {
      ObTablet* tablet = tablet_list_.at(i);
      if (NULL != tablet) tablet->~ObTablet();
    }
  }
  if (OB_SUCCESS == ret) {
    ret = reset();
  }
  return ret;
}

int ObTabletImage::reset() {
  allocator_.free();
  tablet_list_.clear();
  sstable_list_.clear();

  data_version_ = 0;
  max_sstable_file_seq_ = 0;
  ref_count_ = 0;
  cur_iter_idx_ = INVALID_ITER_INDEX;

  return OB_SUCCESS;
}

int ObTabletImage::load_sstable(const ObSSTableId& sstable_id, ObSSTableReader*& reader) {
  char path[OB_MAX_FILE_NAME_LENGTH];
  int ret  = OB_SUCCESS;
  reader = NULL;
  ret = get_sstable_path(sstable_id, path, OB_MAX_FILE_NAME_LENGTH);

  if (OB_SUCCESS == ret) {
    reader = alloc_sstable_object();
    if (NULL != reader) {
      ret = reader->open(sstable_id);
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }

  if (OB_SUCCESS == ret) {
    sstable_list_.push_back(reader);
  }

  return ret;
}

int ObTabletImage::find_sstable(const ObSSTableId& sstable_id, ObSSTableReader*& reader) const {
  int ret = OB_ERROR;

  ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
  for (; it != tablet_list_.end(); ++it) {
    if (OB_SUCCESS == (*it)->find_sstable(sstable_id, reader)) {
      ret = OB_SUCCESS;
      break;
    }
  }

  if (it == tablet_list_.end()) {
    ret = OB_ENTRY_NOT_EXIST;
    reader = NULL;
  }
  return ret;
}

int ObTabletImage::include_sstable(const ObSSTableId& sstable_id) const {
  int ret = OB_ERROR;
  ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
  for (; it != tablet_list_.end(); ++it) {
    if (OB_SUCCESS == (*it)->include_sstable(sstable_id)) {
      ret = OB_SUCCESS;
      break;
    }
  }

  if (it == tablet_list_.end()) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

void ObTabletImage::set_fileinfo_cache(common::IFileInfoMgr& fileinfo_cache) {
  fileinfo_cache_ = &fileinfo_cache;
}

ObSSTableReader* ObTabletImage::alloc_sstable_object() {
  ObSSTableReader* reader = NULL;
  char* ptr = allocator_.alloc(sizeof(ObSSTableReader));
  if (ptr && NULL != fileinfo_cache_) {
    reader = new(ptr) ObSSTableReader(allocator_, *fileinfo_cache_);
  }
  return reader;
}

int ObTabletImage::alloc_tablet_object(const ObRange& range, ObTablet*& tablet) {
  tablet = NULL;
  ObRange copy_range;

  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = deep_copy_range(allocator_, range, copy_range))) {
    TBSYS_LOG(ERROR, "copy range failed.");
  } else if (NULL == (tablet = alloc_tablet_object())) {
    TBSYS_LOG(ERROR, "allocate tablet object failed.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    tablet->set_range(copy_range);
  }

  return ret;
}

ObTablet* ObTabletImage::alloc_tablet_object() {
  ObTablet* tablet = NULL;
  char* ptr = allocator_.alloc(sizeof(ObTablet));
  if (ptr) {
    tablet = new(ptr) ObTablet();
  }
  return tablet;
}

bool compare_tablet(const ObTablet* lhs, const ObTablet* rhs) {
  return lhs->get_range().compare_with_endkey(rhs->get_range()) < 0;
}

bool compare_tablet_range(const ObTablet* lhs, const ObRange& rhs) {
  return lhs->get_range().compare_with_endkey(rhs) < 0;
}

bool equal_tablet_range(const ObTablet* lhs, const ObRange& rhs) {
  return lhs->get_range().equal(rhs);
}

bool unique_tablet(const ObTablet* lhs, const ObTablet* rhs) {
  return lhs->get_range().intersect(rhs->get_range());
}

int ObTabletImage::add_tablet(ObTablet* tablet, const bool load_sstable_file) {
  ObSortedVector<ObTablet*>::iterator it = tablet_list_.end();
  int ret = tablet_list_.insert_unique(tablet, it, compare_tablet, unique_tablet);
  if (OB_SUCCESS != ret) {
    char intersect_buf[OB_RANGE_STR_BUFSIZ];
    char input_buf[OB_RANGE_STR_BUFSIZ];
    tablet->get_range().to_string(intersect_buf, OB_RANGE_STR_BUFSIZ);
    if (it != tablet_list_.end())
      (*it)->get_range().to_string(input_buf, OB_RANGE_STR_BUFSIZ);
    TBSYS_LOG(ERROR, "cannot insert this tablet:%s, maybe intersect with exist tablet:%s",
              intersect_buf, input_buf);
  } else if (load_sstable_file) {
    ret = tablet->load_sstable(this);
  }
  return ret;
}

bool check_forward_inclusive(const ObString& key, const ObBorderFlag& border_flag,
                             const int32_t search_mode, const ObString& border_key, const ObBorderFlag& end_key_border_flag) {
  int cmp = 0;
  bool ret = false;
  if (border_flag.is_max_value()) {
    if (end_key_border_flag.is_max_value()) {
      ret = true;
      cmp = 0;
    } else cmp = 1;
  } else if (end_key_border_flag.is_max_value()) {
    cmp = -1;
  } else {
    cmp = key.compare(border_key);
  }

  // not possible
  if (border_flag.is_min_value()) cmp = -1;

  if (!ret) {
    if (cmp < 0) {
      ret = true;
    } else if (cmp == 0) {
      ret = (end_key_border_flag.inclusive_end()
             && (search_mode != OB_SEARCH_MODE_GREATER_THAN));
    } else {
      ret = false;
    }
  }
  return ret;
}

int ObTabletImage::find_tablet(const common::ObRange& range,
                               const int32_t scan_direction, ObTablet*& tablet) const {
  int ret = OB_SUCCESS;
  tablet = NULL;
  int32_t search_mode = 0;

  if (range.empty()) {
    ret = OB_INVALID_ARGUMENT;
  }

  ObString lookup_key;
  ObBorderFlag border_flag = range.border_flag_;
  if (OB_SUCCESS == ret) {
    if (ObMultiVersionTabletImage::SCAN_FORWARD == scan_direction) {
      lookup_key = range.start_key_;
      if (range.border_flag_.inclusive_start())
        search_mode = OB_SEARCH_MODE_GREATER_EQUAL;
      else
        search_mode = OB_SEARCH_MODE_GREATER_THAN;

      border_flag.unset_max_value();

    } else {
      lookup_key = range.end_key_;
      if (range.border_flag_.inclusive_end())
        search_mode = OB_SEARCH_MODE_LESS_EQUAL;
      else
        search_mode = OB_SEARCH_MODE_LESS_THAN;
    }

    if (range.is_whole_range()) {
      if (ObMultiVersionTabletImage::SCAN_FORWARD == scan_direction) {
        border_flag.unset_max_value();
      } else {
        border_flag.unset_min_value();
      }
    } else {
      if (range.start_key_.compare(range.end_key_) == 0)
        search_mode = OB_SEARCH_MODE_EQUAL;
    }
  }

  if (OB_SUCCESS == ret) {
    ret = find_tablet(range.table_id_, border_flag,
                      lookup_key, search_mode, tablet);
  }

  if (OB_SUCCESS == ret && NULL != tablet) {
    // in the hole?
    if (!range.intersect(tablet->get_range())) {
      ret = OB_CS_TABLET_NOT_EXIST;
      tablet = NULL;
    }
  }

  return ret;
}

int ObTabletImage::find_tablet(const uint64_t table_id,
                               const ObBorderFlag& border_flag, const common::ObString& key,
                               const int32_t search_mode, ObTablet*& tablet) const {
  int ret = OB_SUCCESS;
  if (search_mode < OB_SEARCH_MODE_EQUAL || search_mode > OB_SEARCH_MODE_LESS_EQUAL) {
    ret = OB_INVALID_ARGUMENT;
  }
  if (NULL == key.ptr() || 0 >= key.length()) {
    if (search_mode == OB_SEARCH_MODE_EQUAL) {
      ret = OB_INVALID_ARGUMENT;
    } else if (search_mode > OB_SEARCH_MODE_GREATER_THAN) {
      // less than max_value
      if (!border_flag.is_max_value()) ret = OB_INVALID_ARGUMENT;
    } else if (search_mode <= OB_SEARCH_MODE_GREATER_THAN) {
      // greater than min_value
      if (!border_flag.is_min_value()) ret = OB_INVALID_ARGUMENT;
    }
  }

  if (tablet_list_.size() <= 0) {
    ret = OB_CS_TABLET_NOT_EXIST;
  }

  ObSortedVector<ObTablet*>::iterator it = tablet_list_.end();
  if (OB_SUCCESS == ret) {
    ObRange range;
    range.table_id_ = table_id;
    range.border_flag_ = border_flag;
    range.end_key_ = key;
    it = tablet_list_.lower_bound(range, compare_tablet_range);
    if (it == tablet_list_.end()) {
      // find less than (or less equal) range
      if (search_mode > OB_SEARCH_MODE_GREATER_THAN) {
        ObTablet* last_tablet = *tablet_list_.last();
        if (last_tablet->get_range().table_id_ == table_id) {
          tablet = last_tablet;
        } else {
          ret = OB_CS_TABLET_NOT_EXIST;
        }
      } else {
        ret = OB_CS_TABLET_NOT_EXIST;
      }
    }
  }

  ObTablet* lookup_tablet = NULL;
  if (OB_SUCCESS == ret && it != tablet_list_.end()) {
    lookup_tablet = *it;
    if (lookup_tablet->get_range().table_id_ != table_id) {
      ret = OB_CS_TABLET_NOT_EXIST;
    }
  }

  if (OB_SUCCESS == ret && NULL != lookup_tablet) {
    // lookup backward
    if (search_mode <= OB_SEARCH_MODE_GREATER_THAN) {
      bool inclusive = check_forward_inclusive(
                         key, border_flag, search_mode,
                         lookup_tablet->get_range().end_key_,
                         lookup_tablet->get_range().border_flag_);
      if (inclusive) {
        tablet = lookup_tablet;
      } else {
        // search next tablet check if fufill
        ObSortedVector<ObTablet*>::const_iterator next_it = ++it;
        if (next_it < tablet_list_.end()
            && (*next_it)->get_range().table_id_ == table_id) {
          // (*next_it)->range definitely >= key.
          // cause every range not intersect & sorted & not empty.
          tablet = *next_it;
        } else {
          ret = OB_CS_TABLET_NOT_EXIST;
        }
      }
    } else {
      // %search_mode request the %end_key of tablet, but
      // current tablet's range donot include it
      if (search_mode == OB_SEARCH_MODE_LESS_EQUAL
          && key.compare(lookup_tablet->get_range().end_key_) == 0
          && (!lookup_tablet->get_range().border_flag_.inclusive_end())) {
        // search next tablet check if fulfil
        ObSortedVector<ObTablet*>::const_iterator next_it = ++it;
        if (next_it < tablet_list_.end()
            && (*next_it)->get_range().table_id_ == table_id
            && key.compare((*next_it)->get_range().start_key_) == 0
            && (*next_it)->get_range().border_flag_.inclusive_start()) {
          tablet = *next_it;
        } else {
          tablet = lookup_tablet;
        }
      } else {
        tablet = lookup_tablet;
      }
    }
  }
  return ret;
}

int ObTabletImage::acquire_sstable(const ObSSTableId& sstable_id,
                                   ObSSTableReader*& reader) const {
  int ret =  find_sstable(sstable_id, reader);

  if (OB_SUCCESS != ret) {
    TBSYS_LOG(INFO, "find sstable sstable_id=%ld failed, ret=%d",
              sstable_id.sstable_file_id_, ret);
  } else if (NULL == reader) {
    TBSYS_LOG(INFO, "reader is NULL");
    ret = OB_ERROR;
  } else {
    atomic_inc((atomic_t*) &ref_count_);
  }

  return ret;
}

int ObTabletImage::release_sstable(ObSSTableReader* reader) const {
  //TODO same as release_tablet.
  int ret = OB_SUCCESS;

  if (NULL == reader) {
    TBSYS_LOG(WARN, "invalid param, sstable=%p", reader);
    ret = OB_INVALID_ARGUMENT;
  } else if (ref_count_ <= 0) {
    TBSYS_LOG(WARN, "invalid status, ref_count_=%d", ref_count_);
    ret = OB_ERROR;
  } else {
    atomic_dec((atomic_t*) &ref_count_);
  }

  return ret;
}

int ObTabletImage::remove_tablet(const common::ObRange& range, int32_t& disk_no) {
  int ret = OB_SUCCESS;
  disk_no = -1;
  if (range.empty()) {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret) {
    ObTablet* tablet = NULL;
    ret = tablet_list_.remove_if(range, compare_tablet_range, equal_tablet_range, tablet);
    if (OB_SUCCESS == ret && NULL != tablet) {
      disk_no = tablet->get_disk_no();
      tablet->~ObTablet();
    } else {
      char range_buf[OB_RANGE_STR_BUFSIZ];
      range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(WARN, "remove_tablet : the specific range:%s not exist, ret=%d", range_buf, ret);
    }
  }
  return ret;
}

int ObTabletImage::acquire_tablet(const common::ObRange& range,
                                  const int32_t scan_direction, ObTablet*& tablet) const {
  int ret =  find_tablet(range, scan_direction, tablet);

  if (OB_SUCCESS != ret) {
    // do nothing.
  } else if (NULL == tablet) {
    char range_buf[OB_RANGE_STR_BUFSIZ];
    range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
    TBSYS_LOG(ERROR, "found tablet:%s null, ret=%d", range_buf, ret);
    ret = OB_ERROR;
  } else {
    atomic_inc((atomic_t*) &ref_count_);
  }

  return ret;
}

int ObTabletImage::release_tablet(ObTablet* tablet) const {
  int ret = OB_SUCCESS;

  if (NULL == tablet) {
    TBSYS_LOG(WARN, "invalid param, tablet=%p", tablet);
    ret = OB_INVALID_ARGUMENT;
  } else if (ref_count_ <= 0) {
    TBSYS_LOG(WARN, "invalid status, ref_count_=%d", ref_count_);
    ret = OB_ERROR;
  } else {
    atomic_dec((atomic_t*) &ref_count_);
  }

  return ret;
}

int ObTabletImage::serialize(const int32_t disk_no,
                             char* buf, const int64_t buf_len, int64_t& pos,
                             ObTabletRowkeyBuilder& builder) {
  int ret = OB_SUCCESS;
  int64_t origin_pos = pos;

  // serialize record record_header;
  ObRecordHeader record_header;
  ObTabletMetaHeader meta_header;

  record_header.set_magic_num(ObTabletMetaHeader::TABLET_META_MAGIC);
  record_header.header_length_ = OB_RECORD_HEADER_LENGTH;
  record_header.version_ = 0;
  record_header.reserved_ = 0;

  int64_t payload_pos = pos + OB_RECORD_HEADER_LENGTH;
  int64_t tablet_pos = payload_pos + meta_header.get_serialize_size();

  int64_t tablet_count = 0;
  ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
  for (; it != tablet_list_.end(); ++it) {
    if ((*it)->get_disk_no() == disk_no || 0 == disk_no) {
      ObTabletRangeInfo info;
      (*it)->get_range_info(info);

      ret = info.serialize(buf, buf_len, tablet_pos);
      if (OB_SUCCESS != ret) break;

      ret = (*it)->serialize(buf, buf_len, tablet_pos);
      if (OB_SUCCESS != ret) break;

      ret = builder.append_range((*it)->get_range());
      if (OB_SUCCESS != ret) break;
      ++tablet_count;
    }
  }

  int64_t origin_payload_pos = payload_pos;
  if (OB_SUCCESS == ret) {
    meta_header.tablet_count_ = tablet_count;
    meta_header.data_version_ = get_data_version();
    meta_header.row_key_stream_offset_ = tablet_pos - origin_payload_pos;
    meta_header.row_key_stream_size_ = builder.get_data_size();
    meta_header.reserved64_ = 0;
    ret = meta_header.serialize(buf, buf_len, payload_pos);
  }

  if (OB_SUCCESS == ret) {
    record_header.data_length_ = tablet_pos - origin_payload_pos + builder.get_data_size();
    record_header.data_zlength_ = record_header.data_length_;
    int64_t crc =  common::ob_crc64(
                     0, buf + origin_payload_pos, tablet_pos - origin_payload_pos);
    if (NULL == builder.get_row_key_buf() || 0 >= builder.get_data_size()) {
      record_header.data_checksum_ = crc;
    } else {
      record_header.data_checksum_ = common::ob_crc64(
                                       crc, builder.get_row_key_buf(), builder.get_data_size());
    }
    record_header.set_header_checksum();
    ret = record_header.serialize(buf, buf_len, origin_pos);
  }

  if (OB_SUCCESS == ret) pos = tablet_pos;

  return ret;
}

int ObTabletImage::deserialize(const bool load_sstable,
                               const int32_t disk_no, const char* buf, const int64_t data_len, int64_t& pos) {
  UNUSED(disk_no);

  int ret = OB_SUCCESS;
  int64_t origin_payload_pos = 0;
  ret = ObRecordHeader::check_record(buf + pos, data_len,
                                     ObTabletMetaHeader::TABLET_META_MAGIC);
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "check common record header failed, disk_no=%d", disk_no);
  }

  ObTabletMetaHeader meta_header;
  if (OB_SUCCESS == ret) {
    pos += OB_RECORD_HEADER_LENGTH;
    origin_payload_pos = pos;
    ret = meta_header.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "deserialize meta header failed, disk_no=%d", disk_no);
    } else if (data_version_ != meta_header.data_version_) {
      TBSYS_LOG(ERROR, "data_version_=%ld != header.version=%ld",
                data_version_, meta_header.data_version_);
      ret = OB_ERROR;
    }
  }


  // check the rowkey char stream
  char* row_key_buf = NULL;
  if (OB_SUCCESS == ret) {
    if (origin_payload_pos
        + meta_header.row_key_stream_offset_
        + meta_header.row_key_stream_size_
        > data_len) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      const char* row_key_stream_ptr = buf + origin_payload_pos
                                       + meta_header.row_key_stream_offset_;
      row_key_buf = allocator_.alloc(
                      meta_header.row_key_stream_size_);
      if (NULL == row_key_buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        memcpy(row_key_buf, row_key_stream_ptr,
               meta_header.row_key_stream_size_);
      }
    }
  }

  if (OB_SUCCESS == ret) {
    int64_t row_key_cur_offset = 0;
    for (int64_t i = 0; i < meta_header.tablet_count_; ++i) {
      ObTabletRangeInfo info;
      ret = info.deserialize(buf, data_len, pos);
      if (OB_SUCCESS != ret) break;

      ObTablet* tablet = alloc_tablet_object();
      if (NULL != tablet) {
        ret = tablet->deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret) {
          ret = tablet->set_range_by_info(info,
                                          row_key_buf + row_key_cur_offset,
                                          meta_header.row_key_stream_size_ - row_key_cur_offset);
        }

        if (OB_SUCCESS == ret) {
          row_key_cur_offset += info.start_key_size_ + info.end_key_size_;
        }

        if (OB_SUCCESS == ret && load_sstable) {
          ret = tablet->load_sstable(this);
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret) {
        int64_t max_seq = tablet->get_max_sstable_file_seq();
        if (max_seq > max_sstable_file_seq_) max_sstable_file_seq_ = max_seq;
        tablet->set_disk_no(disk_no);
        ret = add_tablet(tablet);
      }

      if (OB_SUCCESS != ret) break;
    }
  }

  return ret;

}

int ObTabletImage::serialize(const int32_t disk_no, char* buf,
                             const int64_t buf_len, int64_t& pos) {
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0 || pos >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
  }

  ObTabletRowkeyBuilder builder(tablet_list_.size());
  if (OB_SUCCESS == ret) {
    // dump, migrate, create_tablet maybe serialize same disk image concurrency
    ret = serialize(disk_no, buf, buf_len, pos, builder);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "ObTabletImage::serialize error, disk no=%d", disk_no);
    }
  }

  if (OB_SUCCESS == ret) {
    if (pos + builder.get_data_size() > buf_len) {
      TBSYS_LOG(ERROR, "size overflow, "
                "pos=%ld,builder.datasize=%ld, buf_len=%ld",
                pos, builder.get_data_size(), buf_len);
      ret = OB_SIZE_OVERFLOW;
    } else {
      memcpy(buf + pos,  builder.get_row_key_buf(), builder.get_data_size());
      pos += builder.get_data_size();
    }
  }

  return ret;
}

int ObTabletImage::deserialize(const int32_t disk_no,
                               const char* buf, const int64_t buf_len, int64_t& pos) {
  return deserialize(false, disk_no, buf, buf_len, pos);
}

int64_t ObTabletImage::get_max_serialize(const int32_t disk_no) const {
  UNUSED(disk_no);
  int64_t max_serialize_size =
    OB_RECORD_HEADER_LENGTH + sizeof(ObTabletMetaHeader);
  max_serialize_size +=
    (sizeof(ObTabletRangeInfo)
     + sizeof(ObSSTableId) * ObTablet::MAX_SSTABLE_PER_TABLET)
    * tablet_list_.size();
  return max_serialize_size;
}

int32_t ObTabletImage::get_tablets_num() const {
  return tablet_list_.size();
}

int ObTabletImage::write(const int32_t disk_no) {
  int ret = OB_SUCCESS;
  char path[OB_MAX_FILE_NAME_LENGTH];
  ret = get_meta_path(disk_no, true, path, OB_MAX_FILE_NAME_LENGTH);
  if (OB_SUCCESS == ret) {
    ret = write(path, disk_no);
  }
  return ret;
}

int ObTabletImage::read(const int32_t disk_no, const bool load_sstable) {
  int ret = OB_SUCCESS;
  char path[OB_MAX_FILE_NAME_LENGTH];
  ret = get_meta_path(disk_no, true, path, OB_MAX_FILE_NAME_LENGTH);
  if (OB_SUCCESS == ret && FileDirectoryUtils::exists(path)) {
    ret = read(path, disk_no, load_sstable);
  }
  return ret;
}

int ObTabletImage::read(const char* idx_path, const int32_t disk_no, const bool load_sstable) {
  int ret = OB_SUCCESS;
  if (NULL == idx_path || strlen(idx_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!FileDirectoryUtils::exists(idx_path)) {
    TBSYS_LOG(INFO, "meta index file path=%s, disk_no=%d not exist", idx_path, disk_no);
    ret = OB_INVALID_ARGUMENT;
  } else {
    TBSYS_LOG(INFO, "read meta index file path=%s, disk_no=%d", idx_path, disk_no);
  }

  if (OB_SUCCESS == ret) {
    ObString fname(0, strlen(idx_path), const_cast<char*>(idx_path));

    char* file_buf = NULL;
    int64_t file_size = get_file_size(idx_path);
    int64_t read_size = 0;

    if (file_size < static_cast<int64_t>(sizeof(ObTabletMetaHeader))) {
      TBSYS_LOG(INFO, "invalid idx file =%s file_size=%ld", idx_path, file_size);
      ret = OB_ERROR;
    }

    // not use direct io
    FileComponent::BufferFileReader reader;
    if (OB_SUCCESS != (ret = reader.open(fname))) {
      TBSYS_LOG(ERROR, "open %s for read error, %s.", idx_path, strerror(errno));
    }

    if (OB_SUCCESS == ret) {
      file_buf = static_cast<char*>(ob_malloc(file_size));
      if (NULL == file_buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }

    if (OB_SUCCESS == ret) {
      ret = reader.pread(file_buf, file_size, 0, read_size);
      if (ret != OB_SUCCESS || read_size < file_size) {
        TBSYS_LOG(ERROR, "read idx file = %s , ret = %d, read_size = %ld, file_size = %ld, %s.",
                  idx_path, ret, read_size, file_size, strerror(errno));
        ret = OB_IO_ERROR;
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCCESS == ret) {
      int64_t pos = 0;
      ret = deserialize(load_sstable, disk_no, file_buf, file_size, pos);
    }

    if (NULL != file_buf) {
      ob_free(file_buf);
      file_buf = NULL;
    }

  }
  return ret;
}

int ObTabletImage::write(const char* idx_path, const int32_t disk_no, const bool backup_old_idx) {
  int ret = OB_SUCCESS;
  if (NULL == idx_path || strlen(idx_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    TBSYS_LOG(INFO, "write meta index file path=%s, disk_no=%d", idx_path, disk_no);
    if (backup_old_idx && FileDirectoryUtils::exists(idx_path)) {
      char backup_path[OB_MAX_FILE_NAME_LENGTH];
      ret = get_meta_path(disk_no, false, backup_path, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "get backup path failed, idx=%s", idx_path);
      } else if (!FileDirectoryUtils::rename(idx_path, backup_path)) {
        TBSYS_LOG(ERROR, "rename idx file %s to %s failed.", idx_path, backup_path);
        ret = OB_IO_ERROR;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    char* meta_buf = NULL;
    int64_t max_serialize_size = get_max_serialize(disk_no);

    ObString fname(0, strlen(idx_path), const_cast<char*>(idx_path));
    FileComponent::BufferFileAppender appender;

    // not use direct io, create new file, trucate if exist.
    if (OB_SUCCESS != (ret = appender.open(fname, true, true))) {
      TBSYS_LOG(ERROR, "open idx file = %s for write error, %s.", idx_path, strerror(errno));
    } else if (NULL == (meta_buf = static_cast<char*>(ob_malloc(max_serialize_size)))) {
      TBSYS_LOG(ERROR, "allocate memory for serialize image error, size =%ld", max_serialize_size);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }

    if (OB_SUCCESS == ret) {
      ObTabletRowkeyBuilder builder(tablet_list_.size());
      int64_t pos = 0;

      if (OB_SUCCESS != (ret = serialize(disk_no, meta_buf, max_serialize_size, pos, builder))) {
        TBSYS_LOG(ERROR, "serialize error, disk_no=%d", disk_no);
      } else if (OB_SUCCESS != (ret = appender.append(meta_buf, pos, true))) {
        TBSYS_LOG(ERROR, "write meta buffer failed,%s.", strerror(errno));
      } else if (builder.get_data_size() > 0 && OB_SUCCESS != (ret = appender.append(
                                                                 builder.get_row_key_buf(), builder.get_data_size(), true))) {
        TBSYS_LOG(ERROR, "write row key buffer failed,%s.", strerror(errno));
      }
    }

    if (NULL != meta_buf) {
      ob_free(meta_buf);
      meta_buf = NULL;
    }

  }
  return ret;
}

int ObTabletImage::read(const int32_t* disk_no_array, const int32_t size, const bool load_sstable) {
  int ret = OB_SUCCESS;
  if (NULL == disk_no_array || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret) {
    for (int32_t i = 0; i < size; ++i) {
      ret = read(disk_no_array[i], load_sstable);
      // TODO read failed on one disk.
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "read meta info from disk %d failed", disk_no_array[i]);
        continue;
      }
    }
  }
  return ret;
}

int ObTabletImage::write(const int32_t* disk_no_array, const int32_t size) {
  int ret = OB_SUCCESS;
  if (NULL == disk_no_array || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret) {
    for (int32_t i = 0; i < size; ++i) {
      ret = write(disk_no_array[i]);
      // TODO write failed on one disk.
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "write meta info to disk %d failed", disk_no_array[i]);
        continue;
      }
    }
  }
  return ret;
}

int ObTabletImage::begin_scan_tablets() {
  int ret = OB_SUCCESS;
  if (cur_iter_idx_ != INVALID_ITER_INDEX) {
    TBSYS_LOG(ERROR, "scan in progress, cannot luanch another, cur_iter_idx_=%d", cur_iter_idx_);
    ret = OB_CS_EAGAIN;
  } else {
    TBSYS_LOG(INFO, "begin_scan_tablets cur_iter_idx_ = %d", cur_iter_idx_);
  }

  if (OB_SUCCESS == ret) {
    if (tablet_list_.size() == 0) {
      ret = OB_ITER_END;
    }
  }

  if (OB_SUCCESS == ret) {
    cur_iter_idx_ = 0;
  }

  return ret;
}

int ObTabletImage::get_next_tablet(ObTablet*& tablet) {
  int ret = OB_SUCCESS;
  tablet = NULL;
  if (cur_iter_idx_ == INVALID_ITER_INDEX) {
    TBSYS_LOG(ERROR, "not a initialized scan process, call begin_scan_tablets first");
    ret = OB_ERROR;
  }

  if (cur_iter_idx_ >= tablet_list_.size()) {
    ret = OB_ITER_END;
  } else {
    tablet = tablet_list_.at(cur_iter_idx_);
    atomic_inc((atomic_t*) &cur_iter_idx_);
    atomic_inc((atomic_t*) &ref_count_);
  }
  return ret;
}

int ObTabletImage::end_scan_tablets() {
  int ret = OB_SUCCESS;
  if (cur_iter_idx_ == INVALID_ITER_INDEX) {
    TBSYS_LOG(INFO, "scans not begin or has no tablets, cur_iter_idx_=%d", cur_iter_idx_);
    ret = OB_ERROR;
  } else {
    TBSYS_LOG(INFO, "end_scan_tablets cur_iter_idx_=%d", cur_iter_idx_);
  }
  cur_iter_idx_ = INVALID_ITER_INDEX;
  return ret;
}

int ObTabletImage::dump(const bool dump_sstable) const {
  TBSYS_LOG(INFO, "ref_count_=%d, cur_iter_idx_=%d, memory usage=%ld",
            ref_count_, cur_iter_idx_, allocator_.total());

  TBSYS_LOG(INFO, "----->begin dump tablets in image<--------");
  for (int64_t i = 0; i < tablet_list_.size(); ++i) {
    ObTablet* tablet = tablet_list_.at(i);
    TBSYS_LOG(INFO, "----->tablet(%d)<--------", i);
    tablet->dump(dump_sstable);
  }
  TBSYS_LOG(INFO, "----->end dump tablets in image<--------");

  return OB_SUCCESS;
}

//----------------------------------------
// class ObMultiVersionTabletImage
//----------------------------------------
ObMultiVersionTabletImage::ObMultiVersionTabletImage(IFileInfoMgr& fileinfo_cache)
  : newest_index_(0), service_index_(-1), iterator_(*this), fileinfo_cache_(fileinfo_cache) {
  memset(image_tracker_, 0, sizeof(image_tracker_));
}

ObMultiVersionTabletImage::~ObMultiVersionTabletImage() {
  destroy();
}

int64_t ObMultiVersionTabletImage::get_serving_version() const {
  int64_t data_version = 0;
  int64_t index = 0;

  tbsys::CRLockGuard guard(lock_);

  index = service_index_;
  if (index >= 0 && index < MAX_RESERVE_VERSION_COUNT && has_tablet(index)) {
    data_version = image_tracker_[index]->data_version_;

  }
  return data_version;
}

int64_t ObMultiVersionTabletImage::get_eldest_version() const {
  int64_t data_version = 0;
  int64_t index = 0;

  tbsys::CRLockGuard guard(lock_);

  index = get_eldest_index();
  if (index >= 0 && index < MAX_RESERVE_VERSION_COUNT && has_tablet(index)) {
    data_version = image_tracker_[index]->data_version_;

  }
  return data_version;
}

int64_t ObMultiVersionTabletImage::get_newest_version() const {
  int64_t data_version = 0;

  tbsys::CRLockGuard guard(lock_);

  int64_t index = newest_index_;
  do {
    if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
      break;
    }
    if (has_tablet(index)) {
      data_version = image_tracker_[index]->data_version_;
      break;
    }
    // if not found, search older version tablet until iterated every version of tablet.
    if (--index < 0) index = MAX_RESERVE_VERSION_COUNT - 1;
  } while (index != newest_index_);

  return data_version;
}

int64_t ObMultiVersionTabletImage::get_eldest_index() const {
  int64_t index = (newest_index_ + 1) % MAX_RESERVE_VERSION_COUNT;
  do {
    if (has_tablet(index)) {
      break;
    } else {
      index = (index + 1) % MAX_RESERVE_VERSION_COUNT;
    }
  } while (index != newest_index_);

  return index;
}

int ObMultiVersionTabletImage::acquire_tablet(
  const common::ObRange& range,
  const ScanDirection scan_direction,
  const int64_t version,
  ObTablet*& tablet) const {
  return acquire_tablet_all_version(
           range, scan_direction, FROM_SERVICE_INDEX, version, tablet);
}

int ObMultiVersionTabletImage::acquire_tablet_all_version(
  const common::ObRange& range,
  const ScanDirection scan_direction,
  const ScanPosition from_index,
  const int64_t version,
  ObTablet*& tablet) const {
  int ret = OB_SUCCESS;
  if (range.empty() || 0 > version
      || (from_index != FROM_SERVICE_INDEX && from_index != FROM_NEWEST_INDEX)) {
    TBSYS_LOG(WARN, "acquire_tablet invalid argument, "
              "range is emtpy or version=%ld < 0, or from_index =%d illegal",
              version, from_index);
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = OB_CS_TABLET_NOT_EXIST;

    tbsys::CRLockGuard guard(lock_);

    int64_t start_index =
      (from_index == FROM_SERVICE_INDEX) ? service_index_ : newest_index_;
    int64_t index = start_index;

    do {
      if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
        TBSYS_LOG(WARN, "image (index=%ld) not initialized, has no tablets", index);
        break;
      }
      // version == 0 search from newest tablet
      if (version == 0 && has_tablet(index)) {
        ret = image_tracker_[index]->acquire_tablet(range, scan_direction, tablet);
      }
      // version != 0 search from newest tablet which has version less or equal than %version
      if (version != 0 && has_tablet(index) && image_tracker_[index]->data_version_ <= version) {
        ret = image_tracker_[index]->acquire_tablet(range, scan_direction, tablet);
      }

      if (OB_SUCCESS == ret) break;
      // if not found, search older version tablet until iterated every version of tablet.
      if (--index < 0) index = MAX_RESERVE_VERSION_COUNT - 1;
    } while (index != start_index);
  }

  return ret;
}

int ObMultiVersionTabletImage::release_tablet(ObTablet* tablet) const {
  int ret = OB_SUCCESS;
  tbsys::CRLockGuard guard(lock_);

  if (NULL == tablet) {
    TBSYS_LOG(WARN, "release_tablet invalid argument tablet null");
    ret = OB_INVALID_ARGUMENT;
  } else if (!has_match_version_tablet(tablet->get_data_version())) {
    TBSYS_LOG(WARN, "release_tablet version=%ld dont match",
              tablet->get_data_version());
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = get_image(tablet->get_data_version()).release_tablet(tablet);
  }
  return ret;
}

int ObMultiVersionTabletImage::acquire_sstable(
  const sstable::ObSSTableId& sstable_id,
  const int64_t version,
  sstable::ObSSTableReader*& reader) const {
  return acquire_sstable_all_version(sstable_id,
                                     FROM_SERVICE_INDEX, version, reader);
}

int ObMultiVersionTabletImage::acquire_sstable_all_version(
  const sstable::ObSSTableId& sstable_id,
  const ScanPosition from_index,
  const int64_t version,
  sstable::ObSSTableReader*& reader) const {
  int ret = OB_SUCCESS;

  if (0 > version
      || (from_index != FROM_SERVICE_INDEX && from_index != FROM_NEWEST_INDEX)) {
    TBSYS_LOG(ERROR, "acquire_sstable invalid argument, "
              "version=%ld < 0", version);
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    reader = NULL;

    tbsys::CRLockGuard guard(lock_);
    int64_t start_index =
      (from_index == FROM_SERVICE_INDEX) ? service_index_ : newest_index_;
    int64_t index = start_index;

    do {
      if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
        TBSYS_LOG(WARN, "image (index=%ld) not initialized, has no tablets", index);
        break;
      }

      // version == 0 search from newest tablet
      if (version == 0 && has_tablet(index)) {
        ret = image_tracker_[index]->acquire_sstable(sstable_id, reader);
      }

      // version != 0 search from newest tablet which has version less or equal than %version
      if (version != 0 && has_tablet(index) && image_tracker_[index]->data_version_ <= version) {
        ret = image_tracker_[index]->acquire_sstable(sstable_id, reader);
      }

      if (OB_SUCCESS == ret) break;
      // if not found, search older version tablet until iterated every version of tablet.
      if (--index < 0) index = MAX_RESERVE_VERSION_COUNT - 1;
    } while (index != start_index);
  }

  return ret;
}

int ObMultiVersionTabletImage::release_sstable(const int64_t version,
                                               sstable::ObSSTableReader* reader) const {
  int ret = OB_SUCCESS;

  tbsys::CRLockGuard guard(lock_);

  if (0 > version) {
    TBSYS_LOG(ERROR, "release_sstable invalid argument, "
              "version=%ld < 0", version);
    ret = OB_INVALID_ARGUMENT;
  } else if (!has_match_version_tablet(version)) {
    TBSYS_LOG(ERROR, "version = %ld not in tablet image.", version);
    ret = OB_ERROR;
  } else {
    ret = get_image(version).release_sstable(reader);
  }

  return ret;
}

int ObMultiVersionTabletImage::include_sstable(const ObSSTableId& sstable_id) const {
  int ret = OB_SUCCESS;

  if (0 >= sstable_id.sstable_file_id_) {
    TBSYS_LOG(ERROR, "include_sstable invalid argument, "
              "sstable file id = %ld ", sstable_id.sstable_file_id_);
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = OB_ENTRY_NOT_EXIST;

    tbsys::CRLockGuard guard(lock_);
    int64_t start_index = newest_index_;
    int64_t index = start_index;

    do {
      if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
        TBSYS_LOG(WARN, "image (index=%ld) not initialized, has no tablets", index);
        break;
      }

      // version == 0 search from newest tablet
      if (has_tablet(index)) {
        ret = image_tracker_[index]->include_sstable(sstable_id);
      }

      if (OB_SUCCESS == ret) break;
      // if not found, search older version tablet until iterated every version of tablet.
      if (--index < 0) index = MAX_RESERVE_VERSION_COUNT - 1;
    } while (index != start_index);
  }

  return ret;
}

int ObMultiVersionTabletImage::remove_tablet(const common::ObRange& range,
                                             const int64_t version, int32_t& disk_no) {
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(lock_);

  if (range.empty() || 0 >= version) {
    TBSYS_LOG(WARN, "remove_tablet invalid argument, "
              "range is emtpy or version=%ld < 0", version);
    ret = OB_INVALID_ARGUMENT;
  } else if (!has_match_version_tablet(version)) {
    TBSYS_LOG(WARN, "remove_tablet version=%ld dont match", version);
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = get_image(version).remove_tablet(range, disk_no);
  }
  return ret;
}

int ObMultiVersionTabletImage::add_tablet(ObTablet* tablet,
                                          const bool load_sstable /* = false */,
                                          const bool for_create /* = false */) {
  int ret = OB_SUCCESS;

  if (NULL == tablet || tablet->get_data_version() <= 0) {
    TBSYS_LOG(WARN, "add_tablet invalid argument, "
              "tablet=%p or version<0", tablet);
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t new_version = tablet->get_data_version();

    tbsys::CWLockGuard guard(lock_);

    if (OB_SUCCESS != (ret = prepare_tablet_image(new_version, false))) {
      TBSYS_LOG(ERROR, "prepare new version = %ld image error.", new_version);
    } else if (OB_SUCCESS != (ret =
                                get_image(new_version).add_tablet(tablet, load_sstable))) {
      TBSYS_LOG(ERROR, "add tablet error with new version = %ld.", new_version);
    } else if (for_create && service_index_ != newest_index_) {
      service_index_ = newest_index_;
    }
  }
  return ret;
}

int ObMultiVersionTabletImage::upgrade_tablet(
  ObTablet* old_tablet, ObTablet* new_tablet, const bool load_sstable) {
  int ret = OB_SUCCESS;
  if (NULL == old_tablet || NULL == new_tablet
      || old_tablet->get_data_version() <= 0
      || new_tablet->get_data_version() <= 0
      || new_tablet->get_data_version() <= old_tablet->get_data_version()
      || old_tablet->is_merged()) {
    TBSYS_LOG(WARN, "upgrade_tablet invalid argument, "
              "old_tablet=%p or new_tablet=%p, old merged=%d",
              old_tablet, new_tablet, old_tablet->is_merged());
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t new_version = new_tablet->get_data_version();
    char range_buf[OB_RANGE_STR_BUFSIZ];
    old_tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);

    TBSYS_LOG(INFO, "upgrade_tablet range:(%s) old version = %ld, new version = %ld",
              range_buf, old_tablet->get_data_version(), new_tablet->get_data_version());

    tbsys::CWLockGuard guard(lock_);

    if (OB_SUCCESS != (ret = prepare_tablet_image(new_version, true))) {
      TBSYS_LOG(ERROR, "prepare new version = %ld image error.", new_version);
    } else if (OB_SUCCESS != (ret =
                                get_image(new_version).add_tablet(new_tablet, load_sstable))) {
      TBSYS_LOG(ERROR, "add tablet error with new version = %ld.", new_version);
    } else {
      old_tablet->set_merged();
    }
  }
  return ret;
}

int ObMultiVersionTabletImage::upgrade_tablet(ObTablet* old_tablet,
                                              ObTablet* new_tablets[], const int32_t split_size,
                                              const bool load_sstable) {
  int ret = OB_SUCCESS;
  if (NULL == old_tablet || old_tablet->is_merged()
      || NULL == new_tablets || 0 >= split_size) {
    TBSYS_LOG(WARN, "upgrade_tablet invalid argument, "
              "old_tablet=%p or old merged=%d new_tablet=%p, split_size=%d",
              old_tablet, old_tablet->is_merged(), new_tablets, split_size);
    ret = OB_INVALID_ARGUMENT;
  } else {
    int new_version = new_tablets[0]->get_data_version();

    char range_buf[OB_RANGE_STR_BUFSIZ];
    old_tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
    TBSYS_LOG(INFO, "upgrade old range:(%s), old version=%ld",
              range_buf, old_tablet->get_data_version());

    tbsys::CWLockGuard guard(lock_);

    if (new_version <= old_tablet->get_data_version()) {
      TBSYS_LOG(ERROR, "new version =%ld <= old version=%ld",
                new_version, old_tablet->get_data_version());
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_SUCCESS != (ret = prepare_tablet_image(new_version, true))) {
      TBSYS_LOG(ERROR, "prepare new version = %ld image error.", new_version);
    }

    for (int32_t i = 0; i < split_size && OB_SUCCESS == ret; ++i) {
      if (new_tablets[i]->get_data_version() != new_version) {
        TBSYS_LOG(ERROR, "split tablet i=%d, version =%ld <> fisrt tablet version =%ld",
                  i, new_tablets[i]->get_data_version(), new_version);
        ret = OB_INVALID_ARGUMENT;
        break;
      } else if (OB_SUCCESS != (ret =
                                  get_image(new_version).add_tablet(new_tablets[i], load_sstable))) {
        TBSYS_LOG(ERROR, "add tablet error with new version = %ld.", new_version);
      } else {
        new_tablets[i]->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(INFO, "upgrade with new range:(%s), new version=%ld",
                  range_buf, new_version);
      }
    }

    if (OB_SUCCESS == ret) {
      old_tablet->set_merged();
    }
  }
  return ret;
}

int ObMultiVersionTabletImage::upgrade_service() {
  int ret = OB_SUCCESS;

  tbsys::CWLockGuard guard(lock_);
  int64_t new_version = image_tracker_[newest_index_]->data_version_;
  int64_t service_version = 0;
  if (service_index_ >= 0
      && service_index_ < MAX_RESERVE_VERSION_COUNT
      && has_tablet(service_index_)) {
    service_version = image_tracker_[service_index_]->data_version_;
  }

  if (!has_tablet(newest_index_)) {
    TBSYS_LOG(WARN, "there is no tablets on version = %ld, still upgrade.", new_version);
    service_index_ = newest_index_;
  } else if (new_version <= service_version) {
    TBSYS_LOG(WARN, "service version =%ld >= newest version =%ld, cannot upgrade.",
              service_version, new_version);
    ret = OB_ERROR;
  } else {
    // TODO check service_version merged complete?
    TBSYS_LOG(INFO, "upgrade service version =%ld to new version =%ld",
              service_version, new_version);
    service_index_ = newest_index_;
  }

  return ret;
}

int ObMultiVersionTabletImage::alloc_tablet_object(
  const common::ObRange& range, const int64_t version, ObTablet*& tablet) {
  int ret = OB_SUCCESS;

  if (range.empty() || 0 >= version) {
    TBSYS_LOG(WARN, "alloc_tablet_object invalid argument, "
              "range is emtpy or version=%ld < 0", version);
    ret = OB_INVALID_ARGUMENT;
  } else {
    tablet = NULL;
    tbsys::CWLockGuard guard(lock_);

    if (OB_SUCCESS != (ret = prepare_tablet_image(version, true))) {
      TBSYS_LOG(ERROR, "prepare new version = %ld image error.", version);
    } else if (OB_SUCCESS != (ret =
                                get_image(version).alloc_tablet_object(range, tablet))) {
      TBSYS_LOG(ERROR, "cannot alloc tablet object new version = %ld", version);
    } else if (NULL != tablet) {
      tablet->set_data_version(version);
    }
  }
  return ret;
}

int ObMultiVersionTabletImage::prepare_for_merge(const int64_t version) {
  tbsys::CWLockGuard guard(lock_);
  return prepare_tablet_image(version, true);
}

bool ObMultiVersionTabletImage::has_tablets_for_merge(const int64_t version) const {
  bool ret = false;
  if (0 > version) {
    ret = false;
  } else if (!has_match_version_tablet(version)) {
    ret = false;
  } else {
    int64_t index = get_index(version);
    tbsys::CRLockGuard guard(lock_);
    ret = (OB_SUCCESS != tablets_all_merged(index));
  }
  return ret;
}

int ObMultiVersionTabletImage::get_tablets_for_merge(
  const int64_t version, int32_t& size, ObTablet* tablets[]) const {
  int ret = OB_SUCCESS;

  tbsys::CRLockGuard guard(lock_);

  int64_t eldest_index = get_eldest_index();
  int64_t index = eldest_index;
  int merge_count = 0;
  do {
    if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
      ret = OB_ERROR;
      break;
    }

    if (has_tablet(index) && image_tracker_[index]->data_version_ < version) {
      ObSortedVector<ObTablet*>& tablet_list = image_tracker_[index]->tablet_list_;
      ObSortedVector<ObTablet*>::iterator it = tablet_list.begin();
      for (; it != tablet_list.end() && merge_count < size; ++it) {
        if (!(*it)->is_merged()) {
          tablets[merge_count++] = *it;
          // add reference count
          image_tracker_[index]->acquire();
        }
      }
    }
    if (merge_count >= size) break;
    index = (index + 1) % MAX_RESERVE_VERSION_COUNT;
  } while (index != eldest_index);

  TBSYS_LOG(DEBUG, "for merge, version=%ld,size=%d,newest=%ld", version, size, newest_index_);

  size = merge_count;
  return OB_SUCCESS;
}

int ObMultiVersionTabletImage::discard_tablets_not_merged(const int64_t version) {
  int ret = OB_SUCCESS;
  if (0 > version) {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "invalid version = %ld", version);
  } else {
    tbsys::CWLockGuard guard(lock_);

    if (has_match_version_tablet(version)) {
      ObSortedVector<ObTablet*>& tablet_list = get_image(version).tablet_list_;
      ObSortedVector<ObTablet*>::iterator it = tablet_list.begin();
      for (; it != tablet_list.end() ; ++it) {
        if (!(*it)->is_merged()) {
          (*it)->set_merged();
        }
      }
    }
  }
  return ret;
}

int ObMultiVersionTabletImage::prepare_tablet_image(const int64_t version, const bool destroy_exist) {
  int ret = OB_SUCCESS;
  int64_t index = get_index(version);
  if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
    TBSYS_LOG(ERROR, "index=%ld out of range.", index);
    ret = OB_ERROR;
  } else if (NULL == image_tracker_[index]) {
    image_tracker_[index] = new ObTabletImage();
    if (NULL == image_tracker_[index]) {
      TBSYS_LOG(ERROR, "cannot new ObTabletImage object, index=%d", index);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      image_tracker_[index]->set_fileinfo_cache(fileinfo_cache_);
    }
  } else if (version != image_tracker_[index]->data_version_) {
    if (destroy_exist && OB_SUCCESS == (ret = tablets_all_merged(index))) {
      ret = destroy(index);
    } else {
      TBSYS_LOG(ERROR, "new version=%ld not matched with old verion=%ld,"
                "index=%ld cannot destroy" , version, image_tracker_[index]->data_version_, index);
    }
  }

  if (OB_SUCCESS == ret) {
    image_tracker_[index]->data_version_ = version;
    if (!has_tablet(newest_index_)
        || version > image_tracker_[newest_index_]->data_version_) {
      newest_index_ = index;
    }
  }

  return ret;
}

int ObMultiVersionTabletImage::tablets_all_merged(const int64_t index) const {
  int ret = OB_SUCCESS;
  char range_buf[OB_RANGE_STR_BUFSIZ];
  if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
    ret = OB_ERROR;
  } else if (NULL != image_tracker_[index]) {
    ObSortedVector<ObTablet*>& tablet_list = image_tracker_[index]->tablet_list_;
    ObSortedVector<ObTablet*>::iterator it = tablet_list.begin();
    for (; it != tablet_list.end() ; ++it) {
      if (!(*it)->is_merged()) {
        (*it)->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(INFO, "tablet range=<%s> version=%d has not merged",
                  range_buf, (*it)->get_data_version());
        ret = OB_CS_EAGAIN;
        break;
      }
    }
  }
  return ret;
}

int ObMultiVersionTabletImage::destroy(const int64_t index) {
  int ret = OB_SUCCESS;
  if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
    ret = OB_ERROR;
  } else if (has_tablet(index)) {
    ret = image_tracker_[index]->destroy();
  }
  return ret;
}

int ObMultiVersionTabletImage::destroy() {
  int ret = OB_SUCCESS;
  for (int i = 0; i < MAX_RESERVE_VERSION_COUNT && OB_SUCCESS == ret; ++i) {
    if (NULL != image_tracker_[i]) {
      ret = image_tracker_[i]->destroy();
      if (OB_SUCCESS == ret) {
        delete image_tracker_[i];
        image_tracker_[i] = NULL;
      }
    }
  }
  return ret;
}

int ObMultiVersionTabletImage::write(const int64_t version,
                                     const int32_t disk_no) {
  int ret = OB_SUCCESS;
  if (0 >= version || 0 >= disk_no) {
    TBSYS_LOG(WARN, "write image invalid argument, "
              "disk_no =%d <0 or version=%ld < 0", disk_no, version);
    ret = OB_INVALID_ARGUMENT;
  } else if (!has_match_version_tablet(version)) {
    TBSYS_LOG(WARN, "write image version=%ld dont match", version);
    ret = OB_ERROR;
  } else {
    char idx_path[OB_MAX_FILE_NAME_LENGTH];
    ret = get_meta_path(version, disk_no, true, idx_path, OB_MAX_FILE_NAME_LENGTH);
    if (OB_SUCCESS == ret) {
      tbsys::CRLockGuard guard(lock_);
      ret = get_image(version).write(idx_path, disk_no, false);
    }
  }
  return ret;
}

int ObMultiVersionTabletImage::write(const char* idx_path,
                                     const int64_t version, const int32_t disk_no) {
  int ret = OB_SUCCESS;
  if (0 >= version || 0 >= disk_no) {
    TBSYS_LOG(WARN, "write image invalid argument, "
              "disk_no =%d <0 or version=%ld < 0", disk_no, version);
    ret = OB_INVALID_ARGUMENT;
  } else if (!has_match_version_tablet(version)) {
    TBSYS_LOG(WARN, "write image version=%ld dont match", version);
    ret = OB_ERROR;
  } else {
    tbsys::CRLockGuard guard(lock_);
    ret = get_image(version).write(idx_path, disk_no, false);
  }
  return ret;
}

int ObMultiVersionTabletImage::read(const char* idx_path,
                                    const int64_t version, const int32_t disk_no, const bool load_sstable) {
  int ret = OB_SUCCESS;
  if (0 >= version || 0 >= disk_no) {
    TBSYS_LOG(ERROR, "read image invalid argument, "
              "disk_no =%d <0 or version=%ld < 0", disk_no, version);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = prepare_tablet_image(version, true))) {
    TBSYS_LOG(ERROR, "cannot prepare image, version=%ld", version);
  } else if (OB_SUCCESS != (ret = get_image(version).read(
                                    idx_path, disk_no, load_sstable))) {
    TBSYS_LOG(ERROR, "read idx file = %s , disk_no = %d, version = %ld error",
              idx_path, disk_no, version);
  }
  return ret;
}

int ObMultiVersionTabletImage::read(const int64_t version,
                                    const int32_t disk_no, const bool load_sstable) {
  int ret = OB_SUCCESS;
  char idx_path[OB_MAX_FILE_NAME_LENGTH];

  if (OB_SUCCESS != (ret = get_meta_path(version,
                                         disk_no, true, idx_path, OB_MAX_FILE_NAME_LENGTH))) {
    TBSYS_LOG(ERROR, "get meta file path version = %ld, disk_no = %d error",
              version, disk_no);
  } else if (OB_SUCCESS != (ret = read(idx_path,
                                       version, disk_no, load_sstable))) {
    TBSYS_LOG(ERROR, "read idx file =%s version = %ld, disk_no = %d error",
              idx_path, version, disk_no);
  }
  return ret;
}

int ObMultiVersionTabletImage::load_tablets(const int32_t* disk_no_array,
                                            const int32_t size, const bool load_sstable) {
  int ret = OB_SUCCESS;
  if (NULL == disk_no_array || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    char idx_dir_path[OB_MAX_FILE_NAME_LENGTH];
    struct dirent** idx_dirent = NULL;
    const char* idx_file_name = NULL;
    int64_t idx_file_num = 0;
    int64_t version = 0;
    int32_t disk_no = 0;

    for (int32_t i = 0; i < size && (OB_SUCCESS == ret || OB_CS_EAGAIN == ret); ++i) {
      disk_no = disk_no_array[i];
      ret = get_sstable_directory(disk_no, idx_dir_path, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "get sstable directory disk %d failed", disk_no);
        ret = OB_ERROR;
        break;
      }

      idx_file_num = ::scandir(idx_dir_path, &idx_dirent, idx_file_name_filter, ::versionsort);
      if (idx_file_num <= 0) {
        TBSYS_LOG(INFO, "idx directory %s has no idx files.", idx_dir_path);
        continue;
      }

      for (int n = 0; n < idx_file_num; ++n) {
        idx_file_name = idx_dirent[n]->d_name;
        // idx_file_name likes "idx_[version]_[disk]
        ret = sscanf(idx_file_name, "idx_%ld_%d", &version, &disk_no);
        if (ret < 2) {
          ret = OB_SUCCESS;
        } else if (disk_no != disk_no_array[i]) {
          TBSYS_LOG(ERROR, "disk no = %d in idx file name cannot match with disk=%d ",
                    disk_no, disk_no_array[i]);
          ret = OB_ERROR;
        } else {
          ret = read(version, disk_no, load_sstable);
        }

        ::free(idx_dirent[n]);
      }

      ::free(idx_dirent);
      idx_dirent = NULL;

    }  // end for

  } // end else

  if (OB_CS_EAGAIN == ret) {
    // ignore OB_CS_EAGAIN error.
    // there's old meta index file remains sstable dir
    // but read first, it will be replaced by newer tablets,
    ret = OB_SUCCESS;
  }

  if (OB_SUCCESS == ret) {
    if (initialize_service_index() < 0) {
      TBSYS_LOG(ERROR, "search service tablet image failed,"
                "service_index_=%ld", service_index_);
      ret = OB_ERROR;
    } else if (has_tablet(service_index_)) {
      TBSYS_LOG(INFO, "current service tablet version=%ld",
                image_tracker_[service_index_]->data_version_);
    }
  }



  return ret;
}

int64_t ObMultiVersionTabletImage::initialize_service_index() {
  // get eldest index;
  int64_t eldest_index = (newest_index_ + 1) % MAX_RESERVE_VERSION_COUNT;
  int64_t index = eldest_index;
  service_index_ = index;

  do {
    if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
      service_index_ = -1;
      break;
    }

    if (OB_SUCCESS != tablets_all_merged(index)) {
      service_index_ = index;
      break;
    }

    index = (index + 1) % MAX_RESERVE_VERSION_COUNT;
  } while (index != eldest_index);

  return service_index_;
}

int ObMultiVersionTabletImage::begin_scan_tablets() {
  int ret = OB_SUCCESS;
  if (!iterator_.is_stop()) {
    TBSYS_LOG(WARN, "scan in progress, cannot luanch another, cur_vi_=%d", iterator_.cur_vi_);
    ret = OB_CS_EAGAIN;
  } else if (OB_SUCCESS != (ret = iterator_.start())) {
    TBSYS_LOG(WARN, "start failed ret = %d", ret);
  } else {
    TBSYS_LOG(INFO, "start succeed vi=%d, ti=%d", iterator_.cur_vi_, iterator_.cur_ti_);
  }
  return ret;
}

int ObMultiVersionTabletImage::get_next_tablet(ObTablet*& tablet) {
  int ret = OB_SUCCESS;
  tablet = NULL;
  if (iterator_.is_stop()) {
    TBSYS_LOG(ERROR, "not a initialized scan process, call begin_scan_tablets first");
    ret = OB_ERROR;
  } else if (iterator_.is_end()) {
    ret = OB_ITER_END;
  } else {
    if (OB_SUCCESS == ret) {
      tablet = iterator_.get_tablet();
      if (NULL == tablet) {
        ret = OB_ERROR;
      } else {
        iterator_.next();
      }
    }
  }
  return ret;
}

int ObMultiVersionTabletImage::end_scan_tablets() {
  int ret = OB_SUCCESS;
  if (iterator_.is_stop()) {
    TBSYS_LOG(WARN, "scans not begin.");
    ret = OB_ERROR;
  } else {
    iterator_.reset();
  }
  return ret;
}

int64_t ObMultiVersionTabletImage::get_max_sstable_file_seq() const {
  int64_t max_seq = 0;
  for (int32_t index = 0; index < MAX_RESERVE_VERSION_COUNT; ++index) {
    if (has_tablet(index)) {
      int64_t seq = image_tracker_[index]->get_max_sstable_file_seq();
      if (seq > max_seq) max_seq = seq;
    }
  }
  return max_seq;
}

void ObMultiVersionTabletImage::dump(const bool dump_sstable) const {
  int64_t eldest_index = get_eldest_index();
  int64_t index = eldest_index;
  do {
    if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT) {
      break;
    }

    if (has_tablet(index)) {
      image_tracker_[index]->dump(dump_sstable);
    }
    index = (index + 1) % MAX_RESERVE_VERSION_COUNT;
  } while (index != eldest_index);
}

int ObMultiVersionTabletImage::serialize(
  const int32_t index, const int32_t disk_no, char* buf, const int64_t buf_len, int64_t& pos) {
  int ret = OB_SUCCESS;
  if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT || disk_no <= 0) {
    TBSYS_LOG(WARN, "invalid argument index= %d, disk_no=%d", index, disk_no);
    ret = OB_INVALID_ARGUMENT;
  } else if (!has_tablet(index)) {
    TBSYS_LOG(WARN, "has no tablet index= %d, disk_no=%d", index, disk_no);
    ret = OB_ERROR;
  } else {
    ret = image_tracker_[index]->serialize(disk_no, buf, buf_len, pos);
  }

  return ret;
}

int ObMultiVersionTabletImage::deserialize(
  const int32_t disk_no, const char* buf, const int64_t buf_len, int64_t& pos) {
  int ret = OB_SUCCESS;
  int64_t origin_payload_pos = 0;
  ObTabletMetaHeader meta_header;

  if (OB_SUCCESS != (ret = ObRecordHeader::check_record(
                             buf + pos, buf_len, ObTabletMetaHeader::TABLET_META_MAGIC))) {
    TBSYS_LOG(ERROR, "check common record header failed, disk_no=%d", disk_no);
  } else {
    pos += OB_RECORD_HEADER_LENGTH;
    origin_payload_pos = pos;
    ret = meta_header.deserialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "deserialize meta header failed, disk_no=%d", disk_no);
    } else if (OB_SUCCESS != (ret = prepare_tablet_image(meta_header.data_version_, false))) {
      TBSYS_LOG(ERROR, "prepare_tablet_image header.version=%ld error",
                meta_header.data_version_);
      ret = OB_ERROR;
    } else {
      pos = 0;
      get_image(meta_header.data_version_).deserialize(disk_no, buf, buf_len, pos);
    }
  }
  return ret;
}

//----------------------------------------
// class ObMultiVersionTabletImage::Iterator
//----------------------------------------
int ObMultiVersionTabletImage::Iterator::start() {
  int ret = OB_SUCCESS;
  cur_vi_ = image_.get_eldest_index();
  cur_ti_ = 0;
  start_vi_ = cur_vi_;
  end_vi_ = image_.newest_index_;
  if (!image_.has_tablet(cur_vi_)) {
    ret = next();
  }
  return ret;
}

int ObMultiVersionTabletImage::Iterator::next() {
  int ret = OB_SUCCESS;
  if (is_end()) {
    ret = OB_ITER_END;
  } else {
    if (cur_vi_ != end_vi_) {
      // maybe there is newer version tablets
      if (is_last_tablet()) {
        // current version reach the end, find next not null
        while (cur_vi_ != end_vi_) {
          cur_vi_ = (cur_vi_ + 1) % MAX_RESERVE_VERSION_COUNT;
          if (image_.has_tablet(cur_vi_)) break;
        }
        if (cur_vi_ == end_vi_ && (!image_.has_tablet(cur_vi_))) {
          // cannot find it , scan is over.
          ret = OB_ITER_END;
        } else {
          // newer version tablet be found, reset index.
          cur_ti_ = 0;
        }
      } else {
        // current version tablet has more, skip to next.
        ++cur_ti_;
      }
    } else if (is_last_tablet()) {
      // reach last tablet of newest version  tablet image
      // set cur_ti_ to end position, for next() call lately.
      ++cur_ti_;
      ret = OB_ITER_END;
    } else {
      // reach middle of tablet of newest version tablet image.
      ++cur_ti_;
    }
  }
  return ret;
}

} // end namespace chunkserver
} // end namespace sb



