/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_cache.cc for ...
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "ob_ups_cache.h"
#include "common/ob_action_flag.h"

namespace sb {
namespace updateserver {
using namespace common;

ObUpsCache::ObUpsCache(): inited_(false) {
}

ObUpsCache::~ObUpsCache() {
}

int ObUpsCache::init(const int64_t cache_size, const int64_t max_cache_size_limit) {
  int ret = OB_SUCCESS;
  if (inited_) {
    TBSYS_LOG(WARN, "have inited");
    ret = OB_INIT_TWICE;
  } else {
    set_cache_size_limit(max_cache_size_limit);
    set_cache_size(cache_size);
    int64_t cache_size_limit = get_cache_size_limit();
    int64_t cache_size = get_cache_size();
    if (OB_SUCCESS != kv_cache_.init(cache_size)) {
      TBSYS_LOG(WARN, "init kv_cache_ failed");
      ret = OB_ERROR;
    } else {
      TBSYS_LOG(INFO, "cache_size=%ldB,cache_size_limit=%ldB", cache_size, cache_size_limit);
      inited_ = true;
    }
  }
  return ret;
}

int ObUpsCache::destroy() {
  int ret = OB_SUCCESS;
  if (inited_) {
    if (OB_SUCCESS != kv_cache_.destroy()) {
      TBSYS_LOG(ERROR, "destroy cache fail");
      ret = OB_ERROR;
    } else {
      inited_ = false;
    }
  } else {
    TBSYS_LOG(WARN, "have not inited, no need to destroy");
    ret = OB_ERROR;
  }
  return ret;
}

int ObUpsCache::clear() {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else if (OB_SUCCESS != kv_cache_.clear()) {
    TBSYS_LOG(WARN, "clear cache fail");
    ret = OB_ERROR;
  } else {
    //do nothing;
  }
  return ret;
}

int ObUpsCache::get(const uint64_t table_id,
                    const ObString& row_key, ObBufferHandle& buffer_handle,
                    const uint64_t column_id, ObUpsCacheValue& value) {

  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else {
    if (OB_INVALID_ID == table_id || OB_INVALID_ID == column_id
        || NULL == row_key.ptr() || row_key.length() <= 0) {
      TBSYS_LOG(WARN, "invalid param table_id=%lu,column_id=%lu,row_key_ptr=%p,row_key_length=%lu", table_id, column_id, row_key.ptr(), row_key.length());
      ret = OB_ERROR;
    } else {
      ObUpsCacheKey date_key;
      date_key.table_id = table_id;
      date_key.nbyte = row_key.length();
      date_key.column_id = column_id;
      date_key.buffer = const_cast<char*>(row_key.ptr());
      //如果是查询行是否存在于缓存中
      if (0 == column_id) {
        TBSYS_LOG(WARN, "use func is_row_exist to check if the row exist");
        ret = OB_ERROR;
      } else { //在缓存中查询列的值
        ret = kv_cache_.get(date_key, value, buffer_handle.handle_);
        if (OB_SUCCESS == ret) {
          buffer_handle.ups_cache_ = this;
          if (common::ObVarcharType == value.value.get_type()) {
            ObString str_value;
            value.value.get_varchar(str_value);
            buffer_handle.buffer_ = const_cast<char*>(str_value.ptr());
          } else {
            buffer_handle.buffer_ = NULL;
          }
          ret = OB_SUCCESS;
        } else {
          //若不存在，则调用方需要去cs上取数据，然后put进缓存
          TBSYS_LOG(ERROR, "get kv_cache_ failed,ret=%d", ret);
        }
      }
    }
  }
  return ret;
}


// 加入缓存项
int ObUpsCache::put(const uint64_t table_id,
                    const ObString& row_key,
                    const uint64_t column_id,
                    const ObUpsCacheValue& value) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else {
    if (OB_INVALID_ID == table_id || OB_INVALID_ID == column_id
        || NULL == row_key.ptr() || row_key.length() <= 0) {
      TBSYS_LOG(ERROR, "invalid param table_id=%lu,column_id=%lu,row_key_ptr=%p,row_key_length=%lu", table_id, column_id, row_key.ptr(), row_key.length());
      ret = OB_ERROR;
    } else {
      ObString temp_string;
      if (ObNullType == value.value.get_type()) {
        TBSYS_LOG(WARN, "invalid value,value_type=%d", value.value.get_type());
        ret = OB_ERROR;
      }
      value.value.get_varchar(temp_string);
      if (OB_SUCCESS == ret
          && ObVarcharType == value.value.get_type()
          && NULL == temp_string.ptr()) {
        TBSYS_LOG(ERROR, "invalid value,value_varchar_type=%d,value_varchar_ptr=%p",
                  value.value.get_type(), temp_string.ptr());
        ret = OB_ERROR;
      } else {
        //do nothing
      }
    }
  }

  if (OB_SUCCESS == ret) {
    //参数验证正确以后，将参数形成key和value 插入到缓存中
    ObUpsCacheKey cache_key;
    cache_key.table_id = table_id;
    cache_key.column_id = column_id;
    cache_key.nbyte = row_key.length();
    cache_key.buffer = const_cast<char*>(row_key.ptr());
    ret = kv_cache_.put(cache_key, value);
  } else {
    //do nothing
  }
  return ret;
}

// 查询行是否存在
// 如果缓存项存在，返回OB_SUCCESS; 如果缓存项不存在，返回OB_NOT_EXIST；否则，返回OB_ERROR；
int ObUpsCache::is_row_exist(const uint64_t table_id,
                             const ObString& row_key,
                             bool& is_exist,
                             ObBufferHandle& buffer_handle) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else {
    if (OB_INVALID_ID == table_id || NULL == row_key.ptr() || row_key.length() <= 0) {
      TBSYS_LOG(WARN, "invalid param table_id=%lu,row_key_ptr=%p,row_key_length=%lu", table_id, row_key.ptr(), row_key.length());
      ret = OB_ERROR;
    } else {
      ObUpsCacheKey cache_key;
      ObUpsCacheValue cache_value;
      cache_key.table_id = table_id;
      cache_key.column_id = 0;
      cache_key.nbyte = row_key.length();
      cache_key.buffer = const_cast<char*>(row_key.ptr());
      if (OB_SUCCESS == kv_cache_.get(cache_key, cache_value, buffer_handle.handle_)) {
        buffer_handle.ups_cache_ = this;
        buffer_handle.buffer_ = NULL;
        //判断cache_value的值
        int64_t exist_value = 0;
        if (OB_SUCCESS == cache_value.value.get_ext(exist_value)) {
          ret = OB_SUCCESS;
          if (ObActionFlag::OP_ROW_EXIST == exist_value) {
            is_exist = true;
          } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == exist_value) {
            is_exist = false;
          } else {
            ret = OB_ERROR;
          }
        } else {
          ret = OB_ERROR;
        }
      } else {
        ret = OB_UPS_NOT_EXIST;
      }
    }
  }

  return ret;
}

// 设置行是否存在标志，行不存在也需要记录到缓存中
int ObUpsCache::set_row_exist(const uint64_t table_id, const ObString& row_key, const bool is_exist) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    TBSYS_LOG(WARN, "have not inited");
    ret = OB_ERROR;
  } else {
    if (OB_INVALID_ID == table_id
        || NULL == row_key.ptr()
        || row_key.length() <= 0) {
      TBSYS_LOG(WARN, "invalid param,table_id=%lu,row_key_ptr=%p,row_key_length=%lu",
                table_id, row_key.ptr(), row_key.length());
      ret = OB_ERROR;
    } else {
      ObUpsCacheKey cache_key;
      ObUpsCacheValue cache_value;
      cache_key.table_id = table_id;
      cache_key.nbyte = row_key.length();
      cache_key.buffer = const_cast<char*>(row_key.ptr());
      cache_key.column_id = 0;

      if (is_exist) {
        cache_value.value.set_ext(ObActionFlag::OP_ROW_EXIST);
      } else {
        cache_value.value.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
      }
      ret = kv_cache_.put(cache_key, cache_value);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(WARN, "set_row_exist failed,ret=%d", ret);
      }
    }
  }
  return ret;
}

// 设置缓存大小限制，单位为KB
void ObUpsCache::set_cache_size_limit(const int64_t cache_size) {
  if (cache_size < MIN_KVCACHE_SIZE) {
    TBSYS_LOG(WARN, "cache size should not less than 1 m");
    max_cache_size_limit_ = MIN_KVCACHE_SIZE;
  } else {
    max_cache_size_limit_ = cache_size;
  }
}

// 获取缓存大小限制，单位为B
int64_t ObUpsCache::get_cache_size_limit(void) {
  return max_cache_size_limit_;
}
// 设置缓存大小，单位为B
void ObUpsCache::set_cache_size(const int64_t cache_size) {
  if (cache_size > max_cache_size_limit_) {
    TBSYS_LOG(ERROR, "invalid cache size,max_cache_size=%luB", max_cache_size_limit_);
    cache_size_ = max_cache_size_limit_;
  } else if (cache_size  < MIN_KVCACHE_SIZE) {
    TBSYS_LOG(ERROR, "invalid cache size, min_cache_size=%luB", MIN_KVCACHE_SIZE);
    cache_size_ = MIN_KVCACHE_SIZE;
  } else {
    cache_size_ = cache_size ;
  }
}
// 获取缓存占用的内存，单位为B
int64_t ObUpsCache::get_cache_size(void) {
  return cache_size_;
}

}//end of updateserver
} //end of  sb


