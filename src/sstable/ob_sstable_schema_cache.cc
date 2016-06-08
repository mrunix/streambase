/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5567
 *
 * ob_sstable_schema_cache.cc
 *
 * Authors:
 *     huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include "ob_sstable_schema_cache.h"

namespace sb {
namespace sstable {
using namespace common;

ObSSTableSchemaCache::ObSSTableSchemaCache() : schema_cnt_(0) {
  memset(schema_array_, 0, sizeof(ObSchemaNode) * MAX_SCHEMA_VER_COUNT);
}

ObSSTableSchemaCache::~ObSSTableSchemaCache() {
  destroy();
}

ObSSTableSchema* ObSSTableSchemaCache::get_schema(const uint64_t table_id,
                                                  const int64_t version) {
  ObSSTableSchema* schema = NULL;
  int64_t index           = 0;

  if (OB_INVALID_ID == table_id || 0 == table_id || version < 0) {
    TBSYS_LOG(WARN, "invalid param, table_id=%lu, version=%ld, schema_cnt=%ld",
              table_id, version, schema_cnt_);
  } else {
    rwlock_.rdlock();
    index = find_schema_node_index(table_id, version);
    if (index >= 0) {
      schema = schema_array_[index].schema_;
      if (NULL != schema) {
        schema_array_[index].inc_ref_cnt();
      } else {
        TBSYS_LOG(WARN, "sstable schema is NULL in sstable schema cache, index=%ld, "
                  "table_id=%lu, version=%ld, ref_cnt=%ld, schema_cnt=%ld",
                  index, schema_array_[index].table_id_, schema_array_[index].schema_ver_,
                  schema_array_[index].ref_cnt_, schema_cnt_);
      }
    }
    rwlock_.unlock();
  }

  return schema;
}

int ObSSTableSchemaCache::add_schema(ObSSTableSchema* schema, const uint64_t table_id,
                                     const int64_t version) {
  int ret       = OB_SUCCESS;
  int64_t index = 0;
  int64_t i     = 0;

  if (NULL == schema || OB_INVALID_ID == table_id || 0 == table_id || version < 0) {
    TBSYS_LOG(WARN, "invalid param, schema=%p, table_id=%lu, version=%ld, schema_cnt=%ld",
              schema, table_id, version, schema_cnt_);
    ret = OB_ERROR;
  } else if (schema_cnt_ >= MAX_SCHEMA_VER_COUNT) {
    TBSYS_LOG(WARN, "can't add more schema into sstable schema cache, schema_cnt=%ld, "
              "max_schema_cnt=%ld",
              schema_cnt_, MAX_SCHEMA_VER_COUNT);
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret) {
    rwlock_.wrlock();
    index = find_schema_node_index(table_id, version);
    if (index >= 0) {
      TBSYS_LOG(INFO, "sstable schema existent, table_id=%lu, version=%ld, schema_cnt=%ld",
                table_id, version, schema_cnt_);
      ret = OB_ENTRY_EXIST;
    }

    if (OB_SUCCESS == ret) {
      i = upper_bound_index(table_id, version);
      if (i < schema_cnt_) {
        memmove(&schema_array_[i + 1], &schema_array_[i],
                sizeof(ObSchemaNode) * (schema_cnt_ - i));
      }

      schema_array_[i].reset();
      schema_array_[i].table_id_ = table_id;
      schema_array_[i].schema_ = schema;
      schema_array_[i].schema_ver_ = version;
      schema_array_[i].inc_ref_cnt();
      ++schema_cnt_;
    }
    rwlock_.unlock();
  }

  return ret;
}

int ObSSTableSchemaCache::revert_schema(const uint64_t table_id, const int64_t version) {
  int ret                     = OB_SUCCESS;
  int64_t index               = 0;
  int64_t ref_cnt             = 0;
  ObSSTableSchema* free_schema = NULL;

  if (OB_INVALID_ID == table_id || 0 == table_id || version < 0) {
    TBSYS_LOG(WARN, "invalid param, table_id=%lu, version=%ld, schema_cnt=%ld",
              table_id, version, schema_cnt_);
    ret = OB_ERROR;
  } else {
    rwlock_.wrlock();
    index = find_schema_node_index(table_id, version);
    if (index < 0) {
      TBSYS_LOG(WARN, "sstable schema is non-existent, table_id=%lu, "
                "version=%ld, schema_cnt=%ld",
                table_id, version, schema_cnt_);
      ret = OB_ERROR;
    } else {
      ref_cnt = schema_array_[index].dec_ref_cnt();
      if (0 == ref_cnt && NULL != schema_array_[index].schema_) {
        free_schema = schema_array_[index].schema_;
        if (index < schema_cnt_ - 1) {
          memmove(&schema_array_[index], &schema_array_[index + 1],
                  sizeof(ObSchemaNode) * (schema_cnt_ - index));
        }
        --schema_cnt_;
      } else if (ref_cnt < 0 || NULL == schema_array_[index].schema_) {
        TBSYS_LOG(WARN, "wrong schema or reference count, schema=%p, table_id=%lu, "
                  "version=%ld, ref_cnt=%ld",
                  schema_array_[index].schema_, schema_array_[index].table_id_,
                  schema_array_[index].schema_ver_, ref_cnt);
        ret = OB_ERROR;
      }
    }
    rwlock_.unlock();
  }

  if (NULL != free_schema) {
    free_schema->~ObSSTableSchema();
    ob_free(reinterpret_cast<void*>(free_schema));
  }

  return ret;
}

int64_t ObSSTableSchemaCache::find_schema_node_index(const uint64_t table_id,
                                                     const int64_t version) const {
  int64_t ret     = -1;
  int64_t left    = 0;
  int64_t middle  = 0;
  int64_t right   = schema_cnt_ - 1;

  while (left <= right) {
    middle = (left + right) / 2;
    if (schema_array_[middle].compare(table_id, version) > 0) {
      right = middle - 1;
    } else if (schema_array_[middle].compare(table_id, version) < 0) {
      left = middle + 1;
    } else {
      ret = middle;
      break;
    }
  }

  return ret;
}

int64_t ObSSTableSchemaCache::upper_bound_index(const uint64_t table_id,
                                                const int64_t version) const {
  int64_t left    = 0;
  int64_t middle  = 0;
  int64_t right   = schema_cnt_;

  while (left < right) {
    middle = (left + right) / 2;
    if (schema_array_[middle].compare(table_id, version) > 0) {
      right = middle;
    } else {
      left = middle + 1;
    }
  }

  return left;
}

int ObSSTableSchemaCache::clear() {
  int ret = OB_SUCCESS;

  rwlock_.wrlock();
  if (schema_cnt_ > 0) {
    for (int64_t i = 0; i < schema_cnt_; ++i) {
      if (0 != schema_array_[i].ref_cnt_) {
        TBSYS_LOG(WARN, "schema reference count is not 0, ref_cnt=%ld, index=%ld,"
                  "schema=%p, table_id=%lu, version=%ld, schema_cnt=%ld",
                  schema_array_[i].ref_cnt_, i, schema_array_[i].schema_,
                  schema_array_[i].table_id_, schema_array_[i].schema_ver_, schema_cnt_);
        ret = OB_ERROR;
      }

      if (NULL != schema_array_[i].schema_) {
        schema_array_[i].schema_->~ObSSTableSchema();
        ob_free(reinterpret_cast<void*>(schema_array_[i].schema_));
      }
    }
  }
  schema_cnt_ = 0;
  rwlock_.unlock();

  return ret;
}

int ObSSTableSchemaCache::destroy() {
  return clear();
}
} // end namespace sstable
} // end namespace sb
