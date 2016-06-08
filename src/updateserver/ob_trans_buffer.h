/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_trans_buffer.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_TRANS_BUFFER_H_
#define  OCEANBASE_UPDATESERVER_TRANS_BUFFER_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "ob_table_engine.h"

namespace sb {
namespace updateserver {
class TransBuffer {
  static const int64_t DEFAULT_SIZE = 1024;
  typedef common::hash::ObHashMap<TEKey, TEValue> hashmap_t;
 public:
  typedef common::hash::ObHashMap<TEKey, TEValue>::const_iterator iterator;
 public:
  TransBuffer();
  ~TransBuffer();
 public:
  int push(const TEKey& key, const TEValue& value);
  int get(const TEKey& key, TEValue& value) const;
  // 注意: 开始一次mutation 上次的mutation自动被提交
  int start_mutation();
  int end_mutation(bool rollback);
  int clear();
 public:
  iterator begin() const;
  iterator end() const;
 private:
  int commit_();
 private:
  hashmap_t trans_map_;
  hashmap_t patch_map_;
};
}
}

#endif //OCEANBASE_UPDATESERVER_TRANS_BUFFER_H_



