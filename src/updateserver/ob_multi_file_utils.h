/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multi_file_utils.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_MULTI_FILE_UTILS_H_
#define  OCEANBASE_UPDATESERVER_MULTI_FILE_UTILS_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/ob_file.h"
#include "common/ob_list.h"
#include "common/hash/ob_hashutils.h"

namespace sb {
namespace updateserver {
class MultiFileUtils : public common::ObFileAppender {
  typedef common::hash::SimpleAllocer<common::ObFileAppender> FileAlloc;
  static const bool IS_DIRECT = true;
  static const bool IS_CREATE = true;
  static const bool IS_TRUNC = false;
 public:
  MultiFileUtils();
  virtual ~MultiFileUtils();
 public:
  // 多个文件名使用'\0'分隔 最后一个文件名后使用两个'\0'结尾
  virtual int open(const common::ObString& fname, const bool dio, const bool is_create, const bool is_trunc, const int64_t align_size);
  virtual void close();
  virtual int64_t get_file_pos() const;
  virtual int append(const void* buf, const int64_t count, bool is_fsync);
  virtual int fsync();
 private:
  common::ObList<common::ObFileAppender*> flist_;
  FileAlloc file_alloc_;
};
}
}

#endif //OCEANBASE_UPDATESERVER_MULTI_FILE_UTILS_H_



