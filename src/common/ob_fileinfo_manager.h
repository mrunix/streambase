/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fileinfo_manager.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_FILEINFO_MANAGER_H_
#define  OCEANBASE_UPDATESERVER_FILEINFO_MANAGER_H_
#include <sys/types.h>
#include <dirent.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"

namespace sb {
namespace common {
class IFileInfo {
 public:
  virtual ~IFileInfo() {};
 public:
  virtual int get_fd() const = 0;
};

class IFileInfoMgr {
 public:
  virtual ~IFileInfoMgr() {};
 public:
  virtual const IFileInfo* get_fileinfo(const uint64_t key_id) = 0;
  virtual int revert_fileinfo(const IFileInfo* file_info) = 0;
  virtual int erase_fileinfo(const uint64_t key_id) {
    UNUSED(key_id);
    return OB_NOT_SUPPORTED;
  };
};
}
}

#endif //OCEANBASE_UPDATESERVER_FILEINFO_MANAGER_H_


