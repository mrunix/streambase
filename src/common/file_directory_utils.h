/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * file_directory_utils.h for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_FILE_DIRECTORY_UTILS_H_
#define OCEANBASE_COMMON_FILE_DIRECTORY_UTILS_H_

#include <string>
#include <vector>
#include <stdint.h>

namespace sb {
namespace common {
#ifndef S_IRWXUGO
# define S_IRWXUGO (S_IRWXU | S_IRWXG | S_IRWXO)
#endif

class FileDirectoryUtils {
 public:
  static const int MAX_PATH = 512;
  static bool exists(const char* filename);
  static bool is_directory(const char* dirname);
  static bool create_directory(const char* dirname);
  static bool create_full_path(const char* fullpath);
  static bool delete_file(const char* filename);
  static bool delete_directory(const char* dirname);
  static bool delete_directory_recursively(const char* directory);
  static bool rename(const char* srcfilename, const char* destfilename);
  static int64_t get_size(const char* filename);

  static int vsystem(const char* cmd);
  static int cp(const char* src_path, const char* src_name, const char* dst_path, const char* dst_name);
  static int cp_safe(const char* src_path, const char* src_name, const char* dst_path, const char* dst_name);
  static int mv(const char* src_path, const char* src_name, const char* dst_path, const char* dst_name);
  static int rm(const char* path, const char* name);
};

typedef FileDirectoryUtils FSU;
}				//end namespace common
}				//end namespace sb
#endif

