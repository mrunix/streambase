/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_regex.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_COMMON_REGEX_H_
#define  OCEANBASE_COMMON_REGEX_H_

#include <regex.h>

namespace sb {
namespace common {
class ObRegex {
 public:
  ObRegex();
  ~ObRegex();
 public:
  bool init(const char* pattern, int flags);
  bool match(const char* text, int flags);
  void destroy(void);
 private:
  bool init_;
  regmatch_t* match_;
  regex_t reg_;
  size_t nmatch_;
};
}
}

#endif //OCEANBASE_COMMON_REGEX_H_


