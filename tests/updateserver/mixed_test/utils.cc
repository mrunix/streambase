/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * utils.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include <stdint.h>
#include <ctype.h>
#include "utils.h"

int64_t range_rand(int64_t start, int64_t end, int64_t rand) {
  return (rand % (end - start + 1) + start);
}

void build_string(char* buffer, int64_t len, int64_t seed) {
  if (NULL != buffer) {
    struct drand48_data rand_seed;
    srand48_r(seed, &rand_seed);
    for (int64_t i = 0; i < len;) {
      int64_t rand = 0;
      lrand48_r(&rand_seed, &rand);
      int8_t c = range_rand(0, 255, rand);
      if (isalpha(c) || isalnum(c)) {
        buffer[i++] = c;
      }
    }
  }
}


