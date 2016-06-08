/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * murmur_hash.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_MURMURHASH_H_
#define OCEANBASE_COMMON_MURMURHASH_H_
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>
namespace sb {
namespace common {

/**
 * The MurmurHash 2 from Austin Appleby, faster and better mixed (but weaker
 * crypto-wise with one pair of obvious differential) than both Lookup3 and
 * SuperFastHash. Not-endian neutral for speed.
 */

uint32_t murmurhash2(const void* data, int32_t len, uint32_t hash);
uint32_t fnv_hash2(const void* data, int32_t len, uint32_t hash);
struct MurmurHash2 {
  uint32_t operator()(const std::string& s) const {
    return murmurhash2(s.c_str(), s.length(), 0);
  }

  uint32_t operator()(const void* start, int32_t len) const {
    return murmurhash2(start, len, 0);
  }

  uint32_t operator()(const void* start, int32_t len, uint32_t seed) const {
    return murmurhash2(start, len, seed);
  }

  uint32_t operator()(const char* s) const {
    return murmurhash2(s, strlen(s), 0);
  }
};

}
}
#endif

