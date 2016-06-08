/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * utility.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_UTILITY_H_
#define OCEANBASE_COMMON_UTILITY_H_

#include <stdint.h>
#include "tbsys.h"

#define STR_BOOL(b) ((b) ? "true" : "false")

namespace sb {
namespace common {
void hex_dump(const void* data, const int32_t size,
              const bool char_type = true, const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG);
int32_t parse_string_to_int_array(const char* line,
                                  const char del, int32_t* array, int32_t& size);
int32_t hex_to_str(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size);
int32_t str_to_hex(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size);
int64_t lower_align(int64_t input, int64_t align);
int64_t upper_align(int64_t input, int64_t align);
bool is2n(int64_t input);
bool all_zero(const char* buffer, const int64_t size);
bool all_zero_small(const char* buffer, const int64_t size);
char* str_trim(char* str);
void databuff_printf(char* buf, const int64_t buf_len, int64_t& pos, const char* fmt, ...);
} // end namespace common
} // end namespace sb


#endif //OCEANBASE_COMMON_UTILITY_H_


