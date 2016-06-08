/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * utils.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef _MIXED_TEST_UTILS_
#define _MIXED_TEST_UTILS_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define TIMEOUT_MS 10000000
#define SEED_START 0

extern int64_t range_rand(int64_t start, int64_t end, int64_t rand);

extern void build_string(char* buffer, int64_t len, int64_t seed);

#endif // _MIXED_TEST_UTILS_

