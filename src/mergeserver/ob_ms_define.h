/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_define.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_MS_DEFINE_H_
#define OCEANBASE_MERGESERVER_OB_MS_DEFINE_H_
namespace sb {
namespace mergeserver {
/// memory used exceeds this will cause memory free, prevent one thread hold too much memory
static const int64_t OB_MS_THREAD_MEM_CACHE_LOWER_WATER_MARK = 128 * 1024 * 1024;
/// memory used exceeds this will enter into endless loop
static const int64_t OB_MS_THREAD_MEM_CACHE_UPPER_WATER_MARK = 512 * 1024 * 1024;
}
}

#endif /* OCEANBASE_MERGESERVER_OB_MS_DEFINE_H_ */


