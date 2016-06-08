/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_init.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_TEST_INIT_H__
#define __OCEANBASE_CHUNKSERVER_TEST_INIT_H__

#include "common/ob_malloc.h"

int init_mem_pool() {
  ob_init_memory_pool(2 * 1024L * 1024L);
  return 0;
}
static int init_mem_pool_ret = init_mem_pool();

#endif //__TEST_INIT_H__



