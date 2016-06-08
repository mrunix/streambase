/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * main.cc for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "common/base_main.h"
#include "common/ob_malloc.h"
#include "ob_merge_server_main.h"
#include <malloc.h>

using namespace sb::common;
using namespace sb::mergeserver;
namespace {
static const int DEFAULT_MMAP_MAX_VAL = 1024 * 1024 * 1024;
};

int main(int argc, char* argv[]) {
  int rc = OB_SUCCESS;
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
  ob_init_memory_pool();
  BaseMain* mergeServer = ObMergeServerMain::get_instance();
  if (NULL == mergeServer) {
    fprintf(stderr, "mergeserver start failed, instance is NULL.");
    rc = OB_ERROR;
  } else {
    srand(time(NULL));
    rc = mergeServer->start(argc, argv, "merge_server");
  }

  if (NULL != mergeServer) {
    mergeServer->destroy();
  }

  return rc;
}



