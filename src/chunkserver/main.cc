/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: 5623
 *
 * main.cc
 *
 * Authors:
 *     qushan <qushan@taobao.com>
 * Changes:
 *     rizhao <rizhao.ych@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *
 */

#include <new>
#include <malloc.h>
#include "common/ob_define.h"
#include "ob_chunk_server_main.h"

using namespace sb::common;
using namespace sb::chunkserver;

namespace {
const char* PUBLIC_SECTION_NAME = "public";
static const int DEFAULT_MMAP_THRESHOLD = 64 * 1024 + 128;
}


int main(int argc, char* argv[]) {
  ::mallopt(M_MMAP_THRESHOLD, DEFAULT_MMAP_THRESHOLD);
  ob_init_memory_pool();
  ObChunkServerMain* cm = ObChunkServerMain::get_instance();
  int ret = OB_SUCCESS;
  if (NULL == cm) {
    fprintf(stderr, "cannot start chunkserver, new ObChunkServerMain failed\n");
    ret = OB_ERROR;
  } else {
    ret = cm->start(argc, argv, PUBLIC_SECTION_NAME);
    cm->destroy();
  }
  return ret;
}



