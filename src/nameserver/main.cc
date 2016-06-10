/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include <malloc.h>
#include "nameserver_main.h"
#include "common/ob_malloc.h"

using namespace sb::nameserver;
using namespace sb::common;

namespace {
static const int DEFAULT_MMAP_MAX_VAL = 1024 * 1024 * 1024;
};

int main(int argc, char* argv[]) {
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
  ob_init_memory_pool();
  BaseMain* pmain = NameServerMain::get_instance();
  if (pmain == NULL) {
    perror("not enought mem, exit \n");
  } else {
    pmain->start(argc, argv, "name_server");
    pmain->destroy();
  }
  return 0;
}

