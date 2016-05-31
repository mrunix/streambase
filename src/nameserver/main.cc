/*
 * src/nameserver/main.cc
 *
 * Copyright (C) 2016 Michael. All rights reserved.
 */

#include <malloc.h>
#include "tbsys.h"
#include "common/ob_malloc.h"
#include "ob_root_main.h"

using namespace sb::nameserver;
using namespace sb::common;

int main(int argc, char* argv[]) {
  static const int DEFAULT_MMAP_MAX_VAL = 1024 * 1024 * 1024;
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
  ob_init_memory_pool();
  tbsys::WarningBuffer::set_warn_log_on(true);
  BaseMain* pmain = sb::nameserver::NameServerMain::get_instance();
  if (pmain == NULL) {
    perror("not enought mem, exit \n");
  } else {
    pmain->start(argc, argv);
    pmain->destroy();
  }
  return 0;
}
