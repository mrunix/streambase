/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include <tbsys.h>

#include "common/ob_schema.h"
using namespace sb::common;
int main(int argc, char* argv[]) {
  if (argc != 2) {
    printf("usage %s schema_file\n", argv[0]);
    return 0;
  }
  tbsys::CConfig c1;
  ObSchemaManager mm(1);
  if (mm.parse_from_file(argv[1], c1)) {
    mm.print_info();
  } else {
    printf("parse file %s error\n", argv[1]);
  }
  return 0;

}

