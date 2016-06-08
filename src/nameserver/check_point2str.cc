/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include <tbsys.h>

#include "common/ob_schema.h"
#include "common/ob_crc64.h"
#include "nameserver/nameserver_table.h"
using namespace sb::common;
using namespace sb::nameserver;
int main(int argc, char* argv[]) {
  if (argc != 3) {
    printf("usage %s in_file_name out_file_name\n", argv[0]);
    return 0;
  }
  ob_init_memory_pool();
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ObTabletInfoManager* rt_tim = new ObTabletInfoManager();
  NameTable* rt_table = new NameTable(rt_tim);
  if (rt_tim == NULL || rt_table == NULL) {
    printf("new object error\n");
    return 0;
  }
  if (OB_SUCCESS != rt_table->read_from_file(argv[1])) {
    printf("read file error %s\n", argv[1]);
    return 0;
  }
  FILE* stream = fopen(argv[2], "w+");
  if (stream == NULL) {
    printf("open file error %s\n", argv[2]);
    return 0;
  }
  rt_tim->dump_as_hex(stream);
  rt_table->dump_as_hex(stream);
  fclose(stream);
  return 0;

}

