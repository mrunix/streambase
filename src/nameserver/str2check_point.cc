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
    TBSYS_LOG(INFO, "usage %s str_file_name checkpoint_file_name\n", argv[0]);
    return 0;
  }
  ob_init_memory_pool();
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ObTabletInfoManager* rt_tim = new ObTabletInfoManager();
  NameTable* rt_table = new NameTable(rt_tim);
  if (rt_tim == NULL || rt_table == NULL) {
    TBSYS_LOG(INFO, "new object error\n");
    return 0;
  }
  FILE* stream = fopen(argv[1], "r");
  if (stream == NULL) {
    TBSYS_LOG(INFO, "open str file error %s\n", argv[1]);
    return 0;
  }
  rt_tim->read_from_hex(stream);
  rt_table->read_from_hex(stream);
  fclose(stream);

  if (OB_SUCCESS != rt_table->write_to_file(argv[2])) {
    TBSYS_LOG(INFO, "write file error %s\n", argv[2]);
    return 0;
  }
  return 0;

}

