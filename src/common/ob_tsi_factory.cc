/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tsi_factory.cc for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "ob_tsi_factory.h"

namespace sb {
namespace common {
TSIFactory& get_tsi_fatcory() {
  static TSIFactory instance;
  return instance;
}

void tsi_factory_init() {
  get_tsi_fatcory().init();
}

void tsi_factory_destroy() {
  get_tsi_fatcory().destroy();
}

//void  __attribute__((constructor)) init_global_tsi_factory()
//{
//  get_tsi_fatcory().init();
//}

//void  __attribute__((destructor)) destroy_global_tsi_factory()
//{
//  get_tsi_fatcory().destroy();
//}
}
}


