/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * name_server_ups_provider.cc
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "name_server_ups_provider.h"
using namespace sb::nameserver;
using namespace sb::common;

NameServerUpsProvider::NameServerUpsProvider(const common::ObServer& ups)
  : ups_(ups) {
}

NameServerUpsProvider::~NameServerUpsProvider() {
}

int NameServerUpsProvider::get_ups(ObServer& ups) {
  int ret = OB_SUCCESS;
  ups = ups_;
  return ret;
}


