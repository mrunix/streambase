/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * name_server_ups_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_UPS_PROVIDER_H
#define _OB_ROOT_UPS_PROVIDER_H 1

#include "common/roottable/ob_ups_provider.h"
#include "common/ob_server.h"
namespace sb {
namespace nameserver {
class NameServerUpsProvider: public common::ObUpsProvider {
 public:
  NameServerUpsProvider(const common::ObServer& ups);
  virtual ~NameServerUpsProvider();
  virtual int get_ups(common::ObServer& ups);
 private:
  // disallow copy
  NameServerUpsProvider(const NameServerUpsProvider& other);
  NameServerUpsProvider& operator=(const NameServerUpsProvider& other);
 private:
  // data members
  common::ObServer ups_;
};
} // end namespace nameserver
} // end namespace sb

#endif /* _OB_ROOT_UPS_PROVIDER_H */

