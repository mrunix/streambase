/**
 * (C) 2007-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_generator.h for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#ifndef OCEANBASE_TEST_OB_GENERATOR_H_
#define OCEANBASE_TEST_OB_GENERATOR_H_

#include "ob_test_bomb.h"

namespace sb {
namespace test {
class ObGenerator {
 public:
  ObGenerator() {}
  virtual ~ObGenerator() {}
  virtual int gen(ObTestBomb& bomb) = 0;
};
} // end namespace test
} // end namespace sb

#endif // OCEANBASE_TEST_OB_GENERATOR_H_

