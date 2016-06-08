/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ./btree_read_param.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include "btree_node.h"
#include "btree_read_param.h"

namespace sb {
namespace common {
/**
 * BTree搜索过程的辅助结构
 */

BtreeReadParam::BtreeReadParam() {
  node_length_ = 0;
  found_ = -1;
}

/**
 * 析造
 */
BtreeReadParam::~BtreeReadParam() {
}

#ifdef OCEAN_BASE_BTREE_DEBUG
/**
 * 打印出来.
 */
void BtreeReadParam::print() {
  fprintf(stderr, "node_length: %d, found: %ld\n", node_length_, found_);
  for (int i = 0; i < node_length_; i++) {
    fprintf(stderr, " addr: %p pos: %d\n", node_[i], node_pos_[i]);
  }
}
#endif

} // end namespace common
} // end namespace sb

