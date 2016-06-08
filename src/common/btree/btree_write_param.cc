/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ./btree_write_param.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include "btree_read_param.h"
#include "btree_write_param.h"

namespace sb {
namespace common {
/**
 * BTree写过程的辅助结构
 */

BtreeWriteParam::BtreeWriteParam() : BtreeReadParam() {
  tree_id = 0;
  new_root_node_ = NULL;
  memset(next_node_, 0, CONST_MAX_DEPTH * sizeof(BtreeNode*));
}

/**
 * 析造
 */
BtreeWriteParam::~BtreeWriteParam() {
}

} // end namespace common
} // end namespace sb

