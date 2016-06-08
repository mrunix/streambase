/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_iterator.h for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_ITERATOR_H_
#define __OCEANBASE_CHUNKSERVER_OB_ITERATOR_H_

#include "common/ob_common_param.h"

namespace sb {
namespace common {
// interface of iterator
class ObIterator {
 public:
  ObIterator() {
    // empty
  }
  virtual ~ObIterator() {
    // empty
  }
 public:
  // Moves the cursor to next cell.
  // @return OB_SUCCESS if sucess, OB_ITER_END if iter ends, or other error code
  virtual int next_cell() = 0;
  // Gets the current cell.
  virtual int get_cell(sb::common::ObCellInfo** cell) = 0;
  virtual int get_cell(sb::common::ObCellInfo** cell, bool* is_row_changed) {
    if (NULL != is_row_changed) {
      *is_row_changed = false;
    }
    return get_cell(cell);
  }
};
}
}

#endif //__OB_ITERATOR_H__


