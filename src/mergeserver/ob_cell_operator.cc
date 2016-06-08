/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cell_operator.cc for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_cell_operator.h"
#include "common/ob_action_flag.h"
using namespace sb;
using namespace sb::common;
using namespace sb::mergeserver;

int sb::mergeserver::ob_cell_info_apply(ObCellInfo& dst, const ObCellInfo& src) {
  int err = 0;
  if (NULL == dst.row_key_.ptr()) {
    dst = src;
  } else {
    if (dst.table_id_ != src.table_id_
        || dst.table_name_ != src.table_name_
        || dst.row_key_ != src.row_key_
        || dst.column_id_ != src.column_id_
        || dst.column_name_ != src.column_name_) {
      TBSYS_LOG(WARN, "%s", "dst and src not coincident");
      err = OB_INVALID_ARGUMENT;
    } else {
      /// err = ob_obj_apply(dst.value_,src.value_);
      err = dst.value_.apply(src.value_);
    }
  }
  return err;
}


