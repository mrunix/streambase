/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cell_stream.cc for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_cell_stream.h"
using namespace sb;
using namespace sb::common;
using namespace sb::mergeserver;


sb::mergeserver::ObMSGetCellArray::ObMSGetCellArray()
  : cell_array_(NULL), get_param_(NULL) {
}

sb::mergeserver::ObMSGetCellArray::ObMSGetCellArray(ObCellArray& cell_array)
  : cell_array_(&cell_array), get_param_(NULL) {
}

sb::mergeserver::ObMSGetCellArray::ObMSGetCellArray(const ObGetParam& get_param)
  : cell_array_(NULL), get_param_(&get_param) {
}


int64_t sb::mergeserver::ObMSGetCellArray::get_cell_size()const {
  int64_t result = 0;
  if (NULL != get_param_) {
    result = get_param_->get_cell_size();
  } else if (NULL != cell_array_) {
    result = cell_array_->get_cell_size();
  } else {
    result = 0;
  }
  return result;
}

const ObCellInfo& sb::mergeserver::ObMSGetCellArray::operator [](int64_t offset)const {
  const ObCellInfo* result = NULL;
  if (NULL != get_param_) {
    if (offset < get_param_->get_cell_size()) {
      result = get_param_->operator [](offset);
    } else {
      TBSYS_LOG(ERROR, "logic error, offset out of range [offset:%ld,cell_num:%ld]",
                offset, get_param_->get_cell_size());
      result = &default_cell_;
    }
  } else if (NULL != cell_array_) {
    result = &(cell_array_->operator [](offset));
  } else {
    TBSYS_LOG(ERROR, "logic error, offset out of range [offset:%ld,cell_num:%d]", offset, 0);
    result = &default_cell_;
  }
  return *result;
}


