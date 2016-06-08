/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_action_flag.h for ...
 *
 * Authors:
 *   maoqi <maoqi@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_ACTION_FLAG_H__
#define OCEANBASE_COMMON_ACTION_FLAG_H__

#include <stdint.h>

namespace sb {
namespace common {
class ObActionFlag {
 public:
  static const int64_t OP_USE_OB_SEM            = 1;
  static const int64_t OP_USE_DB_SEM            = 2;
  static const int64_t OP_MH_FROZEN             = 3;
  static const int64_t OP_MH_ACTIVE             = 4;
  static const int64_t OP_READ                  = 5;
  static const int64_t OP_UPDATE                = 6;
  static const int64_t OP_INSERT                = 7;
  static const int64_t OP_DEL_ROW               = 8;
  static const int64_t OP_RT_TABLE_TYPE         = 9;
  static const int64_t OP_RT_TABLE_INDEX_TYPE   = 10;
  static const int64_t OP_ROW_DOES_NOT_EXIST    = 11;
  static const int64_t OP_END_FLAG              = 12;
  static const int64_t OP_SYS_DATE              = 13;
  static const int64_t OP_DEL_TABLE             = 14;
  static const int64_t OP_NOP                   = 15;
  static const int64_t OP_ROW_EXIST             = 16;

  // serialize ext obj type
  static const int64_t BASIC_PARAM_FIELD        = 50;
  static const int64_t END_PARAM_FIELD          = 51;
  static const int64_t TABLE_PARAM_FIELD        = 52;
  static const int64_t ROW_KEY_FIELD            = 53;
  static const int64_t TABLE_NAME_FIELD         = 54;
  static const int64_t COLUMN_PARAM_FIELD       = 55;
  static const int64_t SORT_PARAM_FIELD         = 56;
  static const int64_t LIMIT_PARAM_FIELD        = 57;
  static const int64_t FILTER_PARAM_FIELD       = 58;
  static const int64_t MUTATOR_PARAM_FIELD      = 59;
  static const int64_t TABLET_RANGE_FIELD       = 60;
  static const int64_t OBDB_SEMANTIC_FIELD      = 61;
  static const int64_t GROUPBY_PARAM_FIELD      = 62;
  static const int64_t GROUPBY_GRO_COLUMN_FIELD = 63;
  static const int64_t GROUPBY_RET_COLUMN_FIELD = 64;
  static const int64_t GROUPBY_AGG_COLUMN_FIELD = 65;
  static const int64_t UPDATE_COND_PARAM_FIELD  = 66;
  static const int64_t UPDATE_COND_FIELD        = 67;
  static const int64_t RESERVE_PARAM_FIELD      = 68;
};
} /* common */
} /* sb */

#endif

