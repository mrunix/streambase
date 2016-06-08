/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_tsi.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_MS_TSI_H_
#define OCEANBASE_MERGESERVER_OB_MS_TSI_H_
#include "common/ob_define.h"
#include <string.h>
namespace sb {
namespace mergeserver {
/// TSI ids
static const int32_t ORG_PARAM_ID = 1;
static const int32_t DECODED_PARAM_ID = 2;
static const int32_t RESULT_SCANNER_ID = 3;
static const int32_t SCHEMA_DECODER_ASSIS_ID = 4;
struct ObMSSchemaDecoderAssis {
  static const int32_t INVALID_IDX = -1;
  ObMSSchemaDecoderAssis() {
    init();
  }
  void init() {
    for (int32_t i = 0; i < sb::common::OB_MAX_COLUMN_NUMBER; i++) {
      column_idx_in_org_param_[i] = INVALID_IDX;
    }
  }
  int64_t column_idx_in_org_param_[sb::common::OB_MAX_COLUMN_NUMBER];
};
}
}


#endif /* OCEANBASE_MERGESERVER_OB_MS_TSI_H_ */


