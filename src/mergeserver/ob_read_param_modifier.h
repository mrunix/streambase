/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_read_param_modifier.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_MCERGESERVER_OB_READ_PARAM_MODIFIER_H_
#define OCEANBASE_MCERGESERVER_OB_READ_PARAM_MODIFIER_H_
#include "ob_cell_stream.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
namespace sb {
namespace mergeserver {
// check finish
bool is_finish_scan(const sb::common::ObScanParam& param,
                    const sb::common::ObRange& result_range);

/// @fn get next get param
/// @return if all cell has gotten, return OB_ITER_END
int get_next_param(const sb::common::ObReadParam& org_read_param,
                   const ObMSGetCellArray& org_get_cells,
                   const int64_t got_cell_num,
                   sb::common::ObGetParam* get_param);

/// @fn get next scan param
/// @return if all cell has gotten, return OB_ITER_END
int get_next_param(const sb::common::ObScanParam& org_scan_param,
                   const sb::common::ObScanner&  prev_scan_result,
                   sb::common::ObScanParam* scan_param,
                   sb::common::ObMemBuffer& range_buffer);

/// @fn adjust table version according to result from chunkserver
int get_ups_param(sb::common::ObScanParam& param,
                  const sb::common::ObScanner& cs_result,
                  sb::common::ObMemBuffer& range_buffer);

/// @fn adjust table version according to result from chunkserver
int get_ups_param(sb::common::ObGetParam& param,
                  const sb::common::ObScanner& cs_result);
}
}
#endif /* OCENBASE_MERGESERVER_OB_READ_PARAM_MODIFIER_H_ */


