/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_read_param_decoder.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_SCAN_PARAM_DECODER_H_
#define MERGESERVER_OB_SCAN_PARAM_DECODER_H_
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_schema.h"
namespace sb {
namespace mergeserver {
/// change all names (column name, table name) into ids or idx
int ob_decode_scan_param(sb::common::ObScanParam& org_param,
                         const sb::common::ObSchemaManagerV2& schema_mgr,
                         sb::common::ObScanParam& decoded_param);
int ob_decode_get_param(const sb::common::ObGetParam& org_param,
                        const sb::common::ObSchemaManagerV2& schema_mgr,
                        sb::common::ObGetParam& decoded_param);
}
}

#endif /* MERGESERVER_OB_SCAN_PARAM_DECODER_H_ */


