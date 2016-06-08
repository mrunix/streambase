/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cell_operator.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_CELL_OPERATOR_H_
#define OCEANBASE_MERGESERVER_OB_CELL_OPERATOR_H_
#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "common/ob_string.h"
namespace sb {
namespace mergeserver {
/// @fn apply mutation in src to dst
int ob_cell_info_apply(sb::common::ObCellInfo& dst,
                       const sb::common::ObCellInfo& src);
}
}




#endif /* OCEANBASE_MERGESERVER_OB_CELL_OPERATOR_H_ */


