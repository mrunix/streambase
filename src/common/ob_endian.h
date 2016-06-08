/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_endian.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_ENDIAN_H
#define OCEANBASE_COMMON_ENDIAN_H

#include <endian.h>
#if (__BYTE_ORDER == __LITTLE_ENDIAN)
#define OB_LITTLE_ENDIAN
#elif (__BYTE_ORDER == __BIG_ENDIAN)
#define OB_BIG_ENDIAN
#endif

#endif

