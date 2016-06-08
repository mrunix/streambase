/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_crc64.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_COMMON_CRC64_H_
#define  OCEANBASE_COMMON_CRC64_H_

#include <stdint.h>

#define OB_DEFAULT_CRC64_POLYNOM 0xD800000000000000ULL

namespace sb {
namespace common {
/**
  * create the crc64_table and optimized_crc64_table for calculate
  * must be called before ob_crc64
  * if not, the return value of ob_crc64 will be undefined
  * @param crc64 polynom
  */
void ob_init_crc64_table(const uint64_t polynom);

/**
  * Processes a multiblock of a CRC64 calculation.
  *
  * @returns Intermediate CRC64 value.
  * @param   uCRC64  Current CRC64 intermediate value.
  * @param   pv      The data block to process.
  * @param   cb      The size of the data block in bytes.
  */
uint64_t ob_crc64(uint64_t uCRC64, const void* pv, int64_t cb) ;

/**
  * Calculate CRC64 for a memory block.
  *
  * @returns CRC64 for the memory block.
  * @param   pv      Pointer to the memory block.
  * @param   cb      Size of the memory block in bytes.
  */
uint64_t ob_crc64(const void* pv, int64_t cb);

/**
  * Get the static CRC64 table. This function is only used for testing purpose.
  *
  */
const uint64_t* ob_get_crc64_table();
}
}

#endif

