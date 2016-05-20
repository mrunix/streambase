/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_inc_seq.cc
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_inc_seq.h"

using namespace sb;
using namespace updateserver;

const int64_t RWLock::N_THREAD;
const uint64_t SeqLock::N_THREAD;
const uint64_t SeqLock::N_THREAD_MASK;
const int64_t RefCnt::N_THREAD;

