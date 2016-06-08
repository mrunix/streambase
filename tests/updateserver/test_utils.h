/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_utils.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "tbsys.h"
#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_read_common_data.h"
#include "common/page_arena.h"
#include "common/ob_action_flag.h"
#include "common/ob_mutator.h"
#include "updateserver/ob_memtable.h"

using namespace sb;
using namespace common;
using namespace common::hash;
using namespace updateserver;

extern const char* CELL_INFOS_INI;
extern const char* CELL_INFOS_SECTION;
extern const char* SCAN_PARAM_INI;
extern const char* SCAN_PARAM_SECTION;
extern const char* GET_PARAM_INI;
extern const char* GET_PARAM_SECTION;

extern void read_get_param(const char* fname, const char* section, PageArena<char>& allocer, ObGetParam& get_param);

extern void read_scan_param(const char* fname, const char* section, PageArena<char>& allocer, ObScanParam& scan_param);

extern void read_cell_infos(const char* fname, const char* section, PageArena<char>& allocer, ObMutator& mutator, ObMutator& result);

extern void print_cellinfo(const ObCellInfo* ci, const char* ext_info = NULL);

extern bool operator == (const ObCellInfo& a, const ObCellInfo& b);

extern bool operator == (const UpsCellInfo& a, const UpsCellInfo& b);

extern void print_all(PageArena<char>& allocer, MemTable& mt);

extern const char* print_obj(const ObObj& obj);

extern ObVersionRange str2range(const char* str);



