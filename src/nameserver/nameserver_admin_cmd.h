/*
 * src/nameserver/.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for .
 *
 * Library: nameserver
 * Package: nameserver
 * Module :
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef _OB_ROOT_ADMIN_CMD_H
#define _OB_ROOT_ADMIN_CMD_H 1

namespace sb {
namespace nameserver {
static const int OB_RS_ADMIN_CHECKPOINT = 1;
static const int OB_RS_ADMIN_RELOAD_CONFIG = 2;
static const int OB_RS_ADMIN_SWITCH_SCHEMA = 3;
static const int OB_RS_ADMIN_DUMP_ROOT_TABLE = 4;
static const int OB_RS_ADMIN_DUMP_SERVER_INFO = 5;
static const int OB_RS_ADMIN_INC_LOG_LEVEL = 6;
static const int OB_RS_ADMIN_DEC_LOG_LEVEL = 7;
static const int OB_RS_ADMIN_DUMP_UNUSUAL_TABLETS = 8;
static const int OB_RS_ADMIN_DUMP_MIGRATE_INFO = 9;
} // end namespace nameserver
} // end namespace sb

#endif /* _OB_ROOT_ADMIN_CMD_H */

