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


#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_SIGNAL_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_SIGNAL_H_
namespace {
const int LOG_MOVE_TO_DEBUG = 41;
const int LOG_MOVE_TO_ERROR = 42;
const int START_REPORT_SIG = 49;
const int START_MERGE_SIG = 50;
const int DUMP_ROOT_TABLE_TO_LOG = 51;
const int DUMP_AVAILABLE_SEVER_TO_LOG = 52;
const int SWITCH_SCHEMA = 53;
const int RELOAD_CONFIG = 54;
const int DO_CHECK_POINT = 55;
const int DROP_CURRENT_MERGE = 56;
}
#endif

