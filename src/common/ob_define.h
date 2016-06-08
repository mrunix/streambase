/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_define.h for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_DEFINE_H_
#define OCEANBASE_COMMON_OB_DEFINE_H_

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

namespace sb {
namespace common {
const int OB_SUCCESS = EXIT_SUCCESS;
const int OB_ERROR = EXIT_FAILURE;

const uint64_t OB_INVALID_ID = UINT64_MAX;
const int OB_INVALID_INDEX = -1;

//error code for common -1 ---- -1000
const int OB_OBJ_TYPE_ERROR = -1;
const int OB_INVALID_ARGUMENT = -2;
const int OB_ARRAY_OUT_OF_RANGE = -3;
const int OB_SERVER_LISTEN_ERROR = -4;
const int OB_INIT_TWICE = -5;
const int OB_NOT_INIT = -6;
const int OB_NOT_SUPPORTED = -7;
const int OB_ITER_END = -8;
const int OB_IO_ERROR = -9;
const int OB_ERROR_FUNC_VERSION = -10;
const int OB_PACKET_NOT_SENT = -11;
const int OB_RESPONSE_TIME_OUT = -12;
const int OB_ALLOCATE_MEMORY_FAILED = -13;
const int OB_MEM_OVERFLOW = -14;
const int OB_ERR_SYS = -15;
const int OB_ERR_UNEXPECTED = -16;
const int OB_ENTRY_EXIST = -17;
const int OB_ENTRY_NOT_EXIST = -18;
const int OB_SIZE_OVERFLOW = -19;
const int OB_REF_NUM_NOT_ZERO = -20;
const int OB_CONFLICT_VALUE = -21;
const int OB_ITEM_NOT_SETTED = -22;
const int OB_EAGAIN = -23;
const int OB_BUF_NOT_ENOUGH = -24;
const int OB_PARTIAL_FAILED = -25;
const int OB_READ_NOTHING = -26;
const int OB_FILE_NOT_EXIST = -27;
const int OB_DISCONTINUOUS_LOG = -28;
const int OB_SCHEMA_ERROR = -29;
const int OB_DATA_NOT_SERVE = -30;       // not host this data
const int OB_UNKNOWN_OBJ = -31;
const int OB_NO_MONITOR_DATA = -32;
const int OB_SERIALIZE_ERROR = -33;
const int OB_DESERIALIZE_ERROR = -34;
const int OB_AIO_TIMEOUT = -35;
const int OB_NEED_RETRY = -36; // need retry
const int OB_TOO_MANY_SSTABLE = -37;
const int OB_NOT_MASTER = -38;
const int OB_NOT_REGISTERED = -39;

//error code for chunk server -1001 ---- -2000
const int OB_CS_CACHE_NOT_HIT = -1001;   // 缓存没有命中
const int OB_CS_TIMEOUT = -1002;         // 超时
const int OB_CS_TABLET_NOT_EXIST = -1008; // tablet not exist
const int OB_CS_EAGAIN = -1010;           //重试

const int OB_GET_NEXT_COLUMN = -1011;
const int OB_GET_NEXT_ROW = -1012; // for internal use, scan next row.
//const int OB_DESERIALIZE_ERROR = -1013;//反序列化失败
const int OB_INVALID_ROW_KEY = -1014;//不合法的rowKey
const int OB_SEARCH_MODE_NOT_IMPLEMENT = -1015; // search mode not implement, internal error
const int OB_INVALID_BLOCK_INDEX = -1016; // illegal block index data, internal error
const int OB_INVALID_BLOCK_DATA = -1017;  // illegal block data, internal error
const int OB_SEARCH_NOT_FOUND = -1018;    // value not found? for cs_get
const int OB_BEYOND_THE_RANGE = -1020;    // search key or range not in current tablet
const int OB_CS_COMPLETE_TRAVERSAL = -1021; //complete traverse block cache
const int OB_END_OF_ROW = -1022;
const int OB_CS_MERGE_ERROR = -1024;
const int OB_CS_SCHEMA_INCOMPATIBLE = -1025;
const int OB_CS_SERVICE_NOT_STARTED = -1026;
const int OB_CS_LEASE_EXPIRED = -1027;
const int OB_CS_MERGE_HAS_TIMEOUT = -1028;
const int OB_CS_TABLE_HAS_DELETED = -1029;
const int OB_CS_MERGE_CANCELED = -1030;
const int OB_CS_COMPRESS_LIB_ERROR = -1031;
const int OB_CS_OUTOF_DISK_SPACE = -1032;
const int OB_CS_MIGRATE_IN_EXIST = -1033;
const int OB_AIO_BUF_END_BLOCK = -1034;
const int OB_AIO_EOF = -1035;
const int OB_AIO_BUSY = -1036;
const int OB_WRONG_SSTABLE_DATA = -1037;
const int OB_COLUMN_GROUP_NOT_FOUND = -1039;

//error code for update server -2001 ---- -3000
const int OB_UPS_TRANS_RUNNING = -2001;     // 事务正在执行
const int OB_FREEZE_MEMTABLE_TWICE = -2002; // memtable has been frozen
const int OB_DROP_MEMTABLE_TWICE = -2003;   // memtable has been dropped
const int OB_INVALID_START_VERSION = -2004; // memtable start version invalid
const int OB_UPS_NOT_EXIST = -2005;         // not exist
const int OB_UPS_ACQUIRE_TABLE_FAIL = -2006;// acquire table via version fail
const int OB_UPS_INVALID_MAJOR_VERSION = -2007;
const int OB_UPS_TABLE_NOT_FROZEN = -2008;

//error code for root server -3001 ---- -4000
const int OB_ERROR_TIME_STAMP = -3001;
const int OB_ERROR_INTRESECT = -3002;
const int OB_ERROR_OUT_OF_RANGE = -3003;

//error code for merge server -4000 ---- -5000
const int OB_INNER_STAT_ERROR = -4001;     // inner stat check error
const int OB_OLD_SCHEMA_VERSION = -4002;   // find old schema version
const int OB_INPUT_PARAM_ERROR = -4003;    // check input param error
const int OB_NO_EMPTY_ENTRY = -4004;       // not find empty entry
const int OB_RELEASE_SCHEMA_ERROR = -4005; // release schema error
const int OB_ITEM_COUNT_ERROR = -4006;     // fullfill item count error
const int OB_OP_NOT_ALLOW = -4007;         // fetch new schema not allowed
const int OB_CHUNK_SERVER_ERROR = -4008;   // chunk server cached is error
const int OB_NO_NEW_SCHEMA = -4009;        // no new schema when parse error

typedef int64_t    ObDateTime;
typedef int64_t    ObPreciseDateTime;
typedef ObPreciseDateTime ObModifyTime;
typedef ObPreciseDateTime ObCreateTime;

const int64_t OB_OLD_MAX_COLUMN_NUMBER = 50;
const int64_t OB_MAX_SERVER_ADDR_SIZE = 128;
const int64_t OB_MAX_COLUMN_NUMBER = 128;
const int64_t OB_MAX_TABLE_NUMBER = 100;
const int64_t OB_MAX_JOIN_INFO_NUMBER = 10;
const int64_t OB_MAX_ROW_KEY_LENGTH = 16384; // 16KB
const int64_t OB_MAX_COLUMN_NAME_LENGTH = 128;  //this means we can use 127 chars for a name.
const int64_t OB_MAX_APP_NAME_LENGTH = 128;
const int64_t OB_MAX_COMPRESSOR_NAME_LENGTH = 128;
const int64_t OB_MAX_TABLE_NAME_LENGTH = 256;
const int64_t OB_MAX_FILE_NAME_LENGTH = 512;
const int64_t OB_MAX_PACKET_LENGTH = 1 << 21; // max packet length, 2MB
const int64_t OB_MAX_ROW_NUMBER_PER_QUERY = 65536;
const int64_t OB_MAX_BATCH_NUMBER = 100;
const int64_t OB_MAX_TABLET_LIST_NUMBER = 1 << 10;
const int64_t OB_MAX_DISK_NUMBER = 16;
const int64_t OB_IP_STR_BUFF = 30;
const int64_t OB_RANGE_STR_BUFSIZ = 128;
const int64_t OB_MAX_FETCH_CMD_LENGTH = 2048;
const int64_t OB_MAX_COLUMN_GROUP_NUMBER = 16;
const int64_t OB_MAX_THREAD_READ_SSTABLE_NUMBER = 16;
const int64_t OB_MAX_GET_ROW_NUMBER = 10240;
const uint64_t OB_FULL_ROW_COLUMN_ID = 0;
const uint64_t OB_DELETE_ROW_COLUMN_ID = 0;
const int64_t OB_DIRECT_IO_ALIGN = 512;

const int64_t OB_MAX_RESULT_MESSAGE_LENGTH = 1024;
const int64_t OB_MAX_LOG_BUFFER_SIZE = 1966080L;  // 1.875MB

const int32_t OB_SAFE_COPY_COUNT = 3;

// OceanBase Log Synchronization Type
const int64_t OB_LOG_NOSYNC = 0;
const int64_t OB_LOG_SYNC = 1;
const int64_t OB_LOG_DELAYED_SYNC = 2;

enum ObRole {
  OB_ROOTSERVER = 1,
  OB_CHUNKSERVER = 2,
  OB_MERGESERVER = 3,
  OB_UPDATESERVER = 4,
};
} // end namespace common
} // end namespace sb
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);               \
  void operator=(const TypeName&)

// 对于 serialize 函数 pos既是输入参数又是输出参数，
// serialize把序列化的数据从(buf+pos)处开始写入，
// 写入完成后更新pos。如果写入后的数据要超出(buf+buf_len)，
// serialize返回失败。
//
// 对于 deserialize 函数 pos既是输入参数又是输出参数，
// deserialize从(buf+pos)处开始地读出数据进行反序列化，
// 完成后更新pos。如果反序列化所需数据要超出(buf+data_len)，
// deserialize返回失败。

#define NEED_SERIALIZE_AND_DESERIALIZE \
int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
    int64_t get_serialize_size(void) const

#define INLINE_NEED_SERIALIZE_AND_DESERIALIZE \
inline int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  inline int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
    inline int64_t get_serialize_size(void) const

#define VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE \
virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
    virtual int64_t get_serialize_size(void) const

#define DEFINE_SERIALIZE(TypeName) \
  int TypeName::serialize(char* buf, const int64_t buf_len, int64_t& pos) const


#define DEFINE_DESERIALIZE(TypeName) \
  int TypeName::deserialize(const char* buf, const int64_t data_len, int64_t& pos)

#define DEFINE_GET_SERIALIZE_SIZE(TypeName) \
  int64_t TypeName::get_serialize_size(void) const
#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif

#define DATABUFFER_SERIALIZE_INFO \
  data_buffer_.get_data(), data_buffer_.get_capacity(), data_buffer_.get_position()

#define OB_LIKELY(x)       __builtin_expect(!!(x),1)
#define OB_UNLIKELY(x)     __builtin_expect(!!(x),0)

#define ARRAYSIZEOF(a) (sizeof(a)/sizeof(a[0]))

#define OB_ASSERT(x) assert(x)

#endif // OCEANBASE_COMMON_DEFINE_H_


