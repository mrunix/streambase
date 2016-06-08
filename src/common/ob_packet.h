/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_packet.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef OCEANBASE_PACKET_H_
#define OCEANBASE_PACKET_H_

#include <tbnet.h>

#include "ob_record_header.h" // for ObRecordHeader
#include "data_buffer.h"
//#include "ob_malloc.h"
#include "ob_memory_pool.h"
#include "thread_buffer.h"

namespace sb {
namespace common {
enum PacketCode {
  OB_GET_REQUEST = 101,
  OB_GET_RESPONSE = 102,
  OB_BEGIN_MERGE = 103,
  OB_LOAD_NEW_TABLETS = 104,
  OB_DROP_MEM_TABLE = 107,
  OB_PREPARE_SCHEMA = 108,
  OB_HEARTBEAT = 110,
  OB_TRANSFER_TABLETS = 113,
  OB_RESULT = 114,
  OB_BATCH_GET_REQUEST = 115,
  OB_BATCH_GET_RESPONSE = 116,
  OB_HEARTBEAT_RESPONSE = 119,

  OB_SCAN_REQUEST = 122,
  OB_SCAN_RESPONSE = 123,
  OB_CREATE_MEMTABLE_INDEX = 124,
  OB_PING_REQUEST = 151,
  OB_PING_RESPONSE = 152,
  OB_SET_SYNC_LIMIT_REQUEST = 153,
  OB_SET_SYNC_LIMIT_RESPONSE = 154,
  OB_RENEW_LEASE_REQUEST = 155,
  OB_RENEW_LEASE_RESPONSE = 156,
  OB_GRANT_LEASE_REQUEST = 157,
  OB_GRANT_LEASE_RESPONSE = 158,
  OB_CLEAR_REQUEST = 161,
  OB_CLEAR_RESPONSE = 162,

  OB_GET_OBI_ROLE = 163,
  OB_GET_OBI_ROLE_RESPONSE = 164,
  OB_SET_OBI_ROLE = 165,
  OB_SET_OBI_ROLE_RESPONSE = 166,
  OB_RS_ADMIN = 167,
  OB_RS_ADMIN_RESPONSE = 168,
  OB_CHANGE_LOG_LEVEL = 169,
  OB_CHANGE_LOG_LEVLE_RESPONSE = 170,
  OB_RS_STAT = 171,
  OB_RS_STAT_RESPONSE = 172,
  OB_GET_OBI_CONFIG = 173,
  OB_GET_OBI_CONFIG_RESPONSE = 174,
  OB_SET_OBI_CONFIG = 175,
  OB_SET_OBI_CONFIG_RESPONSE = 176,
  OB_SET_UPS_CONFIG = 177,
  OB_SET_UPS_CONFIG_RESPONSE = 178,
  OB_GET_CS_LIST = 179,
  OB_GET_CS_LIST_RESPONSE = 180,
  OB_GET_MS_LIST = 181,
  OB_GET_MS_LIST_RESPONSE = 182,
  OB_GET_CLOG_CURSOR = 183,
  OB_GET_CLOG_CURSOR_RESPONSE = 184,
  OB_GET_CLOG_MASTER = 185,
  OB_GET_CLOG_MASTER_RESPONSE = 186,

  OB_START_MERGE = 201,
  OB_START_MERGE_RESPONSE = 202,
  OB_DROP_OLD_TABLETS = 203,
  OB_DROP_OLD_TABLETS_RESPONSE = 204,
  OB_FETCH_SCHEMA = 205,
  OB_FETCH_SCHEMA_RESPONSE = 206,
  OB_REPORT_TABLETS = 207,
  OB_REPORT_TABLETS_RESPONSE = 208,
  OB_WAITING_JOB_DONE = 209,
  OB_WAITING_JOB_DONE_RESPONSE = 210,
  OB_GET_UPDATE_SERVER_INFO = 211,
  OB_GET_UPDATE_SERVER_INFO_RES = 212,
  OB_SERVER_REGISTER = 213,
  OB_SERVER_REGISTER_RESPONSE = 214,
  OB_MIGRATE_OVER = 215,
  OB_MIGRATE_OVER_RESPONSE = 216,
  OB_CS_MIGRATE = 217,
  OB_CS_MIGRATE_RESPONSE = 218,
  OB_CS_CREATE_TABLE = 219,
  OB_CS_CREATE_TABLE_RESPONSE = 220,
  OB_REPORT_CAPACITY_INFO = 221,
  OB_REPORT_CAPACITY_INFO_RESPONSE = 222,
  OB_FETCH_SCHEMA_VERSION = 223,
  OB_FETCH_SCHEMA_VERSION_RESPONSE = 224,
  OB_FREEZE_MEM_TABLE = 225,
  OB_FREEZE_MEM_TABLE_RESPONSE = 226,
  OB_REQUIRE_HEARTBEAT = 227,
  OB_DUMP_CS_INFO = 229,
  OB_DUMP_CS_INFO_RESPONSE = 230,
  OB_CS_GET_MIGRATE_DEST_LOC = 260,
  OB_CS_GET_MIGRATE_DEST_LOC_RESPONSE = 261,
  OB_CS_DUMP_TABLET_IMAGE = 262,
  OB_CS_DUMP_TABLET_IMAGE_RESPONSE = 263,
  OB_CS_START_GC = 264,
  OB_SWITCH_SCHEMA = 265,
  OB_SWITCH_SCHEMA_RESPONSE = 266,
  OB_UPDATE_SERVER_REPORT_FREEZE = 267,
  OB_GET_UPDATE_SERVER_INFO_FOR_MERGE = 268,
  OB_GET_MERGE_DELAY_INTERVAL = 269,
  OB_GET_MERGE_DELAY_INTERVAL_RES = 270,
  OB_GET_UPS = 271,
  OB_GET_UPS_RESPONSE = 272, // @see ObUpsList in ob_ups_info.h
  OB_GET_CLIENT_CONFIG = 273,
  OB_GET_CLIENT_CONFIG_RESPONSE = 274, // @see ObClientConfig is ob_client_config.h

  OB_WRITE = 301,
  OB_WRITE_RES = 302,
  OB_SEND_LOG = 303,
  OB_SEND_LOG_RES = 304,
  OB_SYNC_SCHEMA = 305,
  OB_SYNC_SCHEMA_RES = 306,
  OB_SLAVE_REG = 307,
  OB_SLAVE_REG_RES = 308,
  OB_GET_MASTER = 309,
  OB_GET_MASTER_RES = 310,
  OB_SLAVE_QUIT = 311,
  OB_SLAVE_QUIT_RES = 312,
  OB_LSYNC_FETCH_LOG = 313,
  OB_LSYNC_FETCH_LOG_RES = 314,

  OB_FETCH_STATS = 401,
  OB_FETCH_STATS_RESPONSE = 402,

  OB_UPS_DUMP_TEXT_MEMTABLE = 1227,
  OB_UPS_DUMP_TEXT_MEMTABLE_RESPONSE = 1228,
  OB_UPS_DUMP_TEXT_SCHEMAS = 1229,
  OB_UPS_DUMP_TEXT_SCHEMAS_RESPONSE = 1230,
  OB_UPS_FORCE_FETCH_SCHEMA = 1231,
  OB_UPS_FORCE_FETCH_SCHEMA_RESPONSE = 1232,
  OB_UPS_RELOAD_CONF = 1233,
  OB_UPS_RELOAD_CONF_RESPONSE = 1234,
  OB_UPS_MEMORY_WATCH = 1235,
  OB_UPS_MEMORY_WATCH_RESPONSE = 1236,
  OB_UPS_MEMORY_LIMIT_SET = 1237,
  OB_UPS_MEMORY_LIMIT_SET_RESPONSE = 1238,
  OB_UPS_CLEAR_ACTIVE_MEMTABLE = 1239,
  OB_UPS_CLEAR_ACTIVE_MEMTABLE_RESPONSE = 1240,
  OB_UPS_GET_LAST_FROZEN_VERSION = 1241,
  OB_UPS_GET_LAST_FROZEN_VERSION_RESPONSE = 1242,
  OB_UPS_CHANGE_VIP_REQUEST = 1243,
  OB_UPS_CHANGE_VIP_RESPONSE = 1244,
  OB_UPS_GET_BLOOM_FILTER = 1245,
  OB_UPS_GET_BLOOM_FILTER_RESPONSE = 1246,
  OB_UPS_PRIV_QUEUE_CONF_SET = 1247,
  OB_UPS_PRIV_QUEUE_CONF_SET_RESPONSE = 1248,
  OB_UPS_STORE_MEM_TABLE = 1249,
  OB_UPS_STORE_MEM_TABLE_RESPONSE = 1250,
  OB_UPS_DROP_MEM_TABLE = 1251,
  OB_UPS_DROP_MEM_TABLE_RESPONSE = 1252,
  OB_UPS_ERASE_SSTABLE = 1253,
  OB_UPS_ERASE_SSTABLE_RESPONSE = 1254,
  OB_UPS_ASYNC_HANDLE_FROZEN = 1255,
  OB_UPS_ASYNC_REPORT_FREEZE = 1256,
  OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE = 1257,
  OB_UPS_MINOR_FREEZE_MEMTABLE = 1258,
  OB_UPS_MINOR_FREEZE_MEMTABLE_RESPONSE = 1259,
  OB_UPS_LOAD_NEW_STORE = 1260,
  OB_UPS_LOAD_NEW_STORE_RESPONSE = 1261,
  OB_UPS_RELOAD_ALL_STORE = 1262,
  OB_UPS_RELOAD_ALL_STORE_RESPONSE = 1263,
  OB_UPS_RELOAD_STORE = 1264,
  OB_UPS_RELOAD_STORE_RESPONSE = 1265,
  OB_UPS_UMOUNT_STORE = 1266,
  OB_UPS_UMOUNT_STORE_RESPONSE = 1267,
  OB_UPS_FORCE_REPORT_FROZEN_VERSION = 1268,
  OB_UPS_FORCE_REPORT_FROZEN_VERSION_RESPONSE = 1269,
  OB_UPS_SWITCH_COMMIT_LOG = 1270,
  OB_UPS_SWITCH_COMMIT_LOG_RESPONSE = 1271,
  OB_UPS_GET_TABLE_TIME_STAMP = 1272,
  OB_UPS_GET_TABLE_TIME_STAMP_RESPONSE = 1273,
  OB_UPS_ENABLE_MEMTABLE_CHECKSUM = 1274,
  OB_UPS_ENABLE_MEMTABLE_CHECKSUM_RESPONSE = 1275,
  OB_UPS_DISABLE_MEMTABLE_CHECKSUM = 1276,
  OB_UPS_DISABLE_MEMTABLE_CHECKSUM_RESPONSE = 1277,
  OB_UPS_ASYNC_FORCE_DROP_MEMTABLE = 1278,
  OB_UPS_DELAY_DROP_MEMTABLE = 1279,
  OB_UPS_DELAY_DROP_MEMTABLE_RESPONSE = 1280,
  OB_UPS_IMMEDIATELY_DROP_MEMTABLE = 1281,
  OB_UPS_IMMEDIATELY_DROP_MEMTABLE_RESPONSE = 1282,
};

enum ServerFlag {
  OB_CLIENT_FLAG = 1,
  OB_CHUNK_SERVER_FLAG = 2,
  OB_UPDATE_SERVER_FLAG = 3,
  OB_ROOT_SERVER_FLAG = 4,
  OB_SELF_FLAG = 5,
};

enum PacketPriority {
  NORMAL_PRI = 0,
  LOW_PRI = 1,
};

class ObPacket : public tbnet::Packet {
  friend class ObPacketQueue;
 public:
  static const int16_t OB_PACKET_CHECKSUM_MAGIC = 0xBCDE;
  ObPacket();
  virtual ~ObPacket();

  int32_t get_packet_code();
  void set_packet_code(const int32_t packet_code);

  int32_t get_source_id() const;
  void set_source_id(const int32_t source_id);

  int32_t get_target_id() const;
  void set_target_id(const int32_t target_id);

  int32_t get_api_version() const;
  void set_api_version(const int32_t api_version);

  void set_data(const ObDataBuffer& buffer);
  ObDataBuffer* get_buffer();
  int32_t get_data_length() const;

  void set_source_timeout(const int64_t& timeout);
  int64_t get_source_timeout() const;
  void set_receive_ts(const int64_t receive_ts);
  int64_t get_receive_ts() const;
  void set_packet_priority(const int32_t priority);
  int32_t get_packet_priority() const;

  tbnet::Connection* get_connection() const;
  void set_connection(tbnet::Connection* connection);

  int serialize();
  int deserialize();

  bool encode(tbnet::DataBuffer* output);
  bool decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header);

  int64_t get_packet_buffer_offset() const;
  void set_packet_buffer_offset(const int64_t buffer_offset);

  ObDataBuffer* get_packet_buffer();
  void set_packet_buffer(char* buffer, const int64_t buffer_length);

 private:
  int do_check_sum();
  int do_sum_check();

  int __deserialize();

 private:
  int32_t api_version_;
  int32_t source_id_;
  int32_t target_id_;
  int32_t timeout_;
  int32_t data_length_;
  int64_t receive_ts_;
  int32_t priority_;
  int64_t buffer_offset_;
  ObDataBuffer buffer_; // user buffer holder
  ObDataBuffer inner_buffer_; // packet inner buffer
  ObRecordHeader header_;
  tbnet::Connection* connection_;

  bool alloc_inner_mem_; // alloc from out_mem_pool_ or not
  static ObVarMemPool out_mem_pool_;
};
} /* common */
} /* oceanbas */

#endif /* end of include guard: OCEANBASE_PACKET_H_ */

