/*
 *   (C) 2010-2012 Taobao Inc.
 *
 *   Version: 0.1 $date
 *
 *   Authors:
 *
 */

#ifndef OCEANBASE_TOOLS_CLIENT_RPC_H_
#define OCEANBASE_TOOLS_CLIENT_RPC_H_

#include "common/ob_schema.h"
#include "common/ob_server.h"
#include "common/ob_tablet_info.h"
#include "common/thread_buffer.h"
#include "common/data_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/thread_buffer.h"
#include "common/ob_scanner.h"
#include "common/ob_statistics.h"
#include "nameserver/ob_chunk_server_manager.h"


class ObClientRpcStub {
 public:
  static const int64_t FRAME_BUFFER_SIZE = 2 * 1024 * 1024L;
  ObClientRpcStub();
  virtual ~ObClientRpcStub();

 public:
  // warning: rpc_buff should be only used by rpc stub for reset
  int initialize(const sb::common::ObServer& remote_server,
                 const sb::common::ObClientManager* rpc_frame);

  int cs_scan(const sb::common::ObScanParam& scan_param,
              sb::common::ObScanner& scanner);

  int cs_get(const sb::common::ObGetParam& get_param,
             sb::common::ObScanner& scanner);

  int rs_scan(const sb::common::ObServer& server, const int64_t timeout,
              const sb::common::ObScanParam& param,
              sb::common::ObScanner& result);

  int get_tablet_info(const uint64_t table_id, const char* table_name,
                      const sb::common::ObRange& range,
                      sb::common::ObTabletLocation location [], int32_t& size);

  int cs_dump_tablet_image(const int32_t index, const int32_t disk_no, sb::common::ObString& image_buf);

  int rs_dump_cs_info(sb::nameserver::ObChunkServerManager& obcsm);

  int fetch_stats(sb::common::ObStatManager& obsm);

  int get_update_server(sb::common::ObServer& update_server);

  int start_merge(const int64_t frozen_memtable_version, const int32_t init_flag);
  int drop_tablets(const int64_t frozen_memtable_version);
  int start_gc(const int32_t reserve);

  const sb::common::ObServer&
  get_remote_server() const { return remote_server_; }


 private:
  static const int32_t DEFAULT_VERSION = 1;

  // check inner stat
  inline bool check_inner_stat(void) const;

  int get_frame_buffer(sb::common::ObDataBuffer& data_buffer) const;


 private:
  bool init_;                                             // init stat for inner check
  sb::common::ObServer remote_server_;               // root server addr
  sb::common::ThreadSpecificBuffer frame_buffer_;
  const sb::common::ObClientManager* rpc_frame_;   // rpc frame for send request
};

// check inner stat
inline bool ObClientRpcStub::check_inner_stat(void) const {
  // check server and packet version
  return (init_ && (NULL != rpc_frame_));
}



#endif // OCEANBASE_TOOLS_CLIENT_RPC_H_
