/*
 *   (C) 2010-2012 Taobao Inc.
 *
 *   Version: 0.1 $date
 *
 *   Authors:
 *
 */

#ifndef _OCEANBASE_TOOLS_CHUNK_SERVER_CLIENT_H_
#define _OCEANBASE_TOOLS_CHUNK_SERVER_CLIENT_H_

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
#include "common/ob_ups_info.h"
#include "rootserver/ob_chunk_server_manager.h"
#include "ob_server_client.h"

namespace sb {
namespace tools {
class ObChunkServerClient : public ObServerClient {
 public:
  ObChunkServerClient();
  virtual ~ObChunkServerClient();

  virtual int initialize(const sb::common::ObServer& server);

 public:
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

  int rs_dump_cs_info(sb::rootserver::ObChunkServerManager& obcsm);

  int fetch_stats(sb::common::ObStatManager& obsm);

  int get_update_server(sb::common::ObServer& update_server);
  int fetch_update_server_list(sb::common::ObUpsList& server_list) ;

  int start_merge(const int64_t frozen_memtable_version, const int32_t init_flag);
  int drop_tablets(const int64_t frozen_memtable_version);
  int start_gc(const int32_t reserve);
  int migrate_tablet(const sb::common::ObServer& dest,
                     const sb::common::ObRange& range, bool keep_src);

 private:
  const int64_t timeout_;    // send_request timeout millionseconds
};
} // namespace tools
}     // namespace sb
#endif // _OCEANBASE_TOOLS_CHUNK_SERVER_CLIENT_H_
