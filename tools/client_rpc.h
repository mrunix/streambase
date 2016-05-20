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

#include <list>
#include "common/ob_schema.h"
#include "common/ob_general_rpc_stub.h"
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
#include "common/ob_bypass_task_info.h"
#include "rootserver/ob_chunk_server_manager.h"


class ObClientRpcStub : public sb::common::ObGeneralRpcStub {
 public:
  static const int64_t FRAME_BUFFER_SIZE = 2 * 1024 * 1024L;
  static const int64_t DEFAULT_TIMEOUT = 1000000;  // send_request timeout millionseconds
  ObClientRpcStub();
  virtual ~ObClientRpcStub();

 public:
  // warning: rpc_buff should be only used by rpc stub for reset
  int initialize(const sb::common::ObServer& remote_server,
                 const sb::common::ObClientManager* rpc_frame, const int64_t timeout = -1) ;

  int cs_scan(const sb::common::ObScanParam& scan_param,
              sb::common::ObScanner& scanner);

  int cs_get(const sb::common::ObGetParam& get_param,
             sb::common::ObScanner& scanner);

  int rs_scan(const sb::common::ObServer& server, const int64_t timeout,
              const sb::common::ObScanParam& param,
              sb::common::ObScanner& result);

  int get_tablet_info(const uint64_t table_id, const char* table_name,
                      const sb::common::ObNewRange& range,
                      sb::common::ObTabletLocation location [], int32_t& size);

  int cs_dump_tablet_image(const int32_t index, const int32_t disk_no, sb::common::ObString& image_buf);

  int rs_dump_cs_info(sb::rootserver::ObChunkServerManager& obcsm);

  int fetch_stats(sb::common::ObStatManager& obsm);

  int cs_disk_maintain(const int8_t is_install, const int32_t disk_no);

  int get_update_server(sb::common::ObServer& update_server);
  int fetch_update_server_list(sb::common::ObUpsList& server_list) ;
  int fetch_schema(const int64_t version, const bool only_core_tables, sb::common::ObSchemaManagerV2& schema);
  int get_bloom_filter(const sb::common::ObNewRange& range,
                       const int64_t tablet_version, const int64_t bf_version, ObString& bf_buffer);

  int request_report_tablet();
  int start_merge(const int64_t frozen_memtable_version, const int32_t init_flag);
  int drop_tablets(const int64_t frozen_memtable_version);
  int start_gc(const int32_t reserve);
  int migrate_tablet(const ObDataSourceDesc& desc);
  int create_tablet(const sb::common::ObNewRange& range, const int64_t last_frozen_version);
  int delete_tablet(const sb::common::ObTabletReportInfoList& info_list,
                    bool is_force = false);
  int show_param();
  int execute_sql(const sb::common::ObString& query);
  int change_log_level(int log_level);
  int stop_server(bool restart = false);
  int sync_all_tablet_images();
  int delete_table(const uint64_t table_id);
  int load_sstables(const sb::common::ObTableImportInfoList& table_list);
  int fetch_cs_sstable_dist(const sb::common::ObServer& server, const int64_t table_version,
                            std::list<std::pair<sb::common::ObNewRange, sb::common::ObString> >& sstable_list,
                            sb::common::ModuleArena& arena);
  int get_frame_buffer(sb::common::ObDataBuffer& data_buffer) const;

  const sb::common::ObServer&
  get_remote_server() const { return remote_server_; }

  int fetch_bypass_table_id(sb::common::ObBypassTaskInfo& info);
  int cs_show_disk(int32_t* disk_no, bool* avail, int32_t& disk_num);

 private:
  sb::common::ObServer remote_server_;               // root server addr
  sb::common::ThreadSpecificBuffer frame_buffer_;
  int64_t timeout_;
};




#endif // OCEANBASE_TOOLS_CLIENT_RPC_H_
