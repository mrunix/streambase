/*
 * =====================================================================================
 *
 *       Filename:  OceanbaseDb.h
 *
 *        Version:  1.0
 *        Created:  04/12/2011 04:48:39 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef OB_API_OCEANBASEDB_H
#define  OB_API_OCEANBASEDB_H

#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_schema.h"
#include "db_table_info.h"
#include <map>
#include <vector>

namespace sb {
namespace api {
class DbRecordSet;

using common::ObString;
using common::ObServer;

const int64_t kDefaultTimeout = 1000000;

class OceanbaseDb {
  typedef std::map< ObString, TabletInfo> CacheRow;
  typedef std::map<std::string, CacheRow > CacheSet;
 public:
  OceanbaseDb(const char* ip, unsigned short port, int64_t timeout = kDefaultTimeout);
  OceanbaseDb(int32_t ip, unsigned short port);
  ~OceanbaseDb();

  int get(std::string& table, std::vector<std::string>& columns, const DbRowKey& rowkey, DbRecordSet& rs);
  int init();
  static int global_init(const char* log_dir, const char* level);
  int get_ms_location(const DbRowKey& row_key, std::string& table_name);
  int get_tablet_location(std::string& table, const DbRowKey& rowkey, common::ObServer& server);
  int fetch_schema(common::ObSchemaManager& schema_manager);

  int search_tablet_cache(std::string& table, const DbRowKey& rowkey, TabletInfo& loc);
  void insert_tablet_cache(std::string& table, const DbRowKey& rowkey, TabletInfo& tablet);
 private:
  int do_server_get(common::ObServer& server, const DbRowKey& row_key, common::ObScanner& scanner,
                    common::ObDataBuffer& data_buff, std::string& table_name,
                    std::vector<std::string>& columns);
  void free_tablet_cache();
  void mark_ms_failure(ObServer& server, std::string& table, const ObString& rowkey);
  void try_mark_server_fail(TabletInfo& tablet_info, ObServer& server, bool& do_erase_tablet);

  common::ObServer root_server_;
  tbnet::Transport transport_;
  common::ObPacketFactory packet_factory_;
  tbnet::DefaultPacketStreamer streamer_;
  common::ObClientManager client_;

  CacheSet cache_set_;
  //            tbsys::CRWLock cache_lock_;
  tbsys::CThreadMutex cache_lock_;

  int64_t timeout_;
  bool inited_;
};
}
}

#endif
