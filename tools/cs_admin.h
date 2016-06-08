/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         cs_admin.h is for what ...
 *
 *  Version: $Id: cs_admin.h 2010年12月03日 13时48分29秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */

#include "client_rpc.h"
#include "base_client.h"
#include <map>
#include <string>

enum {
  CMD_MIN,
  CMD_DUMP_TABLET = 1,
  CMD_DUMP_SERVER_STATS = 2,
  CMD_DUMP_CLUSTER_STATS = 3,
  CMD_MANUAL_MERGE = 4,
  CMD_MANUAL_DROP_TABLET = 5,
  CMD_MANUAL_GC = 6,
  CMD_RS_GET_TABLET_INFO = 7,
  CMD_SCAN_ROOT_TABLE = 8,
  CMD_MAX ,
};

class GFactory {
 public:
  GFactory();
  ~GFactory();
  int initialize(const sb::common::ObServer& chunk_server);
  int start();
  int stop();
  int wait();
  static GFactory& get_instance() { return instance_; }
  inline ObClientRpcStub& get_rpc_stub() { return rpc_stub_; }
  inline BaseClient& get_base_client() { return client_; }
  inline const std::map<std::string, int>& get_cmd_map() const { return cmd_map_; }
 private:
  void init_cmd_map();
  static GFactory instance_;
  ObClientRpcStub rpc_stub_;
  BaseClient client_;
  std::map<std::string, int> cmd_map_;
};


struct Param {
  sb::common::ObServer chunk_server;
  int type;
};



