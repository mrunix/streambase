/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ob_cluster_stats.cc is for what ...
 *
 *  Version: $Id: ob_cluster_stats.cc 2010年12月22日 15时13分21秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#include "nameserver/ob_chunk_server_manager.h"
#include "ob_cluster_stats.h"
#include "client_rpc.h"
#include "cs_admin.h"
using namespace sb::common;
using namespace sb::nameserver;

namespace sb {
namespace tools {

int32_t ObClusterStats::refresh() {
  int ret = 0;
  int32_t server_type = store_.diff.get_server_type();
  if (server_type == ObStatManager::SERVER_TYPE_ROOT) {
    ret = ObServerStats::refresh();
  } else if (server_type == ObStatManager::SERVER_TYPE_UPDATE) {
    ObServer update_server;
    ret = rpc_stub_.get_update_server(update_server);
    ObClientRpcStub update_stub;
    if (OB_SUCCESS == ret) {
      ret = update_stub.initialize(update_server,
                                   &GFactory::get_instance().get_base_client().get_client_manager());
    }
    if (OB_SUCCESS == ret) {
      ret = update_stub.fetch_stats(store_.current);
    }
  } else if (server_type == ObStatManager::SERVER_TYPE_CHUNK
             || server_type == ObStatManager::SERVER_TYPE_MERGE) {
    ObChunkServerManager obsm;
    ret = rpc_stub_.rs_dump_cs_info(obsm);
    if (OB_SUCCESS == ret) {
      ObChunkServerManager::const_iterator it = obsm.begin();
      ObServer node;
      char ipstr[64];
      int alive_server_count = 0;
      while (it != obsm.end()) {
        node = it->server_;
        if (server_type == ObStatManager::SERVER_TYPE_CHUNK)
          node.set_port(it->port_cs_);
        else
          node.set_port(it->port_ms_);

        node.to_string(ipstr, 64);
        /*
        fprintf(stderr, "dump server ip:%s, server status:%ld, serving:%d\n",
            ipstr, it->status_, ObServerStatus::STATUS_SERVING);
            */

        if (it->status_ != ObServerStatus::STATUS_DEAD) {
          ObClientRpcStub node_stub;
          ObStatManager node_stats;
          node_stats.set_server_type(server_type);
          ret = node_stub.initialize(node, &GFactory::get_instance().get_base_client().get_client_manager());
          if (OB_SUCCESS != ret) { ++it; continue ;}
          ret = node_stub.fetch_stats(node_stats);
          //printf("ret=%d, node_stats:%lu\n", ret, node_stats.get_server_type());
          if (OB_SUCCESS != ret) { ++it; continue ;}
          if (alive_server_count == 0)
            store_.current = node_stats;
          else
            store_.current.add(node_stats);
          ++alive_server_count;
        }
        ++it;
      }

      if (0 == alive_server_count) {
        fprintf(stderr, "no any alive servers, cannot collect data.\n");
      }

    }

  }
  return ret;
}



}
}
