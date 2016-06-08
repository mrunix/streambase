/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ob_cluster_stats.h is for what ...
 *
 *  Version: $Id: ob_cluster_stats.h 2010年12月22日 15时10分06秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#ifndef OCEANBASE_TOOLS_CLUSTER_STATS_H
#define OCEANBASE_TOOLS_CLUSTER_STATS_H

#include "ob_server_stats.h"

namespace sb {
namespace tools {

class ObClusterStats : public ObServerStats {
 public:
  ObClusterStats(ObClientRpcStub& stub, const int64_t server_type)
    : ObServerStats(stub, server_type) { }
  virtual ~ObClusterStats() {}
 protected:
  virtual int32_t refresh() ;
};

}
}


#endif //OCEANBASE_TOOLS_CLUSTER_STATS_H
