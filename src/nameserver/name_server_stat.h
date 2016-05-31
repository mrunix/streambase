/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-12-06
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_STAT_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_STAT_H_

#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"
#include "name_server_stat_key.h"

namespace sb {
namespace nameserver {
class NameServerStatManager : public common::ObStatManager {
 public:
  NameServerStatManager(): ObStatManager(common::OB_ROOTSERVER) {
    set_id2name(common::OB_STAT_COMMON, common::ObStatSingleton::common_map, common::COMMON_STAT_MAX);
    set_id2name(common::OB_STAT_ROOTSERVER, common::ObStatSingleton::ns_map, common::ROOTSERVER_STAT_MAX);
  }
};
}
}

#endif // OCEANBASE_ROOTSERVER_OB_ROOT_STAT_H_
