#ifndef OCEANBASE_MERGER_SERVER_BLACKLIST_H_
#define OCEANBASE_MERGER_SERVER_BLACKLIST_H_

#include "common/ob_define.h"
#include "common/ob_ups_info.h"

namespace sb {
namespace mergeserver {
// not thread safe
class ObMergerServerBlackList {
 public:
  ObMergerServerBlackList();
  virtual ~ObMergerServerBlackList();

 public:
  // set max fail count for into error status
  int init(const int32_t max_fail_count, const int64_t timeout,
           const common::ObServerType type, const common::ObUpsList& list);
  // check the server is in error status
  bool check(const int32_t server_index);
  // inc the server failed count
  void fail(const int32_t server_index, const common::ObServer& server);
  // get valid count
  int32_t get_valid_count(void) const;
  // reset all the status to ok
  void reset(void);
  //
  struct ServerStatus {
    int32_t fail_count_;
    int64_t fail_timestamp_;
    common::ObServer server_;
  };

 private:
  const static int32_t DEFAULT_COUNT = 20;
  const static int64_t DEFAULT_TIMEOUT = 60 * 1000 * 1000L;
  const static int32_t MAX_LIST_COUNT = 16;
  int32_t server_count_;
  int32_t max_fail_count_;
  int64_t black_timeout_;
  ServerStatus fail_counter_[MAX_LIST_COUNT];
};
}
}



#endif //OCEANBASE_MERGER_SERVER_BLACKLIST_H_

