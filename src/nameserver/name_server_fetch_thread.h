#ifndef SRC_NAMESERVER_NAME_SERVER_FETCH_THREAD_H_
#define SRC_NAMESERVER_NAME_SERVER_FETCH_THREAD_H_

#include "common/ob_fetch_runnable.h"
#include "nameserver/name_server_log_manager.h"

namespace sb {
namespace nameserver {
class NameServerFetchThread : public common::ObFetchRunnable {
  const static int64_t WAIT_INTERVAL;

 public:
  NameServerFetchThread();

  int wait_recover_done();

  int got_ckpt(uint64_t ckpt_id);

  void set_log_manager(NameServerLogManager* log_manager);

 private:
  int recover_ret_;
  bool is_recover_done_;
  NameServerLogManager* log_manager_;
};
} /* nameserver */
} /* oceanbase */

#endif /* end of include guard: SRC_NAMESERVER_NAME_SERVER_FETCH_THREAD_H_ */
#include "common/ob_fetch_runnable.h"

