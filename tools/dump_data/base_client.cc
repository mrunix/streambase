#include "common/ob_result.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "base_client.h"

using namespace sb::common;
using namespace sb::tools;

int BaseClient::init(const ObServer& server) {
  server_ = server;
  return ObBaseClient::initialize(server);
}

