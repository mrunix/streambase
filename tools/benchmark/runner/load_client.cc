#include "common/ob_result.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "load_client.h"

using namespace sb::common;
using namespace sb::tools;

int LoadClient::init() {
  int ret = OB_SUCCESS;
  streamer_.setPacketFactory(&factory_);
  ret = client_.initialize(&transport_, &streamer_);
  if ((OB_SUCCESS == ret) && (transport_.start() != true)) {
    TBSYS_LOG(ERROR, "%s", "transport start failed");
    ret = OB_ERROR;
  }
  return ret;
}


int LoadClient::destroy() {
  int ret = OB_SUCCESS;
  if (transport_.stop() != true) {
    TBSYS_LOG(ERROR, "%s", "transport stop failed");
    ret = OB_ERROR;
  } else if (transport_.wait() != true) {
    TBSYS_LOG(ERROR, "%s", "transport wait failed");
    ret = OB_ERROR;
  }
  return ret;
}

