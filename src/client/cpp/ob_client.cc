
#include "ob_client.h"

using namespace sb::client;

int ObClient::init(const char* rs_addr, const int rs_port, int max_req) {
  UNUSED(rs_addr);
  UNUSED(rs_port);
  UNUSED(max_req);
  return 0;
}

int ObClient::exec(ObReq* req, const int64_t timeout) {
  UNUSED(req);
  UNUSED(timeout);
  return 0;
}

int ObClient::submit(int reqs_num, ObReq* reqs[]) {
  UNUSED(reqs_num);
  UNUSED(reqs);
  return 0;
}

int ObClient::cancel(int reqs_num, ObReq* reqs[]) {
  UNUSED(reqs_num);
  UNUSED(reqs);
  return 0;
}

int ObClient::get_results(int min_num, int max_num, int64_t timeout, int64_t& num, ObReq* reqs[]) {
  UNUSED(min_num);
  UNUSED(max_num);
  UNUSED(timeout);
  UNUSED(num);
  UNUSED(reqs);
  return 0;
}

int ObClient::acquire_get_req(ObReq*& req) {
  UNUSED(req);
  return 0;
}

int ObClient::acquire_scan_req(ObReq*& req) {
  UNUSED(req);
  return 0;
}

int ObClient::acquire_mutate_req(ObReq*& req) {
  UNUSED(req);
  return 0;
}

int ObClient::release_req(ObReq* req) {
  UNUSED(req);
  return 0;
}

void ObClient::set_resfd(int32_t fd, ObReq* cb) {
  UNUSED(fd);
  UNUSED(cb);
}

