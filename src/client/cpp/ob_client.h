
#include <stdint.h>
#include <ob_get_param.h>
#include <ob_scan_param.h>
#include <ob_mutator.h>
#include <ob_scanner.h>

namespace sb {
namespace client {
using sb::common::ObGetParam;
using sb::common::ObScanParam;
using sb::common::ObMutator;
using sb::common::ObScanner;

enum ObOperationCode {
  OB_GET = 1,
  OB_SCAN = 2,
  OB_MUTATE = 3
};

struct ObReq {        /// OB异步请求描述结构
  int32_t opcode;    /// operation code
  int32_t flags;     /// 是否使用下面的事件通知fd
  int32_t resfd;     /// 进行事件通知的fd
  int32_t req_id;
  void* data;        /// 用户自定数据域
  void* arg;         /// 用户自定数据域
  int32_t res;       /// 返回值
  int32_t padding;
  union {
    ObGetParam* get_param;
    ObScanParam* scan_param;
    ObMutator* mut;
  };
  ObScanner* scanner;/// 返回结果
};

class ObClient {
 public:
  /**
   * ObClient初始化
   * @param rs_addr Rootserver地址
   * @param rs_port Rootserver端口号
   * @param max_req 最大并发数
   */
  int init(const char* rs_addr, const int rs_port, int max_req = 1);

  /**
   * 执行请求, 同步调用
   * @param req 请求
   * @param timeout 超时(us)
   */
  int exec(ObReq* req, const int64_t timeout);

  /**
   * 提交异步请求
   * @param reqs_num 异步请求个数
   * @param reqs 异步请求数组
   */
  int submit(int reqs_num, ObReq* reqs[]);
  /**
   * 取消异步请求
   * @param reqs_num 异步请求个数
   * @param reqs 异步请求数组
   */
  int cancel(int reqs_num, ObReq* reqs[]);
  /**
   * 获取异步结果
   *   当min_num=0或者timeout=0时, 该函数不会阻塞
   *   当timeout=-1时, 表示等待时间是无限长, 直到满足条件
   * @param min_num 本次获取的最少异步完成事件个数
   * @param min_num 本次获取的最多异步完成事件个数
   * @param timeout 超时(us)
   * @param num 输出参数, 异步返回结果个数
   * @param reqs 输出参数, 异步返回结果数组
   */
  int get_results(int min_num, int max_num, int64_t timeout, int64_t& num, ObReq* reqs[]);

  /**
   * 获取get请求的req
   */
  int acquire_get_req(ObReq*& req);
  /**
   * 获取scan请求的req
   */
  int acquire_scan_req(ObReq*& req);
  /**
   * 获取mutate请求的req
   */
  int acquire_mutate_req(ObReq*& req);
  /**
   * 释放req
   */
  int release_req(ObReq* req);

  /**
   * 设置事件通知fd
   */
  void set_resfd(int32_t fd, ObReq* cb);
};
} // end namespace client
} // end namespace sb

