#include <getopt.h>
#include <set>
#include <string.h>

#include "task_worker.h"

using namespace sb::tools;
using namespace sb::common;

static const int64_t TIMEOUT = 1000 * 1000 * 20;  // 20s
static const int64_t RETRY_TIME = 2000 * 1000;    // 2s
static const int64_t MAX_PATH_LEN = 1024;         // 1024B

void print_usage() {
  fprintf(stderr, "task_worker [OPTION]\n");
  fprintf(stderr, "   -a|--addr task server addr\n");
  fprintf(stderr, "   -p|--port task server port\n");
  fprintf(stderr, "   -f|--file output file path\n");
  fprintf(stderr, "   -l|--log  output log file\n");
  fprintf(stderr, "   -h|--help usage help\n");
}

int main_routine(const ObServer& task_server, const char* file_path);

int main(int argc, char** argv) {
  int ret = OB_SUCCESS;
  const char* opt_string = "a:p:f:l:h";
  struct option longopts[] = {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'p'},
    {"file", 1, NULL, 'f'},
    {"log", 1, NULL, 'l'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  int opt = 0;
  const char* log_file = NULL;
  const char* file_path = NULL;
  const char* hostname = NULL;
  int32_t port = 0;
  std::set<std::string> dump_tables;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
    case 'a':
      hostname = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'f':
      file_path = optarg;
      break;
    case 'l':
      log_file = optarg;
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }

  if ((NULL != hostname) && (NULL != log_file) && (0 != port)) {
    ob_init_memory_pool();
    TBSYS_LOGGER.setFileName(log_file, true);
    TBSYS_LOGGER.setLogLevel("INFO");
    TBSYS_LOGGER.setMaxFileSize(1024L * 1024L);
    ObServer task_server(ObServer::IPV4, hostname, port);
    ret = main_routine(task_server, file_path);
  } else {
    ret = OB_ERROR;
    print_usage();
  }
  return ret;
}

int main_routine(const ObServer& task_server, const char* file_path) {
  // task server addr
  uint64_t times = 3;
  TaskWorker client(task_server, TIMEOUT, times);
  RpcStub rpc(client.get_client(), client.get_buffer());
  // output data path
  int ret = client.init(&rpc, file_path);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "client init failed:ret[%d]", ret);
  } else {
    TaskInfo task;
    TaskCounter count;
    char output_file[MAX_PATH_LEN] = "";
    while (true) {
      // step 1. fetch a new task
      ret = client.fetch_task(count, task);
      if (ret != OB_SUCCESS) {
        // timeout because of server not exist or server exit
        if (OB_RESPONSE_TIME_OUT == ret) {
          ret = OB_SUCCESS;
          break;
        }
        TBSYS_LOG(WARN, "fetch new task failed:ret[%d]", ret);
        usleep(RETRY_TIME);
      } else if (count.finish_count_ < count.total_count_) {
        // step 2. do the task
        ret = client.do_task(task, output_file, sizeof(output_file));
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "finish task failed:task[%ld], ret[%d]", task.get_id(), ret);
        } else {
          TBSYS_LOG(INFO, "finish task succ:task[%ld], file[%s]", task.get_id(), output_file);
        }
        // step 3. report the task status
        ret = client.report_task(ret, output_file, task);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "report task failed:task[%ld], ret[%d]", task.get_id(), ret);
        }
      } else {
        ret = OB_SUCCESS;
        TBSYS_LOG(INFO, "finish all the task");
        break;
      }
    }
  }
  return ret;
}


