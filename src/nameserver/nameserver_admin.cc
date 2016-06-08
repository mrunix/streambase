/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include "nameserver/nameserver_admin.h"

#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include "common/ob_define.h"
#include "common/ob_result.h"
#include "common/serialization.h"
#include "common/ob_obi_config.h"
#include "common/ob_server.h"
#include "nameserver/nameserver_admin_cmd.h"
#include "nameserver/nameserver_stat_key.h"

using namespace sb::common;
using namespace sb::nameserver;

const char* build_date();
const char* build_time();

namespace sb {
namespace nameserver {
// @todo show_sstable_dist, dump_migrate_info
void usage() {
  printf("Usage: rs_admin -r <nameserver_ip> -p <nameserver_port> <command> -o <suboptions>\n");
  printf("\n\t-r <nameserver_ip>\tthe default is `127.0.0.1'\n");
  printf("\t-p <nameserver_port>\tthe default is 2500\n");
  printf("\n\t<command>:\n");
  printf("\tstat -o ");
  const char** str = &OB_RS_STAT_KEYSTR[0];
  for (; NULL != *str; ++str) {
    printf("%s|", *str);
  }
  printf("\n");
  printf("\tdo_check_point\n");
  printf("\treload_config\n");
  printf("\tlog_move_to_debug\n");
  printf("\tlog_move_to_error\n");
  printf("\tdump_root_table\n");
  printf("\tdump_unusual_tablets\n");
  printf("\tdump_server_info\n");
  printf("\tdump_migrate_info\n");
  printf("\tswitch_schema\n");
  printf("\tchange_log_level -o ERROR|WARN|INFO|DEBUG\n");
  printf("\tget_obi_role\n");
  printf("\tset_obi_role -o OBI_SLAVE|OBI_MASTER\n");
  printf("\tget_obi_config\n");
  printf("\tset_obi_config -o read_percentage=<read_percentage>\n");
  printf("\tset_obi_config -o rs_ip=<rs_ip>,rs_port=<rs_port>,read_percentage=<read_percentage>\n");
  printf("\tset_ups_config -o ups_ip=<ups_ip>,ups_port=<ups_port>,ms_read_percentage=<percentage>,cs_read_percentage=<percentage>\n");
  printf("\n\t-h\tprint this help message\n");
  printf("\t-V\tprint the version\n");
  printf("\nSee `rs_admin.log' for the detailed execution log.\n");
}

void version() {
  printf("rs_admin (%s %s)\n", PACKAGE_STRING, RELEASEID);
  printf("BUILD_TIME: %s %s\n\n", build_date(), build_time());
  printf("Copyright (c) 2007-2011 Taobao Inc.\n");
}

const char* Arguments::DEFAULT_RS_HOST = "127.0.0.1";

// define all commands string and its pcode here
Command COMMANDS[] = {
  {
    "get_obi_role", OB_GET_OBI_ROLE, do_get_obi_role
  }
  ,
  {
    "set_obi_role", OB_SET_OBI_ROLE, do_set_obi_role
  }
  ,
  {
    "do_check_point", OB_RS_ADMIN_CHECKPOINT, do_rs_admin
  }
  ,
  {
    "reload_config", OB_RS_ADMIN_RELOAD_CONFIG, do_rs_admin
  }
  ,
  {
    "log_move_to_debug", OB_RS_ADMIN_INC_LOG_LEVEL, do_rs_admin
  }
  ,
  {
    "log_move_to_error", OB_RS_ADMIN_DEC_LOG_LEVEL, do_rs_admin
  }
  ,
  {
    "dump_root_table", OB_RS_ADMIN_DUMP_ROOT_TABLE, do_rs_admin
  }
  ,
  {
    "dump_server_info", OB_RS_ADMIN_DUMP_SERVER_INFO, do_rs_admin
  }
  ,
  {
    "dump_migrate_info", OB_RS_ADMIN_DUMP_MIGRATE_INFO, do_rs_admin
  }
  ,
  {
    "switch_schema", OB_RS_ADMIN_SWITCH_SCHEMA, do_rs_admin
  }
  ,
  {
    "dump_unusual_tablets", OB_RS_ADMIN_DUMP_UNUSUAL_TABLETS, do_rs_admin
  }
  ,
  {
    "change_log_level", OB_CHANGE_LOG_LEVEL, do_change_log_level
  }
  ,
  {
    "stat", OB_RS_STAT, do_rs_stat
  }
  ,
  {
    "get_obi_config", OB_GET_OBI_CONFIG, do_get_obi_config
  }
  ,
  {
    "set_obi_config", OB_SET_OBI_CONFIG, do_set_obi_config
  }
  ,
  {
    "set_ups_config", OB_SET_UPS_CONFIG, do_set_ups_config
  }
};

enum {
  OPT_OBI_ROLE_MASTER = 0,
  OPT_OBI_ROLE_SLAVE = 1,
  OPT_LOG_LEVEL_ERROR = 2,
  OPT_LOG_LEVEL_WARN = 3,
  OPT_LOG_LEVEL_INFO = 4,
  OPT_LOG_LEVEL_DEBUG = 5,
  OPT_READ_PERCENTAGE = 6,
  OPT_UPS_IP = 7,
  OPT_UPS_PORT = 8,
  OPT_MS_READ_PERCENTAGE = 9,
  OPT_CS_READ_PERCENTAGE = 10,
  OPT_RS_IP = 11,
  OPT_RS_PORT = 12,
  THE_END
};

// 需要与上面的enum一一对应
const char* SUB_OPTIONS[] = {
  "OBI_MASTER",
  "OBI_SLAVE",
  "ERROR",
  "WARN",
  "INFO",
  "DEBUG",
  "read_percentage",            // 6
  "ups_ip",                     // 7
  "ups_port",                   // 8
  "ms_read_percentage",         // 9
  "cs_read_percentage",         // 10
  "rs_ip",                      // 11
  "rs_port",                    // 12
  NULL
};

int parse_cmd_line(int argc, char* argv[], Arguments& args) {
  int ret = OB_SUCCESS;
  // merge SUB_OPTIONS and OB_RS_STAT_KEYSTR
  int key_num = 0;
  const char** str = &OB_RS_STAT_KEYSTR[0];
  for (; NULL != *str; ++str) {
    key_num++;
  }
  int local_num = ARRAYSIZEOF(SUB_OPTIONS);
  const char** all_sub_options = new(std::nothrow) const char* [key_num + local_num];
  if (NULL == all_sub_options) {
    printf("no memory\n");
    ret = OB_ERROR;
  } else {
    memset(all_sub_options, 0, sizeof(all_sub_options));
    for (int i = 0; i < local_num; ++i) {
      all_sub_options[i] = SUB_OPTIONS[i];
    }
    for (int i = 0; i < key_num; ++i) {
      all_sub_options[i + local_num - 1] = OB_RS_STAT_KEYSTR[i];
    }
    all_sub_options[local_num + key_num - 1] = NULL;

    char* subopts = NULL;
    char* value = NULL;
    int ch = -1;
    int suboptidx = 0;
    while (-1 != (ch = getopt(argc, argv, "hVr:p:o:"))) {
      switch (ch) {
      case '?':
        usage();
        exit(-1);
        break;
      case 'h':
        usage();
        exit(0);
        break;
      case 'V':
        version();
        exit(0);
        break;
      case 'r':
        args.rs_host = optarg;
        break;
      case 'p':
        args.rs_port = atoi(optarg);
        break;
      case 'o':
        subopts = optarg;
        while ('\0' != *subopts) {
          switch (suboptidx = getsubopt(&subopts, (char* const*)all_sub_options, &value)) {
          case -1:
            break;
          case OPT_OBI_ROLE_MASTER:
            args.obi_role.set_role(ObiRole::MASTER);
            break;
          case OPT_OBI_ROLE_SLAVE:
            args.obi_role.set_role(ObiRole::SLAVE);
            break;
          case OPT_LOG_LEVEL_ERROR:
            args.log_level = TBSYS_LOG_LEVEL_ERROR;
            break;
          case OPT_LOG_LEVEL_WARN:
            args.log_level = TBSYS_LOG_LEVEL_WARN;
            break;
          case OPT_LOG_LEVEL_INFO:
            args.log_level = TBSYS_LOG_LEVEL_INFO;
            break;
          case OPT_LOG_LEVEL_DEBUG:
            args.log_level = TBSYS_LOG_LEVEL_DEBUG;
            break;
          case OPT_READ_PERCENTAGE:
            if (NULL == value) {
              printf("read_percentage needs an argument value\n");
              ret = OB_ERROR;
            } else {
              args.obi_read_percentage = atoi(value);
            }
            break;
          case OPT_UPS_IP:
            if (NULL == value) {
              printf("option `ups' needs an argument value\n");
              ret = OB_ERROR;
            } else {
              args.ups_ip = value;
            }
            break;
          case OPT_UPS_PORT:
            if (NULL == value) {
              printf("option `port' needs an argument value\n");
              ret = OB_ERROR;
            } else {
              args.ups_port = atoi(value);
            }
            break;
          case OPT_MS_READ_PERCENTAGE:
            if (NULL == value) {
              printf("option `ms_read_percentage' needs an argument value\n");
              ret = OB_ERROR;
            } else {
              args.ms_read_percentage = atoi(value);
            }
            break;
          case OPT_CS_READ_PERCENTAGE:
            if (NULL == value) {
              printf("option `cs_read_percentage' needs an argument value\n");
              ret = OB_ERROR;
            } else {
              args.cs_read_percentage = atoi(value);
            }
            break;
          case OPT_RS_IP:
            if (NULL == value) {
              printf("option `rs_ip' needs an argument value\n");
              ret = OB_ERROR;
            } else {
              args.ups_ip = value;
            }
            break;
          case OPT_RS_PORT:
            if (NULL == value) {
              printf("option `rs_port' needs an argument value\n");
              ret = OB_ERROR;
            } else {
              args.ups_port = atoi(value);
            }
            break;
          default:
            // stat keys
            args.stat_key = suboptidx - local_num + 1;
            break;
          }
        }
      default:
        break;
      }
    }
    if (OB_SUCCESS != ret) {
      usage();
    } else if (optind >= argc) {
      printf("no command specified\n");
      usage();
      ret = OB_ERROR;
    } else {
      char* cmd = argv[optind];
      for (uint32_t i = 0; i < ARRAYSIZEOF(COMMANDS); ++i) {
        if (0 == strcmp(cmd, COMMANDS[i].cmdstr)) {
          args.command.cmdstr = cmd;
          args.command.pcode = COMMANDS[i].pcode;
          args.command.handler = COMMANDS[i].handler;
          break;
        }
      }
      if (-1 == args.command.pcode) {
        printf("unknown command=%s\n", cmd);
        ret = OB_ERROR;
      } else {
        args.print();
      }
    }
    if (NULL != all_sub_options) {
      delete [] all_sub_options;
      all_sub_options = NULL;
    }
  }
  return ret;
}

void Arguments::print() {
  TBSYS_LOG(INFO, "server_ip=%s port=%d", rs_host, rs_port);
  TBSYS_LOG(INFO, "cmd=%s pcode=%d", command.cmdstr, command.pcode);
  TBSYS_LOG(INFO, "obi_role=%d", obi_role.get_role());
  TBSYS_LOG(INFO, "log_level=%d", log_level);
  TBSYS_LOG(INFO, "stat_key=%d", stat_key);
  TBSYS_LOG(INFO, "obi_read_percentage=%d", obi_read_percentage);
  TBSYS_LOG(INFO, "ups=%s port=%d", ups_ip, ups_port);
  TBSYS_LOG(INFO, "ms_read_percentage=%d", ms_read_percentage);
  TBSYS_LOG(INFO, "cs_read_percentage=%d", cs_read_percentage);
}

int do_get_obi_role(ObBaseClient& client, Arguments& args) {
  int ret = OB_SUCCESS;
  printf("get_obi_role...\n");
  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer msgbuf(buff, buff_size);

  if (OB_SUCCESS != (ret = client.send_recv(OB_GET_OBI_ROLE, MY_VERSION, args.request_timeout_us, msgbuf))) {
    printf("failed to send request, err=%d\n", ret);
  } else {
    ObResultCode result_code;
    ObiRole role;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to deserialize response, err=%d\n", ret);
    } else if (OB_SUCCESS != result_code.result_code_) {
      printf("failed to get obi role, err=%d\n", result_code.result_code_);
    } else if (OB_SUCCESS != (ret = role.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to deserialized role, err=%d\n", ret);
    } else {
      printf("obi_role=%s\n", role.get_role_str());
    }
  }
  return ret;
}

int do_set_obi_role(ObBaseClient& client, Arguments& args) {
  int ret = OB_SUCCESS;
  printf("set_obi_role...role=%d\n", args.obi_role.get_role());
  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer msgbuf(buff, buff_size);
  if (OB_SUCCESS != (ret = args.obi_role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    printf("failed to serialize role, err=%d\n", ret);
  } else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_OBI_ROLE, MY_VERSION, args.request_timeout_us, msgbuf))) {
    printf("failed to send request, err=%d\n", ret);
  } else {
    ObResultCode result_code;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to deserialize response, err=%d\n", ret);
    } else if (OB_SUCCESS != result_code.result_code_) {
      printf("failed to set obi role, err=%d\n", result_code.result_code_);
    } else {
      printf("Okay\n");
    }
  }
  return ret;
}

int do_rs_admin(ObBaseClient& client, Arguments& args) {
  int ret = OB_SUCCESS;
  printf("do_rs_admin, cmd=%d...\n", args.command.pcode);
  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer msgbuf(buff, buff_size);
  // admin消息的内容为一个int32_t类型，定义在nameserver_admin_cmd.h中
  if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.command.pcode))) {
    printf("failed to serialize, err=%d\n", ret);
  } else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN, MY_VERSION, args.request_timeout_us, msgbuf))) {
    printf("failed to send request, err=%d\n", ret);
  } else {
    ObResultCode result_code;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to deserialize response, err=%d\n", ret);
    } else if (OB_SUCCESS != result_code.result_code_) {
      printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
    } else {
      printf("Okay\n");
    }
  }
  return ret;
}

int do_change_log_level(ObBaseClient& client, Arguments& args) {
  int ret = OB_SUCCESS;
  printf("do_change_log_level, level=%d...\n", args.log_level);
  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer msgbuf(buff, buff_size);
  if (Arguments::INVALID_LOG_LEVEL == args.log_level) {
    printf("invalid log level, level=%d\n", args.log_level);
  }
  // change_log_level消息的内容为一个int32_t类型log_level
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.log_level))) {
    printf("failed to serialize, err=%d\n", ret);
  } else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_LOG_LEVEL, MY_VERSION, args.request_timeout_us, msgbuf))) {
    printf("failed to send request, err=%d\n", ret);
  } else {
    ObResultCode result_code;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to deserialize response, err=%d\n", ret);
    } else if (OB_SUCCESS != result_code.result_code_) {
      printf("failed to change_log_level, err=%d\n", result_code.result_code_);
    } else {
      printf("Okay\n");
    }
  }
  return ret;
}

int do_rs_stat(ObBaseClient& client, Arguments& args) {
  int ret = OB_SUCCESS;
  printf("do_rs_stat, key=%d...\n", args.stat_key);
  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 2048;
  char buff[buff_size];
  ObDataBuffer msgbuf(buff, buff_size);
  // rs_stat消息的内容为一个int32_t类型，定义在nameserver_stat_key.h中
  if (0 > args.stat_key) {
    printf("invalid stat_key=%d\n", args.stat_key);
  } else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.stat_key))) {
    printf("failed to serialize, err=%d\n", ret);
  } else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_STAT, MY_VERSION, args.request_timeout_us, msgbuf))) {
    printf("failed to send request, err=%d\n", ret);
  } else {
    ObResultCode result_code;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to deserialize response, err=%d\n", ret);
    } else if (OB_SUCCESS != result_code.result_code_) {
      printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
    } else {
      printf("%s\n", msgbuf.get_data() + msgbuf.get_position());
    }
  }
  return ret;
}

int do_get_obi_config(ObBaseClient& client, Arguments& args) {
  int ret = OB_SUCCESS;
  printf("get_obi_config...\n");
  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer msgbuf(buff, buff_size);

  if (OB_SUCCESS != (ret = client.send_recv(OB_GET_OBI_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf))) {
    printf("failed to send request, err=%d\n", ret);
  } else {
    ObResultCode result_code;
    ObiConfig conf;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to deserialize response, err=%d\n", ret);
    } else if (OB_SUCCESS != result_code.result_code_) {
      printf("failed to get obi role, err=%d\n", result_code.result_code_);
    } else if (OB_SUCCESS != (ret = conf.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to deserialized role, err=%d\n", ret);
    } else {
      printf("read_percentage=%d\n", conf.get_read_percentage());
    }
  }
  return ret;
}

int do_set_obi_config(ObBaseClient& client, Arguments& args) {
  int ret = OB_SUCCESS;
  printf("set_obi_config, read_percentage=%d rs_port=%d...\n", args.obi_read_percentage, args.ups_port);
  static const int32_t MY_VERSION = 2;

  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer msgbuf(buff, buff_size);
  ObiConfig conf;
  conf.set_read_percentage(args.obi_read_percentage);
  ObServer rs_addr;
  if (0 != args.ups_port && !rs_addr.set_ipv4_addr(args.ups_ip, args.ups_port)) {
    printf("invalid param, addr=%s port=%d\n", args.ups_ip, args.ups_port);
    usage();
    ret = OB_INVALID_ARGUMENT;
  } else if (0 > args.obi_read_percentage || 100 < args.obi_read_percentage) {
    printf("invalid param, read_percentage=%d\n", args.obi_read_percentage);
    usage();
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = conf.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    printf("failed to serialize obi_config, err=%d\n", ret);
  } else if (OB_SUCCESS != (ret = rs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    printf("failed to serialize obi_config, err=%d\n", ret);
  } else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_OBI_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf))) {
    printf("failed to send request, err=%d\n", ret);
  } else {
    ObResultCode result_code;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to deserialize response, err=%d\n", ret);
    } else if (OB_SUCCESS != result_code.result_code_) {
      printf("failed to set obi config, err=%d\n", result_code.result_code_);
    } else {
      printf("Okay\n");
    }
  }
  return ret;
}

int do_set_ups_config(ObBaseClient& client, Arguments& args) {
  int ret = OB_SUCCESS;
  common::ObServer ups_addr;
  if (0 > args.ms_read_percentage || 100 < args.ms_read_percentage) {
    printf("invalid param, ms_read_percentage=%d\n", args.ms_read_percentage);
    usage();
    ret = OB_INVALID_ARGUMENT;
  } else if (0 > args.cs_read_percentage || 100 < args.cs_read_percentage) {
    printf("invalid param, cs_read_percentage=%d\n", args.cs_read_percentage);
    usage();
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == args.ups_ip) {
    printf("invalid param, ups_ip is NULL\n");
    usage();
    ret = OB_INVALID_ARGUMENT;
  } else if (0 >= args.ups_port) {
    printf("invalid param, ups_port=%d\n", args.ups_port);
    usage();
    ret = OB_INVALID_ARGUMENT;
  } else if (!ups_addr.set_ipv4_addr(args.ups_ip, args.ups_port)) {
    printf("invalid param, ups_host=%s ups_port=%d\n", args.ups_ip, args.ups_port);
    usage();
    ret = OB_INVALID_ARGUMENT;
  } else {
    printf("set_ups_config, ups_ip=%s ups_port=%d ms_read_percentage=%d cs_read_percentage=%d...\n",
           args.ups_ip, args.ups_port, args.ms_read_percentage, args.cs_read_percentage);

    static const int32_t MY_VERSION = 1;
    const int buff_size = sizeof(ObPacket) + 64;
    char buff[buff_size];
    ObDataBuffer msgbuf(buff, buff_size);

    if (OB_SUCCESS != (ret = ups_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      printf("failed to serialize ups_addr, err=%d\n", ret);
    } else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.ms_read_percentage))) {
      printf("failed to serialize read_percentage, err=%d\n", ret);
    } else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.cs_read_percentage))) {
      printf("failed to serialize read_percentage, err=%d\n", ret);
    } else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf))) {
      printf("failed to send request, err=%d\n", ret);
    } else {
      ObResultCode result_code;
      msgbuf.get_position() = 0;
      if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
        printf("failed to deserialize response, err=%d\n", ret);
      } else if (OB_SUCCESS != result_code.result_code_) {
        printf("failed to set obi config, err=%d\n", result_code.result_code_);
      } else {
        printf("Okay\n");
      }
    }
  }
  return ret;
}

} // end namespace nameserver
}   // end namespace sb
