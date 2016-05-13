/**
 * Copyright (c) 2013 Taobao.com
 * All rights reserved.
 *
 * 文件名称：MsgHeader.h
 * 摘要：数据类型定义
 * 作者：Benkong <benkong@taobao.com>
 * 日期：2013.5.14
 */
#ifndef _MSG_HEADER_H_
#define _MSG_HEADER_H_

#include <stdint.h>

// 消息类型
enum {
  MT_UNKNOWN = 0,
  MT_META,
  MT_FIXED,
  MT_VAR,
  MT_EXT
};

struct MsgHeader {
  uint16_t m_msgType;
  uint16_t m_version;
  uint32_t m_size;
};

#endif

