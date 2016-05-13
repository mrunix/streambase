/**
 * Copyright (c) 2012 Taobao.com
 * All rights reserved.
 *
 * 文件名称：MsgWrapper.h
 * 摘要：流式消息Wrapper
 * 作者：Benkong <benkong@taobao.com>
 * 日期：2013.5.16
 */
#ifndef _MSG_WRAPPER_H_
#define _MSG_WRAPPER_H_

#include <sys/types.h>

// 字符串数组封装
class IStrArray {
 public:
  virtual ~IStrArray() {}

  virtual size_t size() = 0;
  virtual int elementAt(int i, const char*& s, size_t& length) = 0;
  virtual const char* operator[](int i) = 0;
 protected:
  IStrArray() {}
};

// 其它封装

#endif

