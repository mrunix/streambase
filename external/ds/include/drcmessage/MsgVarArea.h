/**
 * Copyright (c) 2012 Taobao.com
 * All rights reserved.
 *
 * 文件名称：MsgVarArea.h
 * 摘要：流式消息格式变长数据区
 * 作者：Benkong <benkong@taobao.com>
 * 日期：2013.5.16
 */
#ifndef _MSG_VAR_AREA_H_
#define _MSG_VAR_AREA_H_

#include "MsgWrapper.h"

#include <stdio.h>
//#include <stdint.h>
#include <string>
#include <vector>
#include <typeinfo>

struct MVAInfo;
class MsgVarArea {
 public:
  MsgVarArea(bool creating = true);
  MsgVarArea(const void* ptr, size_t size);
  ~MsgVarArea();

  // methods for creating
  template <typename T>
  size_t append(T val) { return append(typeid(T).name(), (const void*)&val, sizeof(T)); }
  size_t appendString(const char* s);
  size_t appendString(std::string* s);
  size_t appendStringArray(const char** sa, size_t size);
  size_t appendStringArray(std::vector<std::string*>& sa);
  size_t appendStringArray(std::vector<std::string>& sa);
  size_t appendStringArray(std::vector<const char*>& sa);

  template <typename T>
  size_t appendArray(T* a, size_t size) { return appendArray(typeid(T).name(), (const void*)a, sizeof(T), size); }
  const std::string& getMessage();
 private:
  size_t append(const char* typeName, const void* ptr, size_t size); // return offset
  size_t appendArray(const char* typeName, const void* a, size_t elSize, size_t count);

 public:
  // methods for fetching
  void clear();
  size_t getRealSize();
  int parse(const void* ptr, size_t size);
  int getField(size_t offset, const void*& ptr, size_t& size);
  int getString(size_t offset, const char*& s, size_t& length);
  const char* getString(size_t offset);
  IStrArray* getStringArray(size_t offset);
  int getArray(size_t offset, const void*& a, size_t& elSize, size_t& size);

 private:
  MVAInfo* m_mva;
};

#endif

