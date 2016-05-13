/**
 * Copyright (c) 2012 Taobao.com
 * All rights reserved.
 *
 * 文件名称：MD.h
 * 摘要：db、table、column的meta信息存取接口
 * 作者：Benkong <benkong@taobao.com>
 * 日期：2013.5.22
 */
#ifndef _MD_H_
#define _MD_H_

#include "MsgWrapper.h"
#include <string>
#include <vector>
#include <time.h>

enum drcmsg_field_types {
  DRCMSG_TYPE_DECIMAL, DRCMSG_TYPE_TINY,
  DRCMSG_TYPE_SHORT,  DRCMSG_TYPE_LONG,
  DRCMSG_TYPE_FLOAT,  DRCMSG_TYPE_DOUBLE,
  DRCMSG_TYPE_NULL,   DRCMSG_TYPE_TIMESTAMP,
  DRCMSG_TYPE_LONGLONG, DRCMSG_TYPE_INT24,
  DRCMSG_TYPE_DATE,   DRCMSG_TYPE_TIME,
  DRCMSG_TYPE_DATETIME, DRCMSG_TYPE_YEAR,
  DRCMSG_TYPE_NEWDATE, DRCMSG_TYPE_VARCHAR,
  DRCMSG_TYPE_BIT,
  DRCMSG_TYPE_NEWDECIMAL = 246,
  DRCMSG_TYPE_ENUM = 247,
  DRCMSG_TYPE_SET = 248,
  DRCMSG_TYPE_TINY_BLOB = 249,
  DRCMSG_TYPE_MEDIUM_BLOB = 250,
  DRCMSG_TYPE_LONG_BLOB = 251,
  DRCMSG_TYPE_BLOB = 252,
  DRCMSG_TYPE_VAR_STRING = 253,
  DRCMSG_TYPE_STRING = 254,
  DRCMSG_TYPE_GEOMETRY = 255,
  DRCMSG_TYPES
};

// column的描述信息
class IColMeta {
 public:
  virtual const char* getName() = 0;
  virtual int getType() = 0;
  virtual long getLength() = 0;
  virtual bool isSigned() = 0;
  virtual bool isPK() = 0;
  virtual bool isUK() { return false; }
  virtual bool isNotNull() = 0;
  virtual int getDecimals() = 0;
  virtual const char* getDefault() = 0;
  virtual const char* getEncoding() = 0;
  virtual int getRequired() = 0;
  virtual IStrArray* getValuesOfEnumSet() = 0; // 使用IStrArray后需要delete

  virtual void setName(const char* name) = 0;
  virtual void setType(int type) = 0;
  virtual void setLength(long length) = 0;
  virtual void setSigned(bool b) = 0;
  virtual void setIsPK(bool b) = 0;
  virtual void setIsUK(bool b) { (void)(b); return; }
  virtual void setNotNull(bool b) = 0;
  virtual void setDecimals(int decimals) = 0;
  virtual void setDefault(const char* def) = 0;
  virtual void setEncoding(const char* enc) = 0;
  virtual void setRequired(int required) = 0;
  virtual void setValuesOfEnumSet(std::vector<std::string>& v) = 0;
  virtual void setValuesOfEnumSet(std::vector<const char*>& v) = 0;
  virtual void setValuesOfEnumSet(const char** v, size_t size) = 0;

 public:
  IColMeta() {}
  virtual ~IColMeta() {}
};

// table的描述信息
class IDBMeta;
class ITableMeta {
 public:
  virtual const char* getName() = 0;
  virtual bool hasPK() = 0;
  virtual bool hasUK() { return false; }
  virtual const char* getPKs() = 0;
  virtual const char* getUKs() { return NULL; }
  virtual const char* getEncoding() = 0;
  virtual IDBMeta* getDBMeta() = 0;

  virtual void setName(const char* name) = 0;
  virtual void setHasPK(bool b) = 0;
  virtual void setHasUK(bool b) { (void)(b); return; }
  virtual void setPKs(const char* pks) = 0;
  virtual void setUKs(const char* uks) { (void)uks; return; }
  virtual void setEncoding(const char* enc) = 0;
  virtual void setDBMeta(IDBMeta* dbMeta) = 0;

 public:

  /**
   * 获得所有字段名
   */
  virtual std::vector<std::string>& getColNames() = 0;

  /**
   * 获得primary key名
   */
  virtual std::vector<std::string>& getPKColNames() = 0;

  /**
  * 通过column的名字获取某个column的meta信息
  * @param colName   column的名字
  * @return NULL: 没有colName对应的描述信息; 非NULL: colName对应的描述信息
  */
  virtual IColMeta* getCol(const char* colName) = 0;

  /**
   * 获取Column数目
   * @return Column数目
   */
  virtual int getColCount() = 0;

  /*
   * 通过column在mysql中的内部序号得到某个column的meta信息
   *
   * @param index column的序列号
   * @return NULL: 没有colName对应的描述信息; 非NULL: colName对应的描述信息
   */
  virtual IColMeta* getCol(int index) = 0;
  virtual int getColNum(const char* colName) = 0;

  /**
   * 追加一个column的描述信息
   * @param colName  column名称
   * @param colMeta  column的描述信息
   * @return 0: 成功； <0: 失败
   */
  virtual int append(const char* colName, IColMeta* colMeta) = 0;

 public:
  ITableMeta() {}
  virtual ~ITableMeta() {}
};

// DB的描述信息
class IMetaDataCollections;
class IDBMeta {
 public:
  virtual const char* getName() = 0;
  virtual const char* getEncoding() = 0;
  virtual IMetaDataCollections* getMetaDataCollections() = 0;

  virtual void setName(const char* name) = 0;
  virtual void setEncoding(const char* enc) = 0;
  virtual void setMetaDataCollections(IMetaDataCollections* mdc) = 0;

  /**
   * 获取Table数目
   * @return Table数目
   */
  virtual int getTblCount() = 0;

  /**
  * 获取指定table的描述信息
  * @param tblName  table名称
  * @return NULL: 没有tblName对应的描述信息; 非NULL: tblName对应的描述信息
  */
  virtual ITableMeta* get(const char* tblName) = 0;

  /*
  * 通过Table在DB中的内部序号得到某个table的meta信息
  *
  * @param index table的序列号
  * @return NULL: 没有table对应的描述信息; 非NULL: index对应的描述信息
  */
  virtual ITableMeta* get(int index) = 0;

  /**
   * 加入一个table描述信息
   * @param tblName  table名称
   * @param tblMeta  table描述信息
   * @return 0: 成功; <0: 失败
   */
  virtual int put(const char* tblName, ITableMeta* tblMeta) = 0;

 public:
  IDBMeta() {}
  virtual ~IDBMeta() {}
};

// meta data集合，包括所有的db的描述信息
class IMetaDataCollections {
 public:
  virtual unsigned getMetaVerNum() = 0;
  virtual IMetaDataCollections* getPrev() = 0;
  virtual time_t getTimestamp() = 0;

  virtual void setMetaVerNum(unsigned metaVerNum) = 0;
  virtual void setPrev(IMetaDataCollections* prev) = 0;
  virtual void setTimestamp(time_t timestamp) = 0;
 public:

  /**
   * 获取Db的数目
   * @return Db的数目
   */
  virtual int getDbCount() = 0;

  /**
   * 获取指定名称的Db描述信息
   * @param dbname 指定名称
   * @return 指定Db的描述信息
   */
  virtual IDBMeta* get(const char* dbname) = 0;

  /**
   * 获取指定位置的Db描述信息
   * @param index 指定位置
   * @return 指定Db的描述信息
   */
  virtual IDBMeta* get(int index) = 0;

  /**
   * 获取指定table的描述信息
   * @param dbName   db名称
   * @param tblName  table名称
   * @return NULL: 没有tblName对应的描述信息; 非NULL: tblName对应的描述信息
   */
  virtual ITableMeta* get(const char* dbName, const char* tblName) = 0;

  /**
   * 加入一个db描述信息
   * @param dbName  db名称
   * @param dbMeta  db描述信息
   * @return 0: 成功; <0: 失败
   */
  virtual int put(const char* dbName, IDBMeta* dbMeta) = 0;

 public:
  /**
   * 序列化为字符串
   * @param s  保存序列化后的结果
   * @return 0: 成功; <0: 失败
   */
  virtual int toString(std::string& s) = 0;

  /**
   * 解析只读记录
     * @param ptr   数据区
     * @param size  ptr所占字节数
   * @return 0:成功；<0: 失败
   */
  virtual int parse(const void* ptr, size_t size) = 0;

  /**
   * 解析是否成功
   * @return true/false
   */
  virtual bool parsedOK() = 0;

  /**
   * 只读数据区实际有效的字节数，parse()的时候允许size比比有效数据长
   * @return 字节数
   */
  virtual size_t getRealSize() = 0;

 public:
  IMetaDataCollections() {}
  virtual ~IMetaDataCollections() {}
};

#endif

