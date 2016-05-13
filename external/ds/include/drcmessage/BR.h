/**
 * Copyright (c) 2012 Taobao.com
 * All rights reserved.
 *
 * 文件名称：BinlogRecord.h
 * 摘要：通过Binlog解析出来的记录信息定义
 * 作者：Benkong <benkong@taobao.com>
 *       Jierui.lj <jierui.lj@taobao.com>
 * 日期：2012.3.13
 *       2012.8.9 统一框架新增格式化字段和BinlogRecord, Binlog的功能
 *       2013.5.22 beknong 去掉了依赖具体数据库的格式化操作
 */
#ifndef _BR_H_
#define _BR_H_

#include <string>
#include <vector>
#include <stdint.h>
class IStrArray;

#define BR_FAKE_DDL_COLNAME "ddl"

typedef enum RecordType {
  EINSERT = 0,
  EUPDATE,
  EDELETE,
  EREPLACE,
  HEARTBEAT,
  CONSISTENCY_TEST,
  EBEGIN,
  ECOMMIT,
  EDDL,
  EROLLBACK,
  EUNKNOWN,
} RECORD_TYPE;

enum FORMATERR {
  FORMATOK = 0,
  BUFOVERFLOW,
  TYPENOTSUPPORT,
  UNKNOWNTYPE,
  UNKNOWNOPTYPE,
  NULLTBLMETA,
  NULLCOLMETA,
  ENUMSETNULL
};

enum SOURCETYPE {
  SRC_MYSQL = 0,
  SRC_OCEANBASE,
  SRC_UNKNOWN
};

class ITableMeta;
class IBinlogRecord {
 public:

  virtual ~IBinlogRecord() {}

  /**
   * 获取记录类型的来源
   * @return 来源类型
   */
  virtual int getSrcType() const = 0;

  /**
   * 解析只读记录
     * @param ptr   数据区
     * @param size  ptr所占字节数
   * @return 0:成功；<0: 失败
   */
  virtual int parse(const void* ptr, size_t size) = 0;

  /**
   * 是否解析成功
   * @return true/false
   */
  virtual bool parsedOK() = 0;

  /**
   * 只读数据区实际有效的字节数，parse()的时候允许size比比有效数据长
   * @return 字节数
   */
  virtual size_t getRealSize() = 0;

  /**
   * 清除所有更新前字段
   * @return 0: 成功 非0 失败
   */
  virtual void clearOld() = 0;

  /**
   * 清除更新后的字段
   * @return 0: 成功 非0:失败
   */
  virtual void clearNew() = 0;

  /**
   * 获取更新前column和值的映射表
   * @return column name => column value
   */
  virtual const std::vector<std::string*>& oldCols() = 0;
  virtual IStrArray* parsedOldCols() const = 0;

  /**
   * 获取更新后column和值的映射表
   * @return column name => column value
   */
  virtual const std::vector<std::string*>& newCols() = 0;
  virtual IStrArray* parsedNewCols() const = 0;

  /**
   * 获取字段名
   * @return 全部字段名
   */
  virtual IStrArray* parsedColNames() const = 0;

  /**
   * 获取全字段类型
   * @return 字段类型
   */
  virtual const uint8_t* parsedColTypes() const = 0;

  /**
   * 获取ip-port
   * @return 来源库的ip-port
   */
  virtual const char* instance() const = 0;

  /**
   * 获取记录来源的db名
   * @return db名
   */
  virtual const char* dbname() const = 0;

  /**
   * 获取记录来源的table名
   * @return table名
   */
  virtual const char* tbname() const = 0;

  /**
   * 获取以逗号分隔的主键字符串
   * @return 全部主键名
   */
  virtual const char* pkValue() const = 0;
  virtual const char* ukValue() const { return NULL; }

  /**
   * 判断是否与其它记录冲突
   */
  virtual bool conflicts(const IBinlogRecord*) = 0;

  /**
   * 序列化为字符串
   * @return 非NULL: 成功; NULL: 失败
   */
  virtual std::string* toParsedString(int version = 2) = 0;
  virtual const std::string* toString() = 0;

  /**
   * 清除所有内容以便重用
   */
  virtual void clear() = 0;

  /**
   * 返回记录类型
   * @return 记录操作类型
   */
  virtual int recordType() = 0;

  /**
   * 返回记录字段内容编码
   * @return 编码
   */
  virtual const char* recordEncoding() = 0;

  /**
   * 返回该日志记录创建时间
   * @return 创建时间
   */
  virtual time_t getTimestamp() = 0;

  /**
   * 得到记录的位点的字符串形式
   * @return 位点的字符串形式
   */
  virtual const char* getCheckpoint() = 0;

  /**
   * 获取2个32位的位点描述, 根据mysql和ob的不同
   * mysql: getCheckpoint1获得的是文件编号, getCheckpoint2 记录文件内偏移
   * ob: getCheckpoint1高32位checkpoint, getCheckpoint2低32位
   */
  virtual unsigned getCheckpoint1() = 0;
  virtual unsigned getCheckpoint2() = 0;

  /**
   * 获取记录的id
   * @return 记录id
   */
  virtual uint64_t id() = 0;

  /**
  * 获取记录对应表的meta信息
  * @return TableMeta信息
  */
  virtual ITableMeta* getTableMeta() = 0;

  /**
   * 获取记录的时间线，作性能分析
   * @param length 数组长度
   * @return timemark 时间数组
   */
  virtual long* getTimemark(size_t& length) = 0;

  /**
   * 增加一条时间戳记录，用作性能分析
   * @param time 时间戳
   */
  virtual void addTimemark(long time) = 0;
};

#endif

