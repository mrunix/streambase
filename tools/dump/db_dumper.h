/*
 * =====================================================================================
 *
 *       Filename:  db_dumper.h
 *
 *        Version:  1.0
 *        Created:  04/14/2011 07:26:41 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#ifndef  OB_API_DB_DUMPER_INC
#define  OB_API_DB_DUMPER_INC
#include "sb_db.h"
#include "db_record_set.h"
#include "db_parse_log.h"
#include "db_dumper_config.h"
#include "common/file_utils.h"
#include "common/thread_buffer.h"
#include "db_dumper_writer.h"

#include <pthread.h>
#include <semaphore.h>
#include <vector>
#include <list>
#include <deque>
#include <string>
#include <set>
#include <algorithm>

using namespace sb::common;
using namespace sb::api;
using namespace tbsys;

class DbDumperMgr;

const int kRecordSize = 2 * 1024 * 1024;
const int kWriterBuffSize = 16 * 1024 * 1024;
const int kRowkeyPerThd = 5000;

class DbDumperWriteHandler;

class DbDumper : public RowkeyHandler {
 public:
  struct DbTableDumpWriter {
    uint64_t table_id;
    DbDumperWriteHandler* dumper;
  };

 public:
  DbDumper();

  ~DbDumper();

  int init(OceanbaseDb* db);

  virtual int process_rowkey(const ObCellInfo& cell, int op, const char* timestamp);

  virtual int do_dump_rowkey(uint64_t table_id, const ObString& rowkey, int op, DbRecordSet& rs);

  virtual int db_dump_rowkey(DbTableConfig* cfg, const ObString& rowkey, int op, DbRecordSet& rs);

  int pack_record(DbRecordSet& rs, uint64_t table_id, const ObString& rowkey, int op);

  int64_t get_next_seq();

  void wait_completion(bool rotate_date);
 private:
  int handle_del_row(DbTableConfig* cfg, const ObString& rowkey, int op);
  int push_record(uint64_t table_id, const char* rec, int len);

  int setup_dumpers();
  void destroy_dumpers();
  OceanbaseDb* db_;
  DbRecordSet rs_;

  std::vector<DbTableDumpWriter> dump_writers_;

  CThreadMutex rowkeys_mutex_;
  int64_t total_keys_;

  ThreadSpecificBuffer record_buffer_;
  pthread_spinlock_t seq_lock_;
  int64_t seq_;
};

#endif   /* ----- #ifndef db_dumper_INC  ----- */
