#ifndef __DB_THREAD_MGR_H__
#define  __DB_THREAD_MGR_H__
#include "common/utility.h"
#include "db_dumper.h"
#include "common/ob_string.h"
#include <pthread.h>
#include <semaphore.h>
#include <set>
#include <list>
#include <algorithm>

using namespace sb::common;
using namespace tbsys;

const int kMaxRowkeysInMem = 1000000;
const int kMaxUsedBytes = 1024 * 1024 * 1;

struct TableRowkey {
  ObString rowkey;
  uint64_t table_id;
  int op;
};

struct TableRowkeyComparor {
  int operator()(const TableRowkey& lhs, const TableRowkey& rhs) {
    if (lhs.table_id != rhs.table_id)
      return lhs.table_id < rhs.table_id;
    return lhs.rowkey < rhs.rowkey;
  }
};

class DbThreadMgr : public CDefaultRunnable {
 public:

  typedef std::set<TableRowkey, TableRowkeyComparor>::iterator KeyIterator;
  static DbThreadMgr* get_instance() {
    if (instance_ == NULL)
      instance_ = new(std::nothrow) DbThreadMgr();

    return instance_;
  }

  ~DbThreadMgr() {
    TBSYS_LOG(INFO, "Inserted Lines = %d, Writen Lines = %d", inserted_lines_, writen_lines_);
    TBSYS_LOG(INFO, "all keys size %d", all_keys_.size());
  }

  int destroy() {
    if (running_ == true) {
      stop();
    }

    delete this;
    return OB_SUCCESS;
  }


  int insert_key(const ObString& key, uint64_t table_id, int op) {
    int ret = OB_SUCCESS;

    ObString new_key;
    if (clone_rowkey(key, &new_key) != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't clone rowkey");
      ret = OB_ERROR;
    } else {
      TableRowkey tab_key;
      tab_key.rowkey = new_key;
      tab_key.table_id = table_id;
      tab_key.op = op;

      cond_.lock();
      all_keys_.push_back(tab_key);
      inserted_lines_++;

      cond_.unlock();
      cond_.signal();

      ret = OB_SUCCESS;
    }

    return ret;
  }

  int clone_rowkey(const ObString& rowkey, ObString* out) {
    //char *ptr = allocater_.alloc(rowkey.length());
    int ret = OB_SUCCESS;
    char* ptr = new(std::nothrow) char[rowkey.length()];
    if (ptr == NULL) {
      TBSYS_LOG(INFO, "Can't allocate more memory");
      ret = OB_ERROR;
    }

    memcpy(ptr, rowkey.ptr(), rowkey.length());
    out->assign_ptr(ptr, rowkey.length());
    return ret;
  }

  void free_rowkey(ObString rowkey) {
    delete [] rowkey.ptr();
  }

  int init(DbDumper* dumper) {
    int ret = OB_SUCCESS;

    dumper_ = dumper;
    if (dumper_ == NULL) {
      ret = OB_ERROR;
    }

    return ret;
  }

  void run(CThread* thd, void* arg) {
    TableRowkey key;

    DbRecordSet rs;
    rs.init();

    while (true) {
      bool do_work = false;

      cond_.lock();
      while (!_stop && all_keys_.empty())
        cond_.wait();

      if (!all_keys_.empty()) {
        key = all_keys_.front();
        all_keys_.pop_front();
        do_work = true;
      } else if (_stop) {
        //all work done
        cond_.unlock();
        break;
      }
      cond_.unlock();

      if (do_work) {
        int max_retries = 10;
        int ret = OB_SUCCESS;

        while (max_retries-- > 0) {
          //TODO:ret = do dump key, when not success
          ret = dumper_->do_dump_rowkey(key.table_id, key.rowkey, key.op, rs);
          if (ret == OB_SUCCESS) {
            CThreadGuard guard(&mutex_);
            writen_lines_++;
            break;
          }
        }

        if (max_retries == 0 && ret != OB_SUCCESS) {
          //TODO:dump rowkey
          TBSYS_LOG(ERROR, "Max Retries, skipping record");
        }

        free_rowkey(key.rowkey);
      }
    }

  }

  void stop() {
    cond_.lock();
    _stop = true;
    running_ = false;
    cond_.unlock();
    cond_.broadcast();

    CDefaultRunnable::wait();
    CDefaultRunnable::stop();
  }

  int start(int max_thread) {
    thread_max_ = max_thread;
    if (thread_max_ == 0)
      return OB_ERROR;

    TBSYS_LOG(INFO, "start thread mgr with thread nr [%d]", max_thread);
    setThreadCount(max_thread);
    CDefaultRunnable::start();
    running_ = true;

    return OB_SUCCESS;
  }

  void wait_completion() {
    bool empty = false;
    TBSYS_LOG(INFO, "start waiting thread mgr");
    while (true) {
      cond_.lock();
      empty = all_keys_.empty();
      cond_.unlock();

      if (empty) {
        TBSYS_LOG(INFO, "finish waiting thread mgr");
        break;
      }
      sleep(1);
      //        usleep(100000);
    }
  }

  int64_t inserted_lines() { return inserted_lines_; }
  int64_t writen_lines() { return writen_lines_; }
  int64_t duplicate_lines() { return duplicate_lines_; }

 private:
  DbThreadMgr() {
    running_ = false;
    duplicate_lines_ = inserted_lines_ = writen_lines_ = 0;
    writer_waiting_ = false;
  }

 private:
  DbDumper* dumper_;
  static DbThreadMgr* instance_;
  CThreadMutex mutex_;
  std::list<TableRowkey> all_keys_;

  int thread_max_;
  CThreadCond cond_;
  bool running_;

  int64_t inserted_lines_;
  int64_t writen_lines_;
  int64_t duplicate_lines_;

  bool writer_waiting_;
  CThreadCond insert_cond_;
};

#endif
