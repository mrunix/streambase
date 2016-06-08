/* * ===================================================================================== * *
 * Filename:  db_dumper.cc * *    Description:
 *
 *        Version:  1.0
 *        Created:  04/14/2011 08:26:52 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_dumper.h"
#include "db_utils.h"
#include "db_thread_mgr.h"
#include "db_record_formator.h"
#include "common/ob_action_flag.h"
#include <sstream>

static int64_t del_lines = 0;
static int64_t insert_lines = 0;

int DbDumper::process_rowkey(const ObCellInfo& cell, int op, const char* timestamp) {
  int ret = OB_SUCCESS;

  DbTableConfig* cfg = NULL;

  ret = DUMP_CONFIG->get_table_config(cell.table_id_, cfg);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(DEBUG, "no such table, table_id=%d", cell.table_id_);
  } else {
    DbThreadMgr::get_instance()->insert_key(cell.row_key_, cell.table_id_, op);
    CThreadGuard guard(&rowkeys_mutex_);
    total_keys_++;
  }

  return ret;
}

void DbDumper::wait_completion(bool rotate_date) {
  //wait worker stop
  DbThreadMgr::get_instance()->wait_completion();

  //wait writer stop
  DbTableDumpWriter writer;
  for (size_t i = 0; i < dump_writers_.size(); i++) {
    writer = dump_writers_[i];
    if (writer.dumper) {
      writer.dumper->wait_completion(rotate_date);
    }
  }
}

int DbDumper::setup_dumpers() {
  int ret = OB_SUCCESS;
  std::vector<DbTableConfig>& cfgs = DUMP_CONFIG->get_configs();
  std::string& output = DUMP_CONFIG->get_output_dir();

  for (size_t i = 0; i < cfgs.size(); i++) {
    DbTableDumpWriter writer;

    std::string table_dir = output + "/";
    table_dir += cfgs[i].table();

    writer.table_id = cfgs[i].table_id();
    writer.dumper = new(std::nothrow) DbDumperWriter(writer.table_id, table_dir);
    if (writer.dumper == NULL) {
      TBSYS_LOG(ERROR, "Can't create dumper");
      ret = OB_ERROR;
      break;
    }

    dump_writers_.push_back(writer);
  }

  if (ret == OB_SUCCESS) {
    for (size_t i = 0; i < dump_writers_.size(); i++) {
      ret = dump_writers_[i].dumper->start();
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "start up dumper error");
        break;
      }
    }
  } else {
    //dumper_writer will be cleaned, when destructor is called
    TBSYS_LOG(INFO, "clean dumper writers");
  }

  return ret;
}

int DbDumper::init(OceanbaseDb* db) {
  int ret = OB_SUCCESS;
  db_ = db;

  if (db_ == NULL) {
    TBSYS_LOG(ERROR, "database or schema is NULL");
    ret = OB_ERROR;
  } else {
    ret = rs_.init();
  }

  if (ret == OB_SUCCESS) {
    if (pthread_spin_init(&seq_lock_, PTHREAD_PROCESS_PRIVATE) != 0) {
      TBSYS_LOG(ERROR, "can't init spin lock");
      ret = OB_ERROR;
    }
  }

  if (ret == OB_SUCCESS) {
    ret = setup_dumpers();
  } else {
    TBSYS_LOG(ERROR, "failed when init DbDumper, [retcode=%d]", ret);
    ret = OB_ERROR;
  }

  return ret;
}

DbDumper::DbDumper() {
  total_keys_ = 0;
  seq_ = 0;
}

DbDumper::~DbDumper() {
  TBSYS_LOG(INFO, "waiting dump writer thread stop");
  destroy_dumpers();
  pthread_spin_destroy(&seq_lock_);
  TBSYS_LOG(INFO, "Total rowkyes = %d", total_keys_);
  TBSYS_LOG(INFO, "del_lines= %d, insert_lines=%d", del_lines, insert_lines);
}

void DbDumper::destroy_dumpers() {
  for (size_t i = 0; i < dump_writers_.size(); i++) {
    if (dump_writers_[i].dumper != NULL)
      delete dump_writers_[i].dumper;
  }
  dump_writers_.clear();
}

int DbDumper::db_dump_rowkey(DbTableConfig* cfg, const ObString& rowkey, int op, DbRecordSet& rs) {

  int ret = OB_SUCCESS;
  std::vector<std::string>& columns = cfg->get_columns();
  std::string& table = cfg->table();

  ret = db_->get(table, columns, rowkey, rs);
  if (ret == OB_SUCCESS) {
    //wrap the record, push it in the dumper queue
    ret = pack_record(rs, cfg->table_id(), rowkey, op);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "pack record error");
    }
  } else {
    ThreadSpecificBuffer::Buffer* buffer = record_buffer_.get_buffer();
    buffer->reset();

    //hex_to_str will return 0, if buffer size is not enough
    int len = hex_to_str(rowkey.ptr(), rowkey.length(), buffer->current(), buffer->remain());
    buffer->current()[len * 2] = '\0';

    TBSYS_LOG(ERROR, "Find ERROR return value is %d, %s, table:%s", ret, buffer->current(), table.c_str());
  }

  return ret;
}

int DbDumper::handle_del_row(DbTableConfig* cfg, const ObString& rowkey, int op) {
  int ret = OB_SUCCESS;
  ThreadSpecificBuffer::Buffer* buffer = record_buffer_.get_buffer();
  buffer->reset();
  ObDataBuffer data_buff(buffer->current(), buffer->remain());

  //deleted record,just append header
  UniqFormatorHeader header;
  ret = header.append_header(rowkey, op, get_next_seq(), DUMP_CONFIG->app_name(),
                             cfg->table(), data_buff);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "Unable seiralize header, due to [%d], skip this line", ret);
  }

  if (ret == OB_SUCCESS) {
    int len = append_end_rec(data_buff);
    if (len > 0) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_ERROR;
    }
  }

  if (ret == OB_SUCCESS) {
    char* data = data_buff.get_data();
    int64_t len = data_buff.get_position();

    ret = push_record(cfg->table_id(), data, len);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't push record to dumper writer queue, rec len=%d", len);
    }
  }

  return ret;
}

int DbDumper::do_dump_rowkey(uint64_t table_id, const ObString& rowkey, int op, DbRecordSet& rs) {
  int ret = OB_SUCCESS;

  DbTableConfig* cfg;
  ret = DUMP_CONFIG->get_table_config(table_id, cfg);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "no table find, table_id=%d", table_id);
  } else {
    if (op == ObActionFlag::OP_DEL_ROW) {       /* DELETE */
      del_lines++;
      ret = handle_del_row(cfg, rowkey, op);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "handle del row failed");
      }
    } else {                                    /* INSERT, UPDATE */
      insert_lines++;
      ret = db_dump_rowkey(cfg, rowkey, op, rs);
    }
  }

  return ret;
}

int DbDumper::push_record(uint64_t table_id, const char* rec, int len) {
  int ret = OB_SUCCESS;
  for (size_t i = 0; i < dump_writers_.size(); i++) {
    if (dump_writers_[i].table_id == table_id) {
      ret = dump_writers_[i].dumper->push_record(rec, len);
      break;
    }
  }

  return ret;
}

int DbDumper::pack_record(DbRecordSet& rs, uint64_t table_id, const ObString& rowkey, int op) {
  int ret = OB_SUCCESS;
  DbRecordSet::Iterator itr = rs.begin();

  ThreadSpecificBuffer::Buffer* buffer = record_buffer_.get_buffer();
  buffer->reset();
  ObDataBuffer data_buff(buffer->current(), buffer->remain());

  DbTableConfig* cfg;
  DUMP_CONFIG->get_table_config(table_id, cfg);

  while (itr != rs.end()) {
    data_buff.get_position() = 0;

    DbRecord* recp;
    ret = itr.get_record(&recp);
    if (ret != OB_SUCCESS || recp == NULL) {
      TBSYS_LOG(ERROR, "can't get record, due to[%d]", ret);
      break;
    }

    if (recp->empty()) {                      /* a deleted record call handle del */
      ret = handle_del_row(cfg, rowkey, ObActionFlag::OP_DEL_ROW);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "handle row failed, due to[%d]", ret);
        break;
      }

      itr++;                                      /* step to next row */
      continue;
    }

    //1.filter useless column
    if (ret == OB_SUCCESS && cfg->filter_) {
      bool skip = (*cfg->filter_)(recp);
      if (skip) {
        itr++;                                      /* step to next row */
        continue;
      }
    }

    //2.append output header
    if (ret == OB_SUCCESS) {
      UniqFormatorHeader header;
      ret = header.append_header(rowkey, op, get_next_seq(), DUMP_CONFIG->app_name(),
                                 cfg->table(), data_buff);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "Unable seiralize header, due to [%d], skip this line", ret);
      } else {
        ret = append_header_delima(data_buff);
      }
    }

    //3.append record
    if (ret == OB_SUCCESS) {
      DbRecordFormator formator;
      ret = formator.format_record(table_id, recp, rowkey, data_buff);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "Unable seiralize record, due to [%d], skip this line", ret);
      }
    }

    if (ret == OB_SUCCESS) {
      char* data = data_buff.get_data();
      int64_t len = data_buff.get_position();

      //4.push to dumper writer queue
      ret = push_record(table_id, data, len);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't push record to dumper writer queue, rec len=%d", len);
      }
    }

    itr++;                                      /* step to next row */
  }

  return ret;
}

int64_t DbDumper::get_next_seq() {
  int ret;

  pthread_spin_lock(&seq_lock_);
  ret = seq_++;
  pthread_spin_unlock(&seq_lock_);

  return ret;
}
