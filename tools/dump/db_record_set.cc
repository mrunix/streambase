/*
 * =====================================================================================
 *
 *       Filename:  DbRecordSet.cc
 *
 *        Version:  1.0
 *        Created:  04/12/2011 10:35:22 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_record_set.h"
#include "db_record.h"

namespace sb {
namespace api {

using namespace sb::common;

int DbRecordSet::Iterator::get_record(DbRecord** recp) {
  if (last_err_ == OB_SUCCESS)
    *recp = &record_;
  else
    *recp = NULL;

  return last_err_;
}

DbRecordSet::Iterator::Iterator(DbRecordSet* ds, common::ObScannerIterator cur_pos) {
  ds_ = ds;
  cur_pos_ = rec_end_pos_ = cur_pos;
  fill_record(rec_end_pos_);
  last_err_ = OB_SUCCESS;
}

void DbRecordSet::Iterator::fill_record(common::ObScannerIterator& itr) {
  common::ObCellInfo* cell;
  bool row_changed = false;
  bool row_start = false;
  int ret = OB_SUCCESS;
  int64_t value;

  record_.reset();
  while ((ret = itr.get_cell(&cell, &row_changed)) == common::OB_SUCCESS) {
    if (cell->value_.get_ext(value) == common::OB_SUCCESS) {
      itr++;
      continue;
    }

    record_.append_column(*cell);
    if (row_changed) {
      if (row_start == false)
        row_start = true;
      else {
        break;
      }
    }
    itr++;
  }

  if (ret != common::OB_SUCCESS && ret != common::OB_ITER_END) {
    last_err_ = ret;
    TBSYS_LOG(ERROR, "fill record failed, due to %d", ret);
  } else {
    last_err_ = OB_SUCCESS;
  }
}

DbRecordSet::Iterator& DbRecordSet::Iterator::operator++(int i) {
  cur_pos_ = rec_end_pos_;
  fill_record(rec_end_pos_);
  return *this;
}

bool DbRecordSet::Iterator::operator==(const DbRecordSet::Iterator& itr) {
  return cur_pos_ == itr.cur_pos_;
}

bool DbRecordSet::Iterator::operator!=(const DbRecordSet::Iterator& itr) {
  return cur_pos_ != itr.cur_pos_;
}


DbRecordSet::Iterator DbRecordSet::begin() {
  DbRecordSet::Iterator itr = DbRecordSet::Iterator(this, scanner_.begin());
  return itr;
}

DbRecordSet::Iterator DbRecordSet::end() {
  return DbRecordSet::Iterator(this, scanner_.end());
}

DbRecordSet::~DbRecordSet() {
  if (p_buffer_ != NULL)
    delete [] p_buffer_;
}

int DbRecordSet::init() {
  p_buffer_ = new(std::nothrow) char[cap_];
  if (p_buffer_ == NULL)
    return common::OB_MEM_OVERFLOW;

  ob_buffer_.set_data(p_buffer_, cap_);
  inited_ = true;
  return common::OB_SUCCESS;
}
}
}
