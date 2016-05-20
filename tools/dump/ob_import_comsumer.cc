#include "ob_import_comsumer.h"
#include "slice.h"

#include <string>
#include "common/ob_tsi_factory.h"
using namespace std;
using namespace sb;
using namespace common;

ImportComsumer::ImportComsumer(sb::api::OceanbaseDb* db, ObRowBuilder* builder, const TableParam& param) : param_(param) {
  db_ = db;
  builder_ = builder;
  assert(db_ != NULL);
  assert(builder_ != NULL);
  bad_file_ = NULL;
  line_buffer_ = NULL;
}

int ImportComsumer::init() {
  int ret = OB_SUCCESS;
  if (param_.bad_file_ != NULL) {
    TBSYS_LOG(INFO, "using bad file name = %s", param_.bad_file_);
    ret = AppendableFile::NewAppendableFile(param_.bad_file_, bad_file_);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't create appendable file %s", param_.bad_file_);
    }
  }
  if (OB_SUCCESS == ret) {
    line_buffer_ = (char*)malloc(sizeof(char) * 1024 * 1024 * 2);   // 2M
    if (NULL == line_buffer_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "fail to allocate line buffer");
    }
  }

  return ret;
}

ImportComsumer::~ImportComsumer() {
  if (bad_file_ != NULL) {
    delete bad_file_;
  }

  if (line_buffer_ != NULL) {
    free(line_buffer_);
  }
}

int ImportComsumer::write_bad_record(RecordBlock& rec) {
  Slice slice;
  size_t rec_delima_len = param_.rec_delima.length();
  size_t delima_len = param_.delima.length();
  char delima_buf[4];
  int ret = OB_SUCCESS;

  if (bad_file_ == NULL) {
    return 0;
  }

  param_.rec_delima.append_delima(delima_buf, 0, 4);
  rec.reset();
  while (rec.next_record(slice)) {
    if (delima_len > slice.size()) {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "empty or currupted row meet, skiping");
      continue;
    }

    memcpy(line_buffer_, slice.data(), slice.size() - delima_len);
    memcpy(line_buffer_ + slice.size() - delima_len, delima_buf, rec_delima_len);
    if (bad_file_->Append(line_buffer_, slice.size() - delima_len + rec_delima_len) != OB_SUCCESS) {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "can't write to bad_file name = %s", param_.bad_file_);
      break;
    }
  }

  return ret;
}

int ImportComsumer::comsume(RecordBlock& obj) {
  Slice slice;
  int ret = OB_SUCCESS;

  DbTranscation* tnx = GET_TSI_MULT(DbTranscation, 1);
  if (NULL == tnx) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "can't create new transcation");
  } else {
    tnx->set_db(db_);
    tnx->reset();
  }

  if (ret == OB_SUCCESS) {
    ret = builder_->build_tnx(obj, tnx);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "ObRowBuilder can't build tnx");
    }
  }

  if (ret == OB_SUCCESS) {
    ret = tnx->commit();
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "commit transcation error");
    }
  } else {
    TBSYS_LOG(ERROR, "error ocurrs, so aborting transcation");
    tnx->abort();                             /* abort always success */
  }

  if (ret != OB_SUCCESS) {
    int err = write_bad_record(obj);
    if (err != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't write bad record to file");
    }
  }

  return ret;
}
