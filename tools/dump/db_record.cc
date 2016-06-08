/*
 * =====================================================================================
 *
 *       Filename:  DbRecord.cc
 *
 *        Version:  1.0
 *        Created:  04/12/2011 11:45:59 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_record.h"
#include "db_utils.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_crc64.h"
#include "common/ob_define.h"

#include <sstream>
#include <iostream>

namespace sb {
namespace api {

using namespace sb::common;

DbRecord::~DbRecord() {
  reset();
}

void DbRecord::reset() {
  row_.clear();
}

//desieral whole line, used for debug purpose
int DbRecord::serialize(ObDataBuffer& data_buff) {
  int ret = OB_SUCCESS;
  int len = 0;

  Iterator itr = row_.begin();
  data_buff.get_position() = 0;

  while (itr != row_.end()) {
    len = append_delima(data_buff);
    if (len < 0) {
      ret = OB_ERROR;
      break;
    } else {
      ret = serialize_cell(&itr->second, data_buff);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "I can't Serialize a cell, due to %d, cell: %s, pos:%ld, type:%d",
                  ret, std::string(itr->second.column_name_.ptr(), itr->second.column_name_.length()).c_str(),
                  data_buff.get_position(), itr->second.value_.get_type());
        break;
      }
    }
    itr++;
  }

  if (append_end_rec(data_buff) < 0) {
    TBSYS_LOG(ERROR, "can't append end record symbol");
    ret = OB_ERROR;
  }

  return ret;
}

int DbRecord::get(const char* column_str, common::ObCellInfo** cell) {
  std::string column = column_str;
  return get(column, cell);
}

int DbRecord::get(std::string& column, common::ObCellInfo** cell) {
  int ret = OB_SUCCESS;

  RowData::iterator itr = row_.find(column);
  if (itr == row_.end())
    ret = OB_ENTRY_NOT_EXIST;
  else
    *cell = &itr->second;

  return ret;
}

void DbRecord::append_column(ObCellInfo& cell) {
  std::string column(cell.column_name_.ptr(), cell.column_name_.length());
  row_.insert(std::make_pair(column, cell));
}
}
}
