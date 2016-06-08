/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_final_data.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include <iostream>
#include <vector>
#include <map>
#include <fstream>
#include "common/ob_cell_array.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_schema.h"
struct ObRowInfo {
  sb::common::ObString table_name_;
  sb::common::ObString row_key_;

  bool operator <(const ObRowInfo& other)const;
  bool operator ==(const ObRowInfo& other)const;
};

class ObFinalResult {
 public:
  ObFinalResult();
  virtual ~ObFinalResult();

 public:
  int parse_from_file(const char* filename, const char* schema_fname);
  int add_cell(const sb::common::ObCellInfo& cell);
  bool row_exist(const sb::common::ObCellInfo& add_cell);

 public:
  int get(const sb::common::ObGetParam& get_param, sb::common::ObScanner& scanner,
          bool ret_null = true);
  int scan(const sb::common::ObScanParam& scan_param, sb::common::ObScanner& scanner);

 private:
  int apply_join(sb::common::ObSchemaManager& schema_mgr,
                 sb::common::ObCellInfo& cur_cell,
                 const sb::common::ObJoinInfo& join_info);
  mutable std::map<ObRowInfo, std::vector<sb::common::ObCellInfo> >* row_infos_;
  sb::common::ObCellArray* cell_array_;
};

bool check_result(const sb::common::ObGetParam& get_param,
                  sb::common::ObScanner& ob_result, ObFinalResult& local_result);

bool check_result(const sb::common::ObScanParam& scan_param,
                  sb::common::ObScanner& ob_result, ObFinalResult& local_result);


