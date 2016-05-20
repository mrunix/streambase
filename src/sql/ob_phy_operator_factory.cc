/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_phy_operator_factory.cc
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_phy_operator_factory.h"
#include "ob_project.h"
#include "ob_limit.h"
#include "ob_filter.h"
#include "ob_table_mem_scan.h"
#include "ob_rename.h"
#include "ob_table_rename.h"
#include "ob_sort.h"
#include "ob_mem_sstable_scan.h"
#include "ob_lock_filter.h"
#include "ob_inc_scan.h"
#include "ob_insert_dbsem_filter.h"
#include "ob_ups_modify.h"
#include "ob_multiple_scan_merge.h"
#include "ob_multiple_get_merge.h"
#include "ob_empty_row_filter.h"
#include "ob_phy_operator.h"
#include "ob_row_count.h"
#include "ob_when_filter.h"
#include "ob_dual_table_scan.h"

using namespace sb;
using namespace sql;

#define CASE_CLAUSE(OP_TYPE, OP) \
    case OP_TYPE: \
      tmp = allocator.alloc(sizeof(OP)); \
      if (NULL != tmp) \
      { \
        ret = new(tmp)OP; \
      } \
      break

ObPhyOperator* ObPhyOperatorFactory::get_one(ObPhyOperatorType phy_operator_type, common::ModuleArena& allocator) {
  ObPhyOperator* ret = NULL;
  void* tmp = NULL;
  switch (phy_operator_type) {
  case PHY_INVALID:
    break;
  case PHY_PROJECT:
    //ret = pool_project_.alloc();
    ret = tc_rp_alloc(ObProject);
    break;
  case PHY_FILTER:
    //ret = pool_filter_.alloc();
    ret = tc_rp_alloc(ObFilter);
    break;
  case PHY_WHEN_FILTER:
    //ret = pool_when_filter_.alloc();
    ret = tc_rp_alloc(ObWhenFilter);
    break;
  case PHY_INSERT_DB_SEM_FILTER:
    //ret = pool_insert_db_sem_filter_.alloc();
    ret = tc_rp_alloc(ObInsertDBSemFilter);
    break;
  case PHY_MEM_SSTABLE_SCAN:
    //ret = pool_mem_sstable_scan_.alloc();
    ret = tc_rp_alloc(ObMemSSTableScan);
    break;
  case PHY_EMPTY_ROW_FILTER:
    //ret = pool_empty_row_filter_.alloc();
    ret = tc_rp_alloc(ObEmptyRowFilter);
    break;
  case PHY_ROW_COUNT:
    //ret = pool_row_count_.alloc();
    ret = tc_rp_alloc(ObRowCount);
    break;
  case PHY_MULTIPLE_GET_MERGE:
    //ret = pool_multiple_get_merge_.alloc();
    ret = tc_rp_alloc(ObMultipleGetMerge);
    break;
  case PHY_EXPR_VALUES:
    //ret = pool_expr_values_.alloc();
    ret = tc_rp_alloc(ObExprValues);
    break;
  case PHY_MULTIPLE_SCAN_MERGE:
    //ret = pool_multiple_scan_merge_.alloc();
    ret = tc_rp_alloc(ObMultipleScanMerge);
    break;
    //CASE_CLAUSE(PHY_PROJECT, ObProject);
    CASE_CLAUSE(PHY_LIMIT, ObLimit);
    //CASE_CLAUSE(PHY_FILTER, ObFilter);
    CASE_CLAUSE(PHY_TABLE_MEM_SCAN, ObTableMemScan);
    CASE_CLAUSE(PHY_RENAME, ObRename);
    CASE_CLAUSE(PHY_TABLE_RENAME, ObTableRename);
    CASE_CLAUSE(PHY_SORT, ObSort);
    //CASE_CLAUSE(PHY_MEM_SSTABLE_SCAN, ObMemSSTableScan);
    CASE_CLAUSE(PHY_LOCK_FILTER, ObLockFilter);
    CASE_CLAUSE(PHY_INC_SCAN, ObIncScan);
    //CASE_CLAUSE(PHY_INSERT_DB_SEM_FILTER, ObInsertDBSemFilter);
    CASE_CLAUSE(PHY_UPS_MODIFY, ObUpsModify);
    CASE_CLAUSE(PHY_UPS_MODIFY_WITH_DML_TYPE, ObUpsModifyWithDmlType);
    //CASE_CLAUSE(PHY_MULTIPLE_SCAN_MERGE, ObMultipleScanMerge);
    //CASE_CLAUSE(PHY_MULTIPLE_GET_MERGE, ObMultipleGetMerge);
    //CASE_CLAUSE(PHY_EMPTY_ROW_FILTER, ObEmptyRowFilter);
    //CASE_CLAUSE(PHY_EXPR_VALUES, ObExprValues);
    //CASE_CLAUSE(PHY_ROW_COUNT, ObRowCount);
    //CASE_CLAUSE(PHY_WHEN_FILTER, ObWhenFilter);
    CASE_CLAUSE(PHY_DUAL_TABLE_SCAN, ObDualTableScan);
  default:
    break;
  }
  return ret;
}

void ObPhyOperatorFactory::release_one(ObPhyOperator* opt) {
  if (NULL != opt) {
    switch (opt->get_type()) {
    case PHY_PROJECT:
      //pool_project_.free(dynamic_cast<ObProject*>(opt));
      tc_rp_free(dynamic_cast<ObProject*>(opt));
      break;
    case PHY_FILTER:
      //pool_filter_.free(dynamic_cast<ObFilter*>(opt));
      tc_rp_free(dynamic_cast<ObFilter*>(opt));
      break;
    case PHY_WHEN_FILTER:
      //pool_when_filter_.free(dynamic_cast<ObWhenFilter*>(opt));
      tc_rp_free(dynamic_cast<ObWhenFilter*>(opt));
      break;
    case PHY_INSERT_DB_SEM_FILTER:
      //pool_insert_db_sem_filter_.free(dynamic_cast<ObInsertDBSemFilter*>(opt));
      tc_rp_free(dynamic_cast<ObInsertDBSemFilter*>(opt));
      break;
    case PHY_MEM_SSTABLE_SCAN:
      //pool_mem_sstable_scan_.free(dynamic_cast<ObMemSSTableScan*>(opt));
      tc_rp_free(dynamic_cast<ObMemSSTableScan*>(opt));
      break;
    case PHY_EMPTY_ROW_FILTER:
      //pool_empty_row_filter_.free(dynamic_cast<ObEmptyRowFilter*>(opt));
      tc_rp_free(dynamic_cast<ObEmptyRowFilter*>(opt));
      break;
    case PHY_ROW_COUNT:
      //pool_row_count_.free(dynamic_cast<ObRowCount*>(opt));
      tc_rp_free(dynamic_cast<ObRowCount*>(opt));
      break;
    case PHY_MULTIPLE_GET_MERGE:
      //pool_multiple_get_merge_.free(dynamic_cast<ObMultipleGetMerge*>(opt));
      tc_rp_free(dynamic_cast<ObMultipleGetMerge*>(opt));
      break;
    case PHY_EXPR_VALUES:
      //pool_expr_values_.free(dynamic_cast<ObExprValues*>(opt));
      tc_rp_free(dynamic_cast<ObExprValues*>(opt));
      break;
    case PHY_MULTIPLE_SCAN_MERGE:
      //pool_multiple_scan_merge_.free(dynamic_cast<ObMultipleScanMerge*>(opt));
      tc_rp_free(dynamic_cast<ObMultipleScanMerge*>(opt));
      break;
    default:
      opt->~ObPhyOperator();
      break;
    }
  }
}

