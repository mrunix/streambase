/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_read_param_decoder.cc for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_read_param_decoder.h"
#include "ob_ms_tsi.h"
#include "common/ob_tsi_factory.h"
#include <vector>
using namespace sb;
using namespace sb::common;
using namespace sb::mergeserver;
namespace {
static const ObString OB_SCAN_PARAM_EMPTY_TABLE_NAME;

int ob_decode_scan_param_select_all_column(ObScanParam& org_param,
                                           const ObTableSchema& table_schema,
                                           const ObSchemaManagerV2& schema_mgr,
                                           ObScanParam& decoded_param,
                                           ObMSSchemaDecoderAssis& schema_assis_out) {
  int err = OB_SUCCESS;
  uint64_t table_id = table_schema.get_table_id();
  const ObColumnSchemaV2* column_beg = NULL;
  const ObColumnSchemaV2* cur_column = NULL;
  int32_t column_idx_in_schema_mgr = -1;
  ObString column_name;
  int32_t column_size = 0;
  column_beg = schema_mgr.get_table_schema(table_id, column_size);
  if (NULL == column_beg || 0 >= column_size) {
    TBSYS_LOG(WARN, "fail to get columns of table [table_name:%s,table_id:%lu,column_size:%d,column_beg:%p]",
              table_schema.get_table_name(), table_id, column_size, column_beg);
    err = OB_SCHEMA_ERROR;
  }
  if (OB_SUCCESS == err) {
    decoded_param.set(table_id, OB_SCAN_PARAM_EMPTY_TABLE_NAME, *org_param.get_range());
  }
  if (OB_SUCCESS == err) {
    int32_t got_cell_num = 0;
    for (int32_t i = 0; OB_SUCCESS == err && i < column_size; i ++) {
      cur_column = schema_mgr.get_column_schema(column_beg[i].get_table_id(), column_beg[i].get_id(),
                                                &column_idx_in_schema_mgr);
      if (NULL == cur_column
          || 0 > column_idx_in_schema_mgr
          || column_idx_in_schema_mgr >= OB_MAX_COLUMN_NUMBER) {
        TBSYS_LOG(ERROR, "unexpected error, fail to get column schema [table_id:%lu,column_id:%lu,idx:%d]",
                  column_beg[i].get_table_id(), column_beg[i].get_id(), i);
        err = OB_ERR_UNEXPECTED;
      }
      /// make sure every column should add only once
      if ((OB_SUCCESS == err)
          && (ObMSSchemaDecoderAssis::INVALID_IDX
              == schema_assis_out.column_idx_in_org_param_[column_idx_in_schema_mgr])) {
        schema_assis_out.column_idx_in_org_param_[column_idx_in_schema_mgr] = got_cell_num;;
        got_cell_num ++;
        err = decoded_param.add_column(column_beg[i].get_id());
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to add column to decoded param [err:%d]", err);
        } else {
          /// for result process
          column_name.assign(const_cast<char*>(column_beg[i].get_name()),
                             strlen(column_beg[i].get_name()));
          err = org_param.add_column(column_name);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to add column to org param [err:%d]", err);
          }
        }
      }
    }
  }
  return err;
}

int ob_decode_scan_param_basic_info(const ObScanParam& org_param,
                                    const ObTableSchema& table_schema,
                                    const ObSchemaManagerV2& schema_mgr,
                                    ObScanParam& decoded_param,
                                    ObMSSchemaDecoderAssis& schema_assis_out) {
  int err = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  const ObColumnSchemaV2* column_info = NULL;
  table_id = table_schema.get_table_id();
  ObString table_name;
  table_name.assign(const_cast<char*>(table_schema.get_table_name()), strlen(table_schema.get_table_name()));
  if (OB_SUCCESS == err) {
    decoded_param.set(table_id, OB_SCAN_PARAM_EMPTY_TABLE_NAME, *org_param.get_range());
  }
  int32_t got_cell_num = 0;
  if ((OB_SUCCESS == err)
      && (org_param.get_column_name_size() == 1)
      && (org_param.get_column_name()[0] == ObGroupByParam::COUNT_ROWS_COLUMN_NAME)) {
    int32_t column_count = 0;
    int32_t column_idx_in_schema_mgr = 0;
    const ObColumnSchemaV2* table_columns =  NULL;
    table_columns = schema_mgr.get_table_schema(table_schema.get_table_id(), column_count);
    if ((NULL != table_columns) && (0 < column_count)) {
      column_info = schema_mgr.get_column_schema(table_columns[0].get_table_id(),
                                                 table_columns[0].get_id(),
                                                 &column_idx_in_schema_mgr);
      if (NULL != column_info
          && 0 <= column_idx_in_schema_mgr
          && column_idx_in_schema_mgr < OB_MAX_COLUMN_NUMBER) {
        schema_assis_out.column_idx_in_org_param_[column_idx_in_schema_mgr] = got_cell_num;
        got_cell_num ++;
        err = decoded_param.add_column(column_info->get_id());
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to add column to decoded param [err:%d]", err);
        }
      } else {
        TBSYS_LOG(WARN, "fail to decode column name to column id [table_id:%lu,column_id:%lu]",
                  table_columns[0].get_table_id(), table_columns[0].get_id());
        err = OB_SCHEMA_ERROR;
      }
    } else {
      TBSYS_LOG(WARN, "fail to get column infos [table_name:%.*s, column_count:%d]",
                org_param.get_table_name().length(), org_param.get_table_name().ptr(),
                column_count);
    }
  } else if (OB_SUCCESS == err) {
    int32_t column_idx_in_schema_mgr = 0;
    for (int64_t cell_idx = 0; OB_SUCCESS == err && cell_idx < org_param.get_column_name_size(); cell_idx ++) {
      column_info = schema_mgr.get_column_schema(table_name, org_param.get_column_name()[cell_idx],
                                                 &column_idx_in_schema_mgr);
      if (NULL == column_info
          || 0 > column_idx_in_schema_mgr
          || column_idx_in_schema_mgr >= OB_MAX_COLUMN_NUMBER) {
        TBSYS_LOG(WARN, "fail to decode column name to column id [table_name:%.*s,column_name:%.*s]",
                  org_param.get_table_name().length(), org_param.get_table_name().ptr(),
                  org_param.get_column_name()[cell_idx].length(), org_param.get_column_name()[cell_idx].ptr());
        err = OB_SCHEMA_ERROR;
      } else {
        if (ObMSSchemaDecoderAssis::INVALID_IDX
            == schema_assis_out.column_idx_in_org_param_[column_idx_in_schema_mgr]) {
          schema_assis_out.column_idx_in_org_param_[column_idx_in_schema_mgr] = got_cell_num;
        }
        got_cell_num ++;
        err = decoded_param.add_column(column_info->get_id());
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to add column to decoded param [err:%d]", err);
        }
      }
    }
  }
  return err;
}

int ob_decode_scan_param_filter_info(const ObScanParam& org_param,
                                     const ObTableSchema& table_schema,
                                     const ObSchemaManagerV2& schema_mgr,
                                     ObScanParam& decoded_param,
                                     const ObMSSchemaDecoderAssis& schema_assis_in) {
  int err = OB_SUCCESS;
  const ObRange* range = org_param.get_range();
  if (NULL == range) {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "check scan param range failed:range[%p]", range);
  } else if (false == range->check()) {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "%s", "check param range rowkey and border flag failed");
  } else {
    const ObSimpleFilter& org_filter  = org_param.get_filter_info();
    ObSimpleFilter& decoded_filter = decoded_param.get_filter_info();
    int64_t cond_count = org_filter.get_count();
    uint64_t column_id = OB_INVALID_ID;
    const ObColumnSchemaV2* column_info = NULL;
    const ObSimpleCond*   cond = NULL;
    ObString table_name;
    table_name.assign(const_cast<char*>(table_schema.get_table_name()), strlen(table_schema.get_table_name()));
    for (int64_t i = 0; OB_SUCCESS == err && i < cond_count; i++) {
      int32_t column_idx_in_schema_mgr = 0;
      cond = org_filter[i];
      if (NULL == cond) {
        TBSYS_LOG(ERROR, "unexpected error [org_filter.get_count():%ld,idx:%ld,cond:%p]", cond_count,
                  i, cond);
        err = OB_ERR_UNEXPECTED;
      } else {
        column_info = schema_mgr.get_column_schema(table_name, cond->get_column_name(),
                                                   &column_idx_in_schema_mgr);
        if (NULL == column_info
            || 0 > column_idx_in_schema_mgr
            || column_idx_in_schema_mgr >= OB_MAX_COLUMN_NUMBER) {
          TBSYS_LOG(WARN, "fail to decode column name to column id [table_name:%.*s,column_name:%.*s]",
                    org_param.get_table_name().length(), org_param.get_table_name().ptr(),
                    cond->get_column_name().length(), cond->get_column_name().ptr());
          err = OB_SCHEMA_ERROR;
        } else if (false == cond->check(column_info->get_type())) {
          TBSYS_LOG(WARN, "fail to check filter condition [table_name:%.*s,column_name:%.*s]",
                    org_param.get_table_name().length(), org_param.get_table_name().ptr(),
                    cond->get_column_name().length(), cond->get_column_name().ptr());
          err = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == err) {
        column_id = column_info->get_id();
        int32_t column_idx_in_scan_param
          = schema_assis_in.column_idx_in_org_param_[column_idx_in_schema_mgr];
        if (column_idx_in_scan_param >= decoded_param.get_column_id_size()
            || column_idx_in_scan_param < 0) {
          TBSYS_LOG(WARN, "filter column not in basic info [column_name:%.*s]", cond->get_column_name().length(),
                    cond->get_column_name().ptr());
          err = OB_INVALID_ARGUMENT;
        } else {
          err = decoded_filter.add_cond(column_idx_in_scan_param,
                                        cond->get_logic_operator(), cond->get_right_operand());
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to add condition to decoded param [err:%d]", err);
          }
        }
      }
    }
  }
  return err;
}

int ob_decode_scan_param_groupby_info(const ObScanParam& org_param,
                                      const ObTableSchema& table_schema,
                                      const ObSchemaManagerV2& schema_mgr,
                                      ObScanParam& decoded_param,
                                      const ObMSSchemaDecoderAssis& schema_assis_in) {
  int err = OB_SUCCESS;
  ObString table_name;
  table_name.assign(const_cast<char*>(table_schema.get_table_name()), strlen(table_schema.get_table_name()));
  const ObGroupByParam& org_groupby_param = org_param.get_group_by_param();
  ObGroupByParam& decoded_groupby_param = decoded_param.get_group_by_param();
  const ObArrayHelper<ObGroupByParam::ColumnInfo>& groupby_columns = org_groupby_param.get_groupby_columns();
  const ObArrayHelper<ObGroupByParam::ColumnInfo>& return_columns  = org_groupby_param.get_return_columns();
  const ObArrayHelper<ObAggregateColumn>&           aggregate_columns = org_groupby_param.get_aggregate_columns();
  const ObColumnSchemaV2* column_info = NULL;
  uint64_t column_id = OB_INVALID_ID;
  if (org_groupby_param.get_aggregate_row_width() > 0) {
    int32_t column_idx_in_schema_mgr = -1;
    for (int64_t i = 0; OB_SUCCESS == err && i < groupby_columns.get_array_index(); i++) {
      column_info = schema_mgr.get_column_schema(table_name, groupby_columns.at(i)->column_name_,
                                                 &column_idx_in_schema_mgr);
      if (NULL == column_info
          || 0 > column_idx_in_schema_mgr
          || column_idx_in_schema_mgr >= OB_MAX_COLUMN_NUMBER) {
        TBSYS_LOG(WARN, "fail to decode column name to column id [table_name:%.*s,column_name:%.*s]",
                  org_param.get_table_name().length(), org_param.get_table_name().ptr(),
                  groupby_columns.at(i)->column_name_.length(), groupby_columns.at(i)->column_name_.ptr());
        err = OB_SCHEMA_ERROR;
      } else {
        column_id = column_info->get_id();
        int64_t column_idx_in_scan_param =
          schema_assis_in.column_idx_in_org_param_[column_idx_in_schema_mgr];
        if (column_idx_in_scan_param >= decoded_param.get_column_id_size()
            || column_idx_in_scan_param < 0) {
          TBSYS_LOG(WARN, "group by column not in basic info [column_name:%.*s]", groupby_columns.at(i)->column_name_.length(),
                    groupby_columns.at(i)->column_name_.ptr());
          err = OB_INVALID_ARGUMENT;
        } else {
          err = decoded_groupby_param.add_groupby_column(column_idx_in_scan_param);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to add group by column to decoded param [err:%d]", err);
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCCESS == err && i <  return_columns.get_array_index(); i ++) {
      column_info = schema_mgr.get_column_schema(table_name, return_columns.at(i)->column_name_,
                                                 &column_idx_in_schema_mgr);
      if (NULL == column_info
          || 0 > column_idx_in_schema_mgr
          || column_idx_in_schema_mgr >= OB_MAX_COLUMN_NUMBER) {
        TBSYS_LOG(WARN, "fail to decode column name to column id [table_name:%.*s,column_name:%.*s]",
                  org_param.get_table_name().length(), org_param.get_table_name().ptr(),
                  return_columns.at(i)->column_name_.length(), return_columns.at(i)->column_name_.ptr());
        err = OB_SCHEMA_ERROR;
      } else {
        column_id = column_info->get_id();
        int64_t column_idx_in_scan_param
          =  schema_assis_in.column_idx_in_org_param_[column_idx_in_schema_mgr];
        if (column_idx_in_scan_param >= decoded_param.get_column_id_size()
            || column_idx_in_scan_param < 0) {
          TBSYS_LOG(WARN, "return column not in basic info [column_name:%.*s]", return_columns.at(i)->column_name_.length(),
                    return_columns.at(i)->column_name_.ptr());
          err = OB_INVALID_ARGUMENT;
        } else {
          err = decoded_groupby_param.add_return_column(column_idx_in_scan_param);
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to add return column to decoded param [err:%d]", err);
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCCESS == err && i < aggregate_columns.get_array_index(); i++) {
      column_info = schema_mgr.get_column_schema(table_name, aggregate_columns.at(i)->get_org_column_name(),
                                                 &column_idx_in_schema_mgr);
      if ((NULL == column_info || column_idx_in_schema_mgr < 0
           || column_idx_in_schema_mgr >= OB_MAX_COLUMN_NUMBER)
          && (aggregate_columns.at(i)->get_org_column_name() != ObGroupByParam::COUNT_ROWS_COLUMN_NAME
              || aggregate_columns.at(i)->get_func_type() != COUNT)) {
        TBSYS_LOG(WARN, "fail to decode column name to column id [table_name:%.*s,column_name:%.*s]",
                  org_param.get_table_name().length(), org_param.get_table_name().ptr(),
                  aggregate_columns.at(i)->get_org_column_name().length(), aggregate_columns.at(i)->get_org_column_name().ptr());
        err = OB_SCHEMA_ERROR;
      } else {
        int64_t column_idx_in_scan_param = 0;
        if ((aggregate_columns.at(i)->get_org_column_name() != ObGroupByParam::COUNT_ROWS_COLUMN_NAME
             || aggregate_columns.at(i)->get_func_type() != COUNT)) {
          column_idx_in_scan_param = schema_assis_in.column_idx_in_org_param_[column_idx_in_schema_mgr];
        }
        if (column_idx_in_scan_param >= decoded_param.get_column_id_size()
            || column_idx_in_scan_param < 0) {
          TBSYS_LOG(WARN, "aggregate column not in basic info [column_name:%.*s]",
                    aggregate_columns.at(i)->get_org_column_name().length(),
                    aggregate_columns.at(i)->get_org_column_name().ptr());
          err = OB_INVALID_ARGUMENT;
        } else {
          err = decoded_groupby_param.add_aggregate_column(column_idx_in_scan_param,
                                                           aggregate_columns.at(i)->get_func_type());
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to add aggregate column to decoded param [err:%d]", err);
          }
        }
      }
    }
  }
  return err;
}

int ob_decode_scan_param_orderby_info(const ObScanParam& org_param,
                                      const ObTableSchema& table_schema,
                                      const ObSchemaManagerV2& schema_mgr,
                                      ObScanParam& decoded_param,
                                      const ObMSSchemaDecoderAssis& schema_assis_in) {
  int err = OB_SUCCESS;
  int64_t orderby_count = 0;
  ObString const* orderby_columns = NULL;
  uint8_t  const* order_desc = NULL;
  const ObColumnSchemaV2* column_info = NULL;
  uint64_t column_id = OB_INVALID_ID;
  ObString table_name;
  table_name.assign(const_cast<char*>(table_schema.get_table_name()), strlen(table_schema.get_table_name()));
  org_param.get_orderby_column(orderby_columns, order_desc, orderby_count);
  for (int64_t i = 0; OB_SUCCESS == err && i < orderby_count; i++) {
    if (org_param.get_group_by_param().get_aggregate_row_width() > 0) {
      int64_t column_idx_in_groupby_result = org_param.get_group_by_param().find_column(orderby_columns[i]);
      if (column_idx_in_groupby_result < 0) {
        TBSYS_LOG(WARN, "order by column was not in groupby columns [column_name:%.*s]",
                  orderby_columns[i].length(), orderby_columns[i].ptr());
        err = OB_INVALID_ARGUMENT;
      } else {
        err = decoded_param.add_orderby_column(column_idx_in_groupby_result, static_cast<ObScanParam::Order>(order_desc[i]));
        if (OB_SUCCESS != err) {
          TBSYS_LOG(WARN, "fail to add orderby column to decoded param [err:%d]", err);
        }
      }
    } else {
      int32_t column_idx_in_schema_mgr = 0;
      column_info = schema_mgr.get_column_schema(table_name, orderby_columns[i],
                                                 &column_idx_in_schema_mgr);
      if (NULL == column_info
          || 0 > column_idx_in_schema_mgr
          || column_idx_in_schema_mgr >= OB_MAX_COLUMN_NUMBER) {
        TBSYS_LOG(WARN, "fail to decode column name to column id [table_name:%.*s,column_name:%.*s]",
                  org_param.get_table_name().length(), org_param.get_table_name().ptr(),
                  orderby_columns[i].length(), orderby_columns[i].ptr());
        err = OB_SCHEMA_ERROR;
      } else {
        column_id = column_info->get_id();
        int64_t column_idx_in_scan_param
          = schema_assis_in.column_idx_in_org_param_[column_idx_in_schema_mgr];
        if (column_idx_in_scan_param >= decoded_param.get_column_id_size()
            || column_idx_in_scan_param < 0) {
          TBSYS_LOG(WARN, "orderby column not in basic info [table_name:%.*s, column_name:%.*s]",
                    org_param.get_table_name().length(), org_param.get_table_name().ptr(),
                    orderby_columns[i].length(), orderby_columns[i].ptr());
          err = OB_INVALID_ARGUMENT;
        } else {
          err = decoded_param.add_orderby_column(column_idx_in_scan_param,
                                                 static_cast<ObScanParam::Order>(order_desc[i]));
          if (OB_SUCCESS != err) {
            TBSYS_LOG(WARN, "fail to add orderby column to decoded param [err:%d]", err);
          }
        }
      }
    }
    if (ObScanParam::ASC != order_desc[i]
        && ObScanParam::DESC != order_desc[i]) {
      TBSYS_LOG(WARN, "unrecogonized order [idx:%ld,order:%hhu]", i, order_desc[i]);
      err = OB_INVALID_ARGUMENT;
    }
  }
  return err;
}

bool check_scan_param_compatibility(const ObScanParam& scan_param_in) {
  bool result = true;
  if (ObScanParam::BACKWARD == scan_param_in.get_scan_direction()) {
    if (scan_param_in.get_orderby_column_size() > 0) {
      TBSYS_LOG(WARN, "backward scan do not support orderby operation");
      result = false;
    }
    if (result  && scan_param_in.get_group_by_param().get_aggregate_row_width() > 0) {
      TBSYS_LOG(WARN, "backward scan do not support grouby operation");
      result = false;
    }
  }
  return result;
}
}

int sb::mergeserver::ob_decode_get_param(const sb::common::ObGetParam& org_param,
                                         const sb::common::ObSchemaManagerV2& schema_mgr,
                                         sb::common::ObGetParam& decoded_param) {
  int err = OB_SUCCESS;
  ObCellInfo cell;
  const ObCellInfo* cur_cell = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  decoded_param.reset();
  ObReadParam& read_param = decoded_param;
  read_param = org_param;
  for (int64_t i = 0; OB_SUCCESS == err && i < org_param.get_cell_size(); i++) {
    cur_cell = org_param[i];
    if (NULL == cur_cell) {
      TBSYS_LOG(WARN, "unexpected error [idx:%ld,org_param.get_cell_size():%ld,cur_cell:%p]",
                i, org_param.get_cell_size(), cur_cell);
      err = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS == err) {
      column_schema = schema_mgr.get_column_schema(cur_cell->table_name_, cur_cell->column_name_);
      if (NULL == column_schema) {
        TBSYS_LOG(WARN, "fail to decode column [table_name:%.*s,column_name:%.*s]",
                  cur_cell->table_name_.length(), cur_cell->table_name_.ptr(),
                  cur_cell->column_name_.length(), cur_cell->column_name_.ptr()
                 );
        err = OB_SCHEMA_ERROR;
      }
    }
    if (OB_SUCCESS == err) {
      cell.column_id_ = column_schema->get_id();
      cell.table_id_ = column_schema->get_table_id();
      cell.row_key_ = cur_cell->row_key_;
      err = decoded_param.add_cell(cell);
      if (OB_SUCCESS != err) {
        TBSYS_LOG(WARN, "fail to add cell to decoded param [err:%d]", err);
      }
    }
  }
  return err;
}

int sb::mergeserver::ob_decode_scan_param(ObScanParam& org_param,
                                          const ObSchemaManagerV2& schema_mgr,
                                          ObScanParam& decoded_param) {
  int err = OB_SUCCESS;
  decoded_param.reset();
  ObReadParam& read_param = decoded_param;
  const ObTableSchema* table_schema = NULL;
  read_param = org_param;
  const ObString& table_name = org_param.get_table_name();
  table_schema = schema_mgr.get_table_schema(table_name);
  ObMSSchemaDecoderAssis* schema_assis = GET_TSI_MULT(ObMSSchemaDecoderAssis, SCHEMA_DECODER_ASSIS_ID);
  if (NULL == table_schema) {
    TBSYS_LOG(WARN, "fail to decode table name to table id [table_name:%.*s]",
              org_param.get_table_name().length(), org_param.get_table_name().ptr());
    err = OB_SCHEMA_ERROR;
  }
  if (OB_SUCCESS == err) {
    if (NULL == schema_assis) {
      TBSYS_LOG(WARN, "fail to allocate memory for ObMSSchemaDecoderAssis");
      err = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      schema_assis->init();
    }
  }
  if ((OB_SUCCESS == err)
      && (!check_scan_param_compatibility(org_param))) {
    TBSYS_LOG(WARN, "scan param compatibility check fail");
    err = OB_INVALID_ARGUMENT;
  }
  /// decode basic info
  if (OB_SUCCESS == err) {
    if (org_param.get_column_name_size() == 0) {
      /// select *
      err = ob_decode_scan_param_select_all_column(org_param, *table_schema, schema_mgr,
                                                   decoded_param, *schema_assis);
    } else {
      err = ob_decode_scan_param_basic_info(org_param, *table_schema, schema_mgr,
                                            decoded_param, *schema_assis);
    }
  }
  /// decode filter info
  if (OB_SUCCESS == err) {
    err = ob_decode_scan_param_filter_info(org_param, *table_schema, schema_mgr,
                                           decoded_param, *schema_assis);
  }
  /// decode group by info
  if (OB_SUCCESS == err) {
    err = ob_decode_scan_param_groupby_info(org_param, *table_schema, schema_mgr,
                                            decoded_param, *schema_assis);
  }
  /// decode order by info
  if (OB_SUCCESS == err) {
    err = ob_decode_scan_param_orderby_info(org_param, *table_schema, schema_mgr,
                                            decoded_param, *schema_assis);
  }
  /// decode limit info
  if (OB_SUCCESS == err) {
    int64_t limit_offset = -1;
    int64_t limit_count = -1;
    org_param.get_limit_info(limit_offset, limit_count);
    err = decoded_param.set_limit_info(limit_offset, limit_count);
    if (OB_SUCCESS != err) {
      TBSYS_LOG(WARN, "fail to set limit info [err:%d]", err);
    }
  }
  /// decode scan flag
  if (OB_SUCCESS == err) {
    decoded_param.set_scan_flag(org_param.get_scan_flag());
    if ((ObScanParam::PREREAD != org_param.get_read_mode())
        && (ObScanParam::SYNCREAD != org_param.get_read_mode())) {
      TBSYS_LOG(WARN, "unrecogonized read_mode [org_param.get_scan_flag().read_mode_:%ld]",
                org_param.get_scan_flag().read_mode_);
      err = OB_INVALID_ARGUMENT;
    }
    if ((OB_SUCCESS == err)
        && (ObScanParam::FORWARD != org_param.get_scan_direction())
        && (ObScanParam::BACKWARD != org_param.get_scan_direction())) {
      TBSYS_LOG(WARN, "unrecogonized scan_direction [org_param.get_scan_direction():%ld]",
                org_param.get_scan_direction());
      err = OB_INVALID_ARGUMENT;
    }
  }
  return err;
}


