/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_tmps.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_UPS_TMPS_H_
#define  OCEANBASE_UPDATESERVER_UPS_TMPS_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_trace_log.h"
#include "ob_table_engine.h"
#include "ob_ups_stat.h"
#include "ob_update_server_main.h"

namespace sb {
namespace updateserver {
using namespace common;

template <class Allocator>
bool merge(const TEKey& te_key, TEValue& te_value, Allocator& allocator) {
  typedef hash::ObHashMap<uint64_t, ObCellInfoNode*> column_map_t;
  typedef column_map_t::iterator column_map_iterator_t;
  int64_t timeu = tbsys::CTimeUtil::getTime();

  bool bret = true;
  static __thread column_map_t* column_map = NULL;
  if (NULL == column_map) {
    column_map = GET_TSI(column_map_t);
    if (NULL == column_map
        || 0 != column_map->create(common::OB_MAX_COLUMN_NUMBER)) {
      TBSYS_LOG(ERROR, "column map create fail");
      bret = false;
    }
  } else {
    column_map->clear();
  }

  TEValue new_value = te_value;
  new_value.list_head = NULL;
  new_value.list_tail = NULL;
  new_value.cell_info_cnt = 0;
  new_value.cell_info_size = 0;
  ObCellInfoNode* iter = te_value.list_head;
  while (bret) {
    bret = false;
    if (NULL == iter) {
      TBSYS_LOG(WARN, "te_value list iter null pointer");
    } else if (common::ObExtendType == iter->cell_info.value_.get_type()) {
      if (common::ObActionFlag::OP_DEL_ROW == iter->cell_info.value_.get_ext()
          && te_value.list_head == iter) {
        ObCellInfoNode* new_node = (ObCellInfoNode*)allocator.alloc(sizeof(ObCellInfoNode));
        if (NULL != new_node) {
          *new_node = *iter;
          new_value.cell_info_cnt = 1;
          new_value.cell_info_size = new_node->size();
          new_value.list_head = new_node;
          new_value.list_tail = new_node;
          bret = true;
        }
      } else {
        TBSYS_LOG(WARN, "invalid extend type cellinfo %s %s",
                  te_value.log_str(), print_obj(iter->cell_info.value_));
      }
    } else {
      ObCellInfoNode* cur_node = NULL;
      int hash_ret = column_map->get(iter->cell_info.column_id_, cur_node);
      if (common::hash::HASH_NOT_EXIST == hash_ret) {
        ObCellInfoNode* new_node = (ObCellInfoNode*)allocator.alloc(sizeof(ObCellInfoNode));
        if (NULL != new_node) {
          if (common::hash::HASH_INSERT_SUCC == column_map->set(iter->cell_info.column_id_, new_node, 0)) {
            *new_node = *iter;
            bret = true;
          }
        }
      } else if (common::hash::HASH_EXIST == hash_ret) {
        if (NULL != cur_node) {
          if (common::OB_SUCCESS == cur_node->cell_info.value_.apply(iter->cell_info.value_)) {
            bret = true;
          } else {
            TBSYS_LOG(WARN, "merge cellinfo fail %s %s %s",
                      te_value.log_str(), print_obj(cur_node->cell_info.value_), print_obj(iter->cell_info.value_));
          }
        }
      } else {
        // do nothing
      }
    }

    if (te_value.list_tail == iter) {
      break;
    } else {
      iter = iter->next;
    }
  } // while

  if (bret) {
    column_map_iterator_t map_iter;
    for (map_iter = column_map->begin(); map_iter != column_map->end(); ++map_iter) {
      if (NULL != map_iter->second) {
        if (NULL == new_value.list_head) {
          new_value.list_head = map_iter->second;
          new_value.list_tail = map_iter->second;
          new_value.cell_info_cnt++;
          new_value.cell_info_size += map_iter->second->size();
        } else {
          new_value.list_tail->next = map_iter->second;
          new_value.list_tail = map_iter->second;
          new_value.cell_info_cnt++;
          new_value.cell_info_size += map_iter->second->size();
        }
      } else {
        bret = false;
      }
    }
  }

  timeu = tbsys::CTimeUtil::getTime() - timeu;
  if (bret) {
    TBSYS_TRACE_LOG("merge te_value succ [%s] [%s] ==> [%s] timeu=%ld", te_key.log_str(), te_value.log_str(), new_value.log_str(), timeu);
    new_value.list_tail->next = NULL;
    te_value = new_value;
  } else {
    TBSYS_LOG(WARN, "merge te_value fail %s", te_value.log_str());
  }

  INC_STAT_INFO(UPS_STAT_MERGE_COUNT, 1);
  INC_STAT_INFO(UPS_STAT_MERGE_TIMEU, timeu);

  return bret;
}
}
}

#endif //OCEANBASE_UPDATESERVER_UPS_TMPS_H_



