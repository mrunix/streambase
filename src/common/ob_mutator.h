/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_mutator.h for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_MUTATOR_H__
#define __OCEANBASE_CHUNKSERVER_OB_MUTATOR_H__

#include "ob_read_common_data.h"
#include "ob_string_buf.h"
#include "ob_action_flag.h"
#include "page_arena.h"
#include "ob_update_condition.h"

namespace sb {
namespace tests {
namespace common {
class TestMutatorHelper;
}
}
}

namespace sb {
namespace common {
struct ObMutatorCellInfo {
  ObObj op_type;
  ObCellInfo cell_info;
};

// ObMutator represents a list of cell mutation.
class ObMutator {
  friend class sb::tests::common::TestMutatorHelper;
 public:
  ObMutator();
  virtual ~ObMutator();
  int reset();
 public:
  // Uses ob&db semantic, ob semantic is used by default.
  int use_ob_sem();
  int use_db_sem();

  // Adds update mutation to list
  int update(const ObString& table_name, const ObString& row_key,
             const ObString& column_name, const ObObj& value);
  // Adds insert mutation to list
  int insert(const ObString& table_name, const ObString& row_key,
             const ObString& column_name, const ObObj& value);
  // Adds add mutation to list
  int add(const ObString& table_name, const ObString& row_key,
          const ObString& column_name, const int64_t value);
  int add(const ObString& table_name, const ObString& row_key,
          const ObString& column_name, const float value);
  int add(const ObString& table_name, const ObString& row_key,
          const ObString& column_name, const double value);
  int add_datetime(const ObString& table_name, const ObString& row_key,
                   const ObString& column_name, const ObDateTime& value);
  int add_precise_datetime(const ObString& table_name, const ObString& row_key,
                           const ObString& column_name, const ObPreciseDateTime& value);
  // Adds del_row mutation to list
  int del_row(const ObString& table_name, const ObString& row_key);

  int add_cell(const ObMutatorCellInfo& cell);

 public:
  // get update condition
  const ObUpdateCondition& get_update_condition(void) const;
  ObUpdateCondition& get_update_condition(void);

 public:
  virtual void reset_iter();
  virtual int next_cell();
  virtual int get_cell(ObMutatorCellInfo** cell);

 public:
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
  int64_t get_serialize_size(void) const;

 private:
  struct CellInfoNode {
    ObMutatorCellInfo cell;
    CellInfoNode* next;
  };

  enum IdNameType {
    UNSURE = 0,
    USE_ID = 1,
    USE_NAME = 2,
  };

 private:
  int copy_cell_(const ObMutatorCellInfo& src_cell, ObMutatorCellInfo& dst_cell, int64_t& store_size);
  int add_node_(CellInfoNode* cur_node);

 private:
  int serialize_flag_(char* buf, const int64_t buf_len, int64_t& pos, const int64_t flag) const;
  int64_t get_obj_serialize_size_(const int64_t value, bool is_ext) const;
  int64_t get_obj_serialize_size_(const ObString& str) const;

 private:
  CellInfoNode* list_head_;
  CellInfoNode* list_tail_;
  PageArena<CellInfoNode> page_arena_;
  ObStringBuf str_buf_;
  ObString last_row_key_;
  ObString last_table_name_;
  uint64_t last_table_id_;
  IdNameType id_name_type_;
  int64_t  cell_store_size_;
  CellInfoNode* cur_iter_node_;
  ObUpdateCondition condition_;
  bool has_begin_;
};
}
}

#endif //__OB_MUTATOR_H__


