/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_syschecker_stat.h for define syschecker statistic
 * structure.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SYSCHECKER_STAT_H_
#define OCEANBASE_SYSCHECKER_STAT_H_

#include <stdint.h>
#include "common/ob_define.h"
#include "common/ob_atomic.h"
#include "common/ob_timer.h"

namespace sb {
namespace syschecker {
class ObSyscheckerStat {
 public:
  ObSyscheckerStat();
  ~ObSyscheckerStat();

  int init(int64_t stat_dump_interval);

  void add_write_cell(const uint64_t write_cell);
  void add_write_cell_fail(const uint64_t write_cell_fail);

  void add_read_cell(const uint64_t read_cell);
  void add_read_cell_fail(const uint64_t read_cell_fail);

  void add_mix_opt(const uint64_t mix_opt);

  void add_add_opt(const uint64_t add_opt);
  void add_add_cell(const uint64_t add_cell);
  void add_add_cell_fail(const uint64_t add_cell_fail);

  void add_update_opt(const uint64_t update_opt);
  void add_update_cell(const uint64_t update_cell);
  void add_update_cell_fail(const uint64_t update_cell_fail);

  void add_insert_opt(const uint64_t insert_opt);
  void add_insert_cell(const uint64_t insert_cell);
  void add_insert_cell_fail(const uint64_t insert_cell_fail);

  void add_delete_row_opt(const uint64_t delete_row_opt);
  void add_delete_row(const uint64_t delete_row);
  void add_delete_row_fail(const uint64_t delete_row_fail);

  void add_get_opt(const uint64_t get_opt);
  void add_get_cell(const uint64_t get_cell);
  void add_get_cell_fail(const uint64_t get_cell_fail);

  void add_scan_opt(const uint64_t scan_opt);
  void add_scan_cell(const uint64_t scan_cell);
  void add_scan_cell_fail(const uint64_t scan_cell_fail);

  void dump_stat();
  void dump_stat_cycle();

  inline const int64_t get_stat_dump_interval() const {
    return stat_dump_interval_;
  }

 private:
  class ObStatDumper : public common::ObTimerTask {
   public:
    ObStatDumper(ObSyscheckerStat* stat) : stat_(stat) {}
    ~ObStatDumper() {}
   public:
    virtual void runTimerTask();
   private:
    ObSyscheckerStat* stat_;
  };

 private:
  DISALLOW_COPY_AND_ASSIGN(ObSyscheckerStat);

  uint64_t write_cell_;
  uint64_t write_cell_fail_;

  uint64_t read_cell_;
  uint64_t read_cell_fail_;

  uint64_t mix_opt_;

  uint64_t add_opt_;
  uint64_t add_cell_;
  uint64_t add_cell_fail_;

  uint64_t update_opt_;
  uint64_t update_cell_;
  uint64_t update_cell_fail_;

  uint64_t insert_opt_;
  uint64_t insert_cell_;
  uint64_t insert_cell_fail_;

  uint64_t delete_row_opt_;
  uint64_t delete_row_;
  uint64_t delete_row_fail_;

  uint64_t get_opt_;
  uint64_t get_cell_;
  uint64_t get_cell_fail_;

  uint64_t scan_opt_;
  uint64_t scan_cell_;
  uint64_t scan_cell_fail_;

  uint64_t total_time_;  //unit s
  uint64_t prev_write_cell_;
  uint64_t prev_write_cell_fail_;
  uint64_t prev_read_cell_;
  uint64_t prev_get_cell_;
  uint64_t prev_scan_cell_;

  int64_t stat_dump_interval_;  //unit us
  common::ObTimer timer_;
  ObStatDumper stat_dumper_;
};
} // end namespace syschecker
} // end namespace sb

#endif //OCEANBASE_SYSCHECKER_STAT_H_
