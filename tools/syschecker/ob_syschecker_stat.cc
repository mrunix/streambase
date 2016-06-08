/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_syschecker_stat.cc for define syschecker statistic API.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_syschecker_stat.h"

namespace sb {
namespace syschecker {
using namespace common;

ObSyscheckerStat::ObSyscheckerStat()
  : write_cell_(0), write_cell_fail_(0), read_cell_(0), read_cell_fail_(0),
    mix_opt_(0), add_opt_(0), add_cell_(0), add_cell_fail_(0), update_opt_(0),
    update_cell_(0), update_cell_fail_(0), insert_opt_(0), insert_cell_(0),
    insert_cell_fail_(0), delete_row_opt_(0), delete_row_(0), delete_row_fail_(0),
    get_opt_(0), get_cell_(0), get_cell_fail_(0), scan_opt_(0), scan_cell_(0),
    scan_cell_fail_(0), total_time_(0), prev_write_cell_(0), prev_write_cell_fail_(0),
    prev_read_cell_(0), prev_get_cell_(0), prev_scan_cell_(0),
    stat_dump_interval_(0), stat_dumper_(this) {

}

ObSyscheckerStat::~ObSyscheckerStat() {

}

int ObSyscheckerStat::init(int64_t stat_dump_interval) {
  int ret = OB_SUCCESS;

  stat_dump_interval_ = stat_dump_interval;
  ret = timer_.init();
  if (OB_SUCCESS == ret) {
    ret = timer_.schedule(stat_dumper_, stat_dump_interval_, false);
  }

  return ret;
}

void ObSyscheckerStat::add_write_cell(const uint64_t write_cell) {
  if (write_cell > 0) {
    atomic_add(&write_cell_, write_cell);
  }
}

void ObSyscheckerStat::add_write_cell_fail(const uint64_t write_cell_fail) {
  if (write_cell_fail > 0) {
    atomic_add(&write_cell_fail_, write_cell_fail);
  }
}

void ObSyscheckerStat::add_read_cell(const uint64_t read_cell) {
  if (read_cell > 0) {
    atomic_add(&read_cell_, read_cell);
  }
}

void ObSyscheckerStat::add_read_cell_fail(const uint64_t read_cell_fail) {
  if (read_cell_fail > 0) {
    atomic_add(&read_cell_fail_, read_cell_fail);
  }
}

void ObSyscheckerStat::add_mix_opt(const uint64_t mix_opt) {
  if (mix_opt > 0) {
    atomic_add(&mix_opt_, mix_opt);
  }
}

void ObSyscheckerStat::add_add_opt(const uint64_t add_opt) {
  if (add_opt > 0) {
    atomic_add(&add_opt_, add_opt);
  }
}

void ObSyscheckerStat::add_add_cell(const uint64_t add_cell) {
  if (add_cell > 0) {
    atomic_add(&add_cell_, add_cell);
  }
}

void ObSyscheckerStat::add_add_cell_fail(const uint64_t add_cell_fail) {
  if (add_cell_fail > 0) {
    atomic_add(&add_cell_fail_, add_cell_fail);
  }
}

void ObSyscheckerStat::add_update_opt(const uint64_t update_opt) {
  if (update_opt > 0) {
    atomic_add(&update_opt_, update_opt);
  }
}

void ObSyscheckerStat::add_update_cell(const uint64_t update_cell) {
  if (update_cell > 0) {
    atomic_add(&update_cell_, update_cell);
  }
}

void ObSyscheckerStat::add_update_cell_fail(const uint64_t update_cell_fail) {
  if (update_cell_fail > 0) {
    atomic_add(&update_cell_fail_, update_cell_fail);
  }
}

void ObSyscheckerStat::add_insert_opt(const uint64_t insert_opt) {
  if (insert_opt > 0) {
    atomic_add(&insert_opt_, insert_opt);
  }
}

void ObSyscheckerStat::add_insert_cell(const uint64_t insert_cell) {
  if (insert_cell > 0) {
    atomic_add(&insert_cell_, insert_cell);
  }
}

void ObSyscheckerStat::add_insert_cell_fail(const uint64_t insert_cell_fail) {
  if (insert_cell_fail > 0) {
    atomic_add(&insert_cell_fail_, insert_cell_fail);
  }
}

void ObSyscheckerStat::add_delete_row_opt(const uint64_t delete_row_opt) {
  if (delete_row_opt > 0) {
    atomic_add(&delete_row_opt_, delete_row_opt);
  }
}

void ObSyscheckerStat::add_delete_row(const uint64_t delete_row) {
  if (delete_row > 0) {
    atomic_add(&delete_row_, delete_row);
  }
}

void ObSyscheckerStat::add_delete_row_fail(const uint64_t delete_row_fail) {
  if (delete_row_fail > 0) {
    atomic_add(&delete_row_fail_, delete_row_fail);
  }
}

void ObSyscheckerStat::add_get_opt(const uint64_t get_opt) {
  if (get_opt > 0) {
    atomic_add(&get_opt_, get_opt);
  }
}

void ObSyscheckerStat::add_get_cell(const uint64_t get_cell) {
  if (get_cell > 0) {
    atomic_add(&get_cell_, get_cell);
  }
}

void ObSyscheckerStat::add_get_cell_fail(const uint64_t get_cell_fail) {
  if (get_cell_fail > 0) {
    atomic_add(&get_cell_fail_, get_cell_fail);
  }
}

void ObSyscheckerStat::add_scan_opt(const uint64_t scan_opt) {
  if (scan_opt > 0) {
    atomic_add(&scan_opt_, scan_opt);
  }
}

void ObSyscheckerStat::add_scan_cell(const uint64_t scan_cell) {
  if (scan_cell > 0) {
    atomic_add(&scan_cell_, scan_cell);
  }
}

void ObSyscheckerStat::add_scan_cell_fail(const uint64_t scan_cell_fail) {
  if (scan_cell_fail > 0) {
    atomic_add(&scan_cell_fail_, scan_cell_fail);
  }
}

void ObSyscheckerStat::dump_stat() {
  printf("run_time: %lus \n"
         "stat_dump_interval: %lds \n"
         "write_cell: %lu \n"
         "write_cell_fail: %lu \n"
         "read_cell: %lu \n"
         "read_cell_miss: %lu \n"
         "mix_opt: %ld \n"
         "add_opt: %lu \n"
         "add_cell: %lu \n"
         "add_cell_fail: %lu \n"
         "update_opt: %lu \n"
         "update_cell: %lu \n"
         "update_cell_fail: %lu \n"
         "insert_opt: %lu \n"
         "insert_cell: %lu \n"
         "insert_cell_fail: %lu \n"
         "delete_row_opt: %lu \n"
         "delete_row: %lu \n"
         "delete_row_fail: %lu \n"
         "get_opt: %lu \n"
         "get_cell: %lu \n"
         "get_cell_fail: %lu \n"
         "scan_opt: %lu \n"
         "scan_cell: %lu \n"
         "scan_cell_fail: %lu \n",
         total_time_,
         stat_dump_interval_ / 1000000,
         write_cell_,
         write_cell_fail_,
         read_cell_,
         read_cell_fail_,
         mix_opt_,
         add_opt_,
         add_cell_,
         add_cell_fail_,
         update_opt_,
         update_cell_,
         update_cell_fail_,
         insert_opt_,
         insert_cell_,
         insert_cell_fail_,
         delete_row_opt_,
         delete_row_,
         delete_row_fail_,
         get_opt_,
         get_cell_,
         get_cell_fail_,
         scan_opt_,
         scan_cell_,
         scan_cell_fail_);
}

void ObSyscheckerStat::dump_stat_cycle() {
  uint64_t write_cell_diff = 0;
  uint64_t write_cell_fail_diff = 0;
  uint64_t read_cell_diff = 0;
  uint64_t get_cell_diff = 0;
  uint64_t scan_cell_diff = 0;
  uint64_t interval_time = stat_dump_interval_ / 1000000;
  char buffer[1024];
  int64_t pos = 0;

  total_time_ += interval_time;
  write_cell_diff = write_cell_ - prev_write_cell_;
  prev_write_cell_ = write_cell_;
  write_cell_fail_diff = write_cell_fail_ - prev_write_cell_fail_;
  prev_write_cell_fail_ = write_cell_fail_;
  read_cell_diff = read_cell_ - prev_read_cell_;
  prev_read_cell_ = read_cell_;
  get_cell_diff = get_cell_ - prev_get_cell_;
  prev_get_cell_ = get_cell_;
  scan_cell_diff = scan_cell_ - prev_scan_cell_;
  prev_scan_cell_ = scan_cell_;

  pos += sprintf(buffer, "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n",
                 "write_i", "write_g", "write_fi", "write_fg",
                 "read_i", "read_g", "get_i", "get_g", "scan_i", "scan_g");
  pos += sprintf(buffer + pos, "%-10lu %-10lu %-10lu %-10lu %-10lu "
                 "%-10lu %-10lu %-10lu %-10lu %-10lu\n\n",
                 write_cell_diff / interval_time, write_cell_ / total_time_,
                 write_cell_fail_diff / interval_time, write_cell_fail_ / total_time_,
                 read_cell_diff / interval_time, read_cell_ / total_time_,
                 get_cell_diff / interval_time, get_cell_ / total_time_,
                 scan_cell_diff / interval_time, scan_cell_ / total_time_);

  printf("\33[2J");
  printf("%s", buffer);
  dump_stat();
}

void ObSyscheckerStat::ObStatDumper::runTimerTask() {
  if (NULL != stat_) {
    stat_->dump_stat_cycle();

    // reschedule
    stat_->timer_.schedule(*this, stat_->get_stat_dump_interval(), false);
  }
}
} // end namespace syschecker
} // end namespace sb
