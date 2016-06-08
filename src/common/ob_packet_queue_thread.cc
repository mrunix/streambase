/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_packet_queue_thread.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include "ob_packet_queue_thread.h"

using namespace sb::common;

ObPacketQueueThread::ObPacketQueueThread() {
  _stop = 0;
  wait_finish_ = false;
  waiting_ = false;
  handler_ = NULL;
  args_ = NULL;
  queue_.init();
}

ObPacketQueueThread::~ObPacketQueueThread() {
  stop();
}

void ObPacketQueueThread::setThreadParameter(int thread_count, tbnet::IPacketQueueHandler* handler, void* args) {
  setThreadCount(thread_count);
  handler_ = handler;
  args_ = args;
}

void ObPacketQueueThread::stop(bool wait_finish) {
  cond_.lock();
  _stop = true;
  wait_finish_ = wait_finish;
  cond_.broadcast();
  cond_.unlock();
}

bool ObPacketQueueThread::push(ObPacket* packet, int max_queue_len, bool block) {
  if (_stop || _thread == NULL) {
    return true;
  }

  if (max_queue_len > 0 && queue_.size() >= max_queue_len) {
    pushcond_.lock();
    waiting_ = true;
    while (_stop == false && queue_.size() >= max_queue_len && block) {
      pushcond_.wait(1000);
    }
    waiting_ = false;
    if (queue_.size() >= max_queue_len && !block) {
      pushcond_.unlock();
      return false;
    }
    pushcond_.unlock();

    if (_stop) {
      return true;
    }
  }

  cond_.lock();
  queue_.push(packet);
  cond_.unlock();
  cond_.signal();
  return true;
}

void ObPacketQueueThread::pushQueue(ObPacketQueue& packet_queue, int max_queue_len) {
  if (_stop) {
    return;
  }

  if (max_queue_len > 0 && queue_.size() >= max_queue_len) {
    pushcond_.lock();
    waiting_ = true;
    while (_stop == false && queue_.size() >= max_queue_len) {
      pushcond_.wait(1000);
    }
    waiting_ = false;
    pushcond_.unlock();
    if (_stop) {
      return;
    }
  }

  cond_.lock();
  packet_queue.move_to(&queue_);
  cond_.unlock();
  cond_.signal();
}

void ObPacketQueueThread::run(tbsys::CThread* thread, void* args) {
  UNUSED(thread);
  UNUSED(args);
  ObPacket* packet = NULL;
  while (!_stop) {
    cond_.lock();
    while (!_stop && queue_.size() == 0) {
      cond_.wait();
    }
    if (_stop) {
      cond_.unlock();
      break;
    }

    packet = (ObPacket*)queue_.pop();
    cond_.unlock();

    if (waiting_) {
      pushcond_.lock();
      pushcond_.signal();
      pushcond_.unlock();
    }

    if (packet == NULL) continue;

    if (handler_) {
      handler_->handlePacketQueue(packet, args_);
    }
  }

  cond_.lock();
  while (queue_.size() > 0) {
    packet = (ObPacket*)queue_.pop();
    cond_.unlock();
    if (handler_ && wait_finish_) {
      handler_->handlePacketQueue(packet, args_);
    }
    cond_.lock();
  }
  cond_.unlock();
}

void ObPacketQueueThread::clear() {
  _stop = false;
  delete[] _thread;
  _thread = NULL;
}


