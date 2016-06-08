/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tsi_factory.h for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_COMMON_TSI_FACTORY_H_
#define  OCEANBASE_COMMON_TSI_FACTORY_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "common/ob_mod_define.h"

namespace sb {
namespace common {
#define GET_TSI(type) get_tsi_fatcory().get_instance<type,1>()
#define GET_TSI_MULT(type, num) get_tsi_fatcory().get_instance<type,num>()

class TSINodeBase {
 public:
  TSINodeBase() : next(NULL) {
  };
  virtual ~TSINodeBase() {
    next = NULL;
  };
  TSINodeBase* next;
};

template <class T>
class TSINode : public TSINodeBase {
 public:
  explicit TSINode(T* instance) : instance_(instance) {
  };
  virtual ~TSINode() {
    if (NULL != instance_) {
      TBSYS_LOG(INFO, "delete instance [%s] %p", typeid(T).name(), instance_);
      instance_->~T();
      instance_ = NULL;
    }
  };
 private:
  T* instance_;
};

class ThreadSpecInfo {
  static const int64_t PAGE_SIZE = 2 * 1024 * 1024;
 public:
  ThreadSpecInfo() : list_(NULL),
    mod_(ObModIds::OB_TSI_FACTORY),
    alloc_(PAGE_SIZE, mod_) {
  };
  ~ThreadSpecInfo() {
    TSINodeBase* iter = list_;
    while (NULL != iter) {
      TSINodeBase* tmp = iter;
      iter = iter->next;
      tmp->~TSINodeBase();
    }
    list_ = NULL;
  };
 public:
  template <class T>
  T* get_instance() {
    T* instance = NULL;
    TSINode<T>* node = NULL;
    void* instance_buffer = alloc_.alloc(sizeof(T));
    void* node_buffer = alloc_.alloc(sizeof(TSINode<T>));
    if (NULL == (instance = new(instance_buffer) T())) {
      TBSYS_LOG(WARN, "new instance [%s] fail", typeid(T).name());
    } else {
      node = new(node_buffer) TSINode<T>(instance);
      if (NULL == node) {
        TBSYS_LOG(WARN, "new tsi_node fail [%s]", typeid(T).name());
        instance->~T();
        instance = NULL;
      } else {
        TBSYS_LOG(DEBUG, "new instance succ [%s] %p size=%ld, tsi=%p", typeid(T).name(), instance, sizeof(T), this);
        if (NULL == list_) {
          list_ = node;
          list_->next = NULL;
        } else {
          node->next = list_;
          list_ = node;
        }
      }
    }
    return instance;
  };
 private:
  TSINodeBase* list_;
  common::ModulePageAllocator mod_;
  common::ModuleArena alloc_;
};

class TSIFactory {
  static const pthread_key_t INVALID_THREAD_KEY = INT32_MAX;
 public:
  TSIFactory() : key_(INVALID_THREAD_KEY) {
  };
  ~TSIFactory() {
  };
 public:
  int init() {
    int ret = common::OB_SUCCESS;
    if (0 != pthread_key_create(&key_, destroy_thread_data_)) {
      TBSYS_LOG(WARN, "pthread_key_create fail errno=%u", errno);
      ret = common::OB_ERROR;
    }
    return ret;
  };
  int destroy() {
    int ret = common::OB_SUCCESS;
    if (INVALID_THREAD_KEY != key_) {
      void* ptr = pthread_getspecific(key_);
      destroy_thread_data_(ptr);
      pthread_key_delete(key_);
      key_ = INVALID_THREAD_KEY;
    }
    return ret;
  };
  template <class T, size_t num>
  T* get_instance() {
    static __thread T* instance = NULL;
    if (NULL == instance && INVALID_THREAD_KEY != key_) {
      ThreadSpecInfo* tsi = (ThreadSpecInfo*)pthread_getspecific(key_);
      if (NULL == tsi) {
        tsi = new(std::nothrow) ThreadSpecInfo();
        if (NULL != tsi) {
          TBSYS_LOG(INFO, "new tsi succ %p key=%d", tsi, key_);
        }
        if (NULL != tsi
            && 0 != pthread_setspecific(key_, tsi)) {
          TBSYS_LOG(WARN, "pthread_setspecific fail errno=%u key=%d", errno, key_);
          delete tsi;
          tsi = NULL;
        }
      }
      if (NULL != tsi) {
        instance = tsi->get_instance<T>();
      }
    }
    return instance;
  };
 private:
  static void destroy_thread_data_(void* ptr) {
    if (NULL != ptr) {
      ThreadSpecInfo* tsi = (ThreadSpecInfo*)ptr;
      delete tsi;
    }
  };
 private:
  pthread_key_t key_;
};

extern TSIFactory& get_tsi_fatcory();
extern void tsi_factory_init();
extern void tsi_factory_destroy();
}
}

#endif //OCEANBASE_COMMON_TSI_FACTORY_H_


