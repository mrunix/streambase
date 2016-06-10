/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include "nameserver/balance_candidate_server_manager2.h"
namespace sb {
using namespace common;
namespace nameserver {
ObCandidateServerByDiskManager::ObCandidateServerByDiskManager(): length_(0) {
}
void ObCandidateServerByDiskManager::insert(const int32_t index, ObServerStatus* server_status) {
  if (index < CANDIDATE_SERVER_COUNT) {
    for (int32_t i = length_; i > index; --i) {
      if (i >= CANDIDATE_SERVER_COUNT) continue;
      data_holder[i] = data_holder[i - 1];
    }
    data_holder[index] = server_status;
    if (length_ < CANDIDATE_SERVER_COUNT) length_++;
  }
}
void ObCandidateServerByDiskManager::reset() {
  length_ = 0;
}
//we will only keep the top CANDIDATE_SERVER_COUNT
int ObCandidateServerByDiskManager::add_server(ObServerStatus* server_status) {
  int ret = OB_ERROR;
  if (server_status != NULL && server_status->disk_info_.get_capacity() != 0
      && server_status->status_ != ObServerStatus::STATUS_DEAD) {
    ret = OB_SUCCESS;
    int insert_pos = 0;
    for (int32_t i = 0; i < length_; ++i) {
      if (server_status->disk_info_.get_percent() >= 0 && server_status->disk_info_.get_percent() < data_holder[i]->disk_info_.get_percent()) {
        break;
      }
      insert_pos++;
    }
    insert(insert_pos, server_status);
  }
  return ret;
}
int32_t ObCandidateServerByDiskManager::get_length() const {
  return length_;
}
ObServerStatus* ObCandidateServerByDiskManager::get_server(const int32_t index) {
  ObServerStatus* ret = NULL;
  if (index < length_) {
    ret = data_holder[index];
  }
  return ret;
}
ObCandidateServerBySharedManager2::ObCandidateServerBySharedManager2() {
  shared_infos_.init(CANDIDATE_SERVER_COUNT, data_holder_);
}
ObCandidateServerBySharedManager2::effectiveServer::effectiveServer() {
  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
    server_indexes_[i] = OB_INVALID_INDEX;
  }
}
bool ObCandidateServerBySharedManager2::effectiveServer::is_not_in(int32_t server_index) const {
  bool ret = true;
  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; ++i) {
    if (server_indexes_[i] != OB_INVALID_INDEX && server_indexes_[i] == server_index) {
      ret = false;
      break;
    }
  }
  return ret;
}
ObCandidateServerBySharedManager2::sharedInfo::sharedInfo():
  server_index_(OB_INVALID_INDEX), shared_count_(0) {
}
bool ObCandidateServerBySharedManager2::sharedInfo::operator < (const sharedInfo& rv) const {
  return shared_count_ < rv.shared_count_;
}
void ObCandidateServerBySharedManager2::set_effective_server(const effectiveServer& effective_server) {
  effective_server_ = effective_server;
}
void ObCandidateServerBySharedManager2::init(const effectiveServer& effective_server, const ObChunkServerManager* server_manager) {
  effective_server_ = effective_server;
  shared_infos_.init(CANDIDATE_SERVER_COUNT, data_holder_);
  sharedInfo tmp_info;
  if (server_manager != NULL) {
    ObChunkServerManager::const_iterator it = server_manager->begin();
    ObChunkServerManager::const_iterator start_it = server_manager->begin();
    ObChunkServerManager::const_iterator end_it = server_manager->end();
    int32_t index = 0;
    for (; it != end_it; ++it) {
      if (it->status_ == ObServerStatus::STATUS_DEAD) continue;
      index = it - start_it;
      if (effective_server_.is_not_in(index)) {
        tmp_info.server_index_ = index;
        shared_infos_.push_back(tmp_info);
      }
    }
  }
  return;
}
const ObCandidateServerBySharedManager2::sharedInfo* ObCandidateServerBySharedManager2::begin() const {
  return data_holder_;
}
const ObCandidateServerBySharedManager2::sharedInfo* ObCandidateServerBySharedManager2::end() const {
  return data_holder_ + shared_infos_.get_array_index();
}
ObCandidateServerBySharedManager2::sharedInfo* ObCandidateServerBySharedManager2::begin() {
  return data_holder_;
}
ObCandidateServerBySharedManager2::sharedInfo* ObCandidateServerBySharedManager2::end() {
  return data_holder_ + shared_infos_.get_array_index();
}
void ObCandidateServerBySharedManager2::sort() {
  std::sort(begin(), end());
}
ObCandidateServerBySharedManager2::sharedInfo* ObCandidateServerBySharedManager2::find(int32_t server_index) {
  sharedInfo* ret = NULL;
  for (int32_t i = 0; i < shared_infos_.get_array_index(); ++i) {
    if (data_holder_[i].server_index_ == server_index) {
      ret = &data_holder_[i];
      break;
    }
  }
  return ret;
}
void ObCandidateServerBySharedManager2::scan_root_meta(NameTable::const_iterator it) {
  bool add_it = false;
  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++) {
    if (!effective_server_.is_not_in(it->server_info_indexes_[i])) {
      add_it = true;
      break;
    }
  }
  if (add_it) {
    //this range have server in effctive servers
    sharedInfo* shared_info = NULL;
    for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++) {
      if (it->server_info_indexes_[i] != OB_INVALID_INDEX) {
        shared_info = find(it->server_info_indexes_[i]);
        if (shared_info != NULL) {
          shared_info->shared_count_ ++;
        }
      }
    }
  }
  return;
}
//caculate the shared count by scan root table
void ObCandidateServerBySharedManager2::scan_root_table(NameTable* name_table) {
  NameTable::iterator it = name_table->begin();
  for (; it < name_table->end() ; ++it) {
    scan_root_meta(it);
  }
}
}

}

