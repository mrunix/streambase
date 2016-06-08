/*
 * src/nameserver/.cc
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

#include "nameserver/server_balance_info.h"
namespace {
//const int TABLETS_PER_SERVER_RESERVE = 4000;
//const int SERVER_IN_CLUSTER = 20;
}

namespace sb {
using namespace common;
namespace nameserver {
ObBalancePrameter::ObBalancePrameter() {
  disk_high_level_ = 0;
  disk_trigger_level_ = 0;
  pressure_high_level_ = 0;
  pressure_trigger_level_ = 0;
  pressure_low_level_ = 0;
  disk_adjacent_ = 0;
  pressure_adjacent_ = 0;
  shared_adjacent_ = 0;
}
/*ObServerRelated::ObServerRelated(const common::ObServer& s1, const common::ObServer& s2, const int64_t shared_tablet)
  :shared_tablet_(shared_tablet)
{
  if (s1 < s2)
  {
    s1_ = s1;
    s2_ = s2;
  }
  else
  {
    s1_ = s2;
    s2_ = s1;
  }
}
bool ObServerRelated::operator < (const ObServerRelated& rv) const
{
  bool ret = true;
  if (s1_ == rv.s1_)
  {
    ret = s2_ < rv.s2_;
  }
  else
  {
    ret = s1_ < rv.s1_;
  }
  return ret;
}
//void ObServerRelated::set_shared_tablet(const int64_t number)
//{
//  shared_tablet_ = number;
//}
//int64_t ObServerRelated::get_shared_tablet() const
//{
//  return shared_tablet_;
//}

ObTabletPresure::ObTabletPresure():pressure_(0)
{
}
ObServerBalanceInfo::ObServerBalanceInfo(const ObServer& server ):
  disk_capacity_(0), disk_used_(0),
  pressure_capacity_(0), total_pressure_(0)
{
  server_ = server;
  svec_tablet_pressure_.reserve(TABLETS_PER_SERVER_RESERVE);
}
ObServerBalanceInfo::~ObServerBalanceInfo()
{
  // no need to free,
}
int64_t ObServerBalanceInfo::get_disk_capacity() const
{
  return disk_capacity_;
}
int64_t ObServerBalanceInfo::get_disk_used() const
{
  return disk_used_;
}
int64_t ObServerBalanceInfo::get_pressure_capacity() const
{
  return pressure_capacity_;
}
void ObServerBalanceInfo::update_server_info(
    const int64_t disk_capacity,
    const int64_t disk_used,
    const int64_t pressure_capacity)
{
  disk_capacity_ = disk_capacity;
  disk_used_ = disk_used;
  pressure_capacity_ = pressure_capacity;
}

void ObServerBalanceInfo::update_tablet_pressure(const ObTabletPresure* tablet_pressure)
{
  ObSortedVector<ObTabletPresure*>::iterator it;
  svec_tablet_pressure_.find(tablet_pressure, it, ObTabletPresure::Compare());
  if (it != svec_tablet_pressure_.end())
  {
    total_pressure_ -= (*it)->pressure_;
    (*it)->pressure_ = tablet_pressure->pressure_;
    total_pressure_ += (*it)->pressure_;
  }
  else
  {
    ObTabletPresure* tp = malloc_tablet_pressure();
    if (tp != NULL)
    {
      tp->pressure_ = tablet_pressure->pressure_;
      tp->range_.table_id_ = tablet_pressure->range_.table_id_;
      tp->range_.border_flag_ = tablet_pressure->range_.border_flag_;

      common::ObString::obstr_size_t sk_len = tablet_pressure->range_.start_key_.length();
      char* sk = allocator_.alloc(sk_len);
      memcpy(sk, tablet_pressure->range_.start_key_.ptr(), sk_len);
      tp->range_.start_key_.assign(sk, sk_len);

      common::ObString::obstr_size_t ek_len = tablet_pressure->range_.end_key_.length();
      char* ek = allocator_.alloc(ek_len);
      memcpy(ek, tablet_pressure->range_.end_key_.ptr(), ek_len);
      tp->range_.end_key_.assign(ek, ek_len);

      total_pressure_ += tp->pressure_;
      svec_tablet_pressure_.insert(tp, it, ObTabletPresure::Compare());
    }
    else
    {
      TBSYS_LOG(ERROR, " can not malloc tablet_pressure");
    }
  }
}
int64_t ObServerBalanceInfo::get_total_pressure() const
{
  return total_pressure_;
}

bool ObServerBalanceInfo::Compare::operator() (const ObServerBalanceInfo* lv, const ObServerBalanceInfo* rv) const
{
  bool ret = true;
  if (lv == NULL)
  {
    if (rv == NULL)
    {
      ret = false;
    }
  }
  else
  {
    if (rv == NULL)
    {
      ret = false;
    }
    else
    {
      ret = lv->server_< rv->server_ ;
    }
  }
  return ret;
}

void ObServerBalanceInfo::clone(const ObServerBalanceInfo& rv)
{
  if (svec_tablet_pressure_.size() != 0)
  {
    TBSYS_LOG(ERROR, "you should not do this.");
  }
  else
  {
    disk_capacity_ = rv.disk_capacity_;
    disk_used_ = rv.disk_used_;
    pressure_capacity_ = rv.pressure_capacity_;
    server_ = rv.server_;
    ObSortedVector<ObTabletPresure*>::const_iterator it = rv.svec_tablet_pressure_.begin();
    for (; it != rv.svec_tablet_pressure_.end(); ++it)
    {
      update_tablet_pressure(*it);
    }
  }
}

//find pressure by tablet return -1 if not found
int64_t ObServerBalanceInfo::get_pressure(const common::ObRange& tablet) const
{
  int64_t ret = -1;
  ObTabletPresure tmp;
  tmp.range_ = tablet;
  ObSortedVector<ObTabletPresure*>::const_iterator it =
    svec_tablet_pressure_.lower_bound(&tmp, ObTabletPresure::Compare());
  if (it != svec_tablet_pressure_.end())
  {
    ret = (*it)->pressure_;
  }
  return ret;
}
ObTabletPresure* ObServerBalanceInfo::malloc_tablet_pressure()
{
  char* ptr = allocator_.alloc(sizeof(ObTabletPresure));
  ObTabletPresure* ret = NULL;
  if (ptr != NULL)
  {
    ret = new(ptr) ObTabletPresure();
  }
  return ret;
}
ObServerBalanceInfoManager::ObServerBalanceInfoManager()
{
  svec_balance_info_.reserve(SERVER_IN_CLUSTER);
}
ObServerBalanceInfoManager::~ObServerBalanceInfoManager()
{
}
ObServerBalanceInfo* ObServerBalanceInfoManager::find(const common::ObServer& server) const
{
  ObServerBalanceInfo* ret = NULL;
  ObSortedVector<ObServerBalanceInfo*>::iterator it;
  ObServerBalanceInfo tmp(server);
  svec_balance_info_.find(&tmp, it, ObServerBalanceInfo::Compare());
  if (it != svec_balance_info_.end())
  {
    ret = *it;
  }
  return ret;
}
ObServerBalanceInfo* ObServerBalanceInfoManager::add(const common::ObServer& server)
{
  ObServerBalanceInfo* ret = find(server);
  if (ret == NULL)
  {
    common::ObSortedVector<ObServerBalanceInfo*>::iterator it;
    char* ptr = allocator_.alloc(sizeof(ObServerBalanceInfo));
    if (ptr != NULL)
    {
      ret = new(ptr) ObServerBalanceInfo(server);
      svec_balance_info_.insert(ret, it, ObServerBalanceInfo::Compare());
      ret = *it;
    }
    else
    {
      TBSYS_LOG(ERROR, "can not malloc mem for new ObServerBalanceInfo");
    }
  }
  return ret;
}

ObServerBalanceInfo** ObServerBalanceInfoManager::begin()
{
  return svec_balance_info_.begin();
}
ObServerBalanceInfo** ObServerBalanceInfoManager::end()
{
  return svec_balance_info_.end();
}
ObServerRelatedInfoManager::ObServerRelatedInfoManager()
{
  svec_related_info_.reserve(SERVER_IN_CLUSTER); // this should be C(2,SERVER_IN_CLUSTER) but anyway
}
ObServerRelatedInfoManager::~ObServerRelatedInfoManager()
{
}
ObServerRelated* ObServerRelatedInfoManager::find(const common::ObServer& server1, const common::ObServer& server2)
{
  ObServerRelated* ret = NULL;
  ObSortedVector<ObServerRelated*>::iterator it;
  ObServerRelated tmp(server1, server2);
  svec_related_info_.find(&tmp, it, ObServerRelated::Compare());
  if (it != svec_related_info_.end())
  {
    ret = *it;
  }
  return ret;
}
ObServerRelated* ObServerRelatedInfoManager::add(const common::ObServer& server1, const common::ObServer& server2, int64_t related_tablet )
{
  ObServerRelated* ret = find(server1, server2);
  if (ret == NULL)
  {
    common::ObSortedVector<ObServerRelated*>::iterator it;
    char* ptr = allocator_.alloc(sizeof(ObServerRelated));
    if (ptr != NULL)
    {
      ret = new(ptr)ObServerRelated(server1, server2, related_tablet);
      svec_related_info_.insert(ret, it, ObServerRelated::Compare());
      ret = *it;
    }
    else
    {
      TBSYS_LOG(ERROR, "can not malloc mem for new ObServerRelated");
    }
  }
  else
  {
    ret->shared_tablet_= related_tablet;
  }
  return ret;
}
*/
ObServerDiskInfo::ObServerDiskInfo(): capacity_(0), used_(0) {

}
void ObServerDiskInfo::set_capacity(const int64_t capacity) {
  capacity_ = capacity;
}
void ObServerDiskInfo::set_used(const int64_t used) {
  used_ = used;
}
int64_t ObServerDiskInfo::get_capacity() const {
  return capacity_;
}
int64_t ObServerDiskInfo::get_used() const {
  return used_;
}
int32_t ObServerDiskInfo::get_percent() const {
  int ret = -1;
  if (capacity_ > 0) {
    ret = (int)(used_ * 100 / capacity_);
    if (ret > 100) {
      ret = 100;
    }
  }
  return ret;
}
void ObServerDiskInfo::dump() const {
  TBSYS_LOG(INFO, "disk info capacity = %ld used = %ld", capacity_, used_);
}
DEFINE_SERIALIZE(ObServerDiskInfo) {
  int ret = 0;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, capacity_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::encode_vi64(buf, buf_len, tmp_pos, used_);
  }

  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_DESERIALIZE(ObServerDiskInfo) {
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &capacity_);
  }
  if (OB_SUCCESS == ret) {
    ret = serialization::decode_vi64(buf, data_len, tmp_pos, &used_);
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}
DEFINE_GET_SERIALIZE_SIZE(ObServerDiskInfo) {
  int64_t len = 0;
  len += serialization::encoded_length_vi64(capacity_);
  len += serialization::encoded_length_vi64(used_);
  return len;
}
}
}

