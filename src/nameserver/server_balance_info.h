/*
 * src/nameserver/.h
 *
 * Copyright (C) 2016 Michael(311155@qq.com). All rights reserved.
 */

/*
 * The definition for .
 *
 * Library: nameserver
 * Package: nameserver
 * Module :
 * Author : Michael(Yang Lifeng), 311155@qq.com
 */

#ifndef OCEANBASE_ROOTSERVER_ROOT_SERVER_BALANCE_INFO_H
#define OCEANBASE_ROOTSERVER_ROOT_SERVER_BALANCE_INFO_H
#include "common/ob_vector.h"
#include "common/page_arena.h"
#include "common/ob_range.h"
#include "common/ob_server.h"
namespace sb {
namespace nameserver {

struct ObBalancePrameter {
  static const int percent_times = 100;
  ObBalancePrameter();
  int16_t disk_high_level_; //0 - 100
  int16_t disk_trigger_level_; // 0 - 100
  int16_t pressure_high_level_; //0 - 100  not use for now
  int16_t pressure_trigger_level_; // 0 - 100 not use for now
  int16_t pressure_low_level_;    // 0 - 100 not use for now
  int16_t disk_adjacent_;       //0 - 100 not use for now
  int16_t pressure_adjacent_;    // 0 - 100 not use for now
  int16_t shared_adjacent_;     // 0 - 100
};
/*
class ObServerRelated
{
  public:
    ObServerRelated(const common::ObServer& s1, const common::ObServer& s2, const int64_t shared_tablet = 0);
    bool operator < (const ObServerRelated&) const;
    //void set_shared_tablet(const int64_t number);
    //int64_t get_shared_tablet() const;
    struct Compare
    {
      inline bool operator() (const ObServerRelated* lv, const ObServerRelated* rv) const
      {
        return *lv < * rv;
      }
    };
    int64_t shared_tablet_;
  private:
    common::ObServer s1_;
    common::ObServer s2_;
};
struct ObTabletPresure
{
  ObTabletPresure();
  common::ObRange range_;
  int64_t pressure_;
  struct Compare
  {
    inline bool operator() (const ObTabletPresure* lv, const ObTabletPresure* rv) const
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
          ret = lv->range_.compare_with_endkey(rv->range_) < 0 ;
        }
      }
      return ret;
    }
  };
};
class ObServerBalanceInfo
{
  public:
    explicit ObServerBalanceInfo(const common::ObServer& server);
    virtual ~ObServerBalanceInfo();

    int64_t get_disk_capacity() const;
    int64_t get_disk_used() const;
    int64_t get_pressure_capacity() const;

    void update_server_info(
        const int64_t disk_capacity,
        const int64_t disk_used,
        const int64_t pressure_capacity);

    void update_tablet_pressure(const ObTabletPresure* tablet_pressure);
    int64_t get_total_pressure() const;
    void clone(const ObServerBalanceInfo&);

    //find pressure by tablet return -1 if not found
    int64_t get_pressure(const common::ObRange& tablet) const;

    struct Compare
    {
      bool operator() (const ObServerBalanceInfo* lv, const ObServerBalanceInfo* rv) const;
    };
    common::ObServer inline get_server() const
    {
      return server_;
    }
  private:
    common::ObServer server_;

  private:
    ObTabletPresure* malloc_tablet_pressure();
    int64_t disk_capacity_;
    int64_t disk_used_;
    int64_t pressure_capacity_;
  private:
    int64_t total_pressure_;
    common::ObSortedVector<ObTabletPresure*> svec_tablet_pressure_;
    common::CharArena allocator_; //this will allocate mem for ObTabletPresure , and obstring
};
class ObServerBalanceInfoManager
{
  public:
    ObServerBalanceInfoManager();
    virtual ~ObServerBalanceInfoManager();
    ObServerBalanceInfo* find(const common::ObServer& server) const;
    ObServerBalanceInfo* add(const common::ObServer& server);
    ObServerBalanceInfo** begin();
    ObServerBalanceInfo** end();
  private:
    common::ObSortedVector<ObServerBalanceInfo*> svec_balance_info_;
    common::CharArena allocator_;
};
class ObServerRelatedInfoManager
{
  public:
    ObServerRelatedInfoManager();
    virtual ~ObServerRelatedInfoManager();
    ObServerRelated* find(const common::ObServer& server1, const common::ObServer& server2);
    ObServerRelated* add(const common::ObServer& server, const common::ObServer& server2, int64_t related_tablet = 0);
  private:
    common::ObSortedVector<ObServerRelated*> svec_related_info_;
    common::CharArena allocator_;
};
*/
class ObServerDiskInfo {
 public:
  ObServerDiskInfo();
  void set_capacity(const int64_t capacity);
  void set_used(const int64_t used);
  int64_t get_capacity() const;
  int64_t get_used() const;
  int32_t get_percent() const;
  void dump() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
 private:
  int64_t capacity_;
  int64_t used_;
};
}
}
#endif

