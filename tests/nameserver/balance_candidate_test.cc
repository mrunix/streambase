/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * balance_candidate_test.cc for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */

#include <gtest/gtest.h>

#include "root_server_tester.h"
#include "nameserver/ob_root_server2.h"
#include "common/ob_tablet_info.h"
#include "nameserver/ob_root_meta2.h"
#include "nameserver/ob_root_table2.h"
using namespace sb;
using namespace sb::common;
using namespace sb::nameserver;
TEST(BalanceCandidateTest, ObCandidateServerByDiskManager) {
  ObChunkServerManager server_manager;
  ObServer server1(ObServer::IPV4, "10.10.10.1", 1001);
  ObServer server2(ObServer::IPV4, "10.10.10.2", 1001);
  ObServer server3(ObServer::IPV4, "10.10.10.3", 1001);
  ObServer server4(ObServer::IPV4, "10.10.10.4", 1001);
  ObServer server5(ObServer::IPV4, "10.10.10.5", 1001);
  ObServer server6(ObServer::IPV4, "10.10.10.6", 1001);
  ObServer server7(ObServer::IPV4, "10.10.10.7", 1001);

  server_manager.receive_hb(server1, 1);
  server_manager.receive_hb(server2, 1);
  server_manager.receive_hb(server3, 1);
  server_manager.receive_hb(server4, 1);
  server_manager.receive_hb(server5, 1);
  server_manager.receive_hb(server6, 1);
  server_manager.receive_hb(server7, 1);

  ObServerDiskInfo disk_info;
  disk_info.set_capacity(100);
  disk_info.set_used(10);
  // 5 3 7 4 6 1 2
  server_manager.update_disk_info(server5, disk_info);
  disk_info.set_used(disk_info.get_used() + 5);
  server_manager.update_disk_info(server3, disk_info);
  server_manager.update_disk_info(server7, disk_info);
  disk_info.set_used(disk_info.get_used() + 5);
  server_manager.update_disk_info(server4, disk_info);
  server_manager.update_disk_info(server6, disk_info);
  disk_info.set_used(disk_info.get_used() + 5);
  server_manager.update_disk_info(server1, disk_info);
  disk_info.set_used(disk_info.get_used() + 5);
  server_manager.update_disk_info(server2, disk_info);

  ObCandidateServerByDiskManager disk_manager;

  disk_manager.add_server(server_manager.get_server_status(0));
  disk_manager.add_server(server_manager.get_server_status(1));
  disk_manager.add_server(server_manager.get_server_status(5));
  disk_manager.add_server(server_manager.get_server_status(2));
  disk_manager.add_server(server_manager.get_server_status(3));
  disk_manager.add_server(server_manager.get_server_status(6));
  disk_manager.add_server(server_manager.get_server_status(4));

  ASSERT_TRUE(disk_manager.get_server(0) == server_manager.get_server_status(4)); //5
  ASSERT_TRUE(disk_manager.get_server(1) == server_manager.get_server_status(2)); //3
  ASSERT_TRUE(disk_manager.get_server(2) == server_manager.get_server_status(6)); //7

}
TEST(BalanceCandidateTest, ObCandidateServerBySharedManager2) {
  ObServer server0(ObServer::IPV4, "10.10.10.1", 1001);
  ObServer server1(ObServer::IPV4, "10.10.10.2", 1001);
  ObServer server2(ObServer::IPV4, "10.10.10.3", 1001);
  ObServer server3(ObServer::IPV4, "10.10.10.4", 1001);
  //ObServer server5(ObServer::IPV4, "10.10.10.5", 1001);
  //ObServer server6(ObServer::IPV4, "10.10.10.6", 1001);
  //ObServer server7(ObServer::IPV4, "10.10.10.7", 1001);
  ObChunkServerManager server_manager;

  server_manager.receive_hb(server0, 1);
  server_manager.receive_hb(server1, 1);
  server_manager.receive_hb(server2, 1);
  server_manager.receive_hb(server3, 1);
  //server_manager.receive_hb(server5, 1);
  //server_manager.receive_hb(server6, 1);
  //server_manager.receive_hb(server7, 1);

  ObTabletInfo tablet_info;
  ObTabletInfoManager* info = new ObTabletInfoManager();
  NameTable* name_table = new NameTable(info);

  tablet_info.range_.border_flag_.unset_inclusive_start();
  tablet_info.range_.border_flag_.set_inclusive_end();
  tablet_info.range_.border_flag_.set_min_value();
  tablet_info.range_.border_flag_.set_max_value();

  tablet_info.range_.table_id_ = 1;
  name_table->add(tablet_info, 0, 0);
  name_table->add(tablet_info, 2, 0);  // 0, 2

  tablet_info.range_.table_id_ = 2;
  name_table->add(tablet_info, 1, 0);
  name_table->add(tablet_info, 2, 0);  // 1, 2

  tablet_info.range_.table_id_ = 3;
  name_table->add(tablet_info, 1, 0);
  name_table->add(tablet_info, 3, 0);  // 1,3

  tablet_info.range_.table_id_ = 4;
  name_table->add(tablet_info, 2, 0);
  name_table->add(tablet_info, 3, 0);  // 2, 3

  tablet_info.range_.table_id_ = 5;
  name_table->add(tablet_info, 1, 0);
  name_table->add(tablet_info, 2, 0);  // 1,2

  tablet_info.range_.table_id_ = 6;
  name_table->add(tablet_info, 2, 0);
  name_table->add(tablet_info, 3, 0);  // 2, 3

  ObCandidateServerBySharedManager2 shared_manager;
  ObCandidateServerBySharedManager2::effectiveServer effective_server;
  ObTabletInfoManager* info2 = new ObTabletInfoManager();
  NameTable* root_table2 = new NameTable(info2);
  name_table->sort();
  name_table->shrink_to(root_table2);

  effective_server.server_indexes_[0] = 1;

  shared_manager.init(effective_server, &server_manager);
  shared_manager.scan_root_table(root_table2);
  shared_manager.sort();

  const ObCandidateServerBySharedManager2::sharedInfo* it = shared_manager.begin();
  ASSERT_EQ(0, it->server_index_);
  it++;
  ASSERT_EQ(3, it->server_index_);
  it++;
  ASSERT_EQ(2, it->server_index_);

  effective_server.server_indexes_[0] = 3;
  shared_manager.init(effective_server, &server_manager);
  shared_manager.scan_root_table(root_table2);
  shared_manager.sort();

  it = shared_manager.begin();
  ASSERT_EQ(0, it->server_index_);
  it++;
  ASSERT_EQ(1, it->server_index_);
  it++;
  ASSERT_EQ(2, it->server_index_);

  effective_server.server_indexes_[0] = 0;
  effective_server.server_indexes_[1] = 3;
  shared_manager.init(effective_server, &server_manager);
  shared_manager.scan_root_table(root_table2);
  shared_manager.sort();

  it = shared_manager.begin();
  ASSERT_EQ(1, it->server_index_);
  it++;
  ASSERT_EQ(2, it->server_index_);
  delete name_table;
  delete info;
  delete root_table2;
  delete info2;

}
int main(int argc, char** argv) {
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

