#include "gtest/gtest.h"
#include "ob_malloc.h"
#include "ob_range.h"
#include "ob_read_common_data.h"

using namespace sb;
using namespace common;

int main(int argc, char** argv) {
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TestScanParam: public ::testing::Test {
 public:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};


bool is_equal(const ObScanParam& one, const ObScanParam& two) {
  bool bret = false;
  if (&one == &two) {
    bret = true;
  } else {
    bret = (one.get_table_id() == two.get_table_id())
           && (one.get_table_name() == two.get_table_name())
           && (one.get_is_result_cached() == two.get_is_result_cached())
           && (one.get_is_read_consistency() == two.get_is_read_consistency())
           // && (*one.get_range() == *two.get_range())
           && (one.get_scan_size() == two.get_scan_size())
           && (one.get_scan_direction() == two.get_scan_direction())
           && (one.get_column_name_size() == two.get_column_name_size())
           && (one.get_column_id_size() == two.get_column_id_size())
           && (const_cast<ObScanParam&>(one).get_filter_info() == const_cast<ObScanParam&>(two).get_filter_info())
           && (one.get_orderby_column_size() == two.get_orderby_column_size());

    int64_t limit, limit1, count, count1;
    one.get_limit_info(limit, count);
    two.get_limit_info(limit1, count1);
    bret = bret && (limit == limit1) && (count == count1);
    // not check list items
  }
  return bret;
}

TEST(TestScanParam, set_condition) {
  ObScanParam param;
  param.set_is_result_cached(true);

  // version
  ObVersionRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  range.start_version_ = 11;
  range.end_version_ = 2001;
  param.set_version_range(range);

  // table name
  uint64_t table_id = 123;
  ObString table_name;
  ObRange scan_range;

  ObString key;
  scan_range.table_id_ = 23455;
  char* start_key = "start_row_key_start";
  char* end_key = "end_row_key_end";
  scan_range.border_flag_.set_max_value();
  key.assign(start_key, strlen(start_key));
  scan_range.start_key_ = key;
  key.assign(end_key, strlen(end_key));
  scan_range.end_key_ = key;
  param.set(table_id, table_name, scan_range);

  ObString column;
  char* temp_buff[32];
  for (uint64_t i = 11; i < 21; ++i) {
    temp_buff[i] = new char[32];
    sprintf(temp_buff[i], "%lu_scan_column_%lu", i, i);
    column.assign(temp_buff[i], strlen(temp_buff[i]));
    EXPECT_TRUE(OB_SUCCESS == param.add_column(column));
  }

  ObLogicOperator operate = LIKE;
  char* ptr = "test_test";
  ObString sub_string;
  sub_string.assign(ptr, strlen(ptr));
  ObObj operand;
  operand.set_varchar(sub_string);

  for (uint64_t i = 11; i < 21; ++i) {
    sprintf(temp_buff[i], "%lu_scan_column_%lu", i, i);
    column.assign(temp_buff[i], strlen(temp_buff[i]));
    EXPECT_TRUE(OB_SUCCESS == param.add_cond(column, operate, operand));
  }

  char temp_name[1024] = "";
  for (int64_t i = 22; i < 32; ++i) {
    sprintf(temp_name, "%lu_scan_column_%lu", i, i);
    column.assign(temp_name, strlen(temp_name));
    EXPECT_TRUE(OB_SUCCESS != param.add_cond(column, operate, operand));
  }
}

TEST(TestScanParam, safe_copy) {
  ObScanParam param;
  // cache
  param.set_is_result_cached(true);
  param.set_scan_direction(ObScanParam::BACKWARD);
  // version
  ObVersionRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  range.start_version_ = 11;
  range.end_version_ = 2001;
  param.set_version_range(range);

  // table name
  uint64_t table_id = 123;
  ObString table_name;
  ObRange scan_range;

  char* start_key = "start_row_key_start";
  char* end_key = "end_row_key_end";
  ObString key;
  scan_range.table_id_ = 23455;
  scan_range.border_flag_.set_max_value();
  key.assign(start_key, strlen(start_key));
  scan_range.start_key_ = key;
  key.assign(end_key, strlen(end_key));
  scan_range.end_key_ = key;
  param.set(table_id, table_name, scan_range);
  uint64_t offset = 234;
  uint64_t count = 12345555;
  param.set_limit_info(offset, count);

  for (uint64_t i = 11; i < 21; ++i) {
    EXPECT_TRUE(OB_SUCCESS == param.add_column(i));
  }

  {
    ObScanParam temp_param;
    temp_param.safe_copy(param);
    EXPECT_TRUE(is_equal(temp_param, param));
    temp_param.clear_column();
    EXPECT_FALSE(is_equal(temp_param, param));
  }

  ObScanParam temp_param = param;
  EXPECT_TRUE(is_equal(temp_param, param));
}


TEST(TestScanParam, serialize_int) {
  ObScanParam param;
  // cache
  param.set_is_result_cached(true);
  param.set_is_read_consistency(false);
  param.set_scan_direction(ObScanParam::BACKWARD);
  // version
  ObVersionRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  range.start_version_ = 11;
  range.end_version_ = 2001;
  param.set_version_range(range);

  // table name
  uint64_t table_id = 123;
  ObString table_name;
  ObRange scan_range;

  char* start_key = "start_row_key_start";
  char* end_key = "end_row_key_end";
  ObString key;
  scan_range.table_id_ = 23455;
  scan_range.border_flag_.set_max_value();
  key.assign(start_key, strlen(start_key));
  scan_range.start_key_ = key;
  key.assign(end_key, strlen(end_key));
  scan_range.end_key_ = key;
  param.set(table_id, table_name, scan_range);

  for (uint64_t i = 11; i < 21; ++i) {
    EXPECT_TRUE(OB_SUCCESS == param.add_column(i));
  }

  uint64_t size = 1234;
  param.set_scan_size(size);
  param.set_scan_direction(ObScanParam::BACKWARD);
  for (uint64_t i = 21; i < 33; ++i) {
    if (i % 2) {
      EXPECT_TRUE(OB_SUCCESS == param.add_orderby_column(i, ObScanParam::ASC));
    } else {
      EXPECT_TRUE(OB_SUCCESS == param.add_orderby_column(i, ObScanParam::DESC));
    }
  }

  uint64_t offset = 234;
  uint64_t count = 12345555;
  param.set_limit_info(offset, count);

  // no filter
  size = param.get_serialize_size();
  EXPECT_TRUE(size > 0);

  char* temp = new char[size];
  EXPECT_TRUE(NULL != temp);
  int64_t pos = 0;
  // size small
  EXPECT_TRUE(OB_SUCCESS != param.serialize(temp, size - 1, pos));
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == param.serialize(temp, size, pos));
  EXPECT_TRUE((int64_t)size == pos);

  ObScanParam temp_param;
  int64_t new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS != temp_param.deserialize(temp, size - 1, new_pos));
  new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS == temp_param.deserialize(temp, size, new_pos));
  EXPECT_TRUE(pos == new_pos);

  EXPECT_TRUE(param.get_is_result_cached() == true);
  EXPECT_TRUE(temp_param.get_is_result_cached() == true);
  EXPECT_TRUE(temp_param.get_is_read_consistency() == false);
  EXPECT_TRUE(param.get_is_read_consistency() == false);

  // same as each other
  EXPECT_TRUE(is_equal(temp_param, param));
  EXPECT_TRUE(is_equal(param, temp_param));
  delete temp;
  temp = NULL;
}

TEST(TestScanParam, serialize_string) {
  ObScanParam param;
  // cache
  param.set_is_result_cached(false);
  param.set_scan_direction(ObScanParam::BACKWARD);

  // version
  ObVersionRange range;
  range.border_flag_.set_min_value();
  range.start_version_ = 201;
  range.end_version_ = 1;
  param.set_version_range(range);

  // table name
  char* name = "table_test";
  ObString table_name;
  table_name.assign(name, strlen(name));

  ObRange scan_range;
  char* start_key = "start_row_key_start";
  char* end_key = "end_row_key_end";
  ObString key;

  scan_range.table_id_ = 234;
  scan_range.border_flag_.set_max_value();
  key.assign(start_key, strlen(start_key));
  scan_range.start_key_ = key;
  key.assign(end_key, strlen(end_key));
  scan_range.end_key_ = key;
  param.set(OB_INVALID_ID, table_name, scan_range);

  ObString column;
  char* temp_buff[32];
  for (uint64_t i = 11; i < 21; ++i) {
    temp_buff[i] = new char[32];
    sprintf(temp_buff[i], "%lu_scan_column_%lu", i, i);
    column.assign(temp_buff[i], strlen(temp_buff[i]));
    EXPECT_TRUE(OB_SUCCESS == param.add_column(column));
  }

  uint64_t size = 23;
  param.set_scan_size(size);
  param.set_scan_direction(ObScanParam::FORWARD);
  for (uint64_t i = 21; i < 33; ++i) {
    temp_buff[i] = new char[32];
    sprintf(temp_buff[i], "%lu_order_column_%lu", i, i);
    column.assign(temp_buff[i], strlen(temp_buff[i]));
    if (i % 2) {
      EXPECT_TRUE(OB_SUCCESS == param.add_orderby_column(column, ObScanParam::DESC));
    } else {
      EXPECT_TRUE(OB_SUCCESS == param.add_orderby_column(column, ObScanParam::ASC));
    }
  }

  uint64_t offset = 2340;
  uint64_t count = 1234;
  param.set_limit_info(offset, count);

  // filter
  ObLogicOperator operate = LIKE;
  char* ptr = "test_test";
  ObString sub_string;
  sub_string.assign(ptr, strlen(ptr));
  ObObj operand;
  operand.set_varchar(sub_string);
  ObString column_name;
  char temp_name[1024] = "";
  for (uint64_t i = 11; i < 21; ++i) {
    snprintf(temp_name, sizeof(temp_name), "%lu_scan_column_%lu", i, i);
    column_name.assign(temp_name, strlen(temp_name));
    EXPECT_TRUE(param.add_cond(column_name, operate, operand) == OB_SUCCESS);
  }

  size = param.get_serialize_size();
  EXPECT_TRUE(size > 0);

  char* temp = new char[size];
  EXPECT_TRUE(NULL != temp);
  int64_t pos = 0;
  // size small
  EXPECT_TRUE(OB_SUCCESS != param.serialize(temp, size - 1, pos));
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == param.serialize(temp, size, pos));
  EXPECT_TRUE((int64_t)size == pos);

  ObScanParam temp_param;
  int64_t new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS != temp_param.deserialize(temp, size - 1, new_pos));
  new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS == temp_param.deserialize(temp, size, new_pos));
  EXPECT_TRUE(pos == new_pos);

  EXPECT_TRUE(param.get_is_result_cached() == false);
  EXPECT_TRUE(temp_param.get_is_result_cached() == false);
  EXPECT_TRUE(temp_param.get_is_read_consistency() == true);
  EXPECT_TRUE(param.get_is_read_consistency() == true);

  // same as each other
  EXPECT_TRUE(is_equal(temp_param, param));
  EXPECT_TRUE(is_equal(param, temp_param));
  delete temp;
  temp = NULL;
}

TEST(TestScanParam, serialize_empty) {
  ObScanParam param;
  param.set_scan_direction(ObScanParam::FORWARD);
  // version
  char* name = "table_test";
  ObString table_name;
  table_name.assign(name, strlen(name));

  ObRange scan_range;
  scan_range.border_flag_.unset_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  scan_range.border_flag_.set_min_value();
  scan_range.border_flag_.set_max_value();
  param.set(OB_INVALID_ID, table_name, scan_range);

  // no filter
  int size = param.get_serialize_size();
  EXPECT_TRUE(size > 0);

  char* temp = new char[size];
  EXPECT_TRUE(NULL != temp);
  int64_t pos = 0;
  // size small
  EXPECT_TRUE(OB_SUCCESS != param.serialize(temp, size - 1, pos));
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == param.serialize(temp, size, pos));
  EXPECT_TRUE((int64_t)size == pos);

  ObScanParam temp_param;
  int64_t new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS != temp_param.deserialize(temp, size - 1, new_pos));
  new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS == temp_param.deserialize(temp, size, new_pos));
  EXPECT_TRUE(pos == new_pos);
  // same as each other
  EXPECT_TRUE(is_equal(temp_param, param));
  EXPECT_TRUE(is_equal(param, temp_param));
  delete temp;
  temp = NULL;
}

TEST(TestScanParam, set_both_table_name_and_id) {
  ObScanParam sp;
  char* tn = "table_name";
  int tn_len = strlen(tn);
  char* rk = "123";
  int rk_len = strlen(rk);

  ObString table_name(tn_len, tn_len, tn);
  ObString rowkey(rk_len, rk_len, rk);

  ObRange range;
  range.start_key_ = rowkey;
  range.end_key_ = rowkey;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  ObVersionRange vrange;
  vrange.border_flag_.set_min_value();
  vrange.start_version_ = 1;
  vrange.end_version_ = 10;

  sp.set(1, table_name, range);
  sp.set_version_range(vrange);

  char buf[BUFSIZ];
  int64_t pos = 0;
  ASSERT_EQ(sp.serialize(buf, BUFSIZ, pos), OB_ERROR);
}

