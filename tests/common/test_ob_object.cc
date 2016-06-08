#include "ob_object.h"
#include "ob_define.h"
#include "ob_action_flag.h"
#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace sb;
using namespace sb::common;

#define BLOCK_FUNC()  if (true) \

void write_buf_to_file(const char* filename, const char* buf, int64_t size) {
  if (NULL == buf || size <= 0 || NULL == filename)
    return;
  int fd = open(filename, O_RDWR | O_CREAT, 0755);
  if (fd < 0)
    return;
  if (write(fd, buf, size) != size) {
    printf("write buf to file error\n");
  }
  close(fd);
  return;
}

TEST(ObObj, Add) {
  ObObj t1;
  t1.set_float(80469200.00);
  ObObj t2;
  t2.set_float(6.00, true);
  t1.apply(t2);
}

TEST(ObObj, test_bug) {
  ObObj obj;
  obj.set_int(100, false);
  char buf[2048];
  int64_t len = 2048;
  int64_t pos = 0;
  ASSERT_EQ(obj.serialize(buf, len, pos), OB_SUCCESS);

  pos = 0;
  ObObj temp;
  temp.set_int(200, true);
  temp.reset();

  ASSERT_EQ(temp.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(temp == obj, true);

  ObObj test;
  test.set_int(300, true);
  test = temp;
  ASSERT_EQ(test == temp, true);
}


TEST(ObObj, Serialize_int_add) {
  ObObj t;
  t.set_int(9900);

  int64_t len = t.get_serialize_size();

  ASSERT_EQ(len, 3);

  char* buf = (char*)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t.serialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(pos, len);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(pos, len);

  int64_t r = 0;
  int64_t l = 0;

  ASSERT_EQ(f.get_type(), t.get_type());
  f.get_int(r);

  t.get_int(l);
  ASSERT_EQ(r, l);

  free(buf);
}

TEST(ObObj, Serialize_int) {
  ObObj t;
  t.set_int(5900, true);

  int64_t len = t.get_serialize_size();

  ASSERT_EQ(len, 3);

  char* buf = (char*)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t.serialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(pos, len);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(pos, len);

  int64_t r = 0;
  int64_t l = 0;

  ASSERT_EQ(f.get_type(), t.get_type());
  f.get_int(r);

  t.get_int(l);
  ASSERT_EQ(r, l);
  free(buf);
}

TEST(ObObj, int_boundary) {
  int64_t need_len = 0;

  int64_t data[] = {1, 28, -1, INT32_MIN, static_cast<int64_t>(UINT32_MAX), INT64_MAX, INT64_MIN, UINT64_MAX, 124543900};
  for (uint32_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
    need_len += serialization::encoded_length_int(data[i]);
  }
  char* buf = (char*)malloc(need_len);
  ObObj t;
  int64_t pos = 0;

  for (uint32_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
    t.reset();
    t.set_int(data[i]);
    ASSERT_EQ(t.serialize(buf, need_len, pos), OB_SUCCESS);
  }

  pos = 0;

  int64_t v = 0;
  for (uint32_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
    t.reset();
    ASSERT_EQ(t.deserialize(buf, need_len, pos), OB_SUCCESS);
    ASSERT_EQ(t.get_int(v), OB_SUCCESS);
    ASSERT_EQ(v, data[i]);
  }
  free(buf);
}


TEST(ObObj, Serialize_float) {
  ObObj t;
  t.set_float(59.16);

  int64_t len = t.get_serialize_size();
  ASSERT_EQ(len, 5);

  char* buf = (char*)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t.serialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(pos, len);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(pos, len);

  float r = 0.0;
  float l = 0.0;

  ASSERT_EQ(f.get_type(), t.get_type());
  f.get_float(r);
  t.get_float(l);
  ASSERT_FLOAT_EQ(r, l);

  free(buf);
  buf = NULL;

  double d = 173.235;
  t.set_double(d);

  len = t.get_serialize_size();
  buf = (char*)malloc(len);
  ASSERT_EQ(len, 9);

  pos = 0;
  ASSERT_EQ(t.serialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(pos, len);

  pos = 0;
  ASSERT_EQ(f.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(pos, len);

  double rd = 0.0;
  double ld = 0.0;

  ASSERT_EQ(f.get_type(), t.get_type());
  f.get_double(rd);
  t.get_double(ld);
  ASSERT_DOUBLE_EQ(rd, ld);

  free(buf);
  buf = NULL;
}

TEST(ObObj, Serialize_datetime) {
  ObObj t1;
  t1.set_datetime(1348087492, true);

  int64_t len = t1.get_serialize_size();

  char* buf = (char*)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t1.serialize(buf, len, pos), OB_SUCCESS);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf, len, pos), OB_SUCCESS);

  ASSERT_EQ(f.get_type(), t1.get_type());


  ObDateTime l = 0;
  ObDateTime r = 0;

  f.get_datetime(l);
  t1.get_datetime(r);

  ASSERT_EQ(l, r);

  free(buf);

}

TEST(ObObj, Serialize_precise_datetime) {
  ObObj t1;
  t1.set_precise_datetime(221348087492);

  int64_t len = t1.get_serialize_size();

  char* buf = (char*)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t1.serialize(buf, len, pos), OB_SUCCESS);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf, len, pos), OB_SUCCESS);

  ASSERT_EQ(f.get_type(), t1.get_type());


  ObPreciseDateTime l = 0;
  ObPreciseDateTime r = 0;

  f.get_precise_datetime(l);
  t1.get_precise_datetime(r);

  ASSERT_EQ(l, r);

  free(buf);
}

TEST(ObObj, Serialize_modifytime) {
  ObModifyTime data[] = {1, 28, 1, static_cast<int64_t>(UINT32_MAX), INT64_MAX, 124543900, 221348087492};
  int64_t need_len = 0;
  for (uint32_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
    need_len += serialization::encoded_length_modifytime(data[i]);
  }
  char* buf = (char*)malloc(need_len);
  ObObj t;
  int64_t pos = 0;

  for (uint32_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
    t.reset();
    t.set_modifytime(data[i]);
    ASSERT_EQ(t.serialize(buf, need_len, pos), OB_SUCCESS);
  }

  pos = 0;

  ObModifyTime v = 0;
  for (uint32_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
    t.reset();
    ASSERT_EQ(t.deserialize(buf, need_len, pos), OB_SUCCESS);
    ASSERT_EQ(t.get_modifytime(v), OB_SUCCESS);
    ASSERT_EQ(v, data[i]);
  }

  free(buf);
}

TEST(ObObj, Serialize_createtime) {
  ObCreateTime data[] = {1, 28, 1, static_cast<int64_t>(UINT32_MAX), INT64_MAX, 124543900, 221348087492};
  int64_t need_len = 0;
  for (uint32_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
    need_len += serialization::encoded_length_createtime(data[i]);
  }
  char* buf = (char*)malloc(need_len);
  ObObj t;
  int64_t pos = 0;

  for (uint32_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
    t.reset();
    t.set_createtime(data[i]);
    ASSERT_EQ(t.serialize(buf, need_len, pos), OB_SUCCESS);
  }

  pos = 0;

  ObCreateTime v = 0;
  for (uint32_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
    t.reset();
    ASSERT_EQ(t.deserialize(buf, need_len, pos), OB_SUCCESS);
    ASSERT_EQ(t.get_createtime(v), OB_SUCCESS);
    ASSERT_EQ(v, data[i]);
  }
  free(buf);
}


TEST(ObObj, extend_type) {
  ObObj t1;
  t1.set_ext(90);

  int64_t len = t1.get_serialize_size();

  ASSERT_EQ(len, 2);

  char* buf = (char*)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t1.serialize(buf, len, pos), OB_SUCCESS);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf, len, pos), OB_SUCCESS);

  int64_t e = 0;
  ASSERT_EQ(f.get_ext(e), 0);

  ASSERT_EQ(e, 90);
  free(buf);
}


TEST(ObObj, Compare) {
  // type failed
  BLOCK_FUNC() {
    ObObj t1;
    ObObj t2;
    t1.set_int(1234);
    t2.set_float(1234.0);

    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 <= t2) == false);
    EXPECT_TRUE((t2 <= t1) == false);
    EXPECT_TRUE((t1 >= t2) == false);
    EXPECT_TRUE((t2 >= t1) == false);

    ObString str_temp;
    char* temp = "1234";
    str_temp.assign(temp, strlen(temp));
    t1.set_varchar(str_temp);
    t2.set_datetime(1234);

    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 <= t2) == false);
    EXPECT_TRUE((t2 <= t1) == false);
    EXPECT_TRUE((t1 >= t2) == false);
    EXPECT_TRUE((t2 >= t1) == false);
  }

  // null type
  BLOCK_FUNC() {
    ObObj t1;
    t1.set_null();
    ObObj t2;
    t2.set_null();
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == true);
    EXPECT_TRUE((t2 == t1) == true);
    EXPECT_TRUE((t1 != t2) == false);
    EXPECT_TRUE((t2 != t1) == false);

    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == true);
    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == true);

    t2.set_datetime(1234);
    EXPECT_TRUE((t1 < t2) == true);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == true);

    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == false);
    EXPECT_TRUE((t1 >= t2) == false);
    EXPECT_TRUE((t2 >= t1) == true);
  }

  //lt gt eq ne le ge
  BLOCK_FUNC() {
    ObObj t1;
    ObObj t2;
    t1.set_int(1234);

    t2.set_int(1233);
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == true);
    EXPECT_TRUE((t1 > t2) == true);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 <= t2) == false);
    EXPECT_TRUE((t2 <= t1) == true);
    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == false);

    t2.set_int(1234);
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == true);
    EXPECT_TRUE((t2 == t1) == true);
    EXPECT_TRUE((t1 != t2) == false);
    EXPECT_TRUE((t2 != t1) == false);

    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == true);
    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == true);

    t2.set_int(1235);
    EXPECT_TRUE((t1 < t2) == true);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == true);

    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 >= t2) == false);
    EXPECT_TRUE((t2 >= t1) == true);
    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == false);
  }

  // datetime
  BLOCK_FUNC() {
    ObObj t1;
    ObObj t2;
    t1.set_createtime(1234000001L);

    // le
    t2.set_modifytime(1234000001L);
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == true);
    EXPECT_TRUE((t2 == t1) == true);
    EXPECT_TRUE((t1 != t2) == false);
    EXPECT_TRUE((t2 != t1) == false);

    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == true);
    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == true);

    t2.set_precise_datetime(1234000001L);
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == true);
    EXPECT_TRUE((t2 == t1) == true);
    EXPECT_TRUE((t1 != t2) == false);
    EXPECT_TRUE((t2 != t1) == false);

    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == true);
    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == true);

    //lt
    t2.set_datetime(1233);
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == true);
    EXPECT_TRUE((t1 > t2) == true);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 <= t2) == false);
    EXPECT_TRUE((t2 <= t1) == true);
    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == false);

    t2.set_createtime(1234000000L);
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == true);
    EXPECT_TRUE((t1 > t2) == true);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 <= t2) == false);
    EXPECT_TRUE((t2 <= t1) == true);
    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == false);

    // gt
    t2.set_modifytime(1234000100L);
    EXPECT_TRUE((t1 < t2) == true);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == true);

    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 >= t2) == false);
    EXPECT_TRUE((t2 >= t1) == true);
    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == false);

    t2.set_createtime(1234000010L);
    EXPECT_TRUE((t1 < t2) == true);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == true);

    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 >= t2) == false);
    EXPECT_TRUE((t2 >= t1) == true);
    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == false);

    // eq
    t1.set_createtime(1234000000L);
    t2.set_datetime(1234);
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == true);
    EXPECT_TRUE((t2 == t1) == true);
    EXPECT_TRUE((t1 != t2) == false);
    EXPECT_TRUE((t2 != t1) == false);

    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == true);
    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == true);

    t1.set_datetime(1234);
    t2.set_datetime(1234);
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == true);
    EXPECT_TRUE((t2 == t1) == true);
    EXPECT_TRUE((t1 != t2) == false);
    EXPECT_TRUE((t2 != t1) == false);

    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == true);
    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == true);

    t1.set_precise_datetime(123456);
    t2.set_precise_datetime(123456);
    EXPECT_TRUE((t1 < t2) == false);
    EXPECT_TRUE((t2 < t1) == false);
    EXPECT_TRUE((t1 > t2) == false);
    EXPECT_TRUE((t2 > t1) == false);

    EXPECT_TRUE((t1 == t2) == true);
    EXPECT_TRUE((t2 == t1) == true);
    EXPECT_TRUE((t1 != t2) == false);
    EXPECT_TRUE((t2 != t1) == false);

    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == true);
    EXPECT_TRUE((t1 <= t2) == true);
    EXPECT_TRUE((t2 <= t1) == true);

    t1.set_precise_datetime(123456);
    t2.set_precise_datetime(123454);
    EXPECT_TRUE((t1 == t2) == false);
    EXPECT_TRUE((t2 == t1) == false);
    EXPECT_TRUE((t1 != t2) == true);
    EXPECT_TRUE((t2 != t1) == true);

    EXPECT_TRUE((t1 >= t2) == true);
    EXPECT_TRUE((t2 >= t1) == false);
    EXPECT_TRUE((t1 <= t2) == false);
    EXPECT_TRUE((t2 <= t1) == true);
  }
}


#if  1
TEST(ObObj, Serialize) {
  ObObj t1;
  ObObj t2;
  ObObj t3;
  ObObj t4;
  ObObj t5;
  ObObj t6;
  ObObj t7;
  char buf[1024];
  int64_t len = 1024;

  //int
  t1.set_int(9871345);
  int64_t pos = 0;
  ASSERT_EQ(t1.serialize(buf, len, pos), OB_SUCCESS);
  //str
  ObString d;
  const char* tmp = "Hello";
  d.assign_ptr(const_cast<char*>(tmp), 5);
  t2.set_varchar(d);
  ASSERT_EQ(t2.serialize(buf, len, pos), OB_SUCCESS);
  //null
  t3.set_null();
  ASSERT_EQ(t3.serialize(buf, len, pos), OB_SUCCESS);
  //float
  t4.set_float(59.16);
  ASSERT_EQ(t4.serialize(buf, len, pos), OB_SUCCESS);
  //double
  t5.set_double(59.167);
  ASSERT_EQ(t5.serialize(buf, len, pos), OB_SUCCESS);
  //datetime
  t6.set_datetime(1234898766);
  ASSERT_EQ(t6.serialize(buf, len, pos), OB_SUCCESS);
  //precise_datetime
  t7.set_precise_datetime(2348756909000000L);
  ASSERT_EQ(t7.serialize(buf, len, pos), OB_SUCCESS);

  write_buf_to_file("Object", buf, pos);

  ObObj f1;
  ObObj f2;
  ObObj f3;
  ObObj f4;
  ObObj f5;
  ObObj f6;
  ObObj f7;
  pos = 0;
  //int
  ASSERT_EQ(f1.deserialize(buf, len, pos), OB_SUCCESS);
  int64_t r = 0;
  int64_t l = 0;
  ASSERT_EQ(f1.get_type(), t1.get_type());
  f1.get_int(r);
  t1.get_int(l);
  ASSERT_EQ(r, l);
  //printf("%ld,%ld\n",r,l);

  //str
  ASSERT_EQ(f2.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(f2.get_type(), t2.get_type());
  ObString str;
  ASSERT_EQ(f2.get_varchar(str), OB_SUCCESS);
  //printf("str:%s,len:%d,d:%s,len:%d\n",str.ptr(),str.length(),d.ptr(),d.length());
  ASSERT_TRUE(str == d);

  //null
  ASSERT_EQ(f3.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(f3.get_type(), t3.get_type());

  //float
  ASSERT_EQ(f4.deserialize(buf, len, pos), OB_SUCCESS);
  float rf = 0.0;
  float lf = 0.0;

  ASSERT_EQ(f4.get_type(), t4.get_type());
  f4.get_float(rf);
  t4.get_float(lf);
  ASSERT_FLOAT_EQ(rf, lf);

  //double
  ASSERT_EQ(f5.deserialize(buf, len, pos), OB_SUCCESS);

  double rd = 0.0;
  double ld = 0.0;

  ASSERT_EQ(f5.get_type(), t5.get_type());
  f5.get_double(rd);
  t5.get_double(ld);
  ASSERT_DOUBLE_EQ(rd, ld);

  //second
  ASSERT_EQ(f6.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(f6.get_type(), t6.get_type());

  ObDateTime rt = 0;
  ObDateTime lt = 0;

  f6.get_datetime(rt);
  t6.get_datetime(lt);
  ASSERT_EQ(rt, lt);

  //microsecond
  ASSERT_EQ(f7.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(f7.get_type(), t7.get_type());

  ObPreciseDateTime rm = 0;
  ObPreciseDateTime lm = 0;

  f7.get_precise_datetime(rm);
  t7.get_precise_datetime(lm);
  ASSERT_EQ(rm, lm);
}

TEST(ObObj, performance) {
  // int64_t start_time = tbsys::CTimeUtil::getTime();
  //const int64_t MAX_COUNT = 10 * 1000 * 1000L;
  const int64_t MAX_COUNT = 1000L;
  ObObj obj;
  char buf[2048];
  int64_t len = 2048;
  int64_t pos = 0;
  int64_t data = 0;
  for (register int64_t i = 0; i < MAX_COUNT; ++i) {
    obj.set_int(i);
    pos = 0;
    ASSERT_EQ(obj.serialize(buf, len, pos), OB_SUCCESS);
    pos = 0;
    ASSERT_EQ(obj.deserialize(buf, len, pos), OB_SUCCESS);
    ASSERT_EQ(obj.get_int(data), OB_SUCCESS);
    ASSERT_EQ(data, i);
  }

  const char* tmp = "Hello12344556666777777777777545352454254254354354565463241242354345345345235345";
  ObObj obj2;
  ObString string;
  ObString string2;
  string.assign_ptr(const_cast<char*>(tmp), 1024);
  obj2.set_varchar(string);
  for (register int64_t i = 0; i < MAX_COUNT; ++i) {
    pos = 0;
    ASSERT_EQ(obj2.serialize(buf, len, pos), OB_SUCCESS);
    pos = 0;
    ASSERT_EQ(obj2.deserialize(buf, len, pos), OB_SUCCESS);
    ASSERT_EQ(obj2.get_varchar(string2), OB_SUCCESS);
  }
  // int64_t end_time = tbsys::CTimeUtil::getTime();
  //printf("using time:%ld\n", end_time - start_time);
}

#endif

TEST(ObObj, NOP) {
  ObObj src_obj;
  ObObj mut_obj;
  int64_t val = 0;
  int64_t mutation = 5;
  ///create time
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_createtime(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj), OB_SUCCESS);
  ASSERT_EQ(src_obj.get_createtime(val), OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  ///create time
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_modifytime(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj), OB_SUCCESS);
  ASSERT_EQ(src_obj.get_modifytime(val), OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  ///precise time
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_precise_datetime(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj), OB_SUCCESS);
  ASSERT_EQ(src_obj.get_precise_datetime(val), OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_precise_datetime(mutation, true);
  ASSERT_EQ(src_obj.apply(mut_obj), OB_SUCCESS);
  ASSERT_EQ(src_obj.get_precise_datetime(val), OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_TRUE(src_obj.get_add());

  ///date time
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_datetime(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj), OB_SUCCESS);
  ASSERT_EQ(src_obj.get_datetime(val), OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_datetime(mutation, true);
  ASSERT_EQ(src_obj.apply(mut_obj), OB_SUCCESS);
  ASSERT_EQ(src_obj.get_datetime(val), OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_TRUE(src_obj.get_add());

  /// int
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_int(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj), OB_SUCCESS);
  ASSERT_EQ(src_obj.get_int(val), OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_int(mutation, true);
  ASSERT_EQ(src_obj.apply(mut_obj), OB_SUCCESS);
  ASSERT_EQ(src_obj.get_int(val), OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_TRUE(src_obj.get_add());

  /// obstring
  const char* cname = "cname";
  ObString str;
  ObString res;
  str.assign((char*)cname, strlen(cname));
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_varchar(str);
  ASSERT_EQ(src_obj.apply(mut_obj), OB_SUCCESS);
  ASSERT_EQ(src_obj.get_varchar(res), OB_SUCCESS);
  ASSERT_EQ(res.length(), strlen(cname));
  ASSERT_EQ(memcmp(res.ptr(), cname, res.length()), 0);
  ASSERT_FALSE(src_obj.get_add());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
