/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         test.cc is for what ...
 *
 *  Version: $Id: test.cc 2010年11月17日 16时12分46秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include "common_func.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/utility.h"

using namespace sb::common;

/* 从min和max中返回一个随机值 */

int64_t random_number(int64_t min, int64_t max) {
  static int dev_random_fd = -1;
  char* next_random_byte;
  int bytes_to_read;
  int64_t random_value = 0;

  assert(max > min);

  if (dev_random_fd == -1) {
    dev_random_fd = open("/dev/urandom", O_RDONLY);
    assert(dev_random_fd != -1);
  }

  next_random_byte = (char*)&random_value;
  bytes_to_read = sizeof(random_value);

  /* 因为是从/dev/random中读取，read可能会被阻塞，一次读取可能只能得到一个字节，
   * 循环是为了让我们读取足够的字节数来填充random_value.
   */
  do {
    int bytes_read;
    bytes_read = read(dev_random_fd, next_random_byte, bytes_to_read);
    bytes_to_read -= bytes_read;
    next_random_byte += bytes_read;
  } while (bytes_to_read > 0);

  return min + (abs(random_value) % (max - min + 1));
}


int parse_number_range(const char* number_string,
                       int32_t* number_array, int32_t& number_array_size) {
  int ret = OB_ERROR;
  if (NULL != strstr(number_string, ",")) {
    ret = parse_string_to_int_array(number_string, ',', number_array, number_array_size);
    if (ret) return ret;
  } else if (NULL != strstr(number_string, "~")) {
    int32_t min_max_array[2];
    min_max_array[0] = 0;
    min_max_array[1] = 0;
    int tmp_size = 2;
    ret = parse_string_to_int_array(number_string, '~', min_max_array, tmp_size);
    if (ret) return ret;
    int32_t min = min_max_array[0];
    int32_t max = min_max_array[1];
    int32_t index = 0;
    for (int i = min; i <= max; ++i) {
      if (index > number_array_size) return OB_SIZE_OVERFLOW;
      number_array[index++] = i;
    }
    number_array_size = index;
  } else {
    number_array[0] = strtol(number_string, NULL, 10);
    number_array_size = 1;
  }
  return OB_SUCCESS;
}

int parse_range_str(const char* range_str, int hex_format, ObRange& range) {
  int ret = OB_SUCCESS;
  if (NULL == range_str) {
    ret = OB_INVALID_ARGUMENT;
  }

  int len = 0;
  if (OB_SUCCESS == ret) {
    len = strlen(range_str);
    if (len < 5)
      ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret) {
    const char start_border = range_str[0];
    if (start_border == '[')
      range.border_flag_.set_inclusive_start();
    else if (start_border == '(')
      range.border_flag_.unset_inclusive_start();
    else {
      fprintf(stderr, "start char of range_str(%c) must be [ or (\n", start_border);
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    const char end_border = range_str[len - 1];
    if (end_border == ']')
      range.border_flag_.set_inclusive_end();
    else if (end_border == ')')
      range.border_flag_.unset_inclusive_end();
    else {
      fprintf(stderr, "end char of range_str(%c) must be [ or (\n", end_border);
      ret = OB_ERROR;
    }
  }
  const char* sp = NULL;
  char* del = NULL;
  if (OB_SUCCESS == ret) {
    sp = range_str + 1;
    del = strstr(sp, ",");
    if (NULL == del) {
      fprintf(stderr, "cannot find , in range_str(%s)\n", range_str);
      ret = OB_ERROR;
    }
  }
  int start_len = 0;
  if (OB_SUCCESS == ret) {
    while (*sp == ' ') ++sp; // skip space
    start_len = del - sp;
    if (start_len <= 0) {
      fprintf(stderr, "start key cannot be NULL\n");
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret) {
    if (strncasecmp(sp, "min", 3) == 0) {
      range.border_flag_.set_min_value();
    } else {
      if (hex_format == 0) {
        range.start_key_.assign_ptr((char*)sp, start_len);
      } else {
        char* hexkey = (char*)ob_malloc(OB_RANGE_STR_BUFSIZ);
        int hexlen = str_to_hex(sp, start_len, hexkey, OB_RANGE_STR_BUFSIZ);
        range.start_key_.assign_ptr(hexkey, hexlen / 2);
      }
    }
  }
  int end_len = 0;
  const char* ep = NULL;
  if (OB_SUCCESS == ret) {
    ep = del + 1;
    while (*ep == ' ') ++ep;
    end_len = range_str + len - 1 - ep;
    if (end_len <= 0) {
      fprintf(stderr, "end key cannot be NULL\n");
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret) {
    if (strncasecmp(ep, "max", 3) == 0) {
      range.border_flag_.set_max_value();
    } else {
      if (hex_format == 0) {
        range.end_key_.assign_ptr((char*)ep, end_len);
      } else {
        char* hexkey = (char*)ob_malloc(OB_RANGE_STR_BUFSIZ);
        int hexlen = str_to_hex(ep, end_len, hexkey, OB_RANGE_STR_BUFSIZ);
        range.end_key_.assign_ptr(hexkey, hexlen / 2);
      }
    }
  }

  return ret;
}
