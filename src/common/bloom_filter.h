/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * bloom_filter.h for bloom filter.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_BASIC_BLOOM_FILTER_H_
#define OCEANBASE_COMMON_BASIC_BLOOM_FILTER_H_

#include <limits.h>
#include <string.h>
#include <algorithm>
#include <cmath>
#include "murmur_hash.h"
#include "ob_object.h"
#include "ob_rowkey.h"
#include "ob_malloc.h"
#include "ob_bloomfilter.h"
#include "ob_adapter_allocator.h"
#include "ob_kv_storecache.h"

namespace sb {
namespace common {

/**
 * BloomFilter specification
 */
class BloomFilter {
 public:
  virtual ~BloomFilter() {}
  virtual int64_t get_version() const = 0;

  virtual int insert(const ObObj* objs, const int64_t count) = 0;
  virtual bool may_contain(const ObObj* objs, const int64_t count) const = 0;

  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const = 0;
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0;
  virtual int64_t get_serialize_size() const = 0;
};


//----------------------------------------------------------------------------
// class ObBasicBloomFilter
//----------------------------------------------------------------------------
template <typename Allocator>
class ObBasicBloomFilter : public BloomFilter {
 public:
  static const int64_t BLOOM_FILTER_VERSION = 1;
  static const float BLOOM_FILTER_FALSE_POSITIVE_PROB ;
  struct RowkeyHashFunc {
    uint64_t operator()(const ObRowkey& rowkey, const uint64_t hash) {
      return rowkey.murmurhash64A(hash);
    }
  };
 public:
  ObBasicBloomFilter() {
  }

  virtual ~ObBasicBloomFilter() {
  }

  // calculate k(hash count), N(bits) by element count
  int init(int64_t element_count, float false_positive_prob = BLOOM_FILTER_FALSE_POSITIVE_PROB) {
    int ret = common::OB_SUCCESS;
    if (element_count <= 0) {
      TBSYS_LOG(ERROR, "bloom filter element_count should be > 0 ");
      ret = common::OB_INVALID_ARGUMENT;
    } else if (!(false_positive_prob < 1.0 || false_positive_prob > 0.0)) {
      TBSYS_LOG(ERROR, "bloom filter false_positive_prob should be < 1.0 and > 0.0");
      ret = common::OB_ERROR;
    } else {
      double num_hashes = -std::log(false_positive_prob) / std::log(2);
      int64_t num_bits = static_cast<int64_t>((static_cast<double>(element_count)
                                               * num_hashes / static_cast<double>(std::log(2))));
      int64_t num_bytes = bf_.calc_nbyte(num_bits);
      ret = init(static_cast<int64_t>(num_hashes), num_bytes);
    }
    return ret;
  }

  // set k, N manually
  inline int init(const int64_t nhash, const int64_t nbyte) {
    return bf_.init(nhash, nbyte * CHAR_BIT);
  };

  inline void clear() {
    return bf_.clear();
  }

  inline void destroy() {
    return bf_.destroy();
  }

  inline int64_t get_nbyte() const {
    return bf_.calc_nbyte(bf_.get_nbit());
  }

  inline int64_t get_nhash() const {
    return bf_.get_nhash();
  }

  inline const uint8_t* get_buffer() const {
    return bf_.get_bits();
  }

  inline int set_buffer(const uint8_t* buf, const int64_t nbyte) {
    return bf_.reinit(buf, nbyte);
  }

  inline int64_t get_version() const {
    return BLOOM_FILTER_VERSION;
  }
  inline int insert(const ObObj* objs, const int64_t count) {
    ObRowkey rowkey(const_cast<ObObj*>(objs), count);
    return bf_.insert(rowkey);
  }
  inline int insert(const ObRowkey& rowkey) {
    return bf_.insert(rowkey);
  }
  inline bool may_contain(const ObObj* objs, const int64_t count) const {
    ObRowkey rowkey(const_cast<ObObj*>(objs), count);
    return bf_.may_contain(rowkey);
  }
  inline bool may_contain(const ObRowkey& rowkey) const {
    return bf_.may_contain(rowkey);
  }

  inline int serialize(char* buf, const int64_t buf_len, int64_t& pos) const {
    return bf_.serialize(buf, buf_len, pos);
  }
  inline int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    return bf_.deserialize(buf, data_len, pos);
  }
  inline int64_t get_serialize_size() const {
    return bf_.get_serialize_size();
  }
  inline Allocator* get_allocator() {
    return bf_.get_allocator();
  }
  ObBasicBloomFilter* clone(char* buffer) const {
    ObBasicBloomFilter<AdapterAllocator>* bf = NULL;
    if (buffer != NULL) {
      bf = new(buffer) ObBasicBloomFilter<AdapterAllocator>();
      AdapterAllocator* alloc = bf->get_allocator();
      buffer += sizeof(*bf);
      alloc->init(buffer);
      bf->deep_copy(*this);
    }
    return bf;
  }

  inline int operator | (const ObBasicBloomFilter<Allocator>& other) {
    int ret = OB_SUCCESS;
    const ObBloomFilter<ObRowkey, RowkeyHashFunc, ObTCMalloc>* bf = other.get_bf();
    ret = bf_ | (*bf);
    return ret;
  }
  inline int deep_copy(const ObBasicBloomFilter<Allocator>& other) {
    return bf_.deep_copy(other.bf_);
  }

 private:
  ObBloomFilter<ObRowkey, RowkeyHashFunc, Allocator> bf_;
};


template <typename Allocator>
const float ObBasicBloomFilter<Allocator>::BLOOM_FILTER_FALSE_POSITIVE_PROB = static_cast<float>(0.1);

typedef ObBasicBloomFilter<ObTCMalloc> ObBloomFilterV1;
typedef ObBasicBloomFilter<AdapterAllocator> ObBloomFilterAdapterV1;
namespace KVStoreCacheComponent {
struct ObBloomFilterAdapterV1DeepCopyTag {};
template<>
struct traits<ObBloomFilterAdapterV1> {
  typedef ObBloomFilterAdapterV1DeepCopyTag Tag;
};
inline ObBloomFilterAdapterV1* do_copy(const ObBloomFilterAdapterV1& other, char* buffer, ObBloomFilterAdapterV1DeepCopyTag) {
  return other.clone(buffer);
}
inline int32_t do_size(const ObBloomFilterAdapterV1& bf, ObBloomFilterAdapterV1DeepCopyTag) {
  return static_cast<int32_t>(sizeof(bf) + bf.get_nbyte());
}
inline void do_destroy(ObBloomFilterAdapterV1* data, ObBloomFilterAdapterV1DeepCopyTag) {
  UNUSED(data);
}
}

class TableBloomFilter {
  struct Element {
    uint64_t table_id;
    ObRowkey row_key;
  };
  struct HashFunc {
    uint64_t operator()(const Element& key, const uint64_t hash) const {
      return key.row_key.murmurhash64A(hash) + key.table_id;
    };
  };
 public:
  TableBloomFilter() : bf_() {
  };
  ~TableBloomFilter() {
  };
  DISALLOW_COPY_AND_ASSIGN(TableBloomFilter);
 public:
  inline int init(const int64_t nhash, const int64_t nbyte) {
    return bf_.init(nhash, nbyte * CHAR_BIT);
  };
  inline int reinit(const uint8_t* buf, const int64_t size) {
    return bf_.reinit(buf, size);
  }
  inline void destroy() {
    bf_.destroy();
  };
  inline void clear() {
    bf_.clear();
  };
  inline int deep_copy(const TableBloomFilter& other) {
    return bf_.deep_copy(other.bf_);
  };
  inline int insert(const uint64_t table_id, const ObRowkey& row_key) {
    Element key;
    key.table_id = table_id;
    key.row_key = row_key;
    return bf_.insert(key);
  };
  inline bool may_contain(const uint64_t table_id, const ObRowkey& row_key) const {
    Element key;
    key.table_id = table_id;
    key.row_key = row_key;
    return bf_.may_contain(key);
  };
  inline int serialize(char* buf, const int64_t buf_len, int64_t& pos) const {
    return bf_.serialize(buf, buf_len, pos);
  };
  inline int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    return bf_.deserialize(buf, data_len, pos);
  };
  inline int64_t get_serialize_size(void) const {
    return bf_.get_serialize_size();
  };

  inline const ObBloomFilter<Element, HashFunc, ObTCMalloc>* get_bf() const {
    return &bf_;
  }

  inline int64_t get_nbyte() const {
    int64_t nbit = bf_.get_nbit();
    return bf_.calc_nbyte(nbit);
  }

  inline const uint8_t* get_buffer() const {
    return bf_.get_bits();
  }

  inline int operator | (const TableBloomFilter& other) {
    int ret = OB_SUCCESS;
    const ObBloomFilter<Element, HashFunc, ObTCMalloc>* bf = other.get_bf();
    ret = bf_ | (*bf);

    return ret;
  }

 private:
  ObBloomFilter<Element, HashFunc, ObTCMalloc> bf_;
};

template <typename BloomFilterAllocator>
BloomFilter* create_bloom_filter(const int64_t version) {
  BloomFilter* bf = NULL;
  if (version == ObBasicBloomFilter<BloomFilterAllocator>::BLOOM_FILTER_VERSION) {
    void* p = ob_tc_malloc(sizeof(ObBasicBloomFilter<BloomFilterAllocator>), ObModIds::OB_BLOOM_FILTER);
    if (NULL != p) {
      bf = new(p)(ObBasicBloomFilter<BloomFilterAllocator>);
    }
  }
  return bf;
}

int destroy_bloom_filter(BloomFilter* bf);

}
}

#endif  //OCEANBASE_COMMON_BASIC_BLOOM_FILTER_H_
