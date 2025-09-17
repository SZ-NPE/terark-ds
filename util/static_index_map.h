#pragma once
#include <assert.h>
#include <stdio.h>

#include <iostream>
#include <vector>

#include "db/dbformat.h"
#include "db/map_builder.h"
#include "rocksdb/cleanable.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/terark_namespace.h"
#include "util/coding.h"

namespace TERARKDB_NAMESPACE {

class StaticMapIndex : public Cleanable {
 public:
  StaticMapIndex(const InternalKeyComparator* c, Statistics* s);
  explicit StaticMapIndex(StaticMapIndex& other);

  ~StaticMapIndex();

  char* GetKeyOffset(uint64_t id) const;

  uint64_t GetKeyLen(uint64_t id) const;

  char* GetValueOffset(uint64_t id) const;

  uint64_t GetValueLen(uint64_t id) const;

  Slice GetKey(uint64_t id) const;

  Slice GetValue(uint64_t id) const;

  uint64_t Size() { return key_len_ + value_len_ + key_nums_ * 16 + 16; }

  uint64_t GetIndex(const Slice& key);

  bool FindKey(const Slice& key);

  bool empty() const { return key_nums_ == 0; }

  Status BuildStaticMapIndex(std::unique_ptr<InternalIteratorBase<Slice>> iter);

  static std::atomic<uint64_t> index_key_map_size;

  uint64_t GetKeyNums() { return key_nums_; }

  bool CompareKey(const Slice& key, const Slice& queried_key) {
    return c_->CompareUserKey(key, queried_key) != 0;
  }

 private:
  const InternalKeyComparator* c_;
  Statistics* stats_;
  char* key_buff_;
  char* value_buff_;
  uint64_t* key_offset_;
  uint64_t* value_offset_;
  uint64_t key_nums_;
  uint64_t key_len_;
  uint64_t value_len_;
};

}  // namespace TERARKDB_NAMESPACE
