#include "util/static_index_map.h"

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "table/table_reader.h"

namespace TERARKDB_NAMESPACE {

std::atomic<uint64_t> StaticMapIndex::index_key_map_size(0);

StaticMapIndex::StaticMapIndex(const InternalKeyComparator *c, Statistics *s)
    : c_(c),
      stats_(s),
      key_buff_(nullptr),
      value_buff_(nullptr),
      key_offset_(nullptr),
      value_offset_(nullptr),
      key_nums_(0),
      key_len_(0),
      value_len_(0) {}

StaticMapIndex::~StaticMapIndex() {
  if (key_buff_ != nullptr) {
    delete[] key_buff_;
    delete[] value_buff_;
    delete[] key_offset_;
    delete[] value_offset_;
    index_key_map_size.fetch_sub(Size(), std::memory_order_seq_cst);
    key_nums_ = 0;
    key_len_ = 0;
    value_len_ = 0;
  }
}

StaticMapIndex::StaticMapIndex(StaticMapIndex &other) : Cleanable() {
  if (other.key_buff_ != nullptr) {
    key_buff_ = new char[other.key_len_];
    value_buff_ = new char[other.value_len_];
    uint64_t capacity = other.key_nums_ + 1;
    key_offset_ = new uint64_t[capacity];
    value_offset_ = new uint64_t[capacity];
    memcpy(key_offset_, other.key_offset_, sizeof(uint64_t) * capacity);
    memcpy(value_offset_, other.value_offset_, sizeof(uint64_t) * capacity);
    memcpy(key_buff_, other.key_buff_, sizeof(char) * other.key_len_);
    memcpy(value_buff_, other.value_buff_, sizeof(char) * other.value_len_);

    index_key_map_size.store(other.index_key_map_size);
    key_nums_ = other.key_nums_;
    key_len_ = other.key_len_;
    value_len_ = other.value_len_;
  }
}

char *StaticMapIndex::GetKeyOffset(uint64_t id) const {
  assert(id < key_nums_);
  return key_buff_ + key_offset_[id];
}

uint64_t StaticMapIndex::GetKeyLen(uint64_t id) const {
  assert(id < key_nums_);
  return key_offset_[id + 1] - key_offset_[id];
}

char *StaticMapIndex::GetValueOffset(uint64_t id) const {
  assert(id < key_nums_);
  return value_buff_ + value_offset_[id];
}

uint64_t StaticMapIndex::GetValueLen(uint64_t id) const {
  assert(id < key_nums_);
  return value_offset_[id + 1] - value_offset_[id];
}

Slice StaticMapIndex::GetKey(uint64_t id) const {
  assert(key_buff_ != nullptr);
  return Slice(GetKeyOffset(id), GetKeyLen(id));
}

Slice StaticMapIndex::GetValue(uint64_t id) const {
  assert(value_buff_ != nullptr);
  return Slice(GetValueOffset(id), GetValueLen(id));
}

uint64_t StaticMapIndex::GetIndex(const Slice &key) {
  uint64_t l = 0;
  uint64_t r = key_nums_ - 1;
  while (l <= r) {
    uint64_t mid = l + ((r - l) >> 1);
    if (r == -1) {
      break;
    }
    Slice curr_key = GetKey(mid);
    if (c_->CompareUserKey(key, curr_key) == 0) {
      return mid;
    } else if (c_->CompareUserKey(key, curr_key) < 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  return -1;
}

bool StaticMapIndex::FindKey(const Slice &key) {
  uint64_t index = GetIndex(key);
  if (index >= key_nums_ || c_->CompareUserKey(key, GetKey(index)) != 0) {
    return false;
  } else {
    return true;
  }
}

Status StaticMapIndex::BuildStaticMapIndex(
    std::unique_ptr<InternalIteratorBase<Slice>> iter) {
  Status status = iter->status();
  if (!status.ok()) {
    return status;
  }
  uint64_t key_nums = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    key_nums++;
  }
  if (key_nums == 0) {
    return status;
  }
  uint64_t key_lens = 0;
  uint64_t value_lens = 0;
  uint64_t *key_offset = new uint64_t[key_nums + 1];
  uint64_t *value_offset = new uint64_t[key_nums + 1];
  uint64_t i = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    key_offset[i] = key_lens;
    value_offset[i] = value_lens;
    key_lens += iter->key().size();
    value_lens += iter->value().size();
    i++;
  }
  key_offset[key_nums] = key_lens;
  value_offset[key_nums] = value_lens;
  char *key_buffer = new char[key_lens];
  char *value_buffer = new char[value_lens];
  i = 0;
  IterKey iter_key;
  ParsedInternalKey ikey;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Slice curr_key = iter->key();

    if (!ParseInternalKey(curr_key, &ikey)) {
      status =
          Status::Corruption("ProcessGarbageCollection invalid InternalKey");
      break;
    }
    iter_key.SetInternalKey(ikey.user_key, ikey.sequence, kValueTypeForSeek);
    // Used for GC GetKey: Insert key, value to key_buffer and value_buffer
    // The format adapts to which passed in by the GetKey operation
    // key: user_key + Pack(sequence, kValueTypeForSeek)  value:
    // DataBlockIter->value()
    memcpy(key_buffer + key_offset[i], iter_key.GetInternalKey().data(),
           iter_key.Size());
    memcpy(value_buffer + value_offset[i], iter->value().data(),
           iter->value().size());
    i++;
  }
  key_buff_ = key_buffer;
  value_buff_ = value_buffer;
  key_offset_ = key_offset;
  value_offset_ = value_offset;
  key_nums_ = key_nums;
  key_len_ = key_lens;
  value_len_ = value_lens;
  index_key_map_size.fetch_add(Size(), std::memory_order_seq_cst);
  return status;
}

}  // namespace TERARKDB_NAMESPACE
