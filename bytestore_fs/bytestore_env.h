#pragma once

#include "bytestore_fs.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/utilities/object_registry.h"

namespace TERARKDB_NAMESPACE {

namespace bytestore_fs {

class Buffer {
 public:
  Buffer(char* head, std::size_t capacity, std::size_t size = 0)
      : head_(head), capacity_(capacity), data_(head), size_(size) {}

  Buffer(std::size_t size, char* data) : Buffer(data, size, size) {}

  ~Buffer() {}

  Buffer(const Buffer&) = default;

  Buffer(Buffer&&) = default;

  Buffer& operator=(const Buffer& other) = default;

  Buffer& operator=(Buffer&& other) = default;

  std::size_t GetHeadSpaceSize() const { return data_ - head_; }

  std::size_t GetTailSpaceSize() const {
    return capacity_ - GetHeadSpaceSize() - size_;
  }

  const char* GetData() const { return data_; }

  char* GetData() { return data_; }

  std::size_t GetSize() const { return size_; }

  char* GetTail() { return data_ + size_; }

  std::size_t GetCapacity() const { return capacity_; }

  template <class U = char>
  U* PushBackN(std::size_t n = 1) {
    auto size = sizeof(U) * n;
    assert(size < GetTailSpaceSize());
    char* ptr = data_ + size_;
    size_ += size;
    return reinterpret_cast<U*>(ptr);
  }

  template <class U = char>
  U* PushFrontN(std::size_t n = 1) {
    auto size = sizeof(U) * n;
    assert(size < GetHeadSpaceSize());
    data_ -= size;
    size_ += size;
    return reinterpret_cast<U*>(data_);
  }

  void Reserve(std::size_t size) {
    assert(size + size_ < capacity_);
    if (size_ != 0) {
      if (GetHeadSpaceSize() >= size) {
        return;
      }
      memmove(head_ + size, data_, size_);
    }
    data_ = head_ + size;
  }

  // Pull the data to the head
  void Pull() {
    if (GetHeadSpaceSize() != 0) {
      memmove(head_, data_, size_);
      data_ = head_;
    }
  }

  void Clear() {
    data_ = head_;
    size_ = 0;
  }

  void Resize(std::size_t size) {
    assert(size < size_);
    size_ = size;
  }

  // Return the pointer to the old data.
  template <class T = char>
  T* PopFrontN(std::size_t n = 1) {
    auto size = sizeof(T) * n;
    assert(size < size_);
    char* ptr = data_;
    data_ += size;
    size_ -= size;
    return reinterpret_cast<T*>(ptr);
  }

  // Return the pointer to the new tail.
  template <class T = char>
  T* PopBackN(std::size_t n = 1) {
    auto size = sizeof(T) * n;
    assert(size < size_);
    size_ -= size;
    return reinterpret_cast<T*>(data_ + size_);
  }

  template <class U = char>
  U* PeekN(std::size_t n = 1, std::size_t pos = 0) {
    assert(pos + sizeof(U) * n < GetSize());
    return reinterpret_cast<U*>(GetData() + pos);
  }

 protected:
  char* head_;
  std::size_t capacity_;
  char* data_;
  std::size_t size_ = 0;
};

class BytestoreAppendableFile;
class BytestoreWritableFile final : public WritableFile {
 public:
  BytestoreWritableFile(
      bool skip_sync, std::string fname,
      std::unique_ptr<BytestoreAppendableFile> appendable_file);

  using WritableFile::Append;
  Status Append(const Slice& data) override;

  Status Close() override;

  // NO NEED FOR FLUSH
  Status Flush() override { return Status::OK(); }

  Status Sync() override;

  // @todo: need to confirm
  bool IsSyncThreadSafe() const override { return true; }

 private:
  const bool skip_sync_;

  std::string fname_;
  std::unique_ptr<BytestoreAppendableFile> appendable_file_;
};

class BytestoreWritableEcFile final : public WritableFile {
 public:
  BytestoreWritableEcFile(
      bool skip_sync, std::string fname,
      std::unique_ptr<BytestoreAppendableFile> appendable_file,
      std::size_t alignment);

  using WritableFile::Append;
  Status Append(const Slice& data) override;

  Status Close() override {
    if (!buffer_is_empty_) {
      Sync();
    }
    assert(buffer_is_empty_);
    return Status::OK();
  }

  // NO NEED FOR FLUSH
  Status Flush() override { return Status::OK(); }

  Status Sync() override;

  // @todo: need to confirm
  bool IsSyncThreadSafe() const override { return true; }

 private:
  void SetInvalid() { is_valid_ = false; }

  bool IsValid() const { return is_valid_; }

  std::size_t append(const Slice& data);

  std::size_t appendV(const struct iovec* data, size_t iovcnt,
                      size_t total_size);

  const bool skip_sync_;
  bool is_valid_{true};
  bool buffer_is_empty_{true};
  std::string fname_;
  std::unique_ptr<BytestoreAppendableFile> appendable_file_;
  std::size_t alignment_{0};
  std::string buffer_str_;
  Buffer buffer_;
};

}  // namespace bytestore_fs
}  // namespace TERARKDB_NAMESPACE
