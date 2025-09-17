#include "bytestore_env.h"

#include <sys/uio.h>

#include "bytestore_fs.h"
#include "rocksdb/file_system.h"

namespace TERARKDB_NAMESPACE {

namespace bytestore_fs {

BytestoreWritableFile::BytestoreWritableFile(
    bool skip_sync, std::string fname,
    std::unique_ptr<BytestoreAppendableFile> appendable_file)
    : skip_sync_(skip_sync),
      fname_(std::move(fname)),
      appendable_file_(std::move(appendable_file)) {}

Status BytestoreWritableFile::Append(const Slice &data) {
  auto r = appendable_file_->Append(Slice(data.data(), data.size()));
  if (r != data.size()) {
    return Status::IOError("Failed to append to file, appendable file:" +
                           fname_);
  }
  return Status::OK();
}

Status BytestoreWritableFile::Close() { return Status::OK(); }

Status BytestoreWritableFile::Sync() {
  if (!skip_sync_) {
    appendable_file_->Sync();
  }
  return Status::OK();
}

BytestoreWritableEcFile::BytestoreWritableEcFile(
    bool skip_sync, std::string fname,
    std::unique_ptr<BytestoreAppendableFile> appendable_file,
    std::size_t alignment)
    : skip_sync_(skip_sync),
      fname_(std::move(fname)),
      appendable_file_(std::move(appendable_file)),
      alignment_(alignment),
      buffer_str_(alignment_, '\0'),
      buffer_(const_cast<char *>(buffer_str_.data()), buffer_str_.size()) {}

Status BytestoreWritableEcFile::Append(const Slice &data) {
  if (!IsValid()) {
    return Status::IOError("Already Invalid", fname_);
  }

  auto tmp = data;

  assert(buffer_.GetSize() == alignment_);
  if (buffer_.GetSize() > 0) {
    auto copy_size = std::min(tmp.size(), buffer_.GetTailSpaceSize());
    std::memcpy(buffer_.PushBackN(copy_size), tmp.data(), copy_size);
    tmp.remove_prefix(copy_size);

    if (buffer_.GetSize() == alignment_) {
      auto r = append(Slice(buffer_.GetData(), buffer_.GetSize()));
      if (r != alignment_) {
        SetInvalid();
        assert(0);
        return Status::IOError("invalid append 1");
      }
      buffer_.Clear();
    }
  }

  assert(buffer_.GetSize() < alignment_);
  if (buffer_.GetSize() > 0) {
    assert(tmp.size() == 0ULL);
  } else {
    if (tmp.size() > 0) {
      auto append_size = tmp.size() - (tmp.size() % alignment_);
      auto r = append(Slice(tmp.data(), append_size));
      if (r != append_size) {
        SetInvalid();
        assert(0);
        return Status::IOError("invalid append 2");
      }
      tmp.remove_prefix(append_size);

      assert(tmp.size() < alignment_);
      if (tmp.size() > 0) {
        auto copy_size = tmp.size();
        std::memcpy(buffer_.PushBackN(copy_size), tmp.data(), copy_size);
        tmp.remove_prefix(copy_size);
      }
    }
  }

  buffer_is_empty_ = buffer_.GetSize() == 0;
  return Status::OK();
}

Status BytestoreWritableEcFile::Sync() {
  if (!IsValid()) {
    return Status::IOError();
  }
  if (buffer_.GetSize() > 0) {
    assert(!buffer_is_empty_);
    auto pad_size = buffer_.GetTailSpaceSize() % alignment_;
    std::memset(buffer_.PushBackN(pad_size), 0, pad_size);
    assert(buffer_.GetSize() < alignment_);
    auto r = append(Slice(buffer_.GetData(), buffer_.GetSize()));
    if (r != buffer_.GetSize()) {
      SetInvalid();
      return Status::IOError("invalid append 3");
    }
    buffer_.Clear();
    buffer_is_empty_ = buffer_.GetSize() == 0;
  }
  assert(buffer_is_empty_);
  if (!skip_sync_) {
    appendable_file_->Sync();
  }
  return Status::OK();
}

std::size_t BytestoreWritableEcFile::append(const Slice &data) {
  auto r = appendable_file_->Append(data);
  if (r != data.size()) {
    assert(0);
  }
  return r;
}

std::size_t BytestoreWritableEcFile::appendV(const struct iovec *data,
                                             size_t iovcnt, size_t total_size) {
  auto r = appendable_file_->AsyncAppendV(data, iovcnt);
  if (r != total_size) {
    assert(0);
  }
  return r;
}

}  // namespace bytestore_fs
}  // namespace TERARKDB_NAMESPACE
