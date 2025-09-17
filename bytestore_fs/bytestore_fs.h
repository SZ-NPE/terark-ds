#pragma once

#ifdef USE_BYTESTORE_MOCK
#include "mock.h"
#else
#include <bytestore.h>
#endif

#include <sys/time.h>
#include <sys/uio.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cstdlib>

#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/object_registry.h"

extern thread_local int bts_file_level;
namespace TERARKDB_NAMESPACE {

#ifdef IO_TRACE_DEBUG
extern thread_local std::ostringstream thread_stream;
#endif

namespace bytestore_fs {

#ifdef IO_TRACE_DEBUG
static uint64_t BTSNowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}
#endif

struct BytestoreFileSystemInitOptions {
  std::string process_name;
  std::string log_dir;
  int32_t log_level;
  int32_t bthread_concurrency;
  bool enable_rdma;
};

struct BytestoreFileSystemIOOptions {
  int64_t read_timeout_ms{0};
  int64_t pread_timeout_ms{0};
  int64_t write_timeout_ms{0};
};

struct BytestoreFileSystemOptions {
  std::string prefix;
  std::string region_name;
  std::string cluster_name;
  std::string pool_name;

  BytestoreFileSystemIOOptions io_options;
};

Status parse_uri(const std::string &uri, BytestoreFileSystemOptions *options);

const std::string kTempFileNameSuffix = ".envtmp";

bytestore_blob *bytestore_open_wrapper(const std::string &blob_name, int mode,
                                       bytestore_open_options *opts,
                                       bytestore_message *msg);

struct CreateAppendableFileOptions {
  using StorageMediumType = bytestore_storage_type;
  using PlacementPolicy = bytestore_replicated_policy;
  using StorageEcPolicy = bytestore_ec_policy;

  bool async_mode{false};
  StorageMediumType storage_medium_type{StorageMediumType::STORAGE_TYPE_ANY};
  PlacementPolicy placement_policy{PlacementPolicy::TYPE_REP_FLAT};

  int32_t num_backups{3};
  int64_t max_chunk_size{64 * 1024 * 1024};
  bool using_min_max_copy{false};
  uint32_t write_lag_tolerance{0};

  bool using_erasure_coding{false};
  StorageEcPolicy storage_ec_policy{StorageEcPolicy::TYPE_SINGLE_EC_RS};
  uint32_t ec_packet_size{2048};
  uint16_t ec_data_units{4};
  uint16_t ec_parity_units{2};
};

enum class FileType {
  kFile,
  kDirectory,
  kLast,
};

struct FileStatus {
  FileType type{FileType::kLast};
  int64_t size{-1};
  int64_t mtime{-1};
  std::string unique_id;

  FileStatus() = default;

  FileStatus(FileType _type, int64_t _size, int64_t _mtime,
             std::string _unique_id)
      : type(_type),
        size(_size),
        mtime(_mtime),
        unique_id(std::move(_unique_id)) {}
};

template <class ResType, class... ArgTypes>
class Callable {
 public:
  virtual ~Callable() = default;

  virtual ResType Call(ArgTypes... args) = 0;
};

class BytestoreAppendableFile;

class BytestoreFileSystem : public FileSystemWrapper {
 public:
  static const std::size_t kUniqueIdMaxSize;

  static void Init(const BytestoreFileSystemInitOptions &opts);

  static void Shutdown();

  static std::unique_ptr<FileSystem> New(
      const std::shared_ptr<FileSystem> &base,
      BytestoreFileSystemOptions options);

  using BSStatus = bytestore_status;
  using Message = bytestore_message;
  using OpenBlobOptions = bytestore_open_options;
  using OpenPoolOptions = bytestore_open_pool_options;
  using PoolTraverseOptions = bytestore_traverse_options;
  using Pool = bytestore_pool;
  using Blob = bytestore_blob;
  using Entry = bytestore_entry;
  using Stat = bytestore_stat_t;
  using BlobCondition = bytestore_blob_condition;

  static constexpr const char *kProto = "blob://";

  static const char *kClassName() { return "BytestoreFileSystem"; }

  const char *Name() const override { return "BytestoreFileSystem"; }

  static const char *kNickName() { return "bytestore"; }

  const char *NickName() const { return kNickName(); }

  virtual ~BytestoreFileSystem();

  // std::string GetId() const override;

  IOStatus NewSequentialFile(const std::string &fname,
                             const FileOptions &options,
                             std::unique_ptr<FSSequentialFile> *result,
                             IODebugContext *dbg) override;

  IOStatus NewRandomAccessFile(const std::string &fname,
                               const FileOptions &options,
                               std::unique_ptr<FSRandomAccessFile> *result,
                               IODebugContext *dbg) override;

  IOStatus NewWritableFile(const std::string & /*fname*/,
                           const FileOptions & /*options*/,
                           std::unique_ptr<FSWritableFile> * /*result*/,
                           IODebugContext * /*dbg*/) override;
  IOStatus NewDirectory(const std::string & /*name*/,
                        const IOOptions & /*options*/,
                        std::unique_ptr<FSDirectory> * /*result*/,
                        IODebugContext * /*dbg*/) override;

  IOStatus FileExists(const std::string & /*fname*/,
                      const IOOptions & /*options*/,
                      IODebugContext * /*dbg*/) override;

  IOStatus GetChildren(const std::string & /*path*/,
                       const IOOptions & /*options*/,
                       std::vector<std::string> * /*result*/,
                       IODebugContext * /*dbg*/) override;

  IOStatus DeleteFile(const std::string & /*fname*/,
                      const IOOptions & /*options*/,
                      IODebugContext * /*dbg*/) override;

  IOStatus CreateDir(const std::string & /*name*/,
                     const IOOptions & /*options*/,
                     IODebugContext * /*dbg*/) override {
    return IOStatus::OK();
  }
  IOStatus CreateDirIfMissing(const std::string & /*name*/,
                              const IOOptions & /*options*/,
                              IODebugContext * /*dbg*/) override;

  IOStatus DeleteDir(const std::string & /*name*/,
                     const IOOptions & /*options*/,
                     IODebugContext * /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus GetFileSize(const std::string & /*fname*/,
                       const IOOptions & /*options*/, uint64_t * /*size*/,
                       IODebugContext * /*dbg*/) override;

  IOStatus GetFileModificationTime(const std::string & /*fname*/,
                                   const IOOptions & /*options*/,
                                   uint64_t * /*time*/,
                                   IODebugContext * /*dbg*/) override;

  IOStatus RenameFile(const std::string & /*src*/,
                      const std::string & /*target*/,
                      const IOOptions & /*options*/,
                      IODebugContext * /*dbg*/) override;

  IOStatus LinkFile(const std::string &s, const std::string &t,
                    const IOOptions & /*options*/,
                    IODebugContext * /*dbg*/) override;

  IOStatus LockFile(const std::string & /*fname*/,
                    const IOOptions & /*options*/, FileLock ** /*lock*/,
                    IODebugContext * /*dbg*/) override;

  IOStatus UnlockFile(FileLock * /*lock*/, const IOOptions & /*options*/,
                      IODebugContext * /*dbg*/) override;

  IOStatus IsDirectory(const std::string & /*path*/,
                       const IOOptions & /*options*/, bool * /*is_dir*/,
                       IODebugContext * /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus GetAbsolutePath(const std::string &db_path,
                           const IOOptions & /*opts*/, std::string *output_path,
                           IODebugContext * /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  static std::string TempFileName(const std::string &db_name) {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    std::string uuid_str = std::string(boost::uuids::to_string(uuid));
    return db_name + "/" + uuid_str + kTempFileNameSuffix;
  }

  IOStatus GetTestDirectory(const IOOptions &options, std::string *result,
                            IODebugContext *dbg) override;

  FileOptions OptimizeForLogRead(
      const FileOptions &file_options) const override;

  static constexpr const char *kBytestoreBlobScheme = "blob://";
  static constexpr const char *kBytestoreLocalBlobScheme = "local://";

 private:
  friend class BytestoreFile;
  friend class BytestoreReadableFile;
  friend class BytestoreWritableFile;

  BytestoreFileSystem(const std::shared_ptr<FileSystem> &base,
                      std::string pool_name,
                      BytestoreFileSystemIOOptions io_options);

  IOStatus CreateFile(
      const std::string &path, const CreateAppendableFileOptions &create_opts,
      std::unique_ptr<BytestoreAppendableFile> *appendable_file);

  static OpenBlobOptions ToOpenBlobOptions(
      const CreateAppendableFileOptions &create_opts);

  std::string pool_name_;
  std::string inline_blob_name_;
  BlobCondition blob_condition_;
  BytestoreFileSystemIOOptions io_options_;

  static std::once_flag init_flag_;
};

using WriteCallback =
    Callable<void, std::size_t, BytestoreFileSystem::Message *>;
using ReadCallback =
    Callable<void, std::size_t, BytestoreFileSystem::Message *>;

class BytestoreFile {
 public:
  BytestoreFile() = default;

  ~BytestoreFile() { assert(blob_ == nullptr); }

  // disable copy constructor
  BytestoreFile(const BytestoreFile &) = delete;

  BytestoreFile(BytestoreFile &&other);

  static IOStatus Open(BytestoreFileSystem *filesystem,
                       const std::string &blob_name, int mode,
                       BytestoreFileSystem::OpenBlobOptions *opts,
                       std::unique_ptr<BytestoreFile> *file);

  void Close();

  FileStatus GetStatus() const;

  std::size_t Read(void *buff, std::size_t count);
  std::size_t Pread(void *buff, std::size_t count, off_t offset);
  void AsyncPread(void *buff, std::size_t count, off_t offset,
                  ReadCallback *callback);

  std::size_t Append(const void *buff, std::size_t count);
  void AsyncAppend(const void *buff, std::size_t count,
                   WriteCallback *callback);
  std::size_t AsyncAppendV(const struct iovec *data, size_t iovcnt);

  void Sync();

 private:
  BytestoreFile(BytestoreFileSystem *filesystem,
                BytestoreFileSystem::Blob *blob, std::string name)
      : filesystem_(filesystem), blob_(blob), blob_name_(name) {}

  static void AsyncReadCallback(ssize_t size_read,
                                BytestoreFileSystem::Message *message,
                                void *args);
  static void AsyncWriteCallback(ssize_t size_written,
                                 BytestoreFileSystem::Message *message,
                                 void *args);

  bytestore_io_options GetBytestoreWriteOpts() const {
    auto opts = bytestore_default_write_options();
    if (filesystem_->io_options_.write_timeout_ms > 0) {
      opts.timeout_in_ms_ = filesystem_->io_options_.write_timeout_ms;
    }
    return opts;
  }

  BytestoreFileSystem *filesystem_{nullptr};
  BytestoreFileSystem::Blob *blob_{nullptr};
  std::string blob_name_;
  std::vector<BytestoreFileSystem::Blob *> sub_blobs_;
};

class BytestoreAppendableFile {
 public:
  BytestoreAppendableFile(std::unique_ptr<BytestoreFile> &&file,
                          std::string prefix)
      : file_(std::move(file)), prefix_(std::move(prefix)) {}

  ~BytestoreAppendableFile() { Close(); }

  void Close();

  std::size_t Append(Slice data);

  void AsyncAppend(Slice data, WriteCallback *callback);

  std::size_t AsyncAppendV(const struct iovec *data, size_t iovcnt);

  void Sync();

  FileStatus GetStatus();

  std::string GetPathPrefix();

 private:
  std::unique_ptr<BytestoreFile> file_;
  std::string prefix_;
};

class BytestoreReadableFile : virtual public FSSequentialFile,
                              virtual public FSRandomAccessFile {
 private:
  BytestoreFileSystem *filesystem_;
  BytestoreFileSystem::Blob *blob_;
  std::string blob_name_;

  bytestore_io_options GetBytestoreReadOpts() const {
    auto opts = bytestore_default_read_options();
    if (filesystem_->io_options_.read_timeout_ms > 0) {
      opts.timeout_in_ms_ = filesystem_->io_options_.read_timeout_ms;
    }
    return opts;
  }

 public:
  BytestoreReadableFile(BytestoreFileSystem *fs, const std::string &blob_name,
                        int mode, BytestoreFileSystem::OpenBlobOptions *opts)
      : filesystem_(fs), blob_name_(blob_name) {
    blob_ = bytestore_open_wrapper(blob_name, mode, opts, nullptr);
  }

  ~BytestoreReadableFile() override {
    if (blob_ != nullptr) {
      BytestoreFileSystem::Message msg;
      bytestore_close(blob_, &msg);
      blob_ = nullptr;
      if (msg.status_ != BytestoreFileSystem::BSStatus::STATUS_OK) {
        // TODO zhiyi: exception?
        assert(0);
      }
    }
  }

  // Random access, read data from specified offset in file
  IOStatus Read(uint64_t offset, size_t n, const IOOptions &options,
                Slice *result, char *scratch,
                IODebugContext *dbg) const override {
    if (blob_ == nullptr) {
      return IOStatus::PathNotFound();
    }

#ifdef IO_TRACE_DEBUG
    uint64_t start_time = BTSNowMicros();
    if (is_foreground_operation()) {
      thread_stream << "[R] FG level " << bts_file_level;
    } else if (is_compaction_operation()) {
      thread_stream << "[R] CP level " << bts_file_level;
    } else if (is_flush_operation()) {
      thread_stream << "[R] FL ";
    } else if (is_open_operation()) {
      thread_stream << "[R] OP ";
    } else if (is_garbage_collenction_operation()) {
      thread_stream << "[R] GC ";
    } else {
      thread_stream << "[R] XX ";
    }
#endif

    BytestoreFileSystem::Message msg;
    auto opts = GetBytestoreReadOpts();
    int r = bytestore_pread(blob_, scratch, n, offset, &opts, &msg);
    if (r < 0 || msg.status_ != STATUS_OK) {
      if (msg.status_ == STATUS_TIMEOUT) {
        return status_to_io_status(Status::TimedOut(
            blob_name_ +
            ", bytestore_pread status=" + std::to_string(msg.status_) +
            ", error=" + std::string(msg.message_)));
      } else {
        return status_to_io_status(Status::IOError(
            blob_name_ +
            ", bytestore_pread status=" + std::to_string(msg.status_) +
            ", error=" + std::string(msg.message_)));
      }
    }
    assert(r >= 0);

    *result = Slice(scratch, r);

#ifdef IO_TRACE_DEBUG
    thread_stream << " bytestore_pread: name " << blob_name_ << " off_ "
                  << offset << " size " << n
                  << " time spent (us): " << BTSNowMicros() - start_time
                  << std::endl;
    std::cout << thread_stream.str();
    thread_stream.str("");
    thread_stream.clear();
#endif
    return IOStatus::OK();
  }

  // sequential access, read data at current offset in file
  IOStatus Read(size_t n, const IOOptions &options, Slice *result,
                char *scratch, IODebugContext *dbg) override {
    if (blob_ == nullptr) {
      return IOStatus::PathNotFound();
    }
    IOStatus s;
    BytestoreFileSystem::Message msg;
    auto opts = GetBytestoreReadOpts();
    char *buffer = scratch;
    size_t total_bytes_read = 0;
    size_t remaining_bytes = n;

    while (remaining_bytes > 0) {
#ifdef IO_TRACE_DEBUG
      uint64_t start_time = BTSNowMicros();
      if (is_foreground_operation()) {
        thread_stream << "[R] FG level " << bts_file_level;
      } else if (is_compaction_operation()) {
        thread_stream << "[R] CP level " << bts_file_level;
      } else if (is_flush_operation()) {
        thread_stream << "[R] FL ";
      } else if (is_open_operation()) {
        thread_stream << "[R] OP ";
      } else if (is_garbage_collenction_operation()) {
        thread_stream << "[R] GC ";
      } else {
        thread_stream << "[R] XX ";
      }
#endif
      int bytes_read =
          bytestore_read(blob_, buffer, remaining_bytes, &opts, &msg);
#ifdef IO_TRACE_DEBUG
      thread_stream << " bytestore_read: name " << blob_name_ << " size "
                    << remaining_bytes
                    << " time spent (us): " << BTSNowMicros() - start_time
                    << std::endl;
      std::cout << thread_stream.str();
      thread_stream.str("");
      thread_stream.clear();
#endif

      if (msg.status_ != STATUS_INVALID_ARGUMENT) {
        assert(bytes_read >= 0);
      }
      assert(msg.status_ == STATUS_OK || msg.status_ == STATUS_END_FILE);
      if (bytes_read <= 0) {
        break;
      }
      assert((size_t)bytes_read <= remaining_bytes);
      total_bytes_read += bytes_read;
      remaining_bytes -= bytes_read;
      buffer += bytes_read;
    }
    *result = Slice(scratch, total_bytes_read);

    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    BytestoreFileSystem::Message msg;
    // get current offset from file
    size_t current = bytestore_tellg(blob_, &msg);
    // seek to new offset in file
    size_t newoffset = current + n;
    bytestore_seekg(blob_, newoffset, &msg);
    return IOStatus::OK();
  }
};

class BytestoreWritableFileWrapper : public FSWritableFile {
 public:
  explicit BytestoreWritableFileWrapper(std::unique_ptr<WritableFile> &&_target)
      : target_(std::move(_target)) {}

  IOStatus Append(const Slice &data, const IOOptions & /*options*/,
                  IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->Append(data));
  }
  IOStatus Append(const Slice &data, const IOOptions & /*options*/,
                  const DataVerificationInfo & /*verification_info*/,
                  IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->Append(data));
  }
  IOStatus PositionedAppend(const Slice &data, uint64_t offset,
                            const IOOptions & /*options*/,
                            IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->PositionedAppend(data, offset));
  }
  IOStatus PositionedAppend(const Slice &data, uint64_t offset,
                            const IOOptions & /*options*/,
                            const DataVerificationInfo & /*verification_info*/,
                            IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->PositionedAppend(data, offset));
  }
  IOStatus Truncate(uint64_t size, const IOOptions & /*options*/,
                    IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->Truncate(size));
  }
  IOStatus Close(const IOOptions & /*options*/,
                 IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->Close());
  }
  IOStatus Flush(const IOOptions & /*options*/,
                 IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->Flush());
  }
  IOStatus Sync(const IOOptions & /*options*/,
                IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->Sync());
  }
  IOStatus Fsync(const IOOptions & /*options*/,
                 IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->Fsync());
  }
  bool IsSyncThreadSafe() const override { return target_->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    target_->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return target_->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize(const IOOptions & /*options*/,
                       IODebugContext * /*dbg*/) override {
    return target_->GetFileSize();
  }

  void SetPreallocationBlockSize(size_t size) override {
    target_->SetPreallocationBlockSize(size);
  }

  void GetPreallocationStatus(size_t *block_size,
                              size_t *last_allocated_block) override {
    target_->GetPreallocationStatus(block_size, last_allocated_block);
  }

  size_t GetUniqueId(char *id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return status_to_io_status(target_->InvalidateCache(offset, length));
  }

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                     const IOOptions & /*options*/,
                     IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->RangeSync(offset, nbytes));
  }

  void PrepareWrite(size_t offset, size_t len, const IOOptions & /*options*/,
                    IODebugContext * /*dbg*/) override {
    target_->PrepareWrite(offset, len);
  }

  IOStatus Allocate(uint64_t offset, uint64_t len,
                    const IOOptions & /*options*/,
                    IODebugContext * /*dbg*/) override {
    return status_to_io_status(target_->Allocate(offset, len));
  }

 private:
  std::unique_ptr<WritableFile> target_;
};

class BytestoreFileLock final : public FileLock {
 public:
  ~BytestoreFileLock() override = default;
};

class BytestoreDirectory final : public FSDirectory {
 public:
  IOStatus Fsync(const IOOptions &options, IODebugContext *dbg) override {
    return IOStatus::OK();
  }
};

// std::unique_ptr<Env> NewBytestoreEnv();
Status NewBytestoreFileSystem(const std::string &uri,
                              const std::string &sdk_log_dir, bool enable_rdma,
                              std::unique_ptr<FileSystem> *fs);

}  // namespace bytestore_fs
}  // namespace TERARKDB_NAMESPACE
