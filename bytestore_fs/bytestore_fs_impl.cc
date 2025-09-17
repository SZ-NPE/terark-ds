#include <stdio.h>
#include <sys/time.h>
#include <time.h>

#include <algorithm>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "bytestore_env.h"
#include "util/logging.h"
#include "util/string_util.h"

#ifdef IO_TRACE_DEBUG
thread_local std::ostringstream thread_stream;
#endif

extern thread_local int bts_file_level;

namespace TERARKDB_NAMESPACE {

namespace bytestore_fs {

std::pair<std::string, std::string> GetLocalIp() {
  std::pair<std::string, std::string> r =
      std::make_pair("10.128.29.29", "10.128.29.29");
  const char *ip = std::getenv("BYTED_HOST_IP");
  if (!ip) {
    ip = std::getenv("MY_HOST_IP");
  }
  if (ip) {
    r.first = ip;
  }
  ip = std::getenv("BYTED_HOST_IPV6");
  if (!ip) {
    ip = std::getenv("MY_HOST_IPV6");
  }
  if (ip) {
    r.second = ip;
  }
  return r;
}

std::once_flag BytestoreFileSystem::init_flag_;

const std::size_t BytestoreFileSystem::kUniqueIdMaxSize = 12;

const std::size_t sub_file_count = 4;
const bool use_async_write = true;

std::string supported_uri_scheme() {
  return "bytestore uri should start with " +
         std::string(BytestoreFileSystem::kBytestoreBlobScheme) + " or " +
         std::string(BytestoreFileSystem::kBytestoreLocalBlobScheme);
}

std::string example_bytestore_uri() {
  return "the example of bytestore_uri is: "
         "blob://region-name/cluster-name/pool-name/";
}

Status parse_uri(const std::string &uri,
                 BytestoreFileSystemOptions *bytestore_filesystem_opts) {
  Status s;
  std::string cluster_str = uri;  //  blob://store-hl/lst_ec_test/public/
  auto start_with = [](const std::string &uri, const std::string &prefix) {
    return uri.rfind(prefix, 0) == 0;
  };
  if (s.ok()) {
    if (!start_with(uri, BytestoreFileSystem::kBytestoreBlobScheme) &&
        !start_with(uri, BytestoreFileSystem::kBytestoreLocalBlobScheme)) {
      s = Status::InvalidArgument(uri + " is invalid," +
                                  supported_uri_scheme());
    }
  }
  int index = cluster_str.find(':') + 2;
  // parse prefix
  if (s.ok()) {
    bytestore_filesystem_opts->prefix = cluster_str.substr(0, index + 1);
    cluster_str =
        cluster_str.substr(index + 1); /* store-hl/lst_ec_test/public/ */
    index = cluster_str.find('/');
    if (index == 0 || index == std::string::npos) {
      s = Status::InvalidArgument(uri +
                                  " is invalid, parse region_name failed." +
                                  example_bytestore_uri());
    }
  }
  // parse region name
  if (s.ok()) {
    bytestore_filesystem_opts->region_name =
        cluster_str.substr(0, index);            /* store-hl */
    cluster_str = cluster_str.substr(index + 1); /* lst_ec_test/public/ */
    index = cluster_str.find('/');
    if (index == 0 || index == std::string::npos) {
      s = Status::InvalidArgument(uri +
                                  " is invalid, parse cluster_name failed." +
                                  example_bytestore_uri());
    }
  }
  // parse cluster name
  if (s.ok()) {
    bytestore_filesystem_opts->cluster_name =
        cluster_str.substr(0, index);            /* lst_ec_test */
    cluster_str = cluster_str.substr(index + 1); /* public/ */
    index = cluster_str.find('/');
    if (index == 0 || index == std::string::npos) {
      s = Status::InvalidArgument(uri + " is invalid, parse pool failed." +
                                  example_bytestore_uri());
    }
  }
  // parse pool name
  if (s.ok()) {
    cluster_str = cluster_str.substr(0, index);
    bytestore_filesystem_opts->pool_name = cluster_str; /* public */
  }
  return s;
}

bool init_flag(const char *name, const char *value) {
  bool r = bytestore_set_flag(name, value);
  if (!r) {
    fprintf(stderr, "bytestore_set_flag %s=%s failed\n", name, value);
  }
  return r;
}

bool init_flag(const char *name, const std::string &value) {
  return init_flag(name, value.c_str());
}

bool init_flag(const char *name, bool value) {
  return init_flag(name, value ? "true" : "false");
}

void BytestoreFileSystem::Init(const BytestoreFileSystemInitOptions &opts) {
  std::call_once(init_flag_, [&] {
    init_flag("bytestore_client_process_name", opts.process_name);
    init_flag("bytestore_client_log_level", std::to_string(opts.log_level));
    init_flag("bytestore_common_enable_rdma", opts.enable_rdma);
    init_flag("bytestore_log_dir", opts.log_dir);
    init_flag("bthread_concurrency", std::to_string(opts.bthread_concurrency));
    init_flag("bytestore_client_enable_chunk_layer_checksum", true);

    bytestore_init();

    bool host_affinity = false;
    if (host_affinity) {
      // set read affinity
      bytestore_affinity_options affinity_options =
          bytestore_default_read_affinity_options();
      affinity_options.host_affinity_ = true;
      if (!affinity_options.host_affinity_) {
        std::string read_affinity = "/region-01/zone-01/rack-01";
        affinity_options.read_affinity_ =
            const_cast<char *>(read_affinity.c_str());
      }
      bytestore_message message;
      bytestore_set_read_affinity(&affinity_options, &message);
    }
  });
}

static bool is_rocksdb_replica_mode() {
  const char *env_p = std::getenv("ROCKSDB_REPLICA_MODE");
  return env_p != nullptr && std::atoi(env_p) == 1;
}

void BytestoreFileSystem::Shutdown() { bytestore_shutdown(); }

std::unique_ptr<FileSystem> BytestoreFileSystem::New(
    const std::shared_ptr<FileSystem> &base,
    BytestoreFileSystemOptions options) {
  std::stringstream builder;
  builder << options.prefix << options.region_name << "/"
          << options.cluster_name << "/" << options.pool_name;
  auto complete_pool_name = builder.str();
  if (complete_pool_name.back() == '/') {
    complete_pool_name.resize(complete_pool_name.size() - 1);
  }
  return std::unique_ptr<FileSystem>(new BytestoreFileSystem(
      base, std::move(complete_pool_name), options.io_options));
}

BytestoreFileSystem::BytestoreFileSystem(
    const std::shared_ptr<FileSystem> &base, std::string pool_name,
    BytestoreFileSystemIOOptions io_options)
    : FileSystemWrapper(base),
      pool_name_(std::move(pool_name)),
      io_options_(std::move(io_options)) {
  blob_condition_.lock_name_ = nullptr;
}

// assume that there is one global logger for now. It is not thread-safe,
// but need not be because the logger is initialized at db-open time.
// static rocksdb::Logger *mylog = nullptr;

BytestoreFileSystem::~BytestoreFileSystem() {}

IOStatus BytestoreFileSystem::NewSequentialFile(
    const std::string &fname, const FileOptions &options,
    std::unique_ptr<FSSequentialFile> *result, IODebugContext *dbg) {
  result->reset();
  auto opts = bytestore_default_open_options();
  opts.enable_backup_read_ = false;
  int mode = BS_O_RDONLY | BS_O_AEXCL;
  if (options.bytestore_read_only || is_rocksdb_replica_mode()) {
    mode = BS_O_RDONLY;
  }
  BytestoreReadableFile *f =
      new BytestoreReadableFile(this, fname, mode, &opts);
  result->reset(f);
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::NewRandomAccessFile(
    const std::string &fname, const FileOptions &options,
    std::unique_ptr<FSRandomAccessFile> *result, IODebugContext *dbg) {
  // std::cout << "NewRandomAccessFile: " << fname << std::endl;
  result->reset();
  auto opts = bytestore_default_open_options();
  opts.metrics_tag_id_ =
      bts_file_level > -1 ? 2 : 3;  // 2 - ksst; 3 - blob/vsst
  opts.enable_backup_read_ = false;

  int mode = BS_O_RDONLY | BS_O_AEXCL;
  if (options.bytestore_read_only || is_rocksdb_replica_mode()) {
    mode = BS_O_RDONLY;
  }
  BytestoreReadableFile *f =
      new BytestoreReadableFile(this, fname, mode, &opts);
  result->reset(f);
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::FileExists(const std::string &fname,
                                         const IOOptions &options,
                                         IODebugContext *dbg) {
  std::string blob_name = fname;
  auto opts = bytestore_default_stat_options();
  if (blob_condition_.lock_name_) {
    opts.condition_ = blob_condition_;
  }
  BytestoreFileSystem::Stat stat;
  BytestoreFileSystem::Message msg;
  auto r = bytestore_stat(blob_name.c_str(), &stat, &opts, &msg);
  if (!r) {
    return IOStatus::NotFound();
  }
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::NewDirectory(const std::string &name,
                                           const IOOptions &options,
                                           std::unique_ptr<FSDirectory> *result,
                                           IODebugContext *dbg) {
  result->reset(new BytestoreDirectory());
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::GetTestDirectory(const IOOptions &options,
                                               std::string *result,
                                               IODebugContext *dbg) {
  result->assign(pool_name_ + "/test-000001");
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::CreateDirIfMissing(const std::string & /*name*/,
                                                 const IOOptions & /*options*/,
                                                 IODebugContext * /*dbg*/) {
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::GetChildren(const std::string &path,
                                          const IOOptions &options,
                                          std::vector<std::string> *result,
                                          IODebugContext *dbg) {
  auto open_opts = bytestore_default_open_pool_options();
  BytestoreFileSystem::Message msg;
  if (!Slice(path).starts_with(pool_name_ + '/')) {
    return IOStatus::NotFound();
  }
  // without pool name "blob://dc/cluster/pool/"
  std::string prefix = path.substr(pool_name_.size() + 1);

#ifdef IO_TRACE_DEBUG
  std::cout << "GetChildren pool: " << pool_name_ << ", prefix: " << prefix
            << std::endl;
#endif
  strncpy(open_opts.prefix_blob_name_, prefix.c_str(),
          sizeof(open_opts.prefix_blob_name_) - 1);

  auto pool = bytestore_open_pool(pool_name_.c_str(), &open_opts, &msg);
  if (!pool) {
    return IOStatus::IOError();
  }

  auto traverse_opts = bytestore_default_traverse_options();
  if (blob_condition_.lock_name_) {
    traverse_opts.condition_ = blob_condition_;
  }
  while (true) {
    BytestoreFileSystem::Entry entry;
    bool valid = bytestore_traverse_pool(pool, &entry, &traverse_opts, &msg);
    if (msg.status_ != BytestoreFileSystem::BSStatus::STATUS_OK) {
      std::cout << "GetChildren failed" << std::endl;
      return IOStatus::IOError();
    }
    if (!valid) {
      break;
    }
    std::string entry_name = std::string(entry.name_);
    if (entry_name.find("LOCK") != std::string::npos) {
      continue;
    }
    std::string name = entry_name.substr(prefix.size());
    if (name[0] == '/') {
      name = name.substr(1);
    }
    result->push_back(name);
  }
  bytestore_close_pool(pool, &msg);

  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::DeleteFile(const std::string &path,
                                         const IOOptions &options,
                                         IODebugContext *dbg) {
  std::string blob_name = path;
  auto opts = bytestore_default_delete_options();
  opts.condition_ = blob_condition_;

  BytestoreFileSystem::Message msg;
  auto r = bytestore_delete(blob_name.c_str(), &opts, &msg);
  if (!r) {
    std::string err_msg =
        "delete " + blob_name + "failed " + std::string(msg.message_);
    return IOStatus::IOError(err_msg);
  }

  if (use_async_write && blob_name.find(".log") != std::string::npos) {
    for (int i = 1; i < sub_file_count; i++) {
      std::string sub_blob_name = blob_name + "_" + std::to_string(i);
      bool ret_sub = bytestore_delete(sub_blob_name.c_str(), &opts, &msg);
      if (!ret_sub) {
        std::string err_msg =
            "delete " + sub_blob_name + "failed " + std::string(msg.message_);
        return IOStatus::IOError(err_msg);
      }
    }
  }
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::RenameFile(const std::string &src,
                                         const std::string &dst,
                                         const IOOptions &options,
                                         IODebugContext *dbg) {
  auto opts = bytestore_default_rename_options();
  opts.condition_ = blob_condition_;
  BytestoreFileSystem::Message msg;
  auto r = bytestore_rename(src.c_str(), dst.c_str(), &opts, &msg);
  if (!r) {
    std::string err_msg = "rename " + src + " to " + dst +
                          " failed: " + std::string(msg.message_);
    return IOStatus::IOError(err_msg);
  }
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::LinkFile(const std::string &src,
                                       const std::string &dst,
                                       const IOOptions &options,
                                       IODebugContext *dbg) {
  auto opts = bytestore_default_hardlink_options();
  opts.condition_ = blob_condition_;
  BytestoreFileSystem::Message msg;
  auto r = bytestore_hardlink(src.c_str(), dst.c_str(), &opts, &msg);
  if (!r) {
    std::string err_msg = "hardlink " + src + " to " + dst +
                          " failed: " + std::string(msg.message_);
    return IOStatus::IOError(err_msg);
  }
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::GetFileSize(const std::string &fname,
                                          const IOOptions &options,
                                          uint64_t *size, IODebugContext *dbg) {
  std::string blob_name = fname;
  auto opts = bytestore_default_stat_options();
  if (blob_condition_.lock_name_) {
    opts.condition_ = blob_condition_;
  }
  BytestoreFileSystem::Stat stat;
  BytestoreFileSystem::Message msg;
  auto r = bytestore_stat(blob_name.c_str(), &stat, &opts, &msg);
  if (!r) {
    return IOStatus::NotFound();
  }
  *size = stat.size_;
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::GetFileModificationTime(const std::string &fname,
                                                      const IOOptions &options,
                                                      uint64_t *time,
                                                      IODebugContext *dbg) {
  std::string blob_name = fname;
  auto opts = bytestore_default_stat_options();
  if (blob_condition_.lock_name_) {
    opts.condition_ = blob_condition_;
  }
  BytestoreFileSystem::Stat stat;
  BytestoreFileSystem::Message msg;
  auto r = bytestore_stat(blob_name.c_str(), &stat, &opts, &msg);
  if (!r) {
    return IOStatus::NotFound();
  }
  *time = stat.mtime_;
  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::LockFile(const std::string &path,
                                       const IOOptions &options,
                                       FileLock **lock, IODebugContext *dbg) {
  inline_blob_name_ = path;
  auto generateRandomString = []() {
    const int length = 32;
    std::string result;
    const std::string charset =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    struct timeval time;
    gettimeofday(&time, NULL);
    srand((time.tv_sec * 1000) + (time.tv_usec / 1000));
    for (int i = 0; i < length; ++i) {
      result += charset[rand() % charset.length()];
    }
    return result;
  };

  auto opts = bytestore_default_create_inline_blob_options();
  BytestoreFileSystem::Message msg;
  auto r = bytestore_create_inline_blob(inline_blob_name_.c_str(), &opts, &msg);
  if (!r && msg.status_ != bytestore_status::STATUS_ALREADY_EXIST) {
    return IOStatus::IOError("create inline blob " + inline_blob_name_ +
                             " failed");
  }

  std::string inline_blob_cookie = generateRandomString();
  inline_blob_cookie.erase(
      std::remove(inline_blob_cookie.begin(), inline_blob_cookie.end(), '-'),
      inline_blob_cookie.end());

  bytestore_inline_blob_stat stat;
  std::memcpy(stat.content_, inline_blob_cookie.c_str(),
              inline_blob_cookie.length());

  auto update_opts = bytestore_default_update_inline_blob_options();
  r = bytestore_update_inline_blob(inline_blob_name_.c_str(), &stat,
                                   &update_opts, &msg);
  if (!r) {
    return IOStatus::IOError("update inline blob " + inline_blob_name_ +
                             " failed");
  }

  blob_condition_.lock_name_ = inline_blob_name_.c_str();
  std::memcpy(blob_condition_.content_, inline_blob_cookie.c_str(),
              k_inline_blob_content_size);

  return IOStatus::OK();
}

IOStatus BytestoreFileSystem::UnlockFile(FileLock *lock,
                                         const IOOptions &options,
                                         IODebugContext *dbg) {
  return IOStatus::OK();
}

BytestoreFileSystem::OpenBlobOptions BytestoreFileSystem::ToOpenBlobOptions(
    const CreateAppendableFileOptions &create_opts) {
  auto opts = bytestore_default_open_options();
  opts.placement_.storage_policy_.type_ = create_opts.storage_medium_type;
  opts.max_chunk_size_ = create_opts.max_chunk_size;
  opts.using_erasure_coding_ = create_opts.using_erasure_coding;

  if (!opts.using_erasure_coding_) {
    opts.backup_parameter_.num_backups_ = create_opts.num_backups;
    opts.rep_policy_ = create_opts.placement_policy;
    opts.using_min_max_copy_ = create_opts.using_min_max_copy;
    opts.write_lag_tolerance_ = create_opts.write_lag_tolerance;
  } else {
    opts.ec_policy_ = create_opts.storage_ec_policy;
    opts.backup_parameter_.packet_size_ = create_opts.ec_packet_size;
    opts.backup_parameter_.data_units_ = create_opts.ec_data_units;
    opts.backup_parameter_.parity_units_ = create_opts.ec_parity_units;
  }

  return opts;
}

IOStatus BytestoreFileSystem::CreateFile(
    const std::string &path, const CreateAppendableFileOptions &create_opts,
    std::unique_ptr<BytestoreAppendableFile> *appendable_file) {
  auto blob_name = path;
  auto mode = BS_O_CREATE | BS_O_WRONLY | BS_O_SYNC | BS_O_TRUNC;
  if (create_opts.async_mode) {
    mode |= BS_O_ASYNC;
  }
  auto opts = ToOpenBlobOptions(create_opts);
  if (blob_condition_.lock_name_) {
    opts.condition_ = blob_condition_;
  }

  if (boost::algorithm::ends_with(path, ".sst")) {
    opts.metrics_tag_id_ =
        bts_file_level > -1 ? 2 : 3;  // 2 - ksst; 3 - blob/vsst
  } else if (boost::algorithm::ends_with(path, ".log")) {
    opts.metrics_tag_id_ = 1;  // 1 - wal log
  } else {
    opts.metrics_tag_id_ = 7;  // 7 - other files
  }
  opts.enable_backup_read_ = false;

  std::unique_ptr<BytestoreFile> file;
  auto s = BytestoreFile::Open(this, blob_name, mode, &opts, &file);
  if (s.ok()) {
    appendable_file->reset(new BytestoreAppendableFile(std::move(file), path));
  }
  return s;
}

IOStatus BytestoreFileSystem::NewWritableFile(
    const std::string &fname, const FileOptions &options,
    std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg) {
  assert(!fname.empty());
  assert(result);

  CreateAppendableFileOptions create_opts;
  create_opts.async_mode = false;
  create_opts.ec_packet_size = options.ec_packet_size;
  create_opts.ec_data_units = options.ec_data_units;
  create_opts.ec_parity_units = options.ec_parity_units;
  create_opts.using_erasure_coding = options.enable_erasure_coding;

  if (!boost::algorithm::ends_with(
          fname, ".sst")) {  // log, manifest and other non-sst files
    if (options.enable_erasure_coding) {
      // even enable_erasure_coding, no ec for these files
      create_opts.using_erasure_coding = false;
      create_opts.using_min_max_copy = true;
      create_opts.write_lag_tolerance = 1;  // quorum
    }
  } else {  // SST File
    // create_opts.using_erasure_coding use default value

    // key-value separation
    create_opts.using_erasure_coding =
        (bts_file_level > -1 ? false : options.enable_erasure_coding);
    // create_opts.using_erasure_coding = (bts_file_level == 6 ? false :
    // options.enable_erasure_coding);

    // no key-value separation
    // create_opts.using_erasure_coding = options.enable_erasure_coding;
  }
  // std::cout << "blob name " << fname
  //           << " using_erasure_coding: " << create_opts.using_erasure_coding
  //           << " bts_file_level: " << bts_file_level << std::endl;

  std::unique_ptr<BytestoreAppendableFile> appendable_file_r;
  auto s = CreateFile(fname, create_opts, &appendable_file_r);

  if (!appendable_file_r) {
    return IOStatus::IOError(
        "failed to create appendable file when open writable file.");
  }

  std::unique_ptr<WritableFile> file;
  if (create_opts.using_erasure_coding) {
    file = std::make_unique<BytestoreWritableEcFile>(
        create_opts.async_mode, fname, std::move(appendable_file_r),
        create_opts.ec_packet_size * create_opts.ec_data_units);
  } else {
    file = std::make_unique<BytestoreWritableFile>(
        true, fname, std::move(appendable_file_r));
  }

  result->reset(new BytestoreWritableFileWrapper(std::move(file)));

  return IOStatus::OK();
}

FileOptions BytestoreFileSystem::OptimizeForLogRead(
    const FileOptions &file_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.bytestore_read_only = true;
  return optimized_file_options;
}

IOStatus BSIOError(const std::string &context, const bytestore_message &msg) {
  return status_to_io_status(
      Status::IOError(context + ", status=" + std::to_string(msg.status_) +
                      ", error=" + std::string(msg.message_)));
}

BytestoreFile::BytestoreFile(BytestoreFile &&other)
    : filesystem_(other.filesystem_),
      blob_(other.blob_),
      blob_name_(other.blob_name_),
      sub_blobs_(other.sub_blobs_) {
  other.blob_ = nullptr;
  other.filesystem_ = nullptr;
}

IOStatus BytestoreFile::Open(BytestoreFileSystem *filesystem,
                             const std::string &blob_name, int mode,
                             BytestoreFileSystem::OpenBlobOptions *opts,
                             std::unique_ptr<BytestoreFile> *file) {
  IOStatus s;
  BytestoreFileSystem::Message msg;
  auto blob = bytestore_open_wrapper(blob_name.c_str(), mode, opts, &msg);
  if (blob == nullptr) {
    return IOStatus::IOError("open failed " + std::to_string(msg.status_));
  }
  file->reset(new BytestoreFile(filesystem, blob, blob_name));

  if (use_async_write && blob_name.find("log") != std::string::npos) {
    // create sub wal files
    auto &sub_blobs = file->get()->sub_blobs_;
    sub_blobs.reserve(sub_file_count);
    sub_blobs[0] = blob;
    for (int i = 1; i < sub_file_count; i++) {
      std::string sub_blob_name = blob_name + "_" + std::to_string(i);
      auto sub_blob =
          bytestore_open_wrapper(sub_blob_name.c_str(), mode, opts, &msg);
      if (sub_blob == nullptr) {
        s = IOStatus::IOError("open sub_blob failed " +
                              std::to_string(msg.status_));
      }
      sub_blobs[i] = sub_blob;
    }
  }
  return s;
}

void BytestoreFile::Close() {
  if (blob_ != nullptr) {
    BytestoreFileSystem::Message msg;
    bytestore_close(blob_, &msg);
    blob_ = nullptr;
    if (msg.status_ != BytestoreFileSystem::BSStatus::STATUS_OK) {
      assert(0);
    }
    for (int i = 1; i < sub_blobs_.size(); i++) {
      bytestore_close(sub_blobs_[i], &msg);
      if (msg.status_ != BytestoreFileSystem::BSStatus::STATUS_OK) {
        assert(0);
      }
    }
    sub_blobs_.clear();
  }
}

FileStatus BytestoreFile::GetStatus() const {
  auto opts = bytestore_default_stat_options();
  if (filesystem_->blob_condition_.lock_name_) {
    opts.condition_ = filesystem_->blob_condition_;
  }
  BytestoreFileSystem::Stat stat;
  BytestoreFileSystem::Message msg;
  auto r = bytestore_bstat(blob_, &stat, &opts, &msg);
  if (!r) {
    return FileStatus(FileType::kLast, 0, 0, "");
  }
  // TODO: generate a correct unique_id
  std::string unique_id(std::to_string(stat.blob_id_.sequence_id_) + "_" +
                        std::to_string(stat.blob_id_.unique_pool_id_));
  return FileStatus(FileType::kFile, stat.size_, stat.mtime_,
                    std::move(unique_id));
}

struct async_write_arg {
  ssize_t expect_length_;
  int *write_result_;
  // synchronization
  std::condition_variable *cv_;
  std::mutex *cv_mutex_;
  int *ref_count_;  // down to 0 means finished

  async_write_arg(ssize_t expect_length, int *write_result,
                  std::condition_variable *cv, std::mutex *cv_mutex,
                  int *ref_count)
      : expect_length_(expect_length),
        write_result_(write_result),
        cv_(cv),
        cv_mutex_(cv_mutex),
        ref_count_(ref_count) {}

  ~async_write_arg() { signal(); }

  void signal() {
    std::unique_lock<std::mutex> cv_lock(*cv_mutex_);
    int cnt = --(*ref_count_);
    // printf("signal cnt: %d\n", cnt);
    cv_->notify_all();
  }
};

// Async write callback
static void async_write_callback(ssize_t size_written,
                                 struct bytestore_message *message,
                                 void *args) {
  if (args == nullptr) {
    return;
  }
  async_write_arg *write_arg = reinterpret_cast<async_write_arg *>(args);
  int result = message->status_;
  if (message->status_ == STATUS_OK &&
      size_written == write_arg->expect_length_) {
    // write succeeded
  } else if (message->status_ == STATUS_OK) {
    // partial write
    result = -1;
  } else {
    // write failed
    result = -100;
  }
  *(write_arg->write_result_) = result;
  delete write_arg;
  args = nullptr;
}

void wait_async_write(std::condition_variable &cv, std::mutex &cv_mutex,
                      int &ref_count, int line_called = 0,
                      const char *blob_name = nullptr) {
  // printf("[%s:%d] before wait \"%s\" = %d\n", __func__, line_called,
  //        blob_name, ref_count);
  std::unique_lock<std::mutex> cv_lock(cv_mutex);
  if (ref_count > 0) {
    cv.wait(cv_lock, [&ref_count] { return ref_count == 0; });
  }
  // printf("[%s:%d] after wait \"%s\" = %d\n", __func__, line_called,
  //        blob_name, ref_count);
}

std::size_t BytestoreFile::Append(const void *buff, std::size_t count) {
#ifdef IO_TRACE_DEBUG
  uint64_t start_time = BTSNowMicros();
  bool filter = false;
  if (is_foreground_operation()) {
    filter = true;
    if (!filter) {
      thread_stream << "[W] FG ";
    }
  } else if (is_compaction_operation()) {
    thread_stream << "[W] CP level " << bts_file_level;
  } else if (is_flush_operation()) {
    filter = true;
    if (!filter) {
      thread_stream << "[W] FL level " << bts_file_level;
    }
  } else if (is_open_operation()) {
    thread_stream << "[W] OP ";
  } else if (is_garbage_collenction_operation()) {
    thread_stream << "[W] GC ";
  } else {
    thread_stream << "[W] XX ";
  }
#endif
  BytestoreFileSystem::Message msg;
  auto opts = GetBytestoreWriteOpts();
  bool is_wal = blob_name_.find("log") != std::string::npos;
  int r = 0;
  if (use_async_write && is_wal) {
    const std::size_t sub_buf_size =
        sub_file_count > 0 ? count / sub_file_count : 0;
    if (sub_buf_size >= 16384) {
      // std::cout << ">>> [HUST] Write WAL ASYNC " << std::endl;
      std::condition_variable cv;
      std::mutex cv_mutex;
      int ref_count = sub_file_count;
      int write_result = 0;

      for (int i = 0; i < sub_file_count; i++) {
        std::size_t part_count =
            (i == sub_file_count - 1)
                ? (count - sub_buf_size * (sub_file_count - 1))
                : sub_buf_size;
        auto args = new async_write_arg(part_count, &write_result, &cv,
                                        &cv_mutex, &ref_count);
        const void *part_buff =
            static_cast<const char *>(buff) + i * sub_buf_size;
        bytestore_async_write(sub_blobs_[i], part_buff, args->expect_length_,
                              &opts, &msg, &async_write_callback, args);
        if (msg.status_ != STATUS_OK) {
          delete args;
          write_result = msg.status_;
          return 0;
        }
      }

      wait_async_write(cv, cv_mutex, ref_count, __LINE__, blob_name_.c_str());

      if (write_result == 0) {
        r = count;
      }
#ifdef IO_TRACE_DEBUG
      if (!filter) {
        thread_stream << " bytestore_write: name " << blob_name_ << " size "
                      << count
                      << " time spent (us): " << BTSNowMicros() - start_time
                      << std::endl;
        std::cout << thread_stream.str();
        thread_stream.str("");
        thread_stream.clear();
      }
#endif
      return r;
    } else {
      // std::cout << ">>> [HUST] Write WAL ASYNC with SpinLock" << std::endl;
      int write_result = 0;
      std::condition_variable cv;
      std::mutex cv_mutex;
      int ref_count = 1;
      auto args =
          new async_write_arg(count, &write_result, &cv, &cv_mutex, &ref_count);

      bytestore_async_write(blob_, buff, count, &opts, &msg,
                            &async_write_callback, args);
      if (msg.status_ != STATUS_OK) {
        delete args;
        write_result = msg.status_;
        return 0;
      }

      wait_async_write(cv, cv_mutex, ref_count, __LINE__, blob_name_.c_str());

      if (write_result == 0) {
        r = count;
      }
#ifdef IO_TRACE_DEBUG
      if (!filter) {
        thread_stream << " bytestore_write: name " << blob_name_ << " size "
                      << count
                      << " time spent (us): " << BTSNowMicros() - start_time
                      << std::endl;
        std::cout << thread_stream.str();
        thread_stream.str("");
        thread_stream.clear();
      }
#endif
      return r;
    }
  }
  r = bytestore_write(blob_, buff, count, &opts, &msg);
  if (r < 0) {
    assert(0);
  }
#ifdef IO_TRACE_DEBUG
  if (!filter) {
    thread_stream << " bytestore_write: name " << blob_name_ << " size "
                  << count
                  << " time spent (us): " << BTSNowMicros() - start_time
                  << std::endl;
    std::cout << thread_stream.str();
    thread_stream.str("");
    thread_stream.clear();
  }
#endif
  return r;
}

std::size_t BytestoreFile::AsyncAppendV(const struct iovec *data,
                                        size_t iovcnt) {
#ifdef IO_TRACE_DEBUG
  uint64_t start_time = BTSNowMicros();
  bool filter = false;
  if (is_foreground_operation()) {
    filter = true;
    if (!filter) {
      thread_stream << "[W] FG ";
    }
  } else if (is_compaction_operation()) {
    thread_stream << "[W] CP ";
  } else if (is_flush_operation()) {
    filter = true;
    if (!filter) {
      thread_stream << "[W] FL level " << bts_file_level;
    }
  } else if (is_open_operation()) {
    thread_stream << "[W] OP ";
  } else if (is_garbage_collenction_operation()) {
    thread_stream << "[W] GC ";
  } else {
    thread_stream << "[W] XX ";
  }
#endif
  auto opts = GetBytestoreWriteOpts();
  bytestore_message message;
  int ref_count = 1;
  std::condition_variable cv;
  std::mutex cv_mutex;

  bytestore_status write_status;
  size_t total_length = 0;
  int write_result = 0;
  for (size_t i = 0; i < iovcnt; ++i) {
    total_length += data[i].iov_len;
  }

  auto args = new async_write_arg(total_length, &write_result, &cv, &cv_mutex,
                                  &ref_count);
  bytestore_async_writev(blob_, data, iovcnt, &opts, &message,
                         &async_write_callback, args);
  if (message.status_ != STATUS_OK) {
    delete args;
    write_result = message.status_;
    return 0;
  }

  wait_async_write(cv, cv_mutex, ref_count, __LINE__, blob_name_.c_str());

#ifdef IO_TRACE_DEBUG
  if (!filter) {
    thread_stream << " bytestore_write: name " << blob_name_ << " size "
                  << total_length
                  << " time spent (us): " << BTSNowMicros() - start_time
                  << std::endl;
    std::cout << thread_stream.str();
    thread_stream.str("");
    thread_stream.clear();
  }
#endif
  return total_length;
}

void BytestoreFile::Sync() {
  BytestoreFileSystem::Message msg;
  auto sync_opts = bytestore_default_sync_options();
  bytestore_sync_stat sync_stat;
  auto r = bytestore_sync(blob_, &sync_opts, &sync_stat, &msg);
  if (!r) {
    assert(0);
  }
}

void BytestoreFile::AsyncAppend(const void *buff, std::size_t count,
                                WriteCallback *callback) {
  BytestoreFileSystem::Message msg;
  auto opts = GetBytestoreWriteOpts();
  void *args = static_cast<void *>(callback);
  bytestore_async_write(blob_, buff, count, &opts, &msg, &AsyncWriteCallback,
                        args);
  if (msg.status_ != BytestoreFileSystem::BSStatus::STATUS_OK) {
    AsyncWriteCallback(0, &msg, args);
  }
}

void BytestoreFile::AsyncWriteCallback(ssize_t size_written,
                                       BytestoreFileSystem::Message *message,
                                       void *args) {
  auto callback = static_cast<WriteCallback *>(args);
  auto r = static_cast<std::size_t>(size_written);
  callback->Call(r, message);
}

void BytestoreAppendableFile::Close() { file_->Close(); }

std::size_t BytestoreAppendableFile::Append(Slice data) {
  int64_t curr = 0;
  for (; curr != static_cast<int64_t>(data.size());) {
    auto r = file_->Append(data.data() + curr, data.size() - curr);
    curr += r;
  }
  return static_cast<std::size_t>(curr);
}

std::size_t BytestoreAppendableFile::AsyncAppendV(const struct iovec *data,
                                                  size_t iovcnt) {
  int64_t curr = file_->AsyncAppendV(data, iovcnt);
  return static_cast<std::size_t>(curr);
}

void BytestoreAppendableFile::AsyncAppend(Slice data, WriteCallback *callback) {
  file_->AsyncAppend(data.data(), data.size(), callback);
}

void BytestoreAppendableFile::Sync() {
  file_->Sync();
  return;
}

FileStatus BytestoreAppendableFile::GetStatus() {
  auto r = file_->GetStatus();
  return r;
}

std::string BytestoreAppendableFile::GetPathPrefix() { return prefix_; }

}  // namespace bytestore_fs
}  // namespace TERARKDB_NAMESPACE
