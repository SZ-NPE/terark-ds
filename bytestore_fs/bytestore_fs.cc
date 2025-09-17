#include "bytestore_fs.h"

#include "rocksdb/env.h"
#include "rocksdb/utilities/object_registry.h"

namespace TERARKDB_NAMESPACE {

namespace bytestore_fs {

Status NewBytestoreFileSystem(const std::string& uri,
                              const std::string& sdk_log_dir, bool enable_rdma,
                              std::unique_ptr<FileSystem>* fs) {
  BytestoreFileSystemInitOptions bytestore_filesystem_init_opts;
  bytestore_filesystem_init_opts.bthread_concurrency = 32;
  bytestore_filesystem_init_opts.log_dir = sdk_log_dir;
  bytestore_filesystem_init_opts.log_level = 2;
  bytestore_filesystem_init_opts.process_name = "db_bench";
  bytestore_filesystem_init_opts.enable_rdma = enable_rdma;
  BytestoreFileSystem::Init(bytestore_filesystem_init_opts);

  BytestoreFileSystemOptions bytestore_filesystem_opts;

  Status s = parse_uri(uri, &bytestore_filesystem_opts);
  *fs = BytestoreFileSystem::New(FileSystem::Default(),
                                 bytestore_filesystem_opts);
  return s;
}

bytestore_blob* bytestore_open_wrapper(const std::string& blob_name, int mode,
                                       bytestore_open_options* opts,
                                       bytestore_message* msg) {
  bool no_msg = msg == nullptr;
  if (no_msg) {
    msg = new bytestore_message();
  }
  assert(msg);
  const char* fname = blob_name.c_str();
  int retry = 0;
#ifdef IO_TRACE_DEBUG
  printf("[%s] blob_name=\"%s\", mode=%05x, tag=%u, ec=%d\n", __func__, fname,
         mode, opts->metrics_tag_id_,
         opts->using_erasure_coding_ ? static_cast<int>(opts->ec_policy_) : -1);
#endif
  do {
    bytestore_blob* blob = bytestore_open(fname, mode, opts, msg);
    if (msg->status_ == bytestore_status::STATUS_OK) {
      if (no_msg) {
        delete msg;
      }
      return blob;
    }
    if (++retry >= 3) {
      break;
    }
    fprintf(stderr, "[%s] open \"%s\" mode %05x error %d: %s, retry %d ...\n",
            __func__, fname, mode, msg->status_, msg->message_, retry);
    usleep(20 * 1000);  // retry after 20ms
  } while (true);
  fprintf(stderr, "[%s] open \"%s\" mode %05x error %d: %s, abort\n", __func__,
          fname, mode, msg->status_, msg->message_);
  if (no_msg) {
    delete msg;
  }
  return nullptr;
}

}  // namespace bytestore_fs
}  // namespace TERARKDB_NAMESPACE
