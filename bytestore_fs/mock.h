#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// lib internal: struct

struct bytestore_blob;
struct bytestore_pool;

// public: define flags

#define BS_O_RDONLY 00  // read only
#define BS_O_WRONLY 01  // write only
#define BS_O_RDWR 02    // read and write

#define BS_O_CREATE 0100     // create if not exist
#define BS_O_TRUNC 01000     // truncate on open
#define BS_O_APPEND 02000    // append only
#define BS_O_SYNC 010000     // persist data
#define BS_O_ASYNC 020000    // async mode only
#define BS_O_EXCL 040000     // exclusive open
#define BS_O_DIRECT 0100000  // direct I/O
#define BS_O_AEXCL 0200000   // Aka anti-EXCL

#define k_default_max_chunk_size (64 * 1024 * 1024)
#define k_default_num_replicas (3)
#define k_default_meta_op_timeouts_in_ms (60 * 1000)
#define k_default_io_op_timeouts_in_ms (10 * 1000)

// public: enum and struct

enum bytestore_status {
  STATUS_OK = 0,
  STATUS_NOT_FOUND = -1,
  STATUS_ALREADY_EXIST = -2,
  STATUS_CORRUPTION = -3,
  STATUS_NOT_SUPPORTED = -4,
  STATUS_INVALID_ARGUMENT = -5,
  STATUS_IO_ERROR = -6,
  STATUS_TIMEOUT = -7,
  STATUS_PERMISSION_DENIED = -8,
  STATUS_IO_BUSY = -9,
  STATUS_END_FILE = -10,

  // ...

  STATUS_UNKNOWN_ERR = -100,
};

#define k_bytestore_message_len (400)
struct bytestore_message {
  enum bytestore_status status_;
  char* message_;
};

#define k_inline_blob_content_size (32)
struct bytestore_blob_condition {
  const char* lock_name_;  // should be a null-terminated c string
  char content_[k_inline_blob_content_size];
};

enum bytestore_iopriority {
  PRI_CRITICAL = 0,
  PRI_REALTIME = 1,
  PRI_ELASTIC = 2,
  PRI_BESTEFFORT = 3,
  PRI_SCAVENGER = 4,
};

enum bytestore_blob_type {
  TYPE_BLOB_LOG = 0,
  TYPE_BLOB_EXTENT = 1,
  TYPE_BLOB_INLINE = 2,
  TYPE_BLOB_LOCAL = 3,
  COUNT_TYPE_BLOB = 4,
};

enum bytestore_ec_policy {
  TYPE_SINGLE_EC_RS = 0,   // Flat, Reed-Solomon
  TYPE_SINGLE_EC_XOR = 1,  // Flat, XOR
  // Group: 2, 3
  TYPE_MIRROR_EC_2_GROUPS = 2,
  TYPE_MIRROR_EC_3_GROUPS = 3,
  TYPE_RAID_EC_3_GROUPS = 4,
  // Flat, LRC
  TYPE_SINGLE_EC_LRC_L1 = 5,
  TYPE_SINGLE_EC_LRC_L2 = 6,
  TYPE_SINGLE_EC_LRC_L3 = 7,
  TYPE_SINGLE_EC_LRC_L4 = 8,
  TYPE_SINGLE_EC_LRC_L5 = 9,
};

enum bytestore_replicated_policy {
  TYPE_REP_FLAT = 0,  // Flat
  // Group: 2, 3 and etc.
  TYPE_REP_G2 = 1,
  TYPE_REP_G3 = 2,
  TYPE_REP_G1 = 3,
  TYPE_REP_G4 = 4,
  TYPE_REP_G5 = 5,
  TYPE_REP_G6 = 6,
  MAX_REP_POLICY_TYPES
};

enum bytestore_storage_type {
  STORAGE_TYPE_ANY = 0,       // ignoring the disk's storage type
  STORAGE_TYPE_HDD = 1,       // only choose HDD for this blob
  STORAGE_TYPE_SATA_SSD = 2,  // only choose SATA SSD for this Blob
  STORAGE_TYPE_NVME_SSD = 3,  // only choose NVME SSD for this Blob
  STORAGE_TYPE_RAM_DISK = 4,  // only choose RAM DISK for this Blob
  // choose SATA SSD first, then migrate to HDD
  STORAGE_TYPE_TIERED_SATA_SSD_HDD = 1 << 3,
  // choose NVME SSD first, then migrate to HDD
  STORAGE_TYPE_TIERED_NVME_SSD_HDD = 2 << 3,
  MAX_STORAGE_TYPE = UINT32_MAX - 1
};

struct bytestore_placement_info {
  // ...

  struct bytestore_storage_policy {
    enum bytestore_storage_type type_;
    uint8_t locality_;  // enum 0-6
  } storage_policy_;
  // 0-7, max tolerant number of failed failure-domain nodes
  uint8_t max_failures_;
};

struct bytestore_open_options {
  enum bytestore_blob_type blob_type_;
  bool using_pmem_;            // using persistent memory as writeback buffer
  int channel_index_;          // if < 0, round roubin
  bool using_erasure_coding_;  // enable erasure coding
  bool using_min_max_copy_;    // enable quorum write
  uint32_t max_chunk_size_;

  // only useful when using_erasure_coding_ is set
  enum bytestore_ec_policy ec_policy_;
  union backup_parameter {
    struct {
      uint32_t packet_size_;  // packet_size_ > 2 * 32
      uint16_t data_units_;
      uint16_t parity_units_;  // data_units_ + parity_units_ <= 32
    };  // for ec replicas
    uint64_t num_backups_;  // for backups, only use as min copy now
  } backup_parameter_;
  enum bytestore_replicated_policy rep_policy_;
  struct bytestore_placement_info placement_;
  struct bytestore_blob_condition condition_;
  int64_t timeout_in_ms_;

  // ...

  // How much replicas can be lagged in quorum scenario. default: 0
  uint32_t write_lag_tolerance_;
  // whether blob enable backup read, only valid when metrics_tag_id_ = 0.
  // default: 0
  bool enable_backup_read_;
  uint32_t metrics_tag_id_;

  // ...
};

struct bytestore_storage_info {
  bool using_erasure_coding_;
  // whether allow unaligned writing, only applicable to EC blobs.
  bool ec_unaligned_writing_;
  enum bytestore_blob_type blob_type_;
  enum bytestore_storage_type storage_type_;
  union {
    // for ec replicas
    enum bytestore_ec_policy ec_policy_;
    // for copies replicas
    enum bytestore_replicated_policy rep_policy_;
  };
  union {
    // for ec replicas
    struct {
      uint8_t data_units_;    // No more than 32
      uint8_t parity_units_;  // No more than 32
    };
    // for copies replicas, only use as min copy now
    uint16_t num_replicas_;
  };
  // For EC
  uint32_t packet_size_;
};

struct bytestore_open_pool_options {
  char prefix_blob_name_[1024];
  char start_blob_name_[1024];
  int64_t timeout_in_ms_;
};

struct bytestore_traverse_options {
  struct bytestore_blob_condition condition_;
  int64_t timeout_in_ms_;
  bool return_blob_id;
};

struct bytestore_blob_id {
  uint64_t sequence_id_;
  uint32_t unique_pool_id_;
  uint32_t unique_group_id_;
};

struct bytestore_entry {
  enum bytestore_blob_type type_;
  char name_[4096]; /* At most 4096 bytes */
  struct bytestore_blob_id blob_id_;
};

#define k_additional_statinfo_size (512)
struct bytestore_stat_t {
  size_t size_;
  uint32_t atime_;
  uint32_t mtime_;
  uint32_t ctime_;
  uint32_t nlink_;
  uint32_t mode_;
  uint32_t chunks_;
  struct bytestore_blob_id blob_id_;
  char* additional_info_;
  uint32_t ttl_seconds_;
  uint8_t attr_flags_;          // for advanced user
  uint64_t replica_parameter_;  // for advanced user
  uint32_t user_flags_;         // user set when open
  struct bytestore_storage_info storage_info_;
  bool is_attached_;  //  for tier
  uint32_t open_version_;
};

enum bytestore_qos_type {
  QOS_TYPE_IOPS = 0,
  QOS_TYPE_IOPS_NONE_NORMALIZED = 1,
  QOS_TYPE_BANDWIDTH = 2,
};

struct bytestore_qos_options {
  int64_t qos_id_;
  int64_t reservation_;
  int64_t weight_;
  int64_t limit_;
  enum bytestore_qos_type qos_type_;
};

enum bytestore_retry_strategy {
  STRATEGY_EXPONENTIAL_BACKOFF_INTERVAL = 0,
  STRATEGY_UNIFORM_INTERVAL = 1,
};

enum bytestore_checksum_algorithm {
  ALGO_TYPE_NONE = 0,  // will disable checksum
  ALGO_TYPE_CRC32 = 1,
  ALGO_TYPE_MD5 = 2,  // deprecated
  ALGO_TYPE_CRC32_ISCSI = 3,
  ALGO_TYPE_CRC64_ECMA = 4,
  ALGO_TYPE_CRC64_ECMA_REFL = 5,
  MAX_ALGO_TYPES
};

struct bytestore_io_options {
  enum bytestore_iopriority io_priority_;
  int64_t timeout_in_ms_;
  uint32_t try_count_;
  enum bytestore_retry_strategy retry_strategy_;
  enum bytestore_checksum_algorithm checksum_;
  struct bytestore_qos_options qos_;
};

struct bytestore_affinity_options {
  bool host_affinity_;   // HOST affinity or not
  char* read_affinity_;  // "/dc_name/zone_name/rack_name"
};

struct bytestore_delete_options {
  bool permanent_delete_;
  struct bytestore_blob_condition condition_;
  int64_t timeout_in_ms_;
  bool detach_all_dst_;
};

struct bytestore_rename_options {
  struct bytestore_blob_condition condition_;
  int64_t timeout_in_ms_;
  bool no_replace_;
  bool to_trash_;
};

struct bytestore_hardlink_options {
  struct bytestore_blob_condition condition_;
  int64_t timeout_in_ms_;
  bool enable_blob_ttl_;
  uint32_t ttl_seconds_;
};

struct bytestore_create_inline_blob_options {
  int64_t timeout_in_ms_;
};

struct bytestore_update_inline_blob_options {
  bool enable_cas_;
  char expected_[k_inline_blob_content_size];
  int64_t timeout_in_ms_;
};

struct bytestore_inline_blob_stat {
  char content_[k_inline_blob_content_size];
};

struct bytestore_sync_stat {
  size_t synced_size_;
};

struct bytestore_sync_options {
  struct bytestore_blob_condition condition_;
  int64_t timeout_in_ms_;
  int32_t fault_tolerance_;
  bool meta_sync_;
};

struct bytestore_stat_options {
  struct bytestore_blob_condition condition_;
  bool need_additional_info_;
  bool need_strong_consistency_;
  bool need_shallow_stat_;  // just return cached mets
  int64_t timeout_in_ms_;
  uint32_t try_count_;
};

// public: callback

typedef void (*bytestore_write_callback)(ssize_t size_written,
                                         struct bytestore_message* message,
                                         void* args);

// public: system config api

// call before any pool/blob api
void bytestore_init();

// call after all other api
void bytestore_shutdown();

// call before bytestore_init()
bool bytestore_set_flag(const char* flag_name, const char* value);

// call before bytestore_init()
void bytestore_set_read_affinity(struct bytestore_affinity_options* options,
                                 struct bytestore_message* message);

// call before bytestore_init()
void bytestore_set_write_affinity(
    struct bytestore_write_affinity_options* options,
    struct bytestore_message* message);

// public: manipulate pool

struct bytestore_pool* bytestore_open_pool(
    const char* pool_name, struct bytestore_open_pool_options* options,
    struct bytestore_message* message);

bool bytestore_traverse_pool(struct bytestore_pool* pool,
                             struct bytestore_entry* entry,
                             struct bytestore_traverse_options* options,
                             struct bytestore_message* message);

void bytestore_close_pool(struct bytestore_pool* pool,
                          struct bytestore_message* message);

// public:  manipulate blob

// open blob
// example blob_name: blob://region_name/cluster_name/pool_name/blob_name
struct bytestore_blob* bytestore_open(const char* blob_name, int open_mode,
                                      struct bytestore_open_options* options,
                                      struct bytestore_message* message);

// close blob
void bytestore_close(struct bytestore_blob* blob,
                     struct bytestore_message* message);

// delete blob
bool bytestore_delete(const char* blob_name,
                      struct bytestore_delete_options* options,
                      struct bytestore_message* message);

// stat blob by name
bool bytestore_stat(const char* blob_name, struct bytestore_stat_t* stat,
                    struct bytestore_stat_options* options,
                    struct bytestore_message* message);

// stat blob by instance
bool bytestore_bstat(struct bytestore_blob* blob, struct bytestore_stat_t* stat,
                     struct bytestore_stat_options* options,
                     struct bytestore_message* message);

// read blob at current position
ssize_t bytestore_read(struct bytestore_blob* blob, void* buffer, size_t length,
                       struct bytestore_io_options* options,
                       struct bytestore_message* message);

// read blob at specified position
ssize_t bytestore_pread(struct bytestore_blob* blob, void* buffer,
                        size_t length, size_t offset,
                        struct bytestore_io_options* options,
                        struct bytestore_message* message);

// write blob at current position
ssize_t bytestore_write(struct bytestore_blob* blob, const void* buffer,
                        size_t length, struct bytestore_io_options* options,
                        struct bytestore_message* message);

// async write blob at current position
void bytestore_async_write(struct bytestore_blob* blob, const void* buffer,
                           size_t length, struct bytestore_io_options* options,
                           struct bytestore_message* message,
                           bytestore_write_callback callback, void* args);

// async write blob at multiple positions
void bytestore_async_writev(struct bytestore_blob* blob,
                            const struct iovec* data, size_t iovcnt,
                            struct bytestore_io_options* options,
                            struct bytestore_message* message,
                            bytestore_write_callback callback, void* args);

// Seek the read position.
void bytestore_seekg(struct bytestore_blob* blob, size_t offset,
                     struct bytestore_message* message);

// Seek the write position.
void bytestore_seekp(struct bytestore_blob* blob, size_t offset,
                     struct bytestore_message* message);

// Tell the read position.
ssize_t bytestore_tellg(struct bytestore_blob* blob,
                        struct bytestore_message* message);

// Tell the write position.
ssize_t bytestore_tellp(struct bytestore_blob* blob,
                        struct bytestore_message* message);

// rename blob
bool bytestore_rename(const char* src_blob_name, const char* target_blob_name,
                      struct bytestore_rename_options* options,
                      struct bytestore_message* message);

// hardlink blob
bool bytestore_hardlink(const char* src_blob_name, const char* target_blob_name,
                        struct bytestore_hardlink_options* options,
                        struct bytestore_message* message);

// synchronized blob sync
bool bytestore_sync(struct bytestore_blob* blob,
                    struct bytestore_sync_options* options,
                    struct bytestore_sync_stat* stat,
                    struct bytestore_message* message);

// public: manipulate inline blob

// wrapper for open(BS_O_CREATE) + close
bool bytestore_create_inline_blob(
    const char* blob_name, struct bytestore_create_inline_blob_options* options,
    struct bytestore_message* message);

bool bytestore_update_inline_blob(
    const char* blob_name, const struct bytestore_inline_blob_stat* new_stat,
    struct bytestore_update_inline_blob_options* options,
    struct bytestore_message* message);

// public: default options

static inline struct bytestore_open_options bytestore_default_open_options() {
  static const struct bytestore_open_options k_default_open_options = {
      .max_chunk_size_ = k_default_max_chunk_size,
      .backup_parameter_ = {.num_backups_ = k_default_num_replicas},
      /* ... */};
  return k_default_open_options;
}

static inline struct bytestore_io_options bytestore_default_read_options() {
  static const struct bytestore_io_options k_default_read_options = {
      .io_priority_ = PRI_REALTIME,
      .timeout_in_ms_ = k_default_io_op_timeouts_in_ms,
      /*... */};
  return k_default_read_options;
}

static inline struct bytestore_io_options bytestore_default_write_options() {
  static const struct bytestore_io_options k_default_write_options = {
      .io_priority_ = PRI_ELASTIC,
      .timeout_in_ms_ = k_default_io_op_timeouts_in_ms,
      /* ... */};
  return k_default_write_options;
}

static inline struct bytestore_affinity_options
bytestore_default_read_affinity_options() {
  static const struct bytestore_affinity_options
      k_default_read_affinity_options = {
          .host_affinity_ = false,
          .read_affinity_ = NULL,
      };
  return k_default_read_affinity_options;
}

static inline struct bytestore_stat_options bytestore_default_stat_options() {
  static const struct bytestore_stat_options k_default_stat_options = {
      /*... */};
  return k_default_stat_options;
}

static inline struct bytestore_delete_options
bytestore_default_delete_options() {
  static const struct bytestore_delete_options k_default_delete_options = {
      .timeout_in_ms_ = k_default_meta_op_timeouts_in_ms,
      /* ... */};
  return k_default_delete_options;
}

static inline struct bytestore_hardlink_options
bytestore_default_hardlink_options() {
  static const struct bytestore_hardlink_options k_default_hardlink_options = {
      .timeout_in_ms_ = k_default_meta_op_timeouts_in_ms,
      /* ... */};
  return k_default_hardlink_options;
}

static inline struct bytestore_create_inline_blob_options
bytestore_default_create_inline_blob_options() {
  static const struct bytestore_create_inline_blob_options
      k_default_create_inline_blob_options = {
          .timeout_in_ms_ = k_default_meta_op_timeouts_in_ms,
      };
  return k_default_create_inline_blob_options;
}

static inline struct bytestore_update_inline_blob_options
bytestore_default_update_inline_blob_options() {
  static const struct bytestore_update_inline_blob_options
      k_default_update_inline_blob_options = {
          .enable_cas_ = false,
          .expected_ = {0},
          .timeout_in_ms_ = k_default_meta_op_timeouts_in_ms,
      };
  return k_default_update_inline_blob_options;
}

static inline struct bytestore_open_pool_options
bytestore_default_open_pool_options() {
  static const struct bytestore_open_pool_options k_default_open_pool_options =
      {
          .prefix_blob_name_ = {'\0'},
          .start_blob_name_ = {'\0'},
          .timeout_in_ms_ = k_default_meta_op_timeouts_in_ms,
      };
  return k_default_open_pool_options;
}

static inline struct bytestore_traverse_options
bytestore_default_traverse_options() {
  static const struct bytestore_traverse_options k_default_traverse_options = {
      /* ... */};
  return k_default_traverse_options;
}

static inline struct bytestore_rename_options
bytestore_default_rename_options() {
  static const struct bytestore_rename_options k_default_rename_options = {
      .timeout_in_ms_ = k_default_meta_op_timeouts_in_ms,
      /* ... */};
  return k_default_rename_options;
}

static inline struct bytestore_sync_options bytestore_default_sync_options() {
  static const struct bytestore_sync_options k_default_sync_options = {
      .timeout_in_ms_ = k_default_meta_op_timeouts_in_ms,
      /* ... */};
  return k_default_sync_options;
}

#ifdef __cplusplus
}
#endif
