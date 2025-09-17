#include "mock.h"

#include <assert.h>
#include <iostream>

static bool bytestore_initialized = false;

void bytestore_init() {
  assert(!bytestore_initialized);
  bytestore_initialized = true;
}

void bytestore_shutdown() { assert(bytestore_initialized); }

bool bytestore_set_flag(const char* flag_name, const char* value) {
  assert(!bytestore_initialized);
  return true;
}

void bytestore_set_read_affinity(struct bytestore_affinity_options* options,
                                 struct bytestore_message* message) {
  assert(!bytestore_initialized);
}

void bytestore_set_write_affinity(
    struct bytestore_write_affinity_options* options,
    struct bytestore_message* message) {
  assert(!bytestore_initialized);
}

struct bytestore_pool* bytestore_open_pool(
    const char* pool_name, struct bytestore_open_pool_options* options,
    struct bytestore_message* message) {
  assert(bytestore_initialized);
  return NULL;
}

bool bytestore_traverse_pool(struct bytestore_pool* pool,
                             struct bytestore_entry* entry,
                             struct bytestore_traverse_options* options,
                             struct bytestore_message* message) {
  return true;
}

void bytestore_close_pool(struct bytestore_pool* pool,
                          struct bytestore_message* message) {}

struct bytestore_blob* bytestore_open(const char* blob_name, int open_mode,
                                      struct bytestore_open_options* options,
                                      struct bytestore_message* message) {
  assert(bytestore_initialized);
  fprintf(stderr, "[MOCK] bytestore_open %s\n", blob_name);
  return NULL;
}

void bytestore_close(struct bytestore_blob* blob,
                     struct bytestore_message* message) {}

bool bytestore_delete(const char* blob_name,
                      struct bytestore_delete_options* options,
                      struct bytestore_message* message) {
  return true;
}

bool bytestore_stat(const char* blob_name, struct bytestore_stat_t* stat,
                    struct bytestore_stat_options* options,
                    struct bytestore_message* message) {
  return true;
}

bool bytestore_bstat(struct bytestore_blob* blob, struct bytestore_stat_t* stat,
                     struct bytestore_stat_options* options,
                     struct bytestore_message* message) {
  return true;
}

ssize_t bytestore_read(struct bytestore_blob* blob, void* buffer, size_t length,
                       struct bytestore_io_options* options,
                       struct bytestore_message* message) {
  return 0;
}

ssize_t bytestore_pread(struct bytestore_blob* blob, void* buffer,
                        size_t length, size_t offset,
                        struct bytestore_io_options* options,
                        struct bytestore_message* message) {
  return 0;
}

ssize_t bytestore_write(struct bytestore_blob* blob, const void* buffer,
                        size_t length, struct bytestore_io_options* options,
                        struct bytestore_message* message) {
  return 0;
}

void bytestore_async_write(struct bytestore_blob* blob, const void* buffer,
                           size_t length, struct bytestore_io_options* options,
                           struct bytestore_message* message,
                           bytestore_write_callback callback, void* args) {
  callback(0, message, args);  // runs on another thread
}

void bytestore_async_writev(struct bytestore_blob* blob,
                            const struct iovec* data, size_t iovcnt,
                            struct bytestore_io_options* options,
                            struct bytestore_message* message,
                            bytestore_write_callback callback, void* args) {
  callback(0, message, args);  // runs on another thread
}

void bytestore_seekg(struct bytestore_blob* blob, size_t offset,
                     struct bytestore_message* message) {}

void bytestore_seekp(struct bytestore_blob* blob, size_t offset,
                     struct bytestore_message* message) {}

ssize_t bytestore_tellg(struct bytestore_blob* blob,
                        struct bytestore_message* message) {
  return 0;
}

ssize_t bytestore_tellp(struct bytestore_blob* blob,
                        struct bytestore_message* message) {
  return 0;
}

bool bytestore_rename(const char* src_blob_name, const char* target_blob_name,
                      struct bytestore_rename_options* options,
                      struct bytestore_message* message) {
  return true;
}

bool bytestore_hardlink(const char* src_blob_name, const char* target_blob_name,
                        struct bytestore_hardlink_options* options,
                        struct bytestore_message* message) {
  return true;
}

bool bytestore_sync(struct bytestore_blob* blob,
                    struct bytestore_sync_options* options,
                    struct bytestore_sync_stat* stat,
                    struct bytestore_message* message) {
  return true;
}

bool bytestore_create_inline_blob(
    const char* blob_name, struct bytestore_create_inline_blob_options* options,
    struct bytestore_message* message) {
  return true;
}

bool bytestore_update_inline_blob(
    const char* blob_name, const struct bytestore_inline_blob_stat* new_stat,
    struct bytestore_update_inline_blob_options* options,
    struct bytestore_message* message) {
  return true;
}
