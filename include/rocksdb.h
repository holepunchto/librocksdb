#ifndef ROCKSDB_H
#define ROCKSDB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include <uv.h>

typedef struct rocksdb_options_s rocksdb_options_t;
typedef struct rocksdb_open_s rocksdb_open_t;
typedef struct rocksdb_close_s rocksdb_close_t;
typedef struct rocksdb_slice_s rocksdb_slice_t;
typedef struct rocksdb_batch_s rocksdb_batch_t;
typedef struct rocksdb_s rocksdb_t;

typedef void (*rocksdb_status_cb)(rocksdb_t *db, int status, void *data);
typedef void (*rocksdb_batch_cb)(rocksdb_t *db, int status, void *data);

typedef enum {
  rocksdb_compaction_style_level = 0,
  rocksdb_compaction_style_universal = 1,
  rocksdb_compaction_style_fifo = 2,
  rocksdb_compaction_style_none = 3,
} rocksdb_compaction_style_t;

/** @version 0 */
struct rocksdb_options_s {
  int version;

  /** @since 0 */
  bool read_only;

  /** @since 0 */
  bool create_if_missing;

  /** @since 0 */
  int max_background_jobs;

  /** @since 0 */
  uint64_t bytes_per_sync;

  /** @since 0 */
  rocksdb_compaction_style_t compaction_style;

  /** @since 0 */
  bool enable_blob_files;

  /** @since 0 */
  uint64_t min_blob_size;

  /** @since 0 */
  uint64_t blob_file_size;

  /** @since 0 */
  bool enable_blob_garbage_collection;

  /** @since 0 */
  uint64_t table_block_size;

  /** @since 0 */
  bool table_cache_index_and_filter_blocks;

  /** @since 0 */
  uint32_t table_format_version;
};

struct rocksdb_open_s {
  uv_work_t worker;

  rocksdb_t *db;

  rocksdb_options_t options;

  char path[4096 + 1 /* NULL */];

  char *error;

  rocksdb_status_cb cb;

  void *data;
};

struct rocksdb_close_s {
  uv_work_t worker;

  rocksdb_t *db;

  char *error;

  rocksdb_status_cb cb;

  void *data;
};

struct rocksdb_slice_s {
  const char *data;
  size_t len;
};

struct rocksdb_batch_s {
  uv_work_t worker;

  rocksdb_t *db;

  size_t len;
  size_t capacity;

  rocksdb_slice_t *keys;
  rocksdb_slice_t *values;

  char **errors;

  rocksdb_batch_cb cb;

  void *data;

  // Each batch is a single allocation with an additional buffer at the end
  // used for keys, values, and errors.
  uint8_t buffer[];
};

struct rocksdb_s {
  uv_loop_t *loop;

  void *handle; // Opaque database pointer
};

int
rocksdb_init (uv_loop_t *loop, rocksdb_t *db);

int
rocksdb_open (rocksdb_t *db, rocksdb_open_t *req, const char *path, const rocksdb_options_t *options, void *data, rocksdb_status_cb cb);

int
rocksdb_close (rocksdb_t *db, rocksdb_close_t *req, void *data, rocksdb_status_cb cb);

rocksdb_slice_t
rocksdb_slice_init (const char *data, size_t len);

void
rocksdb_slice_destroy (rocksdb_slice_t *slice);

int
rocksdb_batch_init (rocksdb_batch_t *previous, size_t capacity, rocksdb_batch_t **result);

void
rocksdb_batch_destroy (rocksdb_batch_t *batch);

int
rocksdb_read (rocksdb_t *db, rocksdb_batch_t *req, void *data, rocksdb_batch_cb cb);

int
rocksdb_write (rocksdb_t *db, rocksdb_batch_t *req, void *data, rocksdb_batch_cb cb);

#ifdef __cplusplus
}
#endif

#endif // ROCKSDB_H
