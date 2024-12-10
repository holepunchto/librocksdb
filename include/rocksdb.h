#ifndef ROCKSDB_H
#define ROCKSDB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <intrusive/ring.h>
#include <stdbool.h>
#include <stdint.h>
#include <uv.h>

typedef struct rocksdb_options_s rocksdb_options_t;
typedef struct rocksdb_read_options_s rocksdb_read_options_t;
typedef struct rocksdb_write_options_s rocksdb_write_options_t;
typedef struct rocksdb_req_s rocksdb_req_t;
typedef struct rocksdb_open_s rocksdb_open_t;
typedef struct rocksdb_close_s rocksdb_close_t;
typedef struct rocksdb_slice_s rocksdb_slice_t;
typedef struct rocksdb_range_s rocksdb_range_t;
typedef struct rocksdb_iterator_s rocksdb_iterator_t;
typedef struct rocksdb_read_s rocksdb_read_t;
typedef struct rocksdb_read_batch_s rocksdb_read_batch_t;
typedef struct rocksdb_write_s rocksdb_write_t;
typedef struct rocksdb_write_batch_s rocksdb_write_batch_t;
typedef struct rocksdb_snapshot_s rocksdb_snapshot_t;
typedef struct rocksdb_s rocksdb_t;

typedef void (*rocksdb_open_cb)(rocksdb_open_t *req, int status);
typedef void (*rocksdb_close_cb)(rocksdb_close_t *req, int status);
typedef void (*rocksdb_iterator_cb)(rocksdb_iterator_t *iterator, int status);
typedef void (*rocksdb_read_batch_cb)(rocksdb_read_batch_t *batch, int status);
typedef void (*rocksdb_write_batch_cb)(rocksdb_write_batch_t *batch, int status);

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

/** @version 0 */
struct rocksdb_read_options_s {
  int version;

  /** @since 0 */
  rocksdb_snapshot_t *snapshot;
};

/** @version 0 */
struct rocksdb_write_options_s {
  int version;
};

struct rocksdb_req_s {
  uv_work_t worker;

  rocksdb_t *db;

  intrusive_ring_t reqs;

  bool cancelable;
};

struct rocksdb_open_s {
  rocksdb_req_t req;

  rocksdb_options_t options;

  char path[4096 + 1 /* NULL */];

  char *error;

  rocksdb_open_cb cb;

  void *data;
};

struct rocksdb_close_s {
  rocksdb_req_t req;

  char *error;

  rocksdb_close_cb cb;

  void *data;
};

struct rocksdb_slice_s {
  const char *data;
  size_t len;
};

struct rocksdb_range_s {
  rocksdb_slice_t gt;
  rocksdb_slice_t gte;

  rocksdb_slice_t lt;
  rocksdb_slice_t lte;
};

struct rocksdb_iterator_s {
  rocksdb_req_t req;

  rocksdb_read_options_t options;

  void *handle; // Opaque iterator pointer

  rocksdb_range_t range;
  bool reverse;

  size_t len;
  size_t capacity;

  rocksdb_slice_t *keys;
  rocksdb_slice_t *values;

  char *error;

  rocksdb_iterator_cb cb;

  void *data;
};

typedef enum {
  rocksdb_get = 1,
} rocksdb_read_type_t;

struct rocksdb_read_s {
  rocksdb_read_type_t type;

  union {
    // For get
    struct {
      rocksdb_slice_t key;
      rocksdb_slice_t value;
    };
  };
};

struct rocksdb_read_batch_s {
  rocksdb_req_t req;

  rocksdb_read_options_t options;

  size_t len;

  rocksdb_read_t *reads;

  char **errors;

  rocksdb_read_batch_cb cb;

  void *data;
};

typedef enum {
  rocksdb_put = 1,
  rocksdb_delete,
  rocksdb_delete_range,
} rocksdb_write_type_t;

struct rocksdb_write_s {
  rocksdb_write_type_t type;

  union {
    // For put, delete
    struct {
      rocksdb_slice_t key;
      rocksdb_slice_t value;
    };

    // For delete_range
    struct {
      rocksdb_slice_t start;
      rocksdb_slice_t end;
    };
  };
};

struct rocksdb_write_batch_s {
  rocksdb_req_t req;

  rocksdb_write_options_t options;

  size_t len;

  rocksdb_write_t *writes;

  char *error;

  rocksdb_write_batch_cb cb;

  void *data;
};

struct rocksdb_snapshot_s {
  rocksdb_t *db;

  const void *handle; // Opaque snapshot pointer
};

struct rocksdb_s {
  uv_loop_t *loop;

  void *handle; // Opaque database pointer

  intrusive_ring_t *reqs;

  rocksdb_open_t *open;
  rocksdb_close_t *close;
};

int
rocksdb_init (uv_loop_t *loop, rocksdb_t *db);

int
rocksdb_open (rocksdb_t *db, rocksdb_open_t *req, const char *path, const rocksdb_options_t *options, rocksdb_open_cb cb);

int
rocksdb_close (rocksdb_t *db, rocksdb_close_t *req, rocksdb_close_cb cb);

rocksdb_slice_t
rocksdb_slice_init (const char *data, size_t len);

void
rocksdb_slice_destroy (rocksdb_slice_t *slice);

rocksdb_slice_t
rocksdb_slice_empty (void);

int
rocksdb_iterator_open (rocksdb_t *db, rocksdb_iterator_t *req, rocksdb_range_t range, bool reverse, const rocksdb_read_options_t *options, rocksdb_iterator_cb cb);

int
rocksdb_iterator_close (rocksdb_iterator_t *req, rocksdb_iterator_cb cb);

int
rocksdb_iterator_refresh (rocksdb_iterator_t *req, rocksdb_range_t range, bool reverse, const rocksdb_read_options_t *options, rocksdb_iterator_cb cb);

int
rocksdb_iterator_read (rocksdb_iterator_t *req, rocksdb_slice_t *keys, rocksdb_slice_t *values, size_t capacity, rocksdb_iterator_cb cb);

int
rocksdb_read (rocksdb_t *db, rocksdb_read_batch_t *req, rocksdb_read_t *reads, char **errors, size_t len, const rocksdb_read_options_t *options, rocksdb_read_batch_cb cb);

int
rocksdb_write (rocksdb_t *db, rocksdb_write_batch_t *req, rocksdb_write_t *writes, size_t len, const rocksdb_write_options_t *options, rocksdb_write_batch_cb cb);

int
rocksdb_snapshot_create (rocksdb_t *db, rocksdb_snapshot_t *snapshot);

void
rocksdb_snapshot_destroy (rocksdb_snapshot_t *snapshot);

#ifdef __cplusplus
}
#endif

#endif // ROCKSDB_H
