#ifndef ROCKSDB_H
#define ROCKSDB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include <uv.h>

typedef struct rocksdb_options_s rocksdb_options_t;
typedef struct rocksdb_bloom_filter_options_s rocksdb_bloom_filter_options_t;
typedef struct rocksdb_ribbon_filter_options_s rocksdb_ribbon_filter_options_t;
typedef struct rocksdb_filter_policy_s rocksdb_filter_policy_t;
typedef struct rocksdb_column_family_options_s rocksdb_column_family_options_t;
typedef struct rocksdb_read_options_s rocksdb_read_options_t;
typedef struct rocksdb_write_options_s rocksdb_write_options_t;
typedef struct rocksdb_iterator_options_s rocksdb_iterator_options_t;
typedef struct rocksdb_flush_options_s rocksdb_flush_options_t;
typedef struct rocksdb_compact_range_options_s rocksdb_compact_range_options_t;
typedef struct rocksdb_approximate_size_options_s rocksdb_approximate_size_options_t;
typedef struct rocksdb_column_family_s rocksdb_column_family_t;
typedef struct rocksdb_column_family_descriptor_s rocksdb_column_family_descriptor_t;
typedef struct rocksdb_req_s rocksdb_req_t;
typedef struct rocksdb_open_s rocksdb_open_t;
typedef struct rocksdb_close_s rocksdb_close_t;
typedef struct rocksdb_suspend_s rocksdb_suspend_t;
typedef struct rocksdb_resume_s rocksdb_resume_t;
typedef struct rocksdb_slice_s rocksdb_slice_t;
typedef struct rocksdb_range_s rocksdb_range_t;
typedef struct rocksdb_iterator_s rocksdb_iterator_t;
typedef struct rocksdb_read_s rocksdb_read_t;
typedef struct rocksdb_read_batch_s rocksdb_read_batch_t;
typedef struct rocksdb_write_s rocksdb_write_t;
typedef struct rocksdb_write_batch_s rocksdb_write_batch_t;
typedef struct rocksdb_flush_s rocksdb_flush_t;
typedef struct rocksdb_snapshot_s rocksdb_snapshot_t;
typedef struct rocksdb_compact_range_s rocksdb_compact_range_t;
typedef struct rocksdb_approximate_size_s rocksdb_approximate_size_t;
typedef struct rocksdb_s rocksdb_t;

typedef void (*rocksdb_idle_cb)(rocksdb_t *db);
typedef void (*rocksdb_open_cb)(rocksdb_open_t *req, int status);
typedef void (*rocksdb_close_cb)(rocksdb_close_t *req, int status);
typedef void (*rocksdb_suspend_cb)(rocksdb_suspend_t *req, int status);
typedef void (*rocksdb_resume_cb)(rocksdb_resume_t *req, int status);
typedef void (*rocksdb_iterator_cb)(rocksdb_iterator_t *iterator, int status);
typedef void (*rocksdb_read_batch_cb)(rocksdb_read_batch_t *batch, int status);
typedef void (*rocksdb_write_batch_cb)(rocksdb_write_batch_t *batch, int status);
typedef void (*rocksdb_flush_cb)(rocksdb_flush_t *req, int status);
typedef void (*rocksdb_compact_range_cb)(rocksdb_compact_range_t *req, int status);
typedef void (*rocksdb_approximate_size_cb)(rocksdb_approximate_size_t *req, int status);

/** @version 3 */
struct rocksdb_options_s {
  int version;

  /** @since 0 */
  bool read_only;

  /** @since 0 */
  bool create_if_missing;

  /** @since 0 */
  bool create_missing_column_families;

  /** @since 0 */
  int max_background_jobs;

  /** @since 0 */
  uint64_t bytes_per_sync;

  /** @since 1 */
  int max_open_files;

  /** @since 1 */
  bool use_direct_reads;

  /** @since 2 */
  bool avoid_unnecessary_blocking_io;

  /** @since 2 */
  bool skip_stats_update_on_db_open;

  /** @since 2 */
  bool use_direct_io_for_flush_and_compaction;

  /** @since 2 */
  int max_file_opening_threads;

  /** @since 3 */
  int lock;
};

typedef enum {
  rocksdb_no_compaction = 0,
  rocksdb_level_compaction = 1,
  rocksdb_universal_compaction = 2,
  rocksdb_fifo_compaction = 3,
} rocksdb_compaction_style_t;

typedef enum {
  rocksdb_no_filter_policy = 0,
  rocksdb_bloom_filter_policy = 1,
  rocksdb_ribbon_filter_policy = 2,
} rocksdb_filter_policy_type_t;

typedef enum {
  rocksdb_pin_none = 0,
  rocksdb_pin_flushed_and_similar = 1,
  rocksdb_pin_all = 2,
} rocksdb_pinning_tier_t;

typedef enum {
  rocksdb_default_blob_garbage_collection_policy = 0,
  rocksdb_force_blob_garbage_collection_policy = 1,
  rocksdb_disable_blob_garbage_collection_policy = 2,
} rocksdb_blob_garbage_collection_policy_t;

/** @version 0 */
struct rocksdb_bloom_filter_options_s {
  int version;

  /** @since 0 */
  double bits_per_key;
};

/** @version 0 */
struct rocksdb_ribbon_filter_options_s {
  int version;

  /** @since 0 */
  double bloom_equivalent_bits_per_key;

  /** @since 0 */
  int bloom_before_level;
};

struct rocksdb_filter_policy_s {
  rocksdb_filter_policy_type_t type;

  union {
    rocksdb_bloom_filter_options_t bloom;
    rocksdb_ribbon_filter_options_t ribbon;
  };
};

/** @version 3 */
struct rocksdb_column_family_options_s {
  int version;

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
  uint64_t block_size;

  /** @since 0 */
  bool cache_index_and_filter_blocks;

  /** @since 0 */
  uint32_t format_version;

  /** @since 1 */
  bool optimize_filters_for_memory;

  /** @since 1 */
  bool no_block_cache;

  /** @since 2 */
  rocksdb_filter_policy_t filter_policy;

  /** @since 3 */
  rocksdb_pinning_tier_t top_level_index_pinning_tier;

  /** @since 3 */
  rocksdb_pinning_tier_t partition_pinning_tier;

  /** @since 3 */
  rocksdb_pinning_tier_t unpartitioned_pinning_tier;

  /** @since 4 */
  bool optimize_filters_for_hits;

  /** @since 4 */
  int num_levels;

  /** @since 4 */
  int max_write_buffer_number;
};

/** @version 1 */
struct rocksdb_read_options_s {
  int version;

  /** @since 0 */
  rocksdb_snapshot_t *snapshot;

  /** @since 1 */
  bool async_io;

  /** @since 1 */
  bool fill_cache;
};

/** @version 0 */
struct rocksdb_write_options_s {
  int version;
};

/** @version 0 */
struct rocksdb_iterator_options_s {
  int version;

  /** @since 0 */
  bool reverse;

  /** @since 0 */
  bool keys_only;

  /** @since 0 */
  rocksdb_snapshot_t *snapshot;
};

/** @version 0 */
struct rocksdb_flush_options_s {
  int version;
};

/** @version 1 */
struct rocksdb_compact_range_options_s {
  int version;

  /** @since 0 */
  bool exclusive_manual_compaction;

  /** @since 1 */
  rocksdb_blob_garbage_collection_policy_t blob_garbage_collection_policy;

  /** @since 1 */
  double blob_garbage_collection_age_cutoff;
};

/** @version 0 */
struct rocksdb_approximate_size_options_s {
  int version;

  /** @since 0 */
  bool include_memtables;

  /** @since 0 */
  bool include_files;

  /** @since 0 */
  double files_size_error_margin;
};

struct rocksdb_column_family_s; // Opaque

struct rocksdb_column_family_descriptor_s {
  const char *name;

  rocksdb_column_family_options_t options;
};

struct rocksdb_req_s {
  uv_work_t worker;

  rocksdb_t *db;
};

struct rocksdb_open_s {
  rocksdb_req_t req;

  rocksdb_options_t options;

  char path[4096 + 1 /* NULL */];

  const rocksdb_column_family_descriptor_t *column_families;
  rocksdb_column_family_t **handles;

  size_t len;

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

struct rocksdb_suspend_s {
  rocksdb_req_t req;

  char *error;

  rocksdb_suspend_cb cb;

  void *data;
};

struct rocksdb_resume_s {
  rocksdb_req_t req;

  char *error;

  rocksdb_resume_cb cb;

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

  rocksdb_iterator_options_t options;

  rocksdb_column_family_t *column_family;

  void *handle; // Opaque iterator pointer

  int state;
  bool inflight;

  rocksdb_range_t range;

  size_t len;
  size_t capacity;

  rocksdb_slice_t *keys;
  rocksdb_slice_t *values;

  char *error;

  rocksdb_iterator_cb open;
  rocksdb_iterator_cb close;
  rocksdb_iterator_cb refresh;
  rocksdb_iterator_cb read;

  void *data;
};

typedef enum {
  rocksdb_get = 1,
} rocksdb_read_type_t;

struct rocksdb_read_s {
  rocksdb_read_type_t type;

  rocksdb_column_family_t *column_family;

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

  rocksdb_column_family_t *column_family;

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

struct rocksdb_flush_s {
  rocksdb_req_t req;

  rocksdb_flush_options_t options;

  rocksdb_column_family_t *column_family;

  char *error;

  rocksdb_flush_cb cb;

  void *data;
};

struct rocksdb_compact_range_s {
  rocksdb_req_t req;

  rocksdb_compact_range_options_t options;

  rocksdb_column_family_t *column_family;

  rocksdb_slice_t start;
  rocksdb_slice_t end;

  char *error;

  rocksdb_compact_range_cb cb;

  void *data;
};

struct rocksdb_approximate_size_s {
  rocksdb_req_t req;

  rocksdb_approximate_size_options_t options;

  rocksdb_column_family_t *column_family;

  rocksdb_slice_t start;
  rocksdb_slice_t end;

  char *error;

  rocksdb_approximate_size_cb cb;

  uint64_t result;

  void *data;
};

struct rocksdb_snapshot_s {
  rocksdb_t *db;

  const void *handle; // Opaque snapshot pointer
};

enum {
  rocksdb_active = 1,
  rocksdb_suspending = 2,
  rocksdb_suspended = 3,
  rocksdb_resuming = 4,
  rocksdb_closing = 5,
};

struct rocksdb_s {
  uv_loop_t *loop;

  void *handle; // Opaque database pointer

  int state;
  int inflight;
  int lock;

  rocksdb_idle_cb idle;

  rocksdb_close_t *close;
};

int
rocksdb_open(uv_loop_t *loop, rocksdb_t *db, rocksdb_open_t *req, const char *path, const rocksdb_options_t *options, const rocksdb_column_family_descriptor_t column_families[], rocksdb_column_family_t *handles[], size_t len, rocksdb_idle_cb idle, rocksdb_open_cb cb);

int
rocksdb_close(rocksdb_t *db, rocksdb_close_t *req, rocksdb_idle_cb idle, rocksdb_close_cb cb);

int
rocksdb_suspend(rocksdb_t *db, rocksdb_suspend_t *req, rocksdb_suspend_cb cb);

int
rocksdb_resume(rocksdb_t *db, rocksdb_resume_t *req, rocksdb_resume_cb cb);

rocksdb_column_family_descriptor_t
rocksdb_column_family_descriptor(const char *name, const rocksdb_column_family_options_t *options);

rocksdb_column_family_t *
rocksdb_column_family_default(rocksdb_t *db);

int
rocksdb_column_family_destroy(rocksdb_t *db, rocksdb_column_family_t *column_family);

rocksdb_slice_t
rocksdb_slice_init(const char *data, size_t len);

void
rocksdb_slice_destroy(rocksdb_slice_t *slice);

rocksdb_slice_t
rocksdb_slice_empty(void);

int
rocksdb_iterator_open(rocksdb_t *db, rocksdb_iterator_t *req, rocksdb_column_family_t *column_family, rocksdb_range_t range, const rocksdb_iterator_options_t *options, rocksdb_iterator_cb cb);

int
rocksdb_iterator_close(rocksdb_iterator_t *req, rocksdb_iterator_cb cb);

int
rocksdb_iterator_refresh(rocksdb_iterator_t *req, rocksdb_range_t range, const rocksdb_iterator_options_t *options, rocksdb_iterator_cb cb);

int
rocksdb_iterator_read(rocksdb_iterator_t *req, rocksdb_slice_t *keys, rocksdb_slice_t *values, size_t capacity, rocksdb_iterator_cb cb);

int
rocksdb_read(rocksdb_t *db, rocksdb_read_batch_t *req, rocksdb_read_t *reads, size_t len, const rocksdb_read_options_t *options, rocksdb_read_batch_cb cb);

int
rocksdb_write(rocksdb_t *db, rocksdb_write_batch_t *req, rocksdb_write_t *writes, size_t len, const rocksdb_write_options_t *options, rocksdb_write_batch_cb cb);

int
rocksdb_flush(rocksdb_t *db, rocksdb_flush_t *req, rocksdb_column_family_t *column_family, const rocksdb_flush_options_t *options, rocksdb_flush_cb cb);

int
rocksdb_compact_range(rocksdb_t *db, rocksdb_compact_range_t *req, rocksdb_column_family_t *column_family, rocksdb_slice_t start, rocksdb_slice_t end, const rocksdb_compact_range_options_t *options, rocksdb_compact_range_cb cb);

int
rocksdb_approximate_size(rocksdb_t *db, rocksdb_approximate_size_t *req, rocksdb_column_family_t *column_family, rocksdb_slice_t start, rocksdb_slice_t end, const rocksdb_approximate_size_options_t *options, rocksdb_approximate_size_cb cb);

int
rocksdb_snapshot_create(rocksdb_t *db, rocksdb_snapshot_t *snapshot);

void
rocksdb_snapshot_destroy(rocksdb_snapshot_t *snapshot);

#ifdef __cplusplus
}
#endif

#endif // ROCKSDB_H
