#include <memory>
#include <vector>

#include <rocksdb/advanced_options.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

using namespace rocksdb;

static_assert(sizeof(Slice) == sizeof(rocksdb_slice_t));

static const rocksdb_options_t rocksdb__default_options = {
  .version = 0,
  .read_only = false,
  .create_if_missing = false,
  .max_background_jobs = 2,
  .bytes_per_sync = 0,
  .compaction_style = rocksdb_compaction_style_level,
  .enable_blob_files = false,
  .min_blob_size = 0,
  .blob_file_size = 1 << 28,
  .enable_blob_garbage_collection = false,
  .table_block_size = 4 * 1024,
  .table_cache_index_and_filter_blocks = false,
  .table_format_version = 6,
};

template <auto rocksdb_options_t::*P, typename T>
static inline T
rocksdb__option (const rocksdb_options_t *options, int min_version, T fallback = T(rocksdb__default_options.*P)) {
  return T(options->version >= min_version ? options->*P : fallback);
}

extern "C" int
rocksdb_init (uv_loop_t *loop, rocksdb_t *db) {
  db->loop = loop;
  db->handle = nullptr;

  return 0;
}

template <typename T>
static inline void
rocksdb__on_status (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<T *>(handle->data);

  req->cb(req->db, status);

  if (req->error) free(req->error);
}

static void
rocksdb__on_open (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_open_t *>(handle->data);

  Options options;

  options.create_if_missing = rocksdb__option<&rocksdb_options_t::create_if_missing, bool>(
    &req->options, 0
  );

  options.max_background_jobs = rocksdb__option<&rocksdb_options_t::max_background_jobs, int>(
    &req->options, 0
  );

  options.bytes_per_sync = rocksdb__option<&rocksdb_options_t::bytes_per_sync, uint64_t>(
    &req->options, 0
  );

  options.compaction_style = rocksdb__option<&rocksdb_options_t::compaction_style, CompactionStyle>(
    &req->options, 0
  );

  options.enable_blob_files = rocksdb__option<&rocksdb_options_t::enable_blob_files, bool>(
    &req->options, 0
  );

  options.min_blob_size = rocksdb__option<&rocksdb_options_t::min_blob_size, uint64_t>(
    &req->options, 0
  );

  options.blob_file_size = rocksdb__option<&rocksdb_options_t::blob_file_size, uint64_t>(
    &req->options, 0
  );

  options.enable_blob_garbage_collection = rocksdb__option<&rocksdb_options_t::enable_blob_garbage_collection, bool>(
    &req->options, 0
  );

  BlockBasedTableOptions table_options;

  table_options.block_size = rocksdb__option<&rocksdb_options_t::table_block_size, uint64_t>(
    &req->options, 0
  );

  table_options.cache_index_and_filter_blocks = rocksdb__option<&rocksdb_options_t::table_cache_index_and_filter_blocks, bool>(
    &req->options, 0
  );

  table_options.format_version = rocksdb__option<&rocksdb_options_t::table_format_version, uint32_t>(
    &req->options, 0
  );

  table_options.filter_policy = std::shared_ptr<const FilterPolicy>(NewBloomFilterPolicy(10.0));

  options.table_factory = std::shared_ptr<TableFactory>(NewBlockBasedTableFactory(table_options));

  auto read_only = rocksdb__option<&rocksdb_options_t::read_only, bool>(
    &req->options, 0
  );

  Status status;

  auto db = reinterpret_cast<DB **>(&req->db->handle);

  if (read_only) {
    status = DB::OpenForReadOnly(options, req->path, db);
  } else {
    status = DB::Open(options, req->path, db);
  }

  req->error = status.ok() ? nullptr : strdup(status.getState());
}

extern "C" int
rocksdb_open (rocksdb_t *db, rocksdb_open_t *req, const char *path, const rocksdb_options_t *options, rocksdb_status_cb cb) {
  req->db = db;
  req->options = options ? *options : rocksdb__default_options;
  req->error = nullptr;
  req->cb = cb;

  strcpy(req->path, path);

  req->worker.data = static_cast<void *>(req);

  return uv_queue_work(db->loop, &req->worker, rocksdb__on_open, rocksdb__on_status<rocksdb_open_t>);
}

static void
rocksdb__on_close (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_close_t *>(handle->data);

  auto rocks = reinterpret_cast<DB *>(req->db->handle);

  auto status = rocks->Close();

  delete rocks;

  req->error = status.ok() ? nullptr : strdup(status.getState());
}

extern "C" int
rocksdb_close (rocksdb_t *db, rocksdb_close_t *req, rocksdb_status_cb cb) {
  req->db = db;
  req->error = nullptr;
  req->cb = cb;

  req->worker.data = static_cast<void *>(req);

  return uv_queue_work(db->loop, &req->worker, rocksdb__on_close, rocksdb__on_status<rocksdb_close_t>);
}

extern "C" rocksdb_slice_t
rocksdb_slice_init (const char *data, size_t len) {
  return {.data = data, .len = len};
}

extern "C" void
rocksdb_slice_destroy (rocksdb_slice_t *slice) {
  free(const_cast<char *>(slice->data));
}

extern "C" int
rocksdb_batch_init (rocksdb_batch_t *previous, size_t capacity, rocksdb_batch_t **result) {
  auto batch = static_cast<rocksdb_batch_t *>(realloc(previous, sizeof(rocksdb_batch_t) + 2 * capacity * sizeof(rocksdb_slice_t) + capacity * sizeof(char *)));

  if (batch == nullptr) return -1;

  batch->len = 0;
  batch->capacity = capacity;

  size_t offset = 0;

  batch->keys = reinterpret_cast<rocksdb_slice_t *>(batch->buffer + offset);

  offset += capacity * sizeof(rocksdb_slice_t);

  batch->values = reinterpret_cast<rocksdb_slice_t *>(batch->buffer + offset);

  offset += capacity * sizeof(rocksdb_slice_t);

  batch->errors = reinterpret_cast<char **>(batch->buffer + offset);

  *result = batch;

  return 0;
}

extern "C" void
rocksdb_batch_destroy (rocksdb_batch_t *batch) {
  free(batch);
}

static void
rocksdb__on_after_batch (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_batch_t *>(handle->data);

  req->cb(req->db, status);

  for (size_t i = 0, n = req->len; i < n; i++) {
    if (req->errors[i]) free(req->errors[i]);
  }
}

static void
rocksdb__on_read (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_batch_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->db->handle);

  if (req->len) {
    std::vector<PinnableSlice> values(req->len);

    std::vector<Status> statuses(req->len);

    db->MultiGet(ReadOptions(), db->DefaultColumnFamily(), req->len, reinterpret_cast<Slice *>(req->keys), values.data(), statuses.data());

    for (size_t i = 0, n = req->len; i < n; i++) {
      auto status = statuses[i];

      if (status.ok()) {
        auto len = values[i].size();

        auto data = reinterpret_cast<char *>(malloc(len));

        memcpy(data, values[i].data(), len);

        req->values[i] = {.data = data, .len = len};
        req->errors[i] = nullptr;
      } else if (status.code() == Status::kNotFound) {
        req->values[i] = {.data = nullptr, .len = 0};
        req->errors[i] = nullptr;
      } else {
        req->errors[i] = strdup(status.getState());
      }
    }
  }
}

extern "C" int
rocksdb_read (rocksdb_t *db, rocksdb_batch_t *req, rocksdb_batch_cb cb) {
  req->db = db;
  req->cb = cb;

  req->worker.data = static_cast<void *>(req);

  return uv_queue_work(db->loop, &req->worker, rocksdb__on_read, rocksdb__on_after_batch);
}

static void
rocksdb__on_write (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_batch_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->db->handle);

  if (req->len) {
    WriteBatch batch;

    for (size_t i = 0, n = req->len; i < n; i++) {
      batch.Put(db->DefaultColumnFamily(), reinterpret_cast<Slice &>(req->keys[i]), reinterpret_cast<Slice &>(req->values[i]));
    }

    auto status = db->Write(WriteOptions(), &batch);

    if (status.ok()) {
      for (size_t i = 0, n = req->len; i < n; i++) {
        req->errors[i] = nullptr;
      }
    } else {
      auto error = status.getState();

      for (size_t i = 0, n = req->len; i < n; i++) {
        req->errors[i] = strdup(error);
      }
    }
  }
}

extern "C" int
rocksdb_write (rocksdb_t *db, rocksdb_batch_t *req, rocksdb_batch_cb cb) {
  req->db = db;
  req->cb = cb;

  req->worker.data = static_cast<void *>(req);

  return uv_queue_work(db->loop, &req->worker, rocksdb__on_write, rocksdb__on_after_batch);
}
