#include <memory>
#include <vector>

#include <rocksdb/advanced_options.h>
#include <rocksdb/convenience.h>
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

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
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
rocksdb_open (rocksdb_t *db, rocksdb_open_t *req, const char *path, const rocksdb_options_t *options, rocksdb_open_cb cb) {
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

  CancelAllBackgroundWork(rocks, true);

  auto status = rocks->Close();

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }

  delete rocks;
}

extern "C" int
rocksdb_close (rocksdb_t *db, rocksdb_close_t *req, rocksdb_close_cb cb) {
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

extern "C" rocksdb_slice_t
rocksdb_slice_empty (void) {
  return {.data = nullptr, .len = 0};
}

static inline rocksdb_slice_t
rocksdb__slice_copy (const Slice &slice) {
  auto len = slice.size();

  auto data = reinterpret_cast<char *>(malloc(len));

  memcpy(data, slice.data(), len);

  return {.data = data, .len = len};
}

template <typename T>
static inline void
rocksdb__iterator_seek (Iterator *iterator, T *req) {
  iterator->Seek(reinterpret_cast<const Slice &>(req->start));
}

template <typename T>
static inline Iterator *
rocksdb__iterator_open (T *req) {
  auto db = reinterpret_cast<DB *>(req->db->handle);

  auto iterator = db->NewIterator(ReadOptions());

  rocksdb__iterator_seek(iterator, req);

  return iterator;
}

template <typename T>
static inline void
rocksdb__iterator_read (Iterator *iterator, T *req) {
  while (iterator->Valid() && req->len < req->capacity) {
    auto key = iterator->key();

    if (req->end.len && key.compare(reinterpret_cast<const Slice &>(req->end)) >= 0) {
      break;
    }

    auto value = iterator->value();

    req->keys[req->len] = rocksdb__slice_copy(key);
    req->values[req->len] = rocksdb__slice_copy(value);

    req->len++;

    iterator->Next();
  }
}

static void
rocksdb__on_after_read_range (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_read_range_t *>(handle->data);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_read_range (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_read_range_t *>(handle->data);

  auto iterator = rocksdb__iterator_open(req);

  rocksdb__iterator_read(iterator, req);

  auto status = iterator->status();

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }

  delete iterator;
}

extern "C" int
rocksdb_read_range (rocksdb_t *db, rocksdb_read_range_t *req, rocksdb_slice_t start, rocksdb_slice_t end, rocksdb_slice_t *keys, rocksdb_slice_t *values, size_t capacity, rocksdb_read_range_cb cb) {
  req->db = db;
  req->start = start;
  req->end = end;
  req->keys = keys;
  req->values = values;
  req->len = 0;
  req->capacity = capacity;
  req->cb = cb;

  req->worker.data = static_cast<void *>(req);

  return uv_queue_work(db->loop, &req->worker, rocksdb__on_read_range, rocksdb__on_after_read_range);
}

static void
rocksdb__on_after_delete_range (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_delete_range_t *>(handle->data);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_delete_range (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_delete_range_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->db->handle);

  auto status = db->DeleteRange(WriteOptions(), reinterpret_cast<const Slice &>(req->start), reinterpret_cast<const Slice &>(req->end));

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

extern "C" int
rocksdb_delete_range (rocksdb_t *db, rocksdb_delete_range_t *req, rocksdb_slice_t start, rocksdb_slice_t end, rocksdb_delete_range_cb cb) {
  req->db = db;
  req->start = start;
  req->end = end;
  req->cb = cb;

  req->worker.data = static_cast<void *>(req);

  return uv_queue_work(db->loop, &req->worker, rocksdb__on_delete_range, rocksdb__on_after_delete_range);
}

extern "C" int
rocksdb_iterator_init (rocksdb_t *db, rocksdb_iterator_t *iterator) {
  iterator->db = db;

  iterator->worker.data = static_cast<void *>(iterator);

  return 0;
}

static void
rocksdb__on_after_iterator (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_iterator_open (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  auto iterator = rocksdb__iterator_open(req);

  req->handle = static_cast<void *>(iterator);

  auto status = iterator->status();

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

extern "C" int
rocksdb_iterator_open (rocksdb_iterator_t *iterator, rocksdb_slice_t start, rocksdb_slice_t end, rocksdb_iterator_cb cb) {
  iterator->start = start;
  iterator->end = end;
  iterator->cb = cb;

  return uv_queue_work(iterator->db->loop, &iterator->worker, rocksdb__on_iterator_open, rocksdb__on_after_iterator);
}

static void
rocksdb__on_iterator_close (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  auto iterator = reinterpret_cast<Iterator *>(req->handle);

  delete iterator;
}

extern "C" int
rocksdb_iterator_close (rocksdb_iterator_t *iterator, rocksdb_iterator_cb cb) {
  iterator->cb = cb;

  return uv_queue_work(iterator->db->loop, &iterator->worker, rocksdb__on_iterator_close, rocksdb__on_after_iterator);
}

static void
rocksdb__on_iterator_refresh (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  auto iterator = reinterpret_cast<Iterator *>(req->handle);

  iterator->Refresh();

  rocksdb__iterator_seek(iterator, req);

  auto status = iterator->status();

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

extern "C" int
rocksdb_iterator_refresh (rocksdb_iterator_t *iterator, rocksdb_slice_t start, rocksdb_slice_t end, rocksdb_iterator_cb cb) {
  iterator->start = end;
  iterator->end = end;
  iterator->cb = cb;

  return uv_queue_work(iterator->db->loop, &iterator->worker, rocksdb__on_iterator_close, rocksdb__on_after_iterator);
}

static void
rocksdb__on_iterator_read (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  auto iterator = reinterpret_cast<Iterator *>(req->handle);

  rocksdb__iterator_read(iterator, req);

  auto status = iterator->status();

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

extern "C" int
rocksdb_iterator_read (rocksdb_iterator_t *iterator, rocksdb_slice_t *keys, rocksdb_slice_t *values, size_t capacity, rocksdb_iterator_cb cb) {
  iterator->cb = cb;
  iterator->keys = keys;
  iterator->values = values;
  iterator->len = 0;
  iterator->capacity = capacity;

  return uv_queue_work(iterator->db->loop, &iterator->worker, rocksdb__on_iterator_read, rocksdb__on_after_iterator);
}

extern "C" int
rocksdb_batch_init (rocksdb_t *db, rocksdb_batch_t *batch) {
  batch->db = db;

  batch->worker.data = static_cast<void *>(batch);

  return 0;
}

static void
rocksdb__on_after_read (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_batch_t *>(handle->data);

  req->cb(req, status);

  for (size_t i = 0, n = req->len; i < n; i++) {
    if (req->errors[i]) free(req->errors[i]);
  }
}

static void
rocksdb__on_batch_read (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_batch_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->db->handle);

  if (req->len) {
    std::vector<PinnableSlice> values(req->len);

    std::vector<Status> statuses(req->len);

    db->MultiGet(ReadOptions(), db->DefaultColumnFamily(), req->len, reinterpret_cast<const Slice *>(req->keys), values.data(), statuses.data());

    for (size_t i = 0, n = req->len; i < n; i++) {
      auto status = statuses[i];

      if (status.ok()) {
        req->values[i] = rocksdb__slice_copy(values[i]);
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
rocksdb_batch_read (rocksdb_batch_t *batch, const rocksdb_slice_t *keys, rocksdb_slice_t *values, char **errors, size_t len, rocksdb_batch_cb cb) {
  batch->keys = keys;
  batch->values = values;
  batch->errors = errors;
  batch->len = len;
  batch->cb = cb;

  return uv_queue_work(batch->db->loop, &batch->worker, rocksdb__on_batch_read, rocksdb__on_after_read);
}

static void
rocksdb__on_after_write (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_batch_t *>(handle->data);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_batch_write (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_batch_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->db->handle);

  if (req->len) {
    WriteBatch batch;

    for (size_t i = 0, n = req->len; i < n; i++) {
      if (req->values[i].len == 0) {
        batch.Delete(db->DefaultColumnFamily(), reinterpret_cast<const Slice &>(req->keys[i]));
      } else {
        batch.Put(db->DefaultColumnFamily(), reinterpret_cast<const Slice &>(req->keys[i]), reinterpret_cast<const Slice &>(req->values[i]));
      }
    }

    auto status = db->Write(WriteOptions(), &batch);

    if (status.ok()) {
      req->error = nullptr;
    } else {
      req->error = strdup(status.getState());
    }
  }
}

extern "C" int
rocksdb_batch_write (rocksdb_batch_t *batch, const rocksdb_slice_t *keys, const rocksdb_slice_t *values, size_t len, rocksdb_batch_cb cb) {
  batch->keys = keys;
  batch->values = const_cast<rocksdb_slice_t *>(values);
  batch->error = nullptr;
  batch->len = len;
  batch->cb = cb;

  return uv_queue_work(batch->db->loop, &batch->worker, rocksdb__on_batch_write, rocksdb__on_after_write);
}

static void
rocksdb__on_after_delete (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_batch_t *>(handle->data);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_batch_delete (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_batch_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->db->handle);

  if (req->len) {
    WriteBatch batch;

    for (size_t i = 0, n = req->len; i < n; i++) {
      batch.Delete(db->DefaultColumnFamily(), reinterpret_cast<const Slice &>(req->keys[i]));
    }

    auto status = db->Write(WriteOptions(), &batch);

    if (status.ok()) {
      req->error = nullptr;
    } else {
      req->error = strdup(status.getState());
    }
  }
}

extern "C" int
rocksdb_batch_delete (rocksdb_batch_t *batch, const rocksdb_slice_t *keys, const rocksdb_slice_t *values, size_t len, rocksdb_batch_cb cb) {
  batch->keys = keys;
  batch->values = const_cast<rocksdb_slice_t *>(values);
  batch->error = nullptr;
  batch->len = len;
  batch->cb = cb;

  return uv_queue_work(batch->db->loop, &batch->worker, rocksdb__on_batch_delete, rocksdb__on_after_delete);
}
