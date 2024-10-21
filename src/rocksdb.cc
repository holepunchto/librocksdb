#include <memory>
#include <vector>

#include <path.h>
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

namespace {

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

static const rocksdb_read_options_t rocksdb__default_read_options = {
  .version = 0,
  .snapshot = nullptr,
};

static const rocksdb_write_options_t rocksdb__default_write_options = {
  .version = 0,
};

template <auto rocksdb_options_t::*P, typename T>
static inline T
rocksdb__option (const rocksdb_options_t *options, int min_version, T fallback = T(rocksdb__default_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

template <auto rocksdb_read_options_t::*P, typename T>
static inline T
rocksdb__option (const rocksdb_read_options_t *options, int min_version, T fallback = T(rocksdb__default_read_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

template <auto rocksdb_write_options_t::*P, typename T>
static inline T
rocksdb__option (const rocksdb_write_options_t *options, int min_version, T fallback = T(rocksdb__default_write_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

} // namespace

extern "C" int
rocksdb_init (uv_loop_t *loop, rocksdb_t *db) {
  db->loop = loop;
  db->handle = nullptr;

  return 0;
}

namespace {

template <typename T>
static inline void
rocksdb__on_status (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<T *>(handle->data);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

} // namespace

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

static void
rocksdb__on_recursive_mkdir (uv_fs_t *req) {
  rocksdb_fs_mkdir_step_t *step = (rocksdb_fs_mkdir_step_t *) req->data;

  rocksdb_open_t *handle = (rocksdb_open_t *) step->handle;

  int err = req->result;

  switch (err) {
  case UV_EACCES:
  case UV_ENOSPC:
  case UV_ENOTDIR:
  case UV_EPERM:
    break;

  case 0: {
    if (step->next == NULL) break;

    size_t len = strlen(step->next);

    if (len == strlen(step->req->path)) break;

    step->next[len] = '/';

    uv_fs_req_cleanup(req);

    err = uv_fs_mkdir(req->loop, req, step->next, step->req->mode, rocksdb__on_recursive_mkdir);
    assert(err == 0);

    return;
  }

  case UV_ENOENT: {
    size_t dirname = 0;

    path_dirname(req->path, &dirname, path_behavior_system);

    if (dirname == strlen(req->path)) break;

    if (step->next == NULL) {
      step->next = strdup(step->req->path);
    }

    step->next[dirname - 1] = '\0';

    uv_fs_req_cleanup(req);

    err = uv_fs_mkdir(req->loop, req, step->next, step->req->mode, rocksdb__on_recursive_mkdir);
    assert(err == 0);

    return;
  }
  }

  uv_fs_req_cleanup(req);

  if (step->next != NULL) free(step->next);

  free(step);

  uv_queue_work(handle->db->loop, &handle->worker, rocksdb__on_open, rocksdb__on_status<rocksdb_open_t>);
}

static void
rocksdb__recursive_mkdir (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_open_t *>(handle->data);

  rocksdb_fs_mkdir_t *mkdir_req = (rocksdb_fs_mkdir_t *) malloc(sizeof(rocksdb_fs_mkdir_t));
  mkdir_req->mode = 0777;
  mkdir_req->path = req->path;

  rocksdb_fs_mkdir_step_t *step = (rocksdb_fs_mkdir_step_t *) malloc(sizeof(rocksdb_fs_mkdir_step_t));
  step->req = mkdir_req;
  step->next = NULL;
  step->handle = req;

  mkdir_req->req.data = (void *) step;

  uv_fs_mkdir(req->db->loop, &mkdir_req->req, mkdir_req->path, mkdir_req->mode, rocksdb__on_recursive_mkdir);
}

extern "C" int
rocksdb_open (rocksdb_t *db, rocksdb_open_t *req, const char *path, const rocksdb_options_t *options, rocksdb_open_cb cb) {
  req->db = db;
  req->options = options ? *options : rocksdb__default_options;
  req->error = nullptr;
  req->cb = cb;

  strcpy(req->path, path);

  req->worker.data = static_cast<void *>(req);

  return uv_queue_work(db->loop, &req->worker, rocksdb__recursive_mkdir, NULL);
}

namespace {

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

} // namespace

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

namespace {

static inline rocksdb_slice_t
rocksdb__slice_copy (const Slice &slice) {
  auto len = slice.size();

  auto data = reinterpret_cast<char *>(malloc(len));

  memcpy(data, slice.data(), len);

  return {.data = data, .len = len};
}

static inline const Slice &
rocksdb__slice_cast (const rocksdb_slice_t &slice) {
  return reinterpret_cast<const Slice &>(slice);
}

static inline const rocksdb_slice_t &
rocksdb__slice_cast (const Slice &slice) {
  return reinterpret_cast<const rocksdb_slice_t &>(slice);
}

static inline void
rocksdb__iterator_seek_first (Iterator *iterator, const rocksdb_range_t &range) {
  if (range.gte.len) {
    auto gte = rocksdb__slice_cast(range.gte);

    iterator->Seek(gte);
  } else if (range.gt.len) {
    auto gt = rocksdb__slice_cast(range.gt);

    iterator->Seek(gt);

    if (iterator->Valid() && iterator->key().compare(gt) == 0) {
      iterator->Next();
    }
  } else {
    iterator->SeekToFirst();
  }
}

static inline void
rocksdb__iterator_seek_last (Iterator *iterator, const rocksdb_range_t &range) {
  if (range.lte.len) {
    auto lte = rocksdb__slice_cast(range.lte);

    iterator->Seek(lte);

    if (iterator->Valid()) {
      if (iterator->key().compare(lte) > 0) iterator->Prev();
    } else {
      iterator->SeekToLast();
    }
  } else if (range.lt.len) {

    auto lt = rocksdb__slice_cast(range.lt);

    iterator->Seek(lt);

    if (iterator->Valid()) {
      if (iterator->key().compare(lt) >= 0) iterator->Prev();
    } else {
      iterator->SeekToLast();
    }
  } else {
    iterator->SeekToLast();
  }
}

template <bool reverse>
static inline void
rocksdb__iterator_seek (Iterator *iterator, const rocksdb_range_t &range) {
  if (reverse) {
    rocksdb__iterator_seek_last(iterator, range);
  } else {
    rocksdb__iterator_seek_first(iterator, range);
  }
}

template <typename T>
static inline void
rocksdb__iterator_seek (Iterator *iterator, T *req) {
  const auto &range = req->range;

  if (req->reverse) {
    rocksdb__iterator_seek<true>(iterator, range);
  } else {
    rocksdb__iterator_seek<false>(iterator, range);
  }
}

template <bool reverse = false>
static inline void
rocksdb__iterator_next (Iterator *iterator) {
  if (reverse) {
    iterator->Prev();
  } else {
    iterator->Next();
  }
}

template <typename T>
static inline void
rocksdb__iterator_next (Iterator *iterator, T *req) {
  if (req->reverse) {
    rocksdb__iterator_next<true>(iterator);
  } else {
    rocksdb__iterator_next<false>(iterator);
  }
}

static inline bool
rocksdb__iterator_valid (Iterator *iterator, const rocksdb_range_t &range) {
  if (!iterator->Valid()) return false;

  auto key = iterator->key();

  return (
    (range.lt.len == 0 || key.compare(rocksdb__slice_cast(range.lt)) < 0) &&
    (range.lte.len == 0 || key.compare(rocksdb__slice_cast(range.lte)) <= 0) &&
    (range.gt.len == 0 || key.compare(rocksdb__slice_cast(range.gt)) > 0) &&
    (range.gte.len == 0 || key.compare(rocksdb__slice_cast(range.gte)) >= 0)
  );
}

template <typename T>
static inline bool
rocksdb__iterator_valid (Iterator *iterator, T *req) {
  return rocksdb__iterator_valid(iterator, req->range);
}

template <bool reverse = false>
static inline Iterator *
rocksdb__iterator_open (DB *db, const ReadOptions &options, const rocksdb_range_t &range) {
  auto iterator = db->NewIterator(options);

  rocksdb__iterator_seek<reverse>(iterator, range);

  return iterator;
}

template <typename T>
static inline Iterator *
rocksdb__iterator_open (T *req) {
  auto db = reinterpret_cast<DB *>(req->db->handle);

  const auto &range = req->range;

  auto snapshot = rocksdb__option<&rocksdb_read_options_t::snapshot, rocksdb_snapshot_t *>(
    &req->options, 0
  );

  ReadOptions options;

  if (snapshot) options.snapshot = reinterpret_cast<const Snapshot *>(snapshot->handle);

  if (req->reverse) {
    return rocksdb__iterator_open<true>(db, options, range);
  } else {
    return rocksdb__iterator_open<false>(db, options, range);
  }
}

template <typename T>
static inline void
rocksdb__iterator_read (Iterator *iterator, T *req) {
  while (rocksdb__iterator_valid(iterator, req) && req->len < req->capacity) {
    auto i = req->len++;

    req->keys[i] = rocksdb__slice_copy(iterator->key());
    req->values[i] = rocksdb__slice_copy(iterator->value());

    rocksdb__iterator_next(iterator, req);
  }
}

template <typename T>
static inline void
rocksdb__iterator_refresh (Iterator *iterator, T *req) {
  iterator->Refresh();

  rocksdb__iterator_seek(iterator, req);
}

} // namespace

namespace {

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

} // namespace

extern "C" int
rocksdb_iterator_open (rocksdb_t *db, rocksdb_iterator_t *iterator, rocksdb_range_t range, bool reverse, const rocksdb_read_options_t *options, rocksdb_iterator_cb cb) {
  iterator->db = db;
  iterator->options = options ? *options : rocksdb__default_read_options;
  iterator->range = range;
  iterator->reverse = reverse;
  iterator->cb = cb;

  iterator->worker.data = static_cast<void *>(iterator);

  return uv_queue_work(iterator->db->loop, &iterator->worker, rocksdb__on_iterator_open, rocksdb__on_after_iterator);
}

namespace {

static void
rocksdb__on_iterator_close (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  auto iterator = reinterpret_cast<Iterator *>(req->handle);

  delete iterator;
}

} // namespace

extern "C" int
rocksdb_iterator_close (rocksdb_iterator_t *iterator, rocksdb_iterator_cb cb) {
  iterator->cb = cb;

  return uv_queue_work(iterator->db->loop, &iterator->worker, rocksdb__on_iterator_close, rocksdb__on_after_iterator);
}

namespace {

static void
rocksdb__on_iterator_refresh (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  auto iterator = reinterpret_cast<Iterator *>(req->handle);

  rocksdb__iterator_refresh(iterator, req);

  auto status = iterator->status();

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

} // namespace

extern "C" int
rocksdb_iterator_refresh (rocksdb_iterator_t *iterator, rocksdb_range_t range, bool reverse, const rocksdb_read_options_t *options, rocksdb_iterator_cb cb) {
  iterator->options = options ? *options : rocksdb__default_read_options;
  iterator->range = range;
  iterator->reverse = reverse;
  iterator->cb = cb;

  return uv_queue_work(iterator->db->loop, &iterator->worker, rocksdb__on_iterator_close, rocksdb__on_after_iterator);
}

namespace {

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

} // namespace

extern "C" int
rocksdb_iterator_read (rocksdb_iterator_t *iterator, rocksdb_slice_t *keys, rocksdb_slice_t *values, size_t capacity, rocksdb_iterator_cb cb) {
  iterator->cb = cb;
  iterator->keys = keys;
  iterator->values = values;
  iterator->len = 0;
  iterator->capacity = capacity;

  return uv_queue_work(iterator->db->loop, &iterator->worker, rocksdb__on_iterator_read, rocksdb__on_after_iterator);
}

namespace {

static void
rocksdb__on_after_read (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_read_batch_t *>(handle->data);

  req->cb(req, status);

  for (size_t i = 0, n = req->len; i < n; i++) {
    if (req->errors[i]) free(req->errors[i]);
  }
}

static void
rocksdb__on_read (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_read_batch_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->db->handle);

  if (req->len) {
    std::vector<Slice> keys(req->len);

    for (size_t i = 0, n = req->len; i < n; i++) {
      auto op = &req->reads[i];

      switch (op->type) {
      case rocksdb_get:
        keys[i] = rocksdb__slice_cast(op->key);
        break;
      }
    }

    std::vector<PinnableSlice> values(req->len);

    std::vector<Status> statuses(req->len);

    auto snapshot = rocksdb__option<&rocksdb_read_options_t::snapshot, rocksdb_snapshot_t *>(
      &req->options, 0
    );

    ReadOptions options;

    if (snapshot) options.snapshot = reinterpret_cast<const Snapshot *>(snapshot->handle);

    db->MultiGet(options, db->DefaultColumnFamily(), req->len, keys.data(), values.data(), statuses.data());

    for (size_t i = 0, n = req->len; i < n; i++) {
      auto op = &req->reads[i];

      auto status = statuses[i];

      auto value = rocksdb_slice_empty();

      if (status.ok()) {
        value = rocksdb__slice_copy(values[i]);

        req->errors[i] = nullptr;
      } else if (status.code() == Status::kNotFound) {
        req->errors[i] = nullptr;
      } else {
        req->errors[i] = strdup(status.getState());
        continue;
      }

      switch (op->type) {
      case rocksdb_get:
        op->value = value;
        break;
      }
    }
  }
}

} // namespace

extern "C" int
rocksdb_read (rocksdb_t *db, rocksdb_read_batch_t *batch, rocksdb_read_t *reads, char **errors, size_t len, const rocksdb_read_options_t *options, rocksdb_read_batch_cb cb) {
  batch->db = db;
  batch->options = options ? *options : rocksdb__default_read_options;
  batch->reads = reads;
  batch->errors = errors;
  batch->len = len;
  batch->cb = cb;

  batch->worker.data = static_cast<void *>(batch);

  return uv_queue_work(batch->db->loop, &batch->worker, rocksdb__on_read, rocksdb__on_after_read);
}

namespace {

static void
rocksdb__on_after_write (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_write_batch_t *>(handle->data);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_write (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_write_batch_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->db->handle);

  if (req->len) {
    WriteBatch batch;

    for (size_t i = 0, n = req->len; i < n; i++) {
      auto op = &req->writes[i];

      switch (op->type) {
      case rocksdb_put:
        batch.Put(db->DefaultColumnFamily(), rocksdb__slice_cast(op->key), rocksdb__slice_cast(op->value));
        break;

      case rocksdb_delete:
        batch.Delete(db->DefaultColumnFamily(), rocksdb__slice_cast(op->key));
        break;

      case rocksdb_delete_range:
        batch.DeleteRange(db->DefaultColumnFamily(), rocksdb__slice_cast(op->start), rocksdb__slice_cast(op->end));
        break;
      }
    }

    WriteOptions options;

    auto status = db->Write(options, &batch);

    if (status.ok()) {
      req->error = nullptr;
    } else {
      req->error = strdup(status.getState());
    }
  }
}

} // namespace

extern "C" int
rocksdb_write (rocksdb_t *db, rocksdb_write_batch_t *batch, rocksdb_write_t *writes, size_t len, const rocksdb_write_options_t *options, rocksdb_write_batch_cb cb) {
  batch->db = db;
  batch->options = options ? *options : rocksdb__default_write_options;
  batch->writes = writes;
  batch->error = nullptr;
  batch->len = len;
  batch->cb = cb;

  batch->worker.data = static_cast<void *>(batch);

  return uv_queue_work(batch->db->loop, &batch->worker, rocksdb__on_write, rocksdb__on_after_write);
}

extern "C" int
rocksdb_snapshot_create (rocksdb_t *db, rocksdb_snapshot_t *snapshot) {
  auto handle = reinterpret_cast<DB *>(db->handle)->GetSnapshot();

  if (handle == nullptr) return -1;

  snapshot->db = db;
  snapshot->handle = handle;

  return 0;
}

extern "C" void
rocksdb_snapshot_destroy (rocksdb_snapshot_t *snapshot) {
  reinterpret_cast<DB *>(snapshot->db->handle)->ReleaseSnapshot(reinterpret_cast<const Snapshot *>(snapshot->handle));
}
