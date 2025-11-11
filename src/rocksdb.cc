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
#include "fs.h"

#if defined(__APPLE__)
#include "lock/apple.h"
#elif defined(__linux__)
#include "lock/linux.h"
#elif defined(_WIN32)
#include "lock/win32.h"
#endif

using namespace rocksdb;

static_assert(sizeof(Slice) == sizeof(rocksdb_slice_t));

namespace {

static inline CompactionStyle
rocksdb__from(rocksdb_compaction_style_t compaction_style) {
  switch (compaction_style) {
  case rocksdb_level_compaction:
    return CompactionStyle::kCompactionStyleLevel;
  case rocksdb_universal_compaction:
    return CompactionStyle::kCompactionStyleUniversal;
  case rocksdb_fifo_compaction:
    return CompactionStyle::kCompactionStyleFIFO;
  case rocksdb_no_compaction:
  default:
    return CompactionStyle::kCompactionStyleNone;
  }
}

static inline PinningTier
rocksdb__from(rocksdb_pinning_tier_t pinning_tier) {
  switch (pinning_tier) {
  case rocksdb_pin_none:
  default:
    return PinningTier::kNone;
  case rocksdb_pin_flushed_and_similar:
    return PinningTier::kFlushedAndSimilar;
  case rocksdb_pin_all:
    return PinningTier::kAll;
  }
}

} // namespace

namespace {

static const rocksdb_options_t rocksdb__default_options = {
  .version = 3,
  .read_only = false,
  .create_if_missing = false,
  .create_missing_column_families = false,
  .max_background_jobs = 2,
  .bytes_per_sync = 0,
  .max_open_files = -1,
  .use_direct_reads = false,
  .avoid_unnecessary_blocking_io = false,
  .skip_stats_update_on_db_open = false,
  .use_direct_io_for_flush_and_compaction = false,
  .max_file_opening_threads = 16,
  .lock = -1,
};

static const rocksdb_column_family_options_t rocksdb__default_column_family_options = {
  .version = 3,
  .compaction_style = rocksdb_level_compaction,
  .enable_blob_files = false,
  .min_blob_size = 0,
  .blob_file_size = 1 << 28,
  .enable_blob_garbage_collection = false,
  .block_size = 4 * 1024,
  .cache_index_and_filter_blocks = false,
  .format_version = 6,
  .optimize_filters_for_memory = false,
  .no_block_cache = false,
  .filter_policy = (rocksdb_filter_policy_t) {
    .type = rocksdb_no_filter_policy,
  },
  .top_level_index_pinning_tier = rocksdb_pin_none,
  .partition_pinning_tier = rocksdb_pin_none,
  .unpartitioned_pinning_tier = rocksdb_pin_none,
  .optimize_filters_for_hits = false,
  .num_levels = 7,
  .max_write_buffer_number = 2,
};

static const rocksdb_iterator_options_t rocksdb__default_iterator_options = {
  .version = 0,
  .reverse = false,
  .keys_only = false,
  .snapshot = nullptr,
};

static const rocksdb_read_options_t rocksdb__default_read_options = {
  .version = 0,
  .snapshot = nullptr,
  .async_io = false,
  .fill_cache = true,
};

static const rocksdb_write_options_t rocksdb__default_write_options = {
  .version = 0,
};

static const rocksdb_flush_options_t rocksdb__default_flush_options = {
  .version = 0,
};

static const rocksdb_compact_range_options_t rocksdb__default_compact_range_options = {
  .version = 0,
  .exclusive_manual_compaction = true,
};

static const rocksdb_approximate_size_options_t rocksdb__default_approximate_size_options = {
  .version = 0,
  .include_memtables = false,
  .include_files = true,
  .files_size_error_margin = -1.0,
};

} // namespace

namespace {

template <auto rocksdb_options_t::*P, typename T>
static inline T
rocksdb__option(const rocksdb_options_t *options, int min_version, T fallback = T(rocksdb__default_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

template <auto rocksdb_column_family_options_t::*P, typename T>
static inline T
rocksdb__option(const rocksdb_column_family_options_t *options, int min_version, T fallback = T(rocksdb__default_column_family_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

template <auto rocksdb_iterator_options_t::*P, typename T>
static inline T
rocksdb__option(const rocksdb_iterator_options_t *options, int min_version, T fallback = T(rocksdb__default_iterator_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

template <auto rocksdb_read_options_t::*P, typename T>
static inline T
rocksdb__option(const rocksdb_read_options_t *options, int min_version, T fallback = T(rocksdb__default_read_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

template <auto rocksdb_write_options_t::*P, typename T>
static inline T
rocksdb__option(const rocksdb_write_options_t *options, int min_version, T fallback = T(rocksdb__default_write_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

template <auto rocksdb_compact_range_options_t::*P, typename T>
static inline T
rocksdb__option(const rocksdb_compact_range_options_t *options, int min_version, T fallback = T(rocksdb__default_compact_range_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

template <auto rocksdb_approximate_size_options_t::*P, typename T>
static inline T
rocksdb__option(const rocksdb_approximate_size_options_t *options, int min_version, T fallback = T(rocksdb__default_approximate_size_options.*P)) {
  return options->version >= min_version ? T(options->*P) : fallback;
}

} // namespace

extern "C" int
rocksdb_init(uv_loop_t *loop, rocksdb_t *db) {
  db->loop = loop;
  db->handle = nullptr;
  db->state = 0;
  db->inflight = 0;
  db->lock = -1;
  db->close = nullptr;

  return 0;
}

namespace {

static inline int
rocksdb__close_maybe(rocksdb_t *db);

} // namespace

namespace {

static inline void
rocksdb__add_req(rocksdb_t *db, rocksdb_req_t *req) {
  db->inflight++;
}

template <typename T>
static inline void
rocksdb__add_req(T *req) {
  rocksdb__add_req(req->req.db, &req->req);
}

static inline void
rocksdb__remove_req(rocksdb_t *db, rocksdb_req_t *req) {
  db->inflight--;

  rocksdb__close_maybe(db);
}

template <typename T>
static inline void
rocksdb__remove_req(T *req) {
  rocksdb__remove_req(req->req.db, &req->req);
}

} // namespace

namespace {

static inline void
rocksdb__on_after_open(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_open_t *>(handle->data);

  auto db = req->req.db;

  rocksdb__remove_req(req);

  auto error = req->error;

  if (db->state != rocksdb_closing) {
    if (error == nullptr) db->state = rocksdb_active;
  }

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_open(uv_work_t *handle) {
  int err;

  auto req = reinterpret_cast<rocksdb_open_t *>(handle->data);

  DBOptions options;

  options.create_if_missing = rocksdb__option<&rocksdb_options_t::create_if_missing, bool>(
    &req->options, 0
  );

  options.create_missing_column_families = rocksdb__option<&rocksdb_options_t::create_missing_column_families, bool>(
    &req->options, 0
  );

  options.max_background_jobs = rocksdb__option<&rocksdb_options_t::max_background_jobs, int>(
    &req->options, 0
  );

  options.bytes_per_sync = rocksdb__option<&rocksdb_options_t::bytes_per_sync, uint64_t>(
    &req->options, 0
  );

  options.max_open_files = rocksdb__option<&rocksdb_options_t::max_open_files, int>(
    &req->options, 1
  );

  options.use_direct_reads = rocksdb__option<&rocksdb_options_t::use_direct_reads, bool>(
    &req->options, 1
  );

  options.avoid_unnecessary_blocking_io = rocksdb__option<&rocksdb_options_t::avoid_unnecessary_blocking_io, bool>(
    &req->options, 2
  );

  options.skip_stats_update_on_db_open = rocksdb__option<&rocksdb_options_t::skip_stats_update_on_db_open, bool>(
    &req->options, 2
  );

  options.use_direct_io_for_flush_and_compaction = rocksdb__option<&rocksdb_options_t::use_direct_io_for_flush_and_compaction, bool>(
    &req->options, 2
  );

  options.max_file_opening_threads = rocksdb__option<&rocksdb_options_t::max_file_opening_threads, bool>(
    &req->options, 2
  );

  auto lock = rocksdb__option<&rocksdb_options_t::lock, int>(
    &req->options, 3
  );

  auto read_only = rocksdb__option<&rocksdb_options_t::read_only, bool>(
    &req->options, 0
  );

  if (options.create_if_missing) {
    char *path = strdup(req->path);

    while (true) {
      uv_fs_t fs;
      err = uv_fs_mkdir(NULL, &fs, path, 0775, NULL);

      uv_fs_req_cleanup(&fs);

      switch (err) {
      case UV_EACCES:
      case UV_ENOSPC:
      case UV_ENOTDIR:
      case UV_EPERM:
      default:
        goto done;

      case UV_EEXIST:
      case 0: {
        size_t len = strlen(path);

        if (len == strlen(req->path)) goto done;

        path[len] = '/';

        continue;
      }

      case UV_ENOENT: {
        size_t dirname;
        err = path_dirname(path, &dirname, path_behavior_system);
        assert(err == 0);

        if (dirname == strlen(req->path)) goto done;

        path[dirname - 1] = '\0';

        continue;
      }
      }
    }

  done:
    free(path);
  }

  auto column_families = std::vector<ColumnFamilyDescriptor>(req->len);

  for (size_t i = 0, n = req->len; i < n; i++) {
    auto &column_family = req->column_families[i];

    ColumnFamilyOptions options;

    auto compaction_style = rocksdb__option<&rocksdb_column_family_options_t::compaction_style, rocksdb_compaction_style_t>(
      &column_family.options, 0
    );

    options.compaction_style = rocksdb__from(compaction_style);

    options.enable_blob_files = rocksdb__option<&rocksdb_column_family_options_t::enable_blob_files, bool>(
      &column_family.options, 0
    );

    options.min_blob_size = rocksdb__option<&rocksdb_column_family_options_t::min_blob_size, uint64_t>(
      &column_family.options, 0
    );

    options.blob_file_size = rocksdb__option<&rocksdb_column_family_options_t::blob_file_size, uint64_t>(
      &column_family.options, 0
    );

    options.enable_blob_garbage_collection = rocksdb__option<&rocksdb_column_family_options_t::enable_blob_garbage_collection, bool>(
      &column_family.options, 0
    );

    BlockBasedTableOptions table_options;

    table_options.block_size = rocksdb__option<&rocksdb_column_family_options_t::block_size, uint64_t>(
      &column_family.options, 0
    );

    table_options.cache_index_and_filter_blocks = rocksdb__option<&rocksdb_column_family_options_t::cache_index_and_filter_blocks, bool>(
      &column_family.options, 0
    );

    table_options.format_version = rocksdb__option<&rocksdb_column_family_options_t::format_version, uint32_t>(
      &column_family.options, 0
    );

    table_options.optimize_filters_for_memory = rocksdb__option<&rocksdb_column_family_options_t::optimize_filters_for_memory, bool>(
      &column_family.options, 1
    );

    table_options.no_block_cache = rocksdb__option<&rocksdb_column_family_options_t::no_block_cache, bool>(
      &column_family.options, 1
    );

    auto filter_policy = rocksdb__option<&rocksdb_column_family_options_t::filter_policy, rocksdb_filter_policy_t>(
      &column_family.options, 2
    );

    switch (filter_policy.type) {
    case rocksdb_bloom_filter_policy:
      table_options.filter_policy = std::shared_ptr<const FilterPolicy>(
        NewBloomFilterPolicy(filter_policy.bloom.bits_per_key)
      );
      break;

    case rocksdb_ribbon_filter_policy:
      table_options.filter_policy = std::shared_ptr<const FilterPolicy>(
        NewRibbonFilterPolicy(filter_policy.ribbon.bloom_equivalent_bits_per_key, filter_policy.ribbon.bloom_before_level)
      );
      break;

    case rocksdb_no_filter_policy:
    default:
      table_options.filter_policy = nullptr;
      break;
    }

    auto top_level_index_pinning = rocksdb__option<&rocksdb_column_family_options_t::top_level_index_pinning_tier, rocksdb_pinning_tier_t>(
      &column_family.options, 3
    );

    auto partition_pinning = rocksdb__option<&rocksdb_column_family_options_t::partition_pinning_tier, rocksdb_pinning_tier_t>(
      &column_family.options, 3
    );

    auto unpartitioned_pinning = rocksdb__option<&rocksdb_column_family_options_t::unpartitioned_pinning_tier, rocksdb_pinning_tier_t>(
      &column_family.options, 3
    );

    table_options.metadata_cache_options = {
      .top_level_index_pinning = rocksdb__from(top_level_index_pinning),
      .partition_pinning = rocksdb__from(partition_pinning),
      .unpartitioned_pinning = rocksdb__from(unpartitioned_pinning),
    };

    options.optimize_filters_for_hits = rocksdb__option<&rocksdb_column_family_options_t::optimize_filters_for_hits, bool>(
      &column_family.options, 4
    );

    options.num_levels = rocksdb__option<&rocksdb_column_family_options_t::num_levels, int>(
      &column_family.options, 4
    );

    options.max_write_buffer_number = rocksdb__option<&rocksdb_column_family_options_t::max_write_buffer_number, int>(
      &column_family.options, 4
    );

    options.table_factory = std::shared_ptr<TableFactory>(NewBlockBasedTableFactory(table_options));

    column_families[i] = ColumnFamilyDescriptor(column_family.name, options);
  }

  auto env = NewCompositeEnv(std::make_shared<rocksdb_file_system_t>());

  options.env = env.release();

  auto handles = std::vector<ColumnFamilyHandle *>(req->len);

  Status status;

  std::unique_ptr<DB> ptr;

  if (read_only) {
    status = DB::OpenForReadOnly(options, req->path, column_families, &handles, &ptr);
  } else {
    status = DB::Open(options, req->path, column_families, &handles, &ptr);
  }

  for (size_t i = 0, n = req->len; i < n; i++) {
    req->handles[i] = reinterpret_cast<rocksdb_column_family_t *>(handles[i]);
  }

  auto db = req->req.db;

  if (status.ok()) {
    db->handle = ptr.release();
    db->lock = lock;

    req->error = nullptr;
  } else {
    db->handle = nullptr;
    db->lock = -1;

    req->error = strdup(status.getState());

    if (lock >= 0) {
      uv_fs_t fs;
      err = uv_fs_close(NULL, &fs, lock, NULL);
      assert(err == 0);

      uv_fs_req_cleanup(&fs);
    }
  }
}

} // namespace

extern "C" int
rocksdb_open(rocksdb_t *db, rocksdb_open_t *req, const char *path, const rocksdb_options_t *options, const rocksdb_column_family_descriptor_t column_families[], rocksdb_column_family_t *handles[], size_t len, rocksdb_open_cb cb) {
  req->req.db = db;
  req->options = options ? *options : rocksdb__default_options;
  req->column_families = column_families;
  req->handles = handles;
  req->len = len;
  req->error = nullptr;
  req->cb = cb;

  strcpy(req->path, path);

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(db->loop, &req->req.worker, rocksdb__on_open, rocksdb__on_after_open);
}

namespace {

static inline void
rocksdb__on_after_close(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_close_t *>(handle->data);

  req->req.db->handle = nullptr;

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_close(uv_work_t *handle) {
  int err;

  auto req = reinterpret_cast<rocksdb_close_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  CancelAllBackgroundWork(db, true);

  auto status = db->Close();

  if (status.IsAborted()) { // Snaphots are still held
    req->error = strdup(status.getState());
  } else {
    req->error = nullptr;

    auto env = db->GetEnv();

    delete db;
    delete env;

    auto lock = req->req.db->lock;

    if (lock >= 0) {
      uv_fs_t fs;
      err = uv_fs_close(NULL, &fs, lock, NULL);
      assert(err == 0);

      uv_fs_req_cleanup(&fs);
    }
  }
}

} // namespace

extern "C" int
rocksdb_close(rocksdb_t *db, rocksdb_close_t *req, rocksdb_close_cb cb) {
  db->state = rocksdb_closing;

  req->req.db = db;
  req->error = nullptr;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  db->close = req;

  return rocksdb__close_maybe(db);
}

namespace {

static inline int
rocksdb__close_maybe(rocksdb_t *db) {
  rocksdb_close_t *req = db->close;

  if (db->inflight > 0 || req == nullptr) return 0;

  db->close = nullptr;

  if (db->handle == nullptr) {
    req->cb(req, 0);

    return 0;
  }

  return uv_queue_work(db->loop, &req->req.worker, rocksdb__on_close, rocksdb__on_after_close);
}

} // namespace

namespace {

static inline void
rocksdb__on_after_suspend(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_suspend_t *>(handle->data);

  auto db = req->req.db;

  rocksdb__remove_req(req);

  auto error = req->error;

  if (db->state != rocksdb_closing) {
    if (error == nullptr) db->state = rocksdb_suspended;
    else db->state = rocksdb_active;
  }

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_suspend(uv_work_t *handle) {
  int err;

  auto req = reinterpret_cast<rocksdb_suspend_t *>(handle->data);

  auto lock = req->req.db->lock;

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  auto fs = static_cast<rocksdb_file_system_t *>(db->GetFileSystem());

  auto status = db->PauseBackgroundWork();

  if (status.ok()) {
    fs->suspend();

    if (lock >= 0) rocksdb__unlock(lock);

    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

} // namespace

extern "C" int
rocksdb_suspend(rocksdb_t *db, rocksdb_suspend_t *req, rocksdb_suspend_cb cb) {
  if (db->state != rocksdb_active) {
    return UV_EINVAL;
  }

  db->state = rocksdb_suspending;

  req->req.db = db;
  req->error = nullptr;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(db->loop, &req->req.worker, rocksdb__on_suspend, rocksdb__on_after_suspend);
}

namespace {

static inline void
rocksdb__on_after_resume(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_resume_t *>(handle->data);

  auto db = req->req.db;

  rocksdb__remove_req(req);

  auto error = req->error;

  if (db->state != rocksdb_closing) {
    if (error == nullptr) db->state = rocksdb_active;
    else db->state = rocksdb_suspended;
  }

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_resume(uv_work_t *handle) {
  int err;

  auto req = reinterpret_cast<rocksdb_resume_t *>(handle->data);

  auto lock = req->req.db->lock;

  if (lock >= 0) rocksdb__lock(lock);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  auto fs = static_cast<rocksdb_file_system_t *>(db->GetFileSystem());

  auto status = fs->resume();

  if (status.ok()) {
    auto status = db->ContinueBackgroundWork();

    if (status.ok()) {
      req->error = nullptr;
    } else {
      fs->suspend();

      req->error = strdup(status.getState());
    }
  } else {
    req->error = strdup(status.getState());
  }
}

} // namespace

extern "C" int
rocksdb_resume(rocksdb_t *db, rocksdb_resume_t *req, rocksdb_resume_cb cb) {
  if (db->state != rocksdb_suspended) {
    return UV_EINVAL;
  }

  db->state = rocksdb_resuming;

  req->req.db = db;
  req->error = nullptr;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(db->loop, &req->req.worker, rocksdb__on_resume, rocksdb__on_after_resume);
}

extern "C" rocksdb_column_family_descriptor_t
rocksdb_column_family_descriptor(const char *name, const rocksdb_column_family_options_t *options) {
  rocksdb_column_family_descriptor_t descriptor;

  descriptor.name = name;
  descriptor.options = options ? *options : rocksdb__default_column_family_options;

  return descriptor;
}

extern "C" rocksdb_column_family_t *
rocksdb_column_family_default(rocksdb_t *db) {
  auto handle = reinterpret_cast<DB *>(db->handle)->DefaultColumnFamily();

  return reinterpret_cast<rocksdb_column_family_t *>(handle);
}

extern "C" int
rocksdb_column_family_destroy(rocksdb_t *db, rocksdb_column_family_t *column_family) {
  auto handle = reinterpret_cast<ColumnFamilyHandle *>(column_family);

  auto status = reinterpret_cast<DB *>(db->handle)->DestroyColumnFamilyHandle(handle);

  return status.ok() ? 0 : -1;
}

extern "C" rocksdb_slice_t
rocksdb_slice_init(const char *data, size_t len) {
  return {.data = data, .len = len};
}

extern "C" void
rocksdb_slice_destroy(rocksdb_slice_t *slice) {
  free(const_cast<char *>(slice->data));
}

extern "C" rocksdb_slice_t
rocksdb_slice_empty(void) {
  return {.data = nullptr, .len = 0};
}

namespace {

static inline rocksdb_slice_t
rocksdb__slice_copy(const Slice &slice) {
  auto len = slice.size();

  auto data = reinterpret_cast<char *>(malloc(len));

  memcpy(data, slice.data(), len);

  return {.data = data, .len = len};
}

static inline const Slice &
rocksdb__slice_cast(const rocksdb_slice_t &slice) {
  return reinterpret_cast<const Slice &>(slice);
}

static inline const rocksdb_slice_t &
rocksdb__slice_cast(const Slice &slice) {
  return reinterpret_cast<const rocksdb_slice_t &>(slice);
}

static inline rocksdb_slice_t
rocksdb__slice_missing() {
  return {.data = nullptr, .len = static_cast<size_t>(-1)};
}

} // namespace

namespace {

static inline void
rocksdb__iterator_seek_first(Iterator *iterator, const rocksdb_range_t &range) {
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
rocksdb__iterator_seek_last(Iterator *iterator, const rocksdb_range_t &range) {
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
rocksdb__iterator_seek(Iterator *iterator, const rocksdb_range_t &range) {
  if (reverse) {
    rocksdb__iterator_seek_last(iterator, range);
  } else {
    rocksdb__iterator_seek_first(iterator, range);
  }
}

template <typename T>
static inline void
rocksdb__iterator_seek(Iterator *iterator, T *req) {
  const auto &range = req->range;

  if (req->options.reverse) {
    rocksdb__iterator_seek<true>(iterator, range);
  } else {
    rocksdb__iterator_seek<false>(iterator, range);
  }
}

template <bool reverse = false>
static inline void
rocksdb__iterator_next(Iterator *iterator) {
  if (reverse) {
    iterator->Prev();
  } else {
    iterator->Next();
  }
}

template <typename T>
static inline void
rocksdb__iterator_next(Iterator *iterator, T *req) {
  if (req->options.reverse) {
    rocksdb__iterator_next<true>(iterator);
  } else {
    rocksdb__iterator_next<false>(iterator);
  }
}

static inline bool
rocksdb__iterator_valid(Iterator *iterator, const rocksdb_range_t &range) {
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
rocksdb__iterator_valid(Iterator *iterator, T *req) {
  return rocksdb__iterator_valid(iterator, req->range);
}

template <bool reverse = false>
static inline Iterator *
rocksdb__iterator_open(DB *db, const ReadOptions &options, ColumnFamilyHandle *column_family, const rocksdb_range_t &range) {
  auto iterator = db->NewIterator(options, column_family);

  rocksdb__iterator_seek<reverse>(iterator, range);

  return iterator;
}

template <typename T>
static inline Iterator *
rocksdb__iterator_open(T *req) {
  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  const auto &range = req->range;

  auto snapshot = rocksdb__option<&rocksdb_iterator_options_t::snapshot, rocksdb_snapshot_t *>(
    &req->options, 0
  );

  ReadOptions options;

  if (snapshot) options.snapshot = reinterpret_cast<const Snapshot *>(snapshot->handle);

  auto column_family = reinterpret_cast<ColumnFamilyHandle *>(req->column_family);

  if (req->options.reverse) {
    return rocksdb__iterator_open<true>(db, options, column_family, range);
  } else {
    return rocksdb__iterator_open<false>(db, options, column_family, range);
  }
}

template <typename T>
static inline void
rocksdb__iterator_read(Iterator *iterator, T *req) {
  while (rocksdb__iterator_valid(iterator, req) && req->len < req->capacity) {
    auto i = req->len++;

    req->keys[i] = rocksdb__slice_copy(iterator->key());

    if (!req->options.keys_only) {
      req->values[i] = rocksdb__slice_copy(iterator->value());
    }

    rocksdb__iterator_next(iterator, req);
  }
}

template <typename T>
static inline void
rocksdb__iterator_refresh(Iterator *iterator, T *req) {
  auto snapshot = rocksdb__option<&rocksdb_iterator_options_t::snapshot, rocksdb_snapshot_t *>(
    &req->options, 0
  );

  if (snapshot) {
    iterator->Refresh(reinterpret_cast<const Snapshot *>(snapshot->handle));
  } else {
    iterator->Refresh();
  }

  rocksdb__iterator_seek(iterator, req);
}

} // namespace

namespace {

static void
rocksdb__on_after_iterator_open(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  req->inflight = false;

  auto error = req->error;

  if (error == nullptr) req->state = rocksdb_active;
  else rocksdb__remove_req(req);

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_iterator_open(uv_work_t *handle) {
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
rocksdb_iterator_open(rocksdb_t *db, rocksdb_iterator_t *req, rocksdb_column_family_t *column_family, rocksdb_range_t range, const rocksdb_iterator_options_t *options, rocksdb_iterator_cb cb) {
  if (db->state != rocksdb_active) {
    return UV_EINVAL;
  }

  req->req.db = db;
  req->options = options ? *options : rocksdb__default_iterator_options;
  req->column_family = column_family;
  req->range = range;
  req->state = 0;
  req->inflight = true;
  req->error = nullptr;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_iterator_open, rocksdb__on_after_iterator_open);
}

namespace {

static void
rocksdb__on_after_iterator_close(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  rocksdb__remove_req(req);

  req->inflight = false;

  req->cb(req, status);
}

static void
rocksdb__on_iterator_close(uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  auto iterator = reinterpret_cast<Iterator *>(req->handle);

  delete iterator;
}

} // namespace

extern "C" int
rocksdb_iterator_close(rocksdb_iterator_t *req, rocksdb_iterator_cb cb) {
  auto db = req->req.db;

  if (req->inflight) return UV_EINVAL;

  req->state = rocksdb_closing;
  req->inflight = true;
  req->error = nullptr;
  req->cb = cb;

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_iterator_close, rocksdb__on_after_iterator_close);
}

namespace {

static void
rocksdb__on_after_iterator_refresh(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  req->inflight = false;

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_iterator_refresh(uv_work_t *handle) {
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
rocksdb_iterator_refresh(rocksdb_iterator_t *req, rocksdb_range_t range, const rocksdb_iterator_options_t *options, rocksdb_iterator_cb cb) {
  auto db = req->req.db;

  if (db->state != rocksdb_active || req->state != rocksdb_active || req->inflight) {
    return UV_EINVAL;
  }

  req->options = options ? *options : rocksdb__default_iterator_options;
  req->range = range;
  req->inflight = true;
  req->error = nullptr;
  req->cb = cb;

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_iterator_close, rocksdb__on_after_iterator_refresh);
}

namespace {

static void
rocksdb__on_after_iterator_read(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  req->inflight = false;

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_iterator_read(uv_work_t *handle) {
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
rocksdb_iterator_read(rocksdb_iterator_t *req, rocksdb_slice_t *keys, rocksdb_slice_t *values, size_t capacity, rocksdb_iterator_cb cb) {
  auto db = req->req.db;

  if (db->state != rocksdb_active || req->state != rocksdb_active || req->inflight) {
    return UV_EINVAL;
  }

  req->keys = keys;
  req->values = values;
  req->len = 0;
  req->capacity = capacity;
  req->inflight = true;
  req->error = nullptr;
  req->cb = cb;

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_iterator_read, rocksdb__on_after_iterator_read);
}

namespace {

static void
rocksdb__on_after_read(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_read_batch_t *>(handle->data);

  rocksdb__remove_req(req);

  auto errors = req->errors;
  auto len = req->len;

  req->cb(req, status);

  if (errors) {
    for (size_t i = 0, n = len; i < n; i++) {
      if (errors[i]) free(errors[i]);
    }

    delete[] errors;
  }
}

static void
rocksdb__on_read(uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_read_batch_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  if (req->len) {
    auto column_families = std::vector<ColumnFamilyHandle *>(req->len);

    auto keys = std::vector<Slice>(req->len);

    for (size_t i = 0, n = req->len; i < n; i++) {
      auto op = &req->reads[i];

      column_families[i] = reinterpret_cast<ColumnFamilyHandle *>(op->column_family);

      switch (op->type) {
      case rocksdb_get:
        keys[i] = rocksdb__slice_cast(op->key);
        break;
      }
    }

    auto values = std::vector<PinnableSlice>(req->len);

    auto statuses = std::vector<Status>(req->len);

    auto snapshot = rocksdb__option<&rocksdb_read_options_t::snapshot, rocksdb_snapshot_t *>(
      &req->options, 0
    );

    ReadOptions options;

    options.async_io = rocksdb__option<&rocksdb_read_options_t::async_io, bool>(
      &req->options, 1
    );

    options.fill_cache = rocksdb__option<&rocksdb_read_options_t::fill_cache, bool>(
      &req->options, 1
    );

    if (snapshot) options.snapshot = reinterpret_cast<const Snapshot *>(snapshot->handle);

    db->MultiGet(options, req->len, column_families.data(), keys.data(), values.data(), statuses.data());

    req->errors = new char *[req->len];

    for (size_t i = 0, n = req->len; i < n; i++) {
      auto op = &req->reads[i];

      auto status = statuses[i];

      auto value = rocksdb_slice_empty();

      if (status.ok()) {
        value = rocksdb__slice_copy(values[i]);

        req->errors[i] = nullptr;
      } else if (status.code() == Status::kNotFound) {
        value = rocksdb__slice_missing();

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
rocksdb_read(rocksdb_t *db, rocksdb_read_batch_t *req, rocksdb_read_t *reads, size_t len, const rocksdb_read_options_t *options, rocksdb_read_batch_cb cb) {
  if (db->state != rocksdb_active) {
    return UV_EINVAL;
  }

  req->req.db = db;
  req->options = options ? *options : rocksdb__default_read_options;
  req->reads = reads;
  req->errors = nullptr;
  req->len = len;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_read, rocksdb__on_after_read);
}

namespace {

static void
rocksdb__on_after_write(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_write_batch_t *>(handle->data);

  rocksdb__remove_req(req);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_write(uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_write_batch_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  if (req->len) {
    WriteBatch batch;

    for (size_t i = 0, n = req->len; i < n; i++) {
      auto op = &req->writes[i];

      auto column_family = reinterpret_cast<ColumnFamilyHandle *>(op->column_family);

      switch (op->type) {
      case rocksdb_put:
        batch.Put(column_family, rocksdb__slice_cast(op->key), rocksdb__slice_cast(op->value));
        break;

      case rocksdb_delete:
        batch.Delete(column_family, rocksdb__slice_cast(op->key));
        break;

      case rocksdb_delete_range:
        batch.DeleteRange(column_family, rocksdb__slice_cast(op->start), rocksdb__slice_cast(op->end));
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
rocksdb_write(rocksdb_t *db, rocksdb_write_batch_t *req, rocksdb_write_t *writes, size_t len, const rocksdb_write_options_t *options, rocksdb_write_batch_cb cb) {
  if (db->state != rocksdb_active) {
    return UV_EINVAL;
  }

  req->req.db = db;
  req->options = options ? *options : rocksdb__default_write_options;
  req->writes = writes;
  req->error = nullptr;
  req->len = len;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_write, rocksdb__on_after_write);
}

namespace {

static void
rocksdb__on_after_flush(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_flush_t *>(handle->data);

  rocksdb__remove_req(req);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_flush(uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_flush_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  auto column_family = reinterpret_cast<ColumnFamilyHandle *>(req->column_family);

  FlushOptions options;

  auto status = db->Flush(options, column_family);

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

} // namespace

extern "C" int
rocksdb_flush(rocksdb_t *db, rocksdb_flush_t *req, rocksdb_column_family_t *column_family, const rocksdb_flush_options_t *options, rocksdb_flush_cb cb) {
  if (db->state != rocksdb_active) {
    return UV_EINVAL;
  }

  req->req.db = db;
  req->column_family = column_family;
  req->options = options ? *options : rocksdb__default_flush_options;
  req->error = nullptr;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_flush, rocksdb__on_after_flush);
}

namespace {

static void
rocksdb__on_after_compact_range(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_compact_range_t *>(handle->data);

  rocksdb__remove_req(req);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_compact_range(uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_compact_range_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  auto column_family = reinterpret_cast<ColumnFamilyHandle *>(req->column_family);

  auto start = rocksdb__slice_cast(req->start);
  auto end = rocksdb__slice_cast(req->end);

  CompactRangeOptions options;

  options.exclusive_manual_compaction = rocksdb__option<&rocksdb_compact_range_options_t::exclusive_manual_compaction, bool>(
    &req->options, 0
  );

  auto status = db->CompactRange(
    options,
    column_family,
    start.empty() ? nullptr : &start,
    end.empty() ? nullptr : &end
  );

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

} // namespace

extern "C" int
rocksdb_compact_range(rocksdb_t *db, rocksdb_compact_range_t *req, rocksdb_column_family_t *column_family, rocksdb_slice_t start, rocksdb_slice_t end, const rocksdb_compact_range_options_t *options, rocksdb_compact_range_cb cb) {
  if (db->state != rocksdb_active) {
    return UV_EINVAL;
  }

  req->req.db = db;
  req->column_family = column_family;
  req->start = start;
  req->end = end;
  req->options = options ? *options : rocksdb__default_compact_range_options;
  req->error = nullptr;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_compact_range, rocksdb__on_after_compact_range);
}

namespace {

static void
rocksdb__on_after_approximate_size(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_approximate_size_t *>(handle->data);

  rocksdb__remove_req(req);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_approximate_size(uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_approximate_size_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  auto column_family = reinterpret_cast<ColumnFamilyHandle *>(req->column_family);

  auto start = rocksdb__slice_cast(req->start);
  auto end = rocksdb__slice_cast(req->end);

  SizeApproximationOptions options;

  options.include_memtables = rocksdb__option<&rocksdb_approximate_size_options_t::include_memtables, bool>(
    &req->options, 0
  );

  options.include_files = rocksdb__option<&rocksdb_approximate_size_options_t::include_files, bool>(
    &req->options, 0
  );

  options.files_size_error_margin = rocksdb__option<&rocksdb_approximate_size_options_t::files_size_error_margin, double>(
    &req->options, 0
  );

  Range range;

  range.start = start;
  range.limit = end;

  uint64_t result;

  auto status = db->GetApproximateSizes(options, column_family, &range, 1, &result);

  if (status.ok()) {
    req->error = nullptr;
    req->result = result;
  } else {
    req->error = strdup(status.getState());
    req->result = 0;
  }
}

} // namespace

extern "C" int
rocksdb_approximate_size(rocksdb_t *db, rocksdb_approximate_size_t *req, rocksdb_column_family_t *column_family, rocksdb_slice_t start, rocksdb_slice_t end, const rocksdb_approximate_size_options_t *options, rocksdb_approximate_size_cb cb) {
  if (db->state != rocksdb_active) {
    return UV_EINVAL;
  }

  req->req.db = db;
  req->column_family = column_family;
  req->start = start;
  req->end = end;
  req->options = options ? *options : rocksdb__default_approximate_size_options;
  req->error = nullptr;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_approximate_size, rocksdb__on_after_approximate_size);
}

extern "C" int
rocksdb_snapshot_create(rocksdb_t *db, rocksdb_snapshot_t *snapshot) {
  auto handle = reinterpret_cast<DB *>(db->handle)->GetSnapshot();

  if (handle == nullptr) return -1;

  snapshot->db = db;
  snapshot->handle = handle;

  return 0;
}

extern "C" void
rocksdb_snapshot_destroy(rocksdb_snapshot_t *snapshot) {
  reinterpret_cast<DB *>(snapshot->db->handle)->ReleaseSnapshot(reinterpret_cast<const Snapshot *>(snapshot->handle));
}
