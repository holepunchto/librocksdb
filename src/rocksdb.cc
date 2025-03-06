#include <memory>
#include <set>
#include <vector>

#include <intrusive.h>
#include <intrusive/ring.h>
#include <path.h>
#include <rocksdb/advanced_options.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"
#include "rocksdb/io_status.h"

typedef struct rocksdb_lock_s rocksdb_lock_t;
typedef struct rocksdb_file_system_s rocksdb_file_system_t;

using namespace rocksdb;

static_assert(sizeof(Slice) == sizeof(rocksdb_slice_t));

struct rocksdb_lock_s : FileLock {
  std::string fname;
  FileLock *lock;

  rocksdb_lock_s(std::string fname, FileLock *lock) : fname(fname), lock(lock) {}

  inline auto
  release(const std::shared_ptr<FileSystem> &fs) {
    assert(lock != nullptr);

    IOOptions options;
    IODebugContext dbg;

    auto status = fs->UnlockFile(lock, options, &dbg);

    if (status.ok()) lock = nullptr;

    return status;
  }

  inline auto
  acquire(const std::shared_ptr<FileSystem> &fs) {
    assert(lock == nullptr);

    IOOptions options;
    IODebugContext dbg;

    return fs->LockFile(fname, options, &lock, &dbg);
  }
};

struct rocksdb_file_system_s : FileSystem {
  std::shared_ptr<FileSystem> fs;
  std::atomic<bool> suspended;
  std::set<rocksdb_lock_t *> locks;

  rocksdb_file_system_s() : fs(FileSystem::Default()), suspended(false), locks() {}

  inline void
  suspend() {
    auto expected = false;

    if (suspended.compare_exchange_strong(expected, true)) {
      for (const auto &lock : locks) {
        lock->release(fs);
      }
    }
  }

  inline void
  resume() {
    bool expected = true;

    if (suspended.compare_exchange_strong(expected, false)) {
      for (const auto &lock : locks) {
        lock->acquire(fs);
      }
    }
  }

private:
  const char *Name() const override { return "RocksFS"; }

  IOStatus LockFile(const std::string &fname, const IOOptions &options, FileLock **result, IODebugContext *dbg) override {
    FileLock *lock;

    auto status = fs->LockFile(fname, options, &lock, dbg);

    if (status.ok()) {
      auto wrapper = new rocksdb_lock_t(fname, lock);

      locks.insert(wrapper);

      *result = wrapper;
    }

    return status;
  }

  IOStatus UnlockFile(FileLock *lock, const IOOptions &options, IODebugContext *dbg) override {
    auto wrapper = reinterpret_cast<rocksdb_lock_t *>(lock);

    auto status = fs->UnlockFile(wrapper->lock, options, dbg);

    if (status.ok()) {
      locks.erase(wrapper);
    }

    return status;
  }

  Status RegisterDbPaths(const std::vector<std::string> &paths) override {
    return fs->RegisterDbPaths(paths);
  }

  Status UnregisterDbPaths(const std::vector<std::string> &paths) override {
    return fs->UnregisterDbPaths(paths);
  }

  IOStatus NewSequentialFile(const std::string &fname, const FileOptions &file_opts, std::unique_ptr<FSSequentialFile> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->NewSequentialFile(fname, file_opts, result, dbg);
  }

  IOStatus NewRandomAccessFile(const std::string &fname, const FileOptions &file_opts, std::unique_ptr<FSRandomAccessFile> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->NewRandomAccessFile(fname, file_opts, result, dbg);
  }

  IOStatus NewWritableFile(const std::string &fname, const FileOptions &file_opts, std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->NewWritableFile(fname, file_opts, result, dbg);
  }

  IOStatus ReopenWritableFile(const std::string &fname, const FileOptions &options, std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->ReopenWritableFile(fname, options, result, dbg);
  }

  IOStatus ReuseWritableFile(const std::string &fname, const std::string &old_fname, const FileOptions &file_opts, std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->ReuseWritableFile(fname, old_fname, file_opts, result, dbg);
  }

  IOStatus NewRandomRWFile(const std::string &fname, const FileOptions &options, std::unique_ptr<FSRandomRWFile> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->NewRandomRWFile(fname, options, result, dbg);
  }

  IOStatus NewMemoryMappedFileBuffer(const std::string &fname, std::unique_ptr<MemoryMappedFileBuffer> *result) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->NewMemoryMappedFileBuffer(fname, result);
  }

  IOStatus NewDirectory(const std::string &name, const IOOptions &io_opts, std::unique_ptr<FSDirectory> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->NewDirectory(name, io_opts, result, dbg);
  }

  IOStatus FileExists(const std::string &fname, const IOOptions &options, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->FileExists(fname, options, dbg);
  }

  IOStatus GetChildren(const std::string &dir, const IOOptions &options, std::vector<std::string> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->GetChildren(dir, options, result, dbg);
  }

  IOStatus GetChildrenFileAttributes(const std::string &dir, const IOOptions &options, std::vector<FileAttributes> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->GetChildrenFileAttributes(dir, options, result, dbg);
  }

  IOStatus DeleteFile(const std::string &fname, const IOOptions &options, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->DeleteFile(fname, options, dbg);
  }

  IOStatus Truncate(const std::string &fname, size_t size, const IOOptions &options, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->Truncate(fname, size, options, dbg);
  }

  IOStatus CreateDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->CreateDir(dirname, options, dbg);
  }

  IOStatus CreateDirIfMissing(const std::string &dirname, const IOOptions &options, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->CreateDirIfMissing(dirname, options, dbg);
  }

  IOStatus DeleteDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->DeleteDir(dirname, options, dbg);
  }

  IOStatus GetFileSize(const std::string &fname, const IOOptions &options, uint64_t *file_size, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->GetFileSize(fname, options, file_size, dbg);
  }

  IOStatus GetFileModificationTime(const std::string &fname, const IOOptions &options, uint64_t *file_mtime, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->GetFileModificationTime(fname, options, file_mtime, dbg);
  }

  IOStatus RenameFile(const std::string &src, const std::string &target, const IOOptions &options, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->RenameFile(src, target, options, dbg);
  }

  IOStatus LinkFile(const std::string &src, const std::string &target, const IOOptions &options, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->LinkFile(src, target, options, dbg);
  }

  IOStatus NumFileLinks(const std::string &fname, const IOOptions &options, uint64_t *count, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->NumFileLinks(fname, options, count, dbg);
  }

  IOStatus AreFilesSame(const std::string &first, const std::string &second, const IOOptions &options, bool *res, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->AreFilesSame(first, second, options, res, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions &options, std::string *path, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->GetTestDirectory(options, path, dbg);
  }

  IOStatus NewLogger(const std::string &fname, const IOOptions &io_opts, std::shared_ptr<Logger> *result, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->NewLogger(fname, io_opts, result, dbg);
  }

  IOStatus GetAbsolutePath(const std::string &db_path, const IOOptions &options, std::string *output_path, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->GetAbsolutePath(db_path, options, output_path, dbg);
  }

  void SanitizeFileOptions(FileOptions *opts) const override {
    return fs->SanitizeFileOptions(opts);
  }

  FileOptions OptimizeForLogRead(const FileOptions &file_options) const override {
    return fs->OptimizeForLogRead(file_options);
  }

  FileOptions OptimizeForManifestRead(const FileOptions &file_options) const override {
    return fs->OptimizeForManifestRead(file_options);
  }

  FileOptions OptimizeForLogWrite(const FileOptions &file_options, const DBOptions &db_options) const override {
    return fs->OptimizeForLogWrite(file_options, db_options);
  }

  FileOptions OptimizeForManifestWrite(const FileOptions &file_options) const override {
    return fs->OptimizeForManifestWrite(file_options);
  }

  FileOptions OptimizeForCompactionTableWrite(const FileOptions &file_options, const ImmutableDBOptions &db_options) const override {
    return fs->OptimizeForCompactionTableWrite(file_options, db_options);
  }

  FileOptions OptimizeForCompactionTableRead(const FileOptions &file_options, const ImmutableDBOptions &db_options) const override {
    return fs->OptimizeForCompactionTableRead(file_options, db_options);
  }

  FileOptions OptimizeForBlobFileRead(const FileOptions &file_options, const ImmutableDBOptions &db_options) const override {
    return fs->OptimizeForBlobFileRead(file_options, db_options);
  }

  IOStatus GetFreeSpace(const std::string &path, const IOOptions &options, uint64_t *diskfree, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->GetFreeSpace(path, options, diskfree, dbg);
  }

  IOStatus IsDirectory(const std::string &path, const IOOptions &options, bool *is_dir, IODebugContext *dbg) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->IsDirectory(path, options, is_dir, dbg);
  }

  IOStatus Poll(std::vector<void *> &io_handles, size_t min_completions) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->Poll(io_handles, min_completions);
  }

  IOStatus AbortIO(std::vector<void *> &io_handles) override {
    if (suspended) return IOStatus::Busy("File system is suspended");

    return fs->AbortIO(io_handles);
  }

  void DiscardCacheForDirectory(const std::string &path) override {
    fs->DiscardCacheForDirectory(path);
  }

  void SupportedOps(int64_t &supported_ops) override {
    fs->SupportedOps(supported_ops);
  }
};

namespace {

static const rocksdb_options_t rocksdb__default_options = {
  .version = 0,
  .read_only = false,
  .create_if_missing = false,
  .create_missing_column_families = false,
  .max_background_jobs = 2,
  .bytes_per_sync = 0,
};

static const rocksdb_column_family_options_t rocksdb__default_column_family_options = {
  .version = 1,
  .compaction_style = rocksdb_compaction_style_level,
  .enable_blob_files = false,
  .min_blob_size = 0,
  .blob_file_size = 1 << 28,
  .enable_blob_garbage_collection = false,
  .table_block_size = 4 * 1024,
  .table_cache_index_and_filter_blocks = false,
  .table_format_version = 6,
  .optimize_filters_for_memory = false,
  .no_block_cache = false,
};

static const rocksdb_read_options_t rocksdb__default_read_options = {
  .version = 0,
  .snapshot = nullptr,
};

static const rocksdb_write_options_t rocksdb__default_write_options = {
  .version = 0,
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

} // namespace

extern "C" int
rocksdb_init(uv_loop_t *loop, rocksdb_t *db) {
  db->loop = loop;
  db->handle = nullptr;
  db->close = NULL;
  db->reqs = NULL;

  return 0;
}

namespace {

static inline int
rocksdb__close_maybe(rocksdb_t *db);

} // namespace

namespace {

static inline void
rocksdb__add_req(rocksdb_t *db, rocksdb_req_t *req) {
  auto ring = &req->reqs;

  intrusive_ring_init(ring);

  if (db->reqs == NULL) {
    db->reqs = ring;
  } else {
    db->reqs = intrusive_ring_link(ring, db->reqs);
  }
}

template <typename T>
static inline void
rocksdb__add_req(T *req) {
  rocksdb__add_req(req->req.db, &req->req);
}

static inline void
rocksdb__remove_req(rocksdb_t *db, rocksdb_req_t *req) {
  auto ring = &req->reqs;

  db->reqs = intrusive_ring_remove(ring);

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

  rocksdb__remove_req(req);

  auto error = req->error;

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

    options.compaction_style = rocksdb__option<&rocksdb_column_family_options_t::compaction_style, CompactionStyle>(
      &column_family.options, 0
    );

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

    table_options.block_size = rocksdb__option<&rocksdb_column_family_options_t::table_block_size, uint64_t>(
      &column_family.options, 0
    );

    table_options.cache_index_and_filter_blocks = rocksdb__option<&rocksdb_column_family_options_t::table_cache_index_and_filter_blocks, bool>(
      &column_family.options, 0
    );

    table_options.format_version = rocksdb__option<&rocksdb_column_family_options_t::table_format_version, uint32_t>(
      &column_family.options, 0
    );

    table_options.optimize_filters_for_memory = rocksdb__option<&rocksdb_column_family_options_t::optimize_filters_for_memory, bool>(
      &column_family.options, 1
    );

    table_options.filter_policy = std::shared_ptr<const FilterPolicy>(NewBloomFilterPolicy(10.0));

    table_options.no_block_cache = rocksdb__option<&rocksdb_column_family_options_t::no_block_cache, bool>(
      &column_family.options, 1
    );

    options.table_factory = std::shared_ptr<TableFactory>(NewBlockBasedTableFactory(table_options));

    column_families[i] = ColumnFamilyDescriptor(column_family.name, options);
  }

  auto env = NewCompositeEnv(std::make_shared<rocksdb_file_system_t>());

  options.env = env.release();

  auto handles = std::vector<ColumnFamilyHandle *>(req->len);

  Status status;

  auto db = reinterpret_cast<DB **>(&req->req.db->handle);

  if (read_only) {
    status = DB::OpenForReadOnly(options, req->path, column_families, &handles, db);
  } else {
    status = DB::Open(options, req->path, column_families, &handles, db);
  }

  for (size_t i = 0, n = req->len; i < n; i++) {
    req->handles[i] = reinterpret_cast<rocksdb_column_family_t *>(handles[i]);
  }

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

} // namespace

extern "C" int
rocksdb_open(rocksdb_t *db, rocksdb_open_t *req, const char *path, const rocksdb_options_t *options, const rocksdb_column_family_descriptor_t column_families[], rocksdb_column_family_t *handles[], size_t len, rocksdb_open_cb cb) {
  req->req.db = db;
  req->req.cancelable = true;
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

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_close(uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_close_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  CancelAllBackgroundWork(db, true);

  auto status = db->Close();

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }

  auto env = db->GetEnv();

  delete db;
  delete env;
}

} // namespace

extern "C" int
rocksdb_close(rocksdb_t *db, rocksdb_close_t *req, rocksdb_close_cb cb) {
  req->req.db = db;
  req->req.cancelable = false;
  req->error = nullptr;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  db->close = req;

  intrusive_ring_for_each(next, db->reqs) {
    auto req = intrusive_entry(next, rocksdb_req_t, reqs);

    if (req->cancelable) {
      uv_cancel(reinterpret_cast<uv_req_t *>(&req->worker));
    }
  }

  return rocksdb__close_maybe(db);
}

namespace {

static inline int
rocksdb__close_maybe(rocksdb_t *db) {
  rocksdb_close_t *req = db->close;

  if (db->reqs || req == NULL) return 0;

  if (db->handle == NULL) {
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

  rocksdb__remove_req(req);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_suspend(uv_work_t *handle) {
  int err;

  auto req = reinterpret_cast<rocksdb_suspend_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  auto status = db->PauseBackgroundWork();

  auto fs = reinterpret_cast<rocksdb_file_system_t *>(db->GetFileSystem());

  fs->suspend();

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

} // namespace

extern "C" int
rocksdb_suspend(rocksdb_t *db, rocksdb_suspend_t *req, rocksdb_suspend_cb cb) {
  req->req.db = db;
  req->req.cancelable = true;
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

  rocksdb__remove_req(req);

  auto error = req->error;

  req->cb(req, status);

  if (error) free(error);
}

static void
rocksdb__on_resume(uv_work_t *handle) {
  int err;

  auto req = reinterpret_cast<rocksdb_resume_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->req.db->handle);

  auto fs = reinterpret_cast<rocksdb_file_system_t *>(db->GetFileSystem());

  fs->resume();

  auto status = db->ContinueBackgroundWork();

  if (status.ok()) {
    req->error = nullptr;
  } else {
    req->error = strdup(status.getState());
  }
}

} // namespace

extern "C" int
rocksdb_resume(rocksdb_t *db, rocksdb_resume_t *req, rocksdb_resume_cb cb) {
  req->req.db = db;
  req->req.cancelable = true;
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

  if (req->reverse) {
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
  if (req->reverse) {
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

  auto snapshot = rocksdb__option<&rocksdb_read_options_t::snapshot, rocksdb_snapshot_t *>(
    &req->options, 0
  );

  ReadOptions options;

  if (snapshot) options.snapshot = reinterpret_cast<const Snapshot *>(snapshot->handle);

  auto column_family = reinterpret_cast<ColumnFamilyHandle *>(req->column_family);

  if (req->reverse) {
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
    req->values[i] = rocksdb__slice_copy(iterator->value());

    rocksdb__iterator_next(iterator, req);
  }
}

template <typename T>
static inline void
rocksdb__iterator_refresh(Iterator *iterator, T *req) {
  auto snapshot = rocksdb__option<&rocksdb_read_options_t::snapshot, rocksdb_snapshot_t *>(
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
rocksdb__on_after_iterator(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  rocksdb__remove_req(req);

  auto error = req->error;

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
rocksdb_iterator_open(rocksdb_t *db, rocksdb_iterator_t *req, rocksdb_column_family_t *column_family, rocksdb_range_t range, bool reverse, const rocksdb_read_options_t *options, rocksdb_iterator_cb cb) {
  req->req.db = db;
  req->req.cancelable = true;
  req->options = options ? *options : rocksdb__default_read_options;
  req->column_family = column_family;
  req->range = range;
  req->reverse = reverse;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_iterator_open, rocksdb__on_after_iterator);
}

namespace {

static void
rocksdb__on_iterator_close(uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_iterator_t *>(handle->data);

  auto iterator = reinterpret_cast<Iterator *>(req->handle);

  delete iterator;
}

} // namespace

extern "C" int
rocksdb_iterator_close(rocksdb_iterator_t *req, rocksdb_iterator_cb cb) {
  req->req.cancelable = false;
  req->cb = cb;

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_iterator_close, rocksdb__on_after_iterator);
}

namespace {

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
rocksdb_iterator_refresh(rocksdb_iterator_t *req, rocksdb_range_t range, bool reverse, const rocksdb_read_options_t *options, rocksdb_iterator_cb cb) {
  req->options = options ? *options : rocksdb__default_read_options;
  req->range = range;
  req->reverse = reverse;
  req->cb = cb;

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_iterator_close, rocksdb__on_after_iterator);
}

namespace {

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
  req->cb = cb;
  req->keys = keys;
  req->values = values;
  req->len = 0;
  req->capacity = capacity;

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_iterator_read, rocksdb__on_after_iterator);
}

namespace {

static void
rocksdb__on_after_read(uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_read_batch_t *>(handle->data);

  rocksdb__remove_req(req);

  req->cb(req, status);

  for (size_t i = 0, n = req->len; i < n; i++) {
    if (req->errors[i]) free(req->errors[i]);
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

    if (snapshot) options.snapshot = reinterpret_cast<const Snapshot *>(snapshot->handle);

    db->MultiGet(options, req->len, column_families.data(), keys.data(), values.data(), statuses.data());

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
rocksdb_read(rocksdb_t *db, rocksdb_read_batch_t *req, rocksdb_read_t *reads, char **errors, size_t len, const rocksdb_read_options_t *options, rocksdb_read_batch_cb cb) {
  req->req.db = db;
  req->req.cancelable = true;
  req->options = options ? *options : rocksdb__default_read_options;
  req->reads = reads;
  req->errors = errors;
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
  req->req.db = db;
  req->req.cancelable = true;
  req->options = options ? *options : rocksdb__default_write_options;
  req->writes = writes;
  req->error = nullptr;
  req->len = len;
  req->cb = cb;

  req->req.worker.data = static_cast<void *>(req);

  rocksdb__add_req(req);

  return uv_queue_work(req->req.db->loop, &req->req.worker, rocksdb__on_write, rocksdb__on_after_write);
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
