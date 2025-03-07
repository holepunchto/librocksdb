#include <memory>
#include <set>
#include <thread>

#include <rocksdb/convenience.h>
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/io_status.h>

#undef DeleteFile
#undef GetCurrentTime
#undef GetFreeSpace
#undef LoadLibrary

using namespace rocksdb;

typedef struct rocksdb_file_system_s rocksdb_file_system_t;

struct rocksdb_file_system_s : std::enable_shared_from_this<rocksdb_file_system_s>, FileSystem {
private:
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

  struct rocksdb_sequential_file_s : FSSequentialFile {
    std::shared_ptr<rocksdb_file_system_t> fs;
    std::unique_ptr<FSSequentialFile> file;

    rocksdb_sequential_file_s(std::shared_ptr<rocksdb_file_system_s> &&fs, std::unique_ptr<FSSequentialFile> &&file)
        : fs(std::move(fs)),
          file(std::move(file)) {}

  private:
    bool use_direct_io() const override {
      return file->use_direct_io();
    }

    IOStatus Read(size_t n, const IOOptions &options, Slice *result, char *scratch, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Read(n, options, result, scratch, dbg);
      });
    }

    IOStatus Skip(uint64_t n) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Skip(n);
      });
    }

    size_t GetRequiredBufferAlignment() const override {
      return file->GetRequiredBufferAlignment();
    }

    IOStatus InvalidateCache(size_t offset, size_t length) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->InvalidateCache(offset, length);
      });
    }

    IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions &options, Slice *result, char *scratch, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->PositionedRead(offset, n, options, result, scratch, dbg);
      });
    }

    Temperature GetTemperature() const override {
      return file->GetTemperature();
    }
  };

  struct rocksdb_random_access_file_s : FSRandomAccessFile {
    std::shared_ptr<rocksdb_file_system_t> fs;
    std::unique_ptr<FSRandomAccessFile> file;

    rocksdb_random_access_file_s(std::shared_ptr<rocksdb_file_system_s> &&fs, std::unique_ptr<FSRandomAccessFile> &&file)
        : fs(std::move(fs)),
          file(std::move(file)) {}

  private:
    bool use_direct_io() const override {
      return file->use_direct_io();
    }

    IOStatus Read(uint64_t offset, size_t n, const IOOptions &options, Slice *result, char *scratch, IODebugContext *dbg) const override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Read(offset, n, options, result, scratch, dbg);
      });
    }

    IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Prefetch(offset, n, options, dbg);
      });
    }

    IOStatus MultiRead(FSReadRequest *reqs, size_t num_reqs, const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->MultiRead(reqs, num_reqs, options, dbg);
      });
    }

    size_t GetUniqueId(char *id, size_t max_size) const override {
      return file->GetUniqueId(id, max_size);
    }

    void Hint(AccessPattern pattern) override {
      return file->Hint(pattern);
    }

    size_t GetRequiredBufferAlignment() const override {
      return file->GetRequiredBufferAlignment();
    }

    IOStatus InvalidateCache(size_t offset, size_t length) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->InvalidateCache(offset, length);
      });
    }

    IOStatus ReadAsync(FSReadRequest &req, const IOOptions &options, std::function<void(FSReadRequest &, void *)> cb, void *cb_arg, void **io_handle, IOHandleDeleter *del_fn, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->ReadAsync(req, options, cb, cb_arg, io_handle, del_fn, dbg);
      });
    }

    Temperature GetTemperature() const override {
      return file->GetTemperature();
    }
  };

  struct rocksdb_writable_file_s : FSWritableFile {
    std::shared_ptr<rocksdb_file_system_t> fs;
    std::unique_ptr<FSWritableFile> file;

    rocksdb_writable_file_s(std::shared_ptr<rocksdb_file_system_s> &&fs, std::unique_ptr<FSWritableFile> &&file)
        : fs(std::move(fs)),
          file(std::move(file)) {}

  private:
    bool use_direct_io() const override {
      return file->use_direct_io();
    }

    IOStatus Append(const Slice &data, const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Append(data, options, dbg);
      });
    }

    IOStatus Append(const Slice &data, const IOOptions &options, const DataVerificationInfo &verification_info, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Append(data, options, dbg);
      });
    }

    IOStatus PositionedAppend(const Slice &data, uint64_t offset, const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->PositionedAppend(data, offset, options, dbg);
      });
    }

    IOStatus PositionedAppend(const Slice &data, uint64_t offset, const IOOptions &options, const DataVerificationInfo &verification_info, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->PositionedAppend(data, offset, options, dbg);
      });
    }

    IOStatus Truncate(uint64_t size, const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Truncate(size, options, dbg);
      });
    }

    IOStatus Close(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Close(options, dbg);
      });
    }

    IOStatus Flush(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Flush(options, dbg);
      });
    }

    IOStatus Sync(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Sync(options, dbg);
      });
    }

    IOStatus Fsync(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Fsync(options, dbg);
      });
    }

    bool IsSyncThreadSafe() const override {
      return file->IsSyncThreadSafe();
    }

    size_t GetRequiredBufferAlignment() const override {
      return file->GetRequiredBufferAlignment();
    }

    void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
      file->SetWriteLifeTimeHint(hint);
    }

    Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
      return file->GetWriteLifeTimeHint();
    }

    void SetIOPriority(Env::IOPriority pri) override {
      file->SetIOPriority(pri);
    }

    Env::IOPriority GetIOPriority() override {
      return file->GetIOPriority();
    }

    uint64_t GetFileSize(const IOOptions &options, IODebugContext *dbg) override {
      return file->GetFileSize(options, dbg);
    }

    void SetPreallocationBlockSize(size_t size) override {
      file->SetPreallocationBlockSize(size);
    }

    void GetPreallocationStatus(size_t *block_size, size_t *last_allocated_block) override {
      file->GetPreallocationStatus(block_size, last_allocated_block);
    }

    size_t GetUniqueId(char *id, size_t max_size) const override {
      return file->GetUniqueId(id, max_size);
    }

    IOStatus InvalidateCache(size_t offset, size_t length) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->InvalidateCache(offset, length);
      });
    }

    IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->RangeSync(offset, nbytes, options, dbg);
      });
    }

    void PrepareWrite(size_t offset, size_t len, const IOOptions &options, IODebugContext *dbg) override {
      return file->PrepareWrite(offset, len, options, dbg);
    }

    IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Allocate(offset, len, options, dbg);
      });
    }
  };

  struct rocksdb_random_rw_file_s : FSRandomRWFile {
    std::shared_ptr<rocksdb_file_system_t> fs;
    std::unique_ptr<FSRandomRWFile> file;

    rocksdb_random_rw_file_s(std::shared_ptr<rocksdb_file_system_s> &&fs, std::unique_ptr<FSRandomRWFile> &&file)
        : fs(std::move(fs)),
          file(std::move(file)) {}

  private:
    bool use_direct_io() const override {
      return file->use_direct_io();
    }

    size_t GetRequiredBufferAlignment() const override {
      return file->GetRequiredBufferAlignment();
    }

    IOStatus Write(uint64_t offset, const Slice &data, const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Write(offset, data, options, dbg);
      });
    }

    IOStatus Read(uint64_t offset, size_t n, const IOOptions &options, Slice *result, char *scratch, IODebugContext *dbg) const override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Read(offset, n, options, result, scratch, dbg);
      });
    }

    IOStatus Flush(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Flush(options, dbg);
      });
    }

    IOStatus Sync(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Sync(options, dbg);
      });
    }

    IOStatus Fsync(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Fsync(options, dbg);
      });
    }

    IOStatus Close(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return file->Close(options, dbg);
      });
    }

    Temperature GetTemperature() const override {
      return file->GetTemperature();
    }
  };

  struct rocksdb_directory_s : FSDirectory {
    std::shared_ptr<rocksdb_file_system_t> fs;
    std::unique_ptr<FSDirectory> dir;

    rocksdb_directory_s(std::shared_ptr<rocksdb_file_system_s> &&fs, std::unique_ptr<FSDirectory> &&dir)
        : fs(std::move(fs)),
          dir(std::move(dir)) {}

  private:
    IOStatus Fsync(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return dir->Fsync(options, dbg);
      });
    }

    IOStatus FsyncWithDirOptions(const IOOptions &options, IODebugContext *dbg, const DirFsyncOptions &dir_fsync_options) override {
      return fs->unless_suspended<IOStatus>([&] {
        return dir->FsyncWithDirOptions(options, dbg, dir_fsync_options);
      });
    }

    IOStatus Close(const IOOptions &options, IODebugContext *dbg) override {
      return fs->unless_suspended<IOStatus>([&] {
        return dir->Close(options, dbg);
      });
    }

    size_t GetUniqueId(char *id, size_t max_size) const override {
      return dir->GetUniqueId(id, max_size);
    }
  };

public:
  std::shared_ptr<FileSystem> fs;
  std::atomic<bool> suspended;
  std::atomic<int> pending;
  std::mutex mutex;
  std::condition_variable drained;
  std::set<rocksdb_lock_s *> locks;

  rocksdb_file_system_s() : fs(FileSystem::Default()), suspended(false), pending(0), mutex(), drained(), locks() {}

  inline void
  suspend() {
    auto expected = false;

    if (suspended.compare_exchange_strong(expected, true)) {
      std::unique_lock lock(mutex);

      drained.wait(lock, [this] {
        return pending.load(std::memory_order_acquire) == 0;
      });

      for (const auto &lock : locks) {
        auto status = lock->release(fs);

        assert(status.ok());
      }
    }
  }

  inline auto
  resume() {
    if (suspended.load()) {
      auto reacquired = std::set<rocksdb_lock_s *>();

      for (const auto &lock : locks) {
        auto status = lock->acquire(fs);

        if (status.ok()) reacquired.insert(lock);
        else {
          for (const auto &lock : reacquired) {
            auto status = lock->release(fs);

            assert(status.ok());
          }

          return Status(status);
        }
      }

      suspended.store(true);
    }

    return Status::OK();
  }

private:
  template <typename T>
  inline T
  unless_suspended(const std::function<T()> &fn) {
    if (suspended) return IOStatus::Busy("File system is suspended");

    pending.fetch_add(1, std::memory_order_acquire);

    T result = fn();

    pending.fetch_sub(1, std::memory_order_release);

    if (pending.load(std::memory_order_acquire) == 0) {
      std::scoped_lock lock(mutex);

      drained.notify_all();
    }

    return result;
  }

private:
  const char *Name() const override { return "RocksFS"; }

  IOStatus LockFile(const std::string &fname, const IOOptions &options, FileLock **result, IODebugContext *dbg) override {
    FileLock *lock;

    auto status = fs->LockFile(fname, options, &lock, dbg);

    if (status.ok()) {
      auto wrapper = new rocksdb_lock_s(fname, lock);

      locks.insert(wrapper);

      *result = wrapper;
    } else {
      *result = nullptr;
    }

    return status;
  }

  IOStatus UnlockFile(FileLock *lock, const IOOptions &options, IODebugContext *dbg) override {
    auto wrapper = reinterpret_cast<rocksdb_lock_s *>(lock);

    if (wrapper->lock == nullptr) return IOStatus::OK();

    auto status = fs->UnlockFile(wrapper->lock, options, dbg);

    if (status.ok()) locks.erase(wrapper);

    return status;
  }

  Status RegisterDbPaths(const std::vector<std::string> &paths) override {
    return unless_suspended<Status>([&] {
      return fs->RegisterDbPaths(paths);
    });
  }

  Status UnregisterDbPaths(const std::vector<std::string> &paths) override {
    return unless_suspended<Status>([&] {
      return fs->UnregisterDbPaths(paths);
    });
  }

  IOStatus NewSequentialFile(const std::string &fname, const FileOptions &options, std::unique_ptr<FSSequentialFile> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      std::unique_ptr<FSSequentialFile> file;

      auto status = fs->NewSequentialFile(fname, options, &file, dbg);

      if (status.ok()) {
        auto wrapper = std::make_unique<rocksdb_sequential_file_s>(shared_from_this(), std::move(file));

        *result = std::move(wrapper);
      } else {
        *result = nullptr;
      }

      return status;
    });
  }

  IOStatus NewRandomAccessFile(const std::string &fname, const FileOptions &options, std::unique_ptr<FSRandomAccessFile> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      std::unique_ptr<FSRandomAccessFile> file;

      auto status = fs->NewRandomAccessFile(fname, options, &file, dbg);

      if (status.ok()) {
        auto wrapper = std::make_unique<rocksdb_random_access_file_s>(shared_from_this(), std::move(file));

        *result = std::move(wrapper);
      } else {
        *result = nullptr;
      }

      return status;
    });
  }

  IOStatus NewWritableFile(const std::string &fname, const FileOptions &options, std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      std::unique_ptr<FSWritableFile> file;

      auto status = fs->NewWritableFile(fname, options, &file, dbg);

      if (status.ok()) {
        auto wrapper = std::make_unique<rocksdb_writable_file_s>(shared_from_this(), std::move(file));

        *result = std::move(wrapper);
      } else {
        *result = nullptr;
      }

      return status;
    });
  }

  IOStatus ReopenWritableFile(const std::string &fname, const FileOptions &options, std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      std::unique_ptr<FSWritableFile> file;

      auto status = fs->ReopenWritableFile(fname, options, &file, dbg);

      if (status.ok()) {
        auto wrapper = std::make_unique<rocksdb_writable_file_s>(shared_from_this(), std::move(file));

        *result = std::move(wrapper);
      } else {
        *result = nullptr;
      }

      return status;
    });
  }

  IOStatus ReuseWritableFile(const std::string &fname, const std::string &old_fname, const FileOptions &options, std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      std::unique_ptr<FSWritableFile> file;

      auto status = fs->ReuseWritableFile(fname, old_fname, options, &file, dbg);

      if (status.ok()) {
        auto wrapper = std::make_unique<rocksdb_writable_file_s>(shared_from_this(), std::move(file));

        *result = std::move(wrapper);
      } else {
        *result = nullptr;
      }

      return status;
    });
  }

  IOStatus NewRandomRWFile(const std::string &fname, const FileOptions &options, std::unique_ptr<FSRandomRWFile> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      std::unique_ptr<FSRandomRWFile> file;

      auto status = fs->NewRandomRWFile(fname, options, &file, dbg);

      if (status.ok()) {
        auto wrapper = std::make_unique<rocksdb_random_rw_file_s>(shared_from_this(), std::move(file));

        *result = std::move(wrapper);
      } else {
        *result = nullptr;
      }

      return status;
    });
  }

  IOStatus NewDirectory(const std::string &name, const IOOptions &options, std::unique_ptr<FSDirectory> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      std::unique_ptr<FSDirectory> dir;

      auto status = fs->NewDirectory(name, options, &dir, dbg);

      if (status.ok()) {
        auto wrapper = std::make_unique<rocksdb_directory_s>(shared_from_this(), std::move(dir));

        *result = std::move(wrapper);
      } else {
        *result = nullptr;
      }

      return status;
    });
  }

  IOStatus FileExists(const std::string &fname, const IOOptions &options, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->FileExists(fname, options, dbg);
    });
  }

  IOStatus GetChildren(const std::string &dir, const IOOptions &options, std::vector<std::string> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->GetChildren(dir, options, result, dbg);
    });
  }

  IOStatus GetChildrenFileAttributes(const std::string &dir, const IOOptions &options, std::vector<FileAttributes> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->GetChildrenFileAttributes(dir, options, result, dbg);
    });
  }

  IOStatus DeleteFile(const std::string &fname, const IOOptions &options, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->DeleteFile(fname, options, dbg);
    });
  }

  IOStatus Truncate(const std::string &fname, size_t size, const IOOptions &options, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->Truncate(fname, size, options, dbg);
    });
  }

  IOStatus CreateDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->CreateDir(dirname, options, dbg);
    });
  }

  IOStatus CreateDirIfMissing(const std::string &dirname, const IOOptions &options, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->CreateDirIfMissing(dirname, options, dbg);
    });
  }

  IOStatus DeleteDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->DeleteDir(dirname, options, dbg);
    });
  }

  IOStatus GetFileSize(const std::string &fname, const IOOptions &options, uint64_t *file_size, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->GetFileSize(fname, options, file_size, dbg);
    });
  }

  IOStatus GetFileModificationTime(const std::string &fname, const IOOptions &options, uint64_t *file_mtime, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->GetFileModificationTime(fname, options, file_mtime, dbg);
    });
  }

  IOStatus RenameFile(const std::string &src, const std::string &target, const IOOptions &options, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->RenameFile(src, target, options, dbg);
    });
  }

  IOStatus LinkFile(const std::string &src, const std::string &target, const IOOptions &options, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->LinkFile(src, target, options, dbg);
    });
  }

  IOStatus NumFileLinks(const std::string &fname, const IOOptions &options, uint64_t *count, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->NumFileLinks(fname, options, count, dbg);
    });
  }

  IOStatus AreFilesSame(const std::string &first, const std::string &second, const IOOptions &options, bool *res, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->AreFilesSame(first, second, options, res, dbg);
    });
  }

  IOStatus GetTestDirectory(const IOOptions &options, std::string *path, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->GetTestDirectory(options, path, dbg);
    });
  }

  IOStatus NewLogger(const std::string &fname, const IOOptions &options, std::shared_ptr<Logger> *result, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->NewLogger(fname, options, result, dbg);
    });
  }

  IOStatus GetAbsolutePath(const std::string &db_path, const IOOptions &options, std::string *output_path, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->GetAbsolutePath(db_path, options, output_path, dbg);
    });
  }

  void SanitizeFileOptions(FileOptions *file_options) const override {
    return fs->SanitizeFileOptions(file_options);
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
    return unless_suspended<IOStatus>([&] {
      return fs->GetFreeSpace(path, options, diskfree, dbg);
    });
  }

  IOStatus IsDirectory(const std::string &path, const IOOptions &options, bool *is_dir, IODebugContext *dbg) override {
    return unless_suspended<IOStatus>([&] {
      return fs->IsDirectory(path, options, is_dir, dbg);
    });
  }

  IOStatus Poll(std::vector<void *> &io_handles, size_t min_completions) override {
    return unless_suspended<IOStatus>([&] {
      return fs->Poll(io_handles, min_completions);
    });
  }

  IOStatus AbortIO(std::vector<void *> &io_handles) override {
    return unless_suspended<IOStatus>([&] {
      return fs->AbortIO(io_handles);
    });
  }

  void DiscardCacheForDirectory(const std::string &path) override {
    fs->DiscardCacheForDirectory(path);
  }

  void SupportedOps(int64_t &supported_ops) override {
    fs->SupportedOps(supported_ops);
  }
};
