#pragma once

#include <uv.h>

namespace {

static void
rocksdb__lock(int fd) {
  HANDLE handle = uv_get_osfhandle(fd);

  size_t length = SIZE_MAX;

  OVERLAPPED data = {
    .hEvent = 0,
  };

  LockFileEx(handle, LOCKFILE_EXCLUSIVE_LOCK, 0, length, length >> 32, &data);
}

static void
rocksdb__unlock(int fd) {
  HANDLE handle = uv_get_osfhandle(fd);

  size_t length = SIZE_MAX;

  OVERLAPPED data = {
    .hEvent = 0,
  };

  UnlockFileEx(handle, 0, length, length >> 32, &data);
}

} // namespace
