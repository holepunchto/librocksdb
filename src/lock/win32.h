#pragma once

#include <uv.h>

namespace {

static void
rocksdb__lock(int fd) {
  size_t length = SIZE_MAX;

  OVERLAPPED data = {
    .hEvent = 0,
  };

  LockFileEx(fd, LOCKFILE_EXCLUSIVE_LOCK, 0, length, length >> 32, &data);
}

static void
rocksdb__unlock(int fd) {
  size_t length = SIZE_MAX;

  OVERLAPPED data = {
    .hEvent = 0,
  };

  UnlockFileEx(fd, 0, length, length >> 32, &data);
}

} // namespace
