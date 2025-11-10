#pragma once

#include <fcntl.h>

namespace {

static void
rocksdb__lock(int fd) {
  flock(fd, LOCK_EX);
}

static void
rocksdb__unlock(int fd) {
  flock(fd, LOCK_UN);
}

} // namespace
