#pragma once

#include <fcntl.h>

namespace {

static void
rocksdb__lock(int fd) {
  struct flock data = {
    .l_pid = 0,
    .l_type = F_WRLCK,
    .l_whence = SEEK_SET,
  };

  fcntl(fd, F_OFD_SETLKW, &data);
}

static void
rocksdb__unlock(int fd) {
  struct flock data = {
    .l_pid = 0,
    .l_type = F_UNLCK,
    .l_whence = SEEK_SET,
  };

  fcntl(fd, F_OFD_SETLK, &data);
}

} // namespace
