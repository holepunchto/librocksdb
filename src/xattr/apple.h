#pragma once

#include <uv.h>

#include <sys/xattr.h>

namespace {

static int
rocksdb__get_xattr(int fd, const char *name, uv_buf_t *value) {
  int res = fgetxattr(fd, name, NULL, 0, 0, 0);

  if (res == -1) {
    if (errno == ENOATTR) return UV_ENODATA;

    return uv_translate_sys_error(errno);
  }

  *value = uv_buf_init(static_cast<char *>(malloc(res)), res);

  res = fgetxattr(fd, name, value->base, value->len, 0, 0);

  return res == -1 ? uv_translate_sys_error(errno) : 0;
}

static int
rocksdb__set_xattr(int fd, const char *name, const uv_buf_t *value) {
  int res = fsetxattr(fd, name, value->base, value->len, 0, 0);

  return res == -1 ? uv_translate_sys_error(errno) : 0;
}

} // namespace
