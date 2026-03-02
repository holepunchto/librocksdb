#pragma once

#include <uv.h>

#include <ntifs.h>

namespace {

static int
rocksdb__get_xattr(int fd, const char *name, uv_buf_t *value) {
  HANDLE handle = uv_get_osfhandle(fd);
  HANDLE stream_handle;

  size_t len = strlen(name);

  WCHAR unicode_name[MAX_PATH] = L":";
  MultiByteToWideChar(CP_OEMCP, 0, name, -1, &unicode_name[1], len + 1);

  len = wcslen(unicode_name) * 2;

  UNICODE_STRING object_name = {
    .Length = len,
    .MaximumLength = len + 2,
    .Buffer = unicode_name,
  };

  OBJECT_ATTRIBUTES object_attributes = {
    .Length = sizeof(object_attributes),
    .RootDirectory = handle,
    .ObjectName = &object_name,
  };

  IO_STATUS_BLOCK status;

  NTSTATUS res = NtOpenFile(
    &stream_handle,
    GENERIC_READ,
    &object_attributes,
    &status,
    FILE_SHARE_READ,
    0
  );

  if (res < 0) {
    if (res == STATUS_OBJECT_NAME_NOT_FOUND) return UV_ENODATA;

    return UV_EIO;
  }

  FILE_STANDARD_INFORMATION info;

  res = NtQueryInformationFile(
    stream_handle,
    &status,
    &info,
    sizeof(info),
    FileStandardInformation
  );

  if (res < 0) {
    NtClose(stream_handle);

    return UV_EIO;
  }

  size_t length = info.EndOfFile.QuadPart;

  *value = uv_buf_init(malloc(length), length);

  LARGE_INTEGER offset = {
    .QuadPart = 0,
  };

  res = NtReadFile(
    stream_handle,
    NULL,
    NULL,
    NULL,
    &status,
    value->base,
    value->len,
    &offset,
    NULL
  );

  NtClose(stream_handle);

  if (res < 0) return UV_EIO;

  return 0;
}

static int
rocksdb__set_xattr(uv_os_fd_t fd, const char *name, const uv_buf_t *value) {
  HANDLE handle = uv_get_osfhandle(fd);
  HANDLE stream_handle;

  size_t len = strlen(name);

  WCHAR unicode_name[MAX_PATH] = L":";
  MultiByteToWideChar(CP_OEMCP, 0, name, -1, &unicode_name[1], len + 1);

  len = wcslen(unicode_name) * 2;

  UNICODE_STRING object_name = {
    .Length = len,
    .MaximumLength = len + 2,
    .Buffer = unicode_name,
  };

  OBJECT_ATTRIBUTES object_attributes = {
    .Length = sizeof(object_attributes),
    .RootDirectory = handle,
    .ObjectName = &object_name,
  };

  IO_STATUS_BLOCK status;

  NTSTATUS res = NtCreateFile(
    &stream_handle,
    GENERIC_WRITE,
    &object_attributes,
    &status,
    NULL,
    FILE_ATTRIBUTE_NORMAL,
    0,
    FILE_OVERWRITE_IF,
    0,
    NULL,
    0
  );

  if (res < 0) return UV_EIO;

  LARGE_INTEGER offset = {
    .QuadPart = 0,
  };

  res = NtWriteFile(
    stream_handle,
    NULL,
    NULL,
    NULL,
    &status,
    value->base,
    value->len,
    &offset,
    NULL
  );

  NtClose(stream_handle);

  if (res < 0) return UV_EIO;

  return 0;
}

} // namespace
