#include <rocksdb/db.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

using namespace rocksdb;

static const rocksdb_options_t rocksdb__default_options = {};

static inline int
rocksdb__batch_resize (rocksdb_batch_t *batch, size_t capacity, rocksdb_batch_t **result) {
  batch = static_cast<rocksdb_batch_t *>(realloc(batch, sizeof(rocksdb_batch_t) + 2 * capacity * sizeof(rocksdb_slice_t) + capacity * sizeof(char *)));

  if (batch == nullptr) return -1;

  batch->len = 0;
  batch->capacity = capacity;

  batch->keys = reinterpret_cast<rocksdb_slice_t *>(batch->buffer);
  batch->values = reinterpret_cast<rocksdb_slice_t *>(batch->buffer + capacity);

  batch->errors = reinterpret_cast<char **>(batch->buffer + 2 * capacity);

  return 0;
}

template <typename T>
static inline void
rocksdb__on_status (T *req) {
  req->db->on_status(req->db, req->error ? -1 : 0);

  if (req->error) free(req->error);
}

int
rocksdb_init (uv_loop_t *loop, rocksdb_t *db) {
  memset(&db->worker, 0, sizeof(uv_work_t));

  db->loop = loop;
  db->handle = nullptr;

  db->read = nullptr;
  db->write = nullptr;

  db->on_status = nullptr;
  db->on_batch = nullptr;

  return 0;
}

static void
rocksdb__on_open (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_open_t *>(handle->data);

  Options options;

  options.create_if_missing = req->options.create_if_missing;

  Status status;

  if (req->options.read_only) {
    status = DB::OpenForReadOnly(options, req->path, reinterpret_cast<DB **>(&req->db->handle));
  } else {
    status = DB::Open(options, req->path, reinterpret_cast<DB **>(&req->db->handle));
  }

  req->error = status.ok() ? nullptr : strdup(status.getState());
}

static void
rocksdb__on_after_open (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_open_t *>(handle->data);

  rocksdb__on_status(req);
}

int
rocksdb_open (rocksdb_t *db, rocksdb_open_t *req, const char *path, const rocksdb_options_t *options, rocksdb_status_cb cb) {
  req->db = db;
  req->options = options ? *options : rocksdb__default_options;
  req->error = nullptr;

  strcpy(req->path, path);

  db->worker.data = static_cast<void *>(req);

  db->on_status = cb;

  return uv_queue_work(db->loop, &db->worker, rocksdb__on_open, rocksdb__on_after_open);
}

static void
rocksdb__on_close (uv_work_t *handle) {
  auto req = reinterpret_cast<rocksdb_close_t *>(handle->data);

  auto db = reinterpret_cast<DB *>(req->db->handle);

  auto status = db->Close();

  delete db;

  req->error = status.ok() ? nullptr : strdup(status.getState());
}

static void
rocksdb__on_after_close (uv_work_t *handle, int status) {
  auto req = reinterpret_cast<rocksdb_close_t *>(handle->data);

  rocksdb__on_status(req);

  auto db = req->db;

  if (db->read) free(db->read);
  if (db->write) free(db->write);
}

int
rocksdb_close (rocksdb_t *db, rocksdb_close_t *req, rocksdb_status_cb cb) {
  req->db = db;
  req->error = nullptr;

  db->worker.data = static_cast<void *>(req);

  db->on_status = cb;

  return uv_queue_work(db->loop, &db->worker, rocksdb__on_close, rocksdb__on_after_close);
}
