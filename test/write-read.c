#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uv_loop_t *loop;

static rocksdb_t db;
static rocksdb_column_family_t *family;

static void
on_close(rocksdb_close_t *req, int status) {
  assert(status == 0);

  assert(req->error == NULL);
}

static void
on_read(rocksdb_read_batch_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->len == 1);

  rocksdb_read_t read = req->reads[0];

  assert(strcmp(read.value.data, "world") == 0);

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  static rocksdb_close_t close;
  e = rocksdb_close(&db, &close, NULL, on_close);
  assert(e == 0);
}

static void
on_write(rocksdb_write_batch_t *req, int status) {
  int e;

  assert(status == 0);

  static rocksdb_read_t read;
  read.type = rocksdb_get;
  read.column_family = family;
  read.key = rocksdb_slice_init("hello", 5);
  read.value = rocksdb_slice_empty();

  static rocksdb_read_batch_t batch;
  e = rocksdb_read(&db, &batch, &read, 1, NULL, on_read);
  assert(e == 0);
}

static void
on_open(rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->error == NULL);

  static rocksdb_write_t write;
  write.type = rocksdb_put;
  write.column_family = family;
  write.key = rocksdb_slice_init("hello", 5);
  write.value = rocksdb_slice_init("world", 6);

  static rocksdb_write_batch_t batch;
  e = rocksdb_write(&db, &batch, &write, 1, NULL, on_write);
  assert(e == 0);
}

int
main() {
  int e;

  loop = uv_default_loop();

  rocksdb_options_t options = {
    .create_if_missing = true,
  };

  rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", NULL);

  static rocksdb_open_t open;
  e = rocksdb_open(loop, &db, &open, "test/fixtures/write-read.db", &options, &descriptor, &family, 1, NULL, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
