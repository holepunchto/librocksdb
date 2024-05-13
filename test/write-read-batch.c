#include <assert.h>
#include <stdbool.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uv_loop_t *loop;

static rocksdb_t db;

static rocksdb_open_t open_req;
static rocksdb_close_t close_req;

static rocksdb_batch_t *batch;

static void
on_close (rocksdb_close_t *req, int status) {
  assert(status == 0);

  rocksdb_batch_destroy(batch);
}

static void
on_read (rocksdb_batch_t *req, int status) {
  int e;

  assert(status == 0);

  assert(batch->errors[0] == NULL);

  assert(strcmp(batch->values[0].data, "world") == 0);

  rocksdb_slice_destroy(&batch->values[0]);

  e = rocksdb_close(&db, &close_req, on_close);
  assert(e == 0);
}

static void
on_write (rocksdb_batch_t *req, int status) {
  int e;

  assert(status == 0);

  batch->len = 1;
  batch->keys[0] = rocksdb_slice_init("hello", 5);
  batch->values[0] = rocksdb_slice_init(NULL, 0);

  e = rocksdb_batch_read(batch, on_read);
  assert(e == 0);
}

static void
on_open (rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0);

  batch->len = 1;
  batch->keys[0] = rocksdb_slice_init("hello", 5);
  batch->values[0] = rocksdb_slice_init("world", 6);

  e = rocksdb_batch_write(batch, on_write);
  assert(e == 0);
}

int
main () {
  int e;

  loop = uv_default_loop();

  e = rocksdb_init(loop, &db);
  assert(e == 0);

  rocksdb_options_t options = {
    .create_if_missing = true,
  };

  e = rocksdb_batch_init(&db, NULL, 8, &batch);
  assert(e == 0);

  e = rocksdb_open(&db, &open_req, "test/fixtures/test.db", &options, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
