#include <assert.h>
#include <stdbool.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uv_loop_t *loop;

static rocksdb_t db;

static rocksdb_open_t open_req;
static rocksdb_close_t close_req;
static rocksdb_read_range_t read_req;

static rocksdb_batch_t *batch;

static rocksdb_slice_t values[4];

static void
on_close (rocksdb_close_t *req, int status) {
  assert(status == 0);

  rocksdb_batch_destroy(batch);
}

static void
on_read (rocksdb_read_range_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->len == 3);
  assert(req->error == NULL);

#define V(i, value) \
  assert(strcmp(values[i].data, value) == 0); \
\
  rocksdb_slice_destroy(&values[i]);

  V(0, "b")
  V(1, "c")
  V(2, "d")
#undef V

  e = rocksdb_close(&db, &close_req, on_close);
  assert(e == 0);
}

static void
on_write (rocksdb_batch_t *req, int status) {
  int e;

  assert(status == 0);

  rocksdb_slice_t start = rocksdb_slice_init("b", 2);
  rocksdb_slice_t end = rocksdb_slice_init("e", 2);

  e = rocksdb_read_range(&db, &read_req, start, end, values, 4, on_read);
  assert(e == 0);
}

static void
on_open (rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0);

  batch->len = 5;

#define V(i, key) \
  batch->keys[i] = rocksdb_slice_init(key, 2); \
  batch->values[i] = rocksdb_slice_init(key, 2);

  V(0, "a")
  V(1, "b")
  V(2, "c")
  V(3, "d")
  V(4, "e")
#undef V

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
