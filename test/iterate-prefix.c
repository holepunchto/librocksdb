#include <assert.h>
#include <stdbool.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uv_loop_t *loop;

static rocksdb_t db;

static rocksdb_open_t open_req;
static rocksdb_close_t close_req;

static rocksdb_batch_t batch;
static rocksdb_iterator_t iterator;

static rocksdb_slice_t keys[8];
static rocksdb_slice_t values[8];

static void
on_close (rocksdb_close_t *req, int status) {
  assert(status == 0);
}

static void
on_iterator_close (rocksdb_iterator_t *req, int status) {
  int e;

  assert(status == 0);

  e = rocksdb_close(&db, &close_req, on_close);
  assert(e == 0);
}

static void
on_iterator_read (rocksdb_iterator_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->len == 4);
  assert(req->error == NULL);

#define V(i, key) \
  assert(strcmp(keys[i].data, key) == 0); \
  assert(strcmp(values[i].data, key) == 0); \
\
  rocksdb_slice_destroy(&keys[i]); \
  rocksdb_slice_destroy(&values[i]);

  V(0, "aa")
  V(1, "ab")
  V(2, "ac")
  V(3, "ad")
#undef V

  e = rocksdb_iterator_close(&iterator, on_iterator_close);
  assert(e == 0);
}

static void
on_iterator_open (rocksdb_iterator_t *req, int status) {
  int e;

  assert(status == 0);

  e = rocksdb_iterator_read(&iterator, keys, values, 4, on_iterator_read);
  assert(e == 0);
}

static void
on_batch_write (rocksdb_batch_t *req, int status) {
  int e;

  assert(status == 0);

  rocksdb_slice_t start = rocksdb_slice_init("a", 2);
  rocksdb_slice_t end = rocksdb_slice_init("b", 2);

  e = rocksdb_iterator_open(&iterator, start, end, on_iterator_open);
  assert(e == 0);
}

static void
on_open (rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0);

#define V(i, key) \
  keys[i] = rocksdb_slice_init(key, 3); \
  values[i] = rocksdb_slice_init(key, 3);

  V(0, "aa")
  V(1, "ab")
  V(2, "ba")
  V(3, "bb")
  V(4, "bc")
  V(5, "bd")
  V(6, "ac")
  V(7, "ad")
#undef V

  e = rocksdb_batch_write(&batch, keys, values, 8, on_batch_write);
  assert(e == 0);
}

int
main () {
  int e;

  loop = uv_default_loop();

  e = rocksdb_init(loop, &db);
  assert(e == 0);

  e = rocksdb_batch_init(&db, &batch);
  assert(e == 0);

  e = rocksdb_iterator_init(&db, &iterator);
  assert(e == 0);

  rocksdb_options_t options = {
    .create_if_missing = true,
  };

  e = rocksdb_open(&db, &open_req, "test/fixtures/iterate-prefix.db", &options, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
