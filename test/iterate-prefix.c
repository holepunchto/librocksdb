#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uv_loop_t *loop;

static rocksdb_t db;
static rocksdb_column_family_t *family;

static void
on_close (rocksdb_close_t *req, int status) {
  assert(status == 0);

  assert(req->error == NULL);
}

static void
on_iterator_close (rocksdb_iterator_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->error == NULL);

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  static rocksdb_close_t close;
  e = rocksdb_close(&db, &close, on_close);
  assert(e == 0);
}

static void
on_iterator_read (rocksdb_iterator_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->len == 4);
  assert(req->error == NULL);

#define V(i, k) \
  assert(strcmp(req->keys[i].data, k) == 0); \
  assert(strcmp(req->values[i].data, k) == 0); \
\
  rocksdb_slice_destroy(&req->keys[i]); \
  rocksdb_slice_destroy(&req->values[i]);

  V(0, "aa")
  V(1, "ab")
  V(2, "ac")
  V(3, "ad")
#undef V

  e = rocksdb_iterator_close(req, on_iterator_close);
  assert(e == 0);
}

static void
on_iterator_open (rocksdb_iterator_t *req, int status) {
  int e;

  assert(status == 0);

  static rocksdb_slice_t keys[4];
  static rocksdb_slice_t values[4];

  e = rocksdb_iterator_read(req, keys, values, 4, on_iterator_read);
  assert(e == 0);
}

static void
on_write (rocksdb_write_batch_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->error == NULL);

  rocksdb_range_t range = {
    .gte = rocksdb_slice_init("a", 2),
    .lt = rocksdb_slice_init("b", 2),
  };

  static rocksdb_iterator_t iterator;
  e = rocksdb_iterator_open(&db, &iterator, family, range, false, NULL, on_iterator_open);
  assert(e == 0);
}

static void
on_open (rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->error == NULL);

  static rocksdb_write_t writes[8];

#define V(i, k) \
  writes[i].type = rocksdb_put; \
  writes[i].key = rocksdb_slice_init(k, 3); \
  writes[i].value = rocksdb_slice_init(k, 3);

  V(0, "aa")
  V(1, "ab")
  V(2, "ba")
  V(3, "bb")
  V(4, "bc")
  V(5, "bd")
  V(6, "ac")
  V(7, "ad")
#undef V

  static rocksdb_write_batch_t batch;
  e = rocksdb_write(&db, &batch, writes, 8, NULL, on_write);
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

  rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", NULL);

  static rocksdb_open_t open;
  e = rocksdb_open(&db, &open, "test/fixtures/iterate-prefix.db", &options, &descriptor, &family, 1, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
