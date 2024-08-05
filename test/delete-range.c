#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uv_loop_t *loop;

static rocksdb_t db;

static void
on_close (rocksdb_close_t *req, int status) {
  assert(status == 0);
}

static void
on_read (rocksdb_read_batch_t *req, int status) {
  int e;

  assert(status == 0);

#define V(i, v) \
  assert(req->errors[i] == NULL); \
\
  if (v) { \
    assert(strcmp(req->reads[i].value.data, v) == 0); \
\
    rocksdb_slice_destroy(&req->reads[i].value); \
  } else { \
    assert(req->reads[i].value.len == 0); \
  }

  V(0, "a")
  V(1, NULL)
  V(2, NULL)
  V(3, NULL)
  V(4, "e")
#undef V

  static rocksdb_close_t close;
  e = rocksdb_close(&db, &close, on_close);
  assert(e == 0);
}

static void
on_delete (rocksdb_write_batch_t *req, int status) {
  int e;

  assert(status == 0);

  static rocksdb_read_t reads[5];

#define V(i, k) \
  reads[i].type = rocksdb_get; \
  reads[i].key = rocksdb_slice_init(k, 2);

  V(0, "a")
  V(1, "b")
  V(2, "c")
  V(3, "d")
  V(4, "e")
#undef V

  static char *errors[5];

  static rocksdb_read_batch_t batch;
  e = rocksdb_read(&db, &batch, reads, errors, 5, NULL, on_read);
  assert(e == 0);
}

static void
on_write (rocksdb_write_batch_t *req, int status) {
  int e;

  assert(status == 0);

  static rocksdb_write_t write;
  write.type = rocksdb_delete_range;
  write.start = rocksdb_slice_init("b", 2);
  write.end = rocksdb_slice_init("e", 2);

  static rocksdb_write_batch_t batch;
  e = rocksdb_write(&db, &batch, &write, 1, NULL, on_delete);
  assert(e == 0);
}

static void
on_open (rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0);

  static rocksdb_write_t writes[5];

#define V(i, k) \
  writes[i].type = rocksdb_put; \
  writes[i].key = rocksdb_slice_init(k, 2); \
  writes[i].value = rocksdb_slice_init(k, 2);

  V(0, "a")
  V(1, "b")
  V(2, "c")
  V(3, "d")
  V(4, "e")
#undef V

  static rocksdb_write_batch_t batch;
  e = rocksdb_write(&db, &batch, writes, 5, NULL, on_write);
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

  static rocksdb_open_t open;
  e = rocksdb_open(&db, &open, "test/fixtures/delete-range.db", &options, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
