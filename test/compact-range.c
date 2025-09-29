#include <assert.h>
#include <stdbool.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uv_loop_t *loop;

static rocksdb_t db;
static rocksdb_column_family_t *family;

static void
on_close(rocksdb_close_t *req) {
  assert(req->error == NULL);
}

static void
on_compact_range(rocksdb_compact_range_t *req) {
  int e;

  assert(req->error == NULL);

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  static rocksdb_close_t close;
  e = rocksdb_close(&db, &close, on_close);
  assert(e == 0);
}

static void
on_write(rocksdb_write_batch_t *req) {
  int e;

  assert(req->error == NULL);

  static rocksdb_compact_range_t compact_range;

  rocksdb_slice_t start = rocksdb_slice_init("b", 2);
  rocksdb_slice_t end = rocksdb_slice_init("e", 2);

  e = rocksdb_compact_range(&db, &compact_range, family, start, end, NULL, on_compact_range);
  assert(e == 0);
}

static void
on_open(rocksdb_open_t *req) {
  int e;

  assert(req->error == NULL);

  static rocksdb_write_t writes[5];

#define V(i, k) \
  writes[i].type = rocksdb_put; \
  writes[i].column_family = family; \
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
main() {
  int e;

  loop = uv_default_loop();

  e = rocksdb_init(loop, &db);
  assert(e == 0);

  rocksdb_options_t options = {
    .create_if_missing = true,
  };

  rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", NULL);

  static rocksdb_open_t open;
  e = rocksdb_open(&db, &open, "test/fixtures/compact-range.db", &options, &descriptor, &family, 1, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
