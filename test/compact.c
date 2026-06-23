#include <assert.h>
#include <stdbool.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uv_loop_t *loop;

static rocksdb_t db;
static rocksdb_column_family_t *family;

static void
on_close(rocksdb_close_t *req, int status) {
  assert(status == 0);

  assert(req->error == NULL);

  rocksdb_close_cleanup(req);
}

static void
on_compact(rocksdb_compact_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->error == NULL);

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  static rocksdb_close_t close;
  e = rocksdb_close(&db, &close, NULL, on_close);
  assert(e == 0);

  rocksdb_compact_cleanup(req);
}

static void
on_write(rocksdb_write_batch_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->error == NULL);

  static rocksdb_compact_t compact;

  e = rocksdb_compact(&db, &compact, family, NULL, on_compact);
  assert(e == 0);

  rocksdb_write_cleanup(req);
}

static void
on_open(rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0);

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

  rocksdb_open_cleanup(req);
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
  e = rocksdb_open(loop, &db, &open, "test/fixtures/compact.db", &options, &descriptor, &family, 1, NULL, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
