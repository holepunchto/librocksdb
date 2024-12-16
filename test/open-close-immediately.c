#include <assert.h>
#include <stdbool.h>
#include <uv.h>

#include "../include/rocksdb.h"

static rocksdb_t db;
static rocksdb_column_family_t *family;

static bool open_called = false;
static bool close_called = false;

static void
on_close (rocksdb_close_t *req, int status) {
  assert(status == 0);

  assert(req->error == NULL);

  close_called = true;
}

static void
on_open (rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0 || status == UV_ECANCELED);

  assert(req->error == NULL);

  open_called = true;

  if (status == 0) {
    e = rocksdb_column_family_destroy(&db, family);
    assert(e == 0);
  }
}

int
main () {
  int e;

  uv_loop_t *loop = uv_default_loop();

  e = rocksdb_init(loop, &db);
  assert(e == 0);

  rocksdb_options_t options = {
    .create_if_missing = true,
  };

  rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", NULL);

  static rocksdb_open_t open;
  e = rocksdb_open(&db, &open, "test/fixtures/open-close-immediately.db", &options, &descriptor, &family, 1, on_open);
  assert(e == 0);

  static rocksdb_close_t close;
  e = rocksdb_close(&db, &close, on_close);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);

  assert(open_called && close_called);
}
