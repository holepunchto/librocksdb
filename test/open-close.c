#include <assert.h>
#include <stdbool.h>
#include <uv.h>

#include "../include/rocksdb.h"

static bool open_called = false;
static bool close_called = false;

static rocksdb_open_t open_req;
static rocksdb_close_t close_req;

static void
on_close (rocksdb_t *db, int status, void *data) {
  assert(status == 0);

  close_called = true;
}

static void
on_open (rocksdb_t *db, int status, void *data) {
  int e;

  assert(status == 0);

  open_called = true;

  e = rocksdb_close(db, &close_req, NULL, on_close);
  assert(e == 0);
}

int
main () {
  int e;

  uv_loop_t *loop = uv_default_loop();

  rocksdb_t db;
  e = rocksdb_init(loop, &db);
  assert(e == 0);

  rocksdb_options_t options = {
    .create_if_missing = true,
  };

  e = rocksdb_open(&db, &open_req, "test/fixtures/test.db", &options, NULL, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);

  assert(open_called && close_called);
}
