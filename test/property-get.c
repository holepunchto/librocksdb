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

  rocksdb_close_cleanup(req);
}

static void
on_write(rocksdb_write_batch_t *req, int status) {
  int e;
  assert(status == 0);

  rocksdb_slice_t value = rocksdb_slice_empty();

  e = rocksdb_property_get(&db, "rocksdb.options-statistics", &value);
  assert(e == 0);
  assert(value.len > 0);

  static const char header[] = "rocksdb.block.cache.miss COUNT : 0";
  assert(memcmp(value.data, header, sizeof(header) - 1) == 0);

  rocksdb_slice_destroy(&value);

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  static rocksdb_close_t close;
  e = rocksdb_close(&db, &close, NULL, on_close);
  assert(e == 0);

  rocksdb_write_cleanup(req);
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

  rocksdb_open_cleanup(req);
}

int
main() {
  int e;

  loop = uv_default_loop();

  rocksdb_options_t options = {
    .version = 6,
    .create_if_missing = true,
    .enable_statistics = true
  };

  rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", NULL);

  static rocksdb_open_t open;
  e = rocksdb_open(loop, &db, &open, "test/fixtures/property-get.db", &options, &descriptor, &family, 1, NULL, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
