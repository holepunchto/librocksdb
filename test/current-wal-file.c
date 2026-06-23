#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

static void
on_current_wal_file(rocksdb_current_wal_file_t *req, int status) {
  assert(status == 0);
  assert(req->error == NULL);

  rocksdb_wal_file_t *file = &req->result;

  assert(strlen(file->path) > 0);
  assert(file->number > 0);
  assert(file->type == rocksdb_wal_file_alive);
  assert(file->size > 0);

  rocksdb_current_wal_file_cleanup(req);
}

int
main() {
  int e;

  uv_loop_t *loop = uv_default_loop();

  rocksdb_t db;
  rocksdb_column_family_t *family;

  rocksdb_options_t options = {
    .create_if_missing = true,
  };

  rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", NULL);

  rocksdb_open_t open;
  e = rocksdb_open(loop, &db, &open, "test/fixtures/current-wal-file.db", &options, &descriptor, &family, 1, NULL, NULL);
  assert(e == 0);
  assert(open.error == NULL);
  rocksdb_open_cleanup(&open);

  rocksdb_write_t write;
  write.type = rocksdb_put;
  write.column_family = family;
  write.key = rocksdb_slice_init("hello", 5);
  write.value = rocksdb_slice_init("world", 6);

  rocksdb_write_batch_t write_batch;
  e = rocksdb_write(&db, &write_batch, &write, 1, NULL, NULL);
  assert(e == 0);
  assert(write_batch.error == NULL);
  rocksdb_write_cleanup(&write_batch);

  rocksdb_current_wal_file_t current;
  e = rocksdb_current_wal_file(&db, &current, on_current_wal_file);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  rocksdb_close_t close;
  e = rocksdb_close(&db, &close, NULL, NULL);
  assert(e == 0);
  assert(close.error == NULL);
  rocksdb_close_cleanup(&close);
}
