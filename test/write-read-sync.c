#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

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
  e = rocksdb_open(loop, &db, &open, "test/fixtures/write-read-sync.db", &options, &descriptor, &family, 1, NULL, NULL);
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

  rocksdb_read_t read;
  read.type = rocksdb_get;
  read.column_family = family;
  read.key = rocksdb_slice_init("hello", 5);
  read.value = rocksdb_slice_empty();

  rocksdb_read_batch_t read_batch;
  e = rocksdb_read(&db, &read_batch, &read, 1, NULL, NULL);
  assert(e == 0);
  assert(read_batch.errors[0] == NULL);
  assert(strcmp(read.value.data, "world") == 0);
  rocksdb_slice_destroy(&read.value);
  rocksdb_read_cleanup(&read_batch);

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  rocksdb_close_t close;
  e = rocksdb_close(&db, &close, NULL, NULL);
  assert(e == 0);
  assert(close.error == NULL);
  rocksdb_close_cleanup(&close);
}
