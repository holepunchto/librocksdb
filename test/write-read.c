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
}

static void
on_read(rocksdb_read_batch_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->len == 1);

  rocksdb_read_t read = req->reads[0];

  assert(strcmp(read.value.data, "world") == 0);

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  static rocksdb_close_t close;
  e = rocksdb_close(&db, &close, on_close);
  assert(e == 0);
}

static void
on_write(rocksdb_write_batch_t *req, int status) {
  int e;

  assert(status == 0);

  static rocksdb_read_t read;
  read.type = rocksdb_get;
  read.column_family = family;
  read.key = rocksdb_slice_init("hello", 5);
  read.value = rocksdb_slice_empty();

  rocksdb_read_options_t read_options = {
    .async_io = true,
    .fill_cache = false,
  };

  static rocksdb_read_batch_t batch;
  e = rocksdb_read(&db, &batch, &read, 1, &read_options, on_read);
  assert(e == 0);
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
}

int
main() {
  int e;

  loop = uv_default_loop();

  e = rocksdb_init(loop, &db);
  assert(e == 0);

  rocksdb_options_t options = {
    .create_if_missing = true,
    .use_direct_reads = true,
    .avoid_unnecessary_blocking_io = true,
    .skip_stats_update_on_db_open = true,
    .use_direct_io_for_flush_and_compaction = true,
    .max_file_opening_threads = -1,
  };

  rocksdb_column_family_options_t column_family_options = {
    .optimize_filters_for_memory = true,
    .optimize_filters_for_hits = true,
    .num_levels = 5,
    .max_write_buffer_number = 1,
  };

  rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", &column_family_options);

  static rocksdb_open_t open;
  e = rocksdb_open(&db, &open, "test/fixtures/write-read.db", &options, &descriptor, &family, 1, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
