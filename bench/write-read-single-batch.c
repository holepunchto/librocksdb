#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

#define ops (100000)

static uv_loop_t *loop;

static rocksdb_t db;
static rocksdb_column_family_t *family;

static int64_t start;

static void
on_close(rocksdb_close_t *req, int status) {
  assert(status == 0);

  assert(req->error == NULL);
}

static void
on_read(rocksdb_read_batch_t *req, int status) {
  int e;

  assert(status == 0);

  int64_t elapsed = uv_hrtime() - start;

  printf("%f reads/s\n", ops / (elapsed / 1e9));

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  static rocksdb_close_t close;
  e = rocksdb_close(&db, &close, NULL, on_close);
  assert(e == 0);
}

static void
on_write(rocksdb_write_batch_t *req, int status) {
  int e;

  assert(status == 0);

  int64_t elapsed = uv_hrtime() - start;

  printf("%f writes/s\n", ops / (elapsed / 1e9));

  static rocksdb_read_t reads[ops];

  union {
    char buffer[4];
    uint32_t uint;
  } key;

  for (int i = 0; i < ops; i++) {
    rocksdb_read_t *read = &reads[i];

    key.uint = i + 1;

    char *buffer = malloc(sizeof(key));

    memcpy(buffer, key.buffer, sizeof(key));

    read->type = rocksdb_get;
    read->column_family = family;
    read->key = rocksdb_slice_init(buffer, sizeof(key));
    read->value = rocksdb_slice_empty();
  }

  start = uv_hrtime();

  static rocksdb_read_batch_t batch;
  e = rocksdb_read(&db, &batch, reads, ops, NULL, on_read);
  assert(e == 0);
}

static void
on_open(rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->error == NULL);

  static rocksdb_write_t writes[ops];

  union {
    char buffer[4];
    uint32_t uint;
  } key;

  static char value[512] = {0};

  for (int i = 0; i < ops; i++) {
    rocksdb_write_t *write = &writes[i];

    key.uint = i + 1;

    char *buffer = malloc(sizeof(key));

    memcpy(buffer, key.buffer, sizeof(key));

    write->type = rocksdb_put;
    write->column_family = family;
    write->key = rocksdb_slice_init(buffer, sizeof(key));
    write->value = rocksdb_slice_init(value, sizeof(value));
  }

  start = uv_hrtime();

  static rocksdb_write_batch_t batch;
  e = rocksdb_write(&db, &batch, writes, ops, NULL, on_write);
  assert(e == 0);
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
  e = rocksdb_open(loop, &db, &open, "bench/write-read.db", &options, &descriptor, &family, 1, NULL, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
