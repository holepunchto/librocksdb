#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uv_loop_t *loop;

static rocksdb_t db;
static rocksdb_column_family_t *family;

static int ops = 100000;
static int reading = 0;
static int writing = 0;

static int64_t start;

static void
on_close(rocksdb_close_t *req, int status) {
  assert(status == 0);

  assert(req->error == NULL);
}

static void
loop_read(void);

static void
on_read(rocksdb_read_batch_t *req, int status) {
  int e;

  assert(status == 0);

  if (reading < ops) loop_read();
  else {
    int64_t elapsed = uv_hrtime() - start;

    printf("%f reads/s\n", ops / (elapsed / 1e9));

    e = rocksdb_column_family_destroy(&db, family);
    assert(e == 0);

    static rocksdb_close_t close;
    e = rocksdb_close(&db, &close, on_close);
    assert(e == 0);
  }
}

static void
loop_read(void) {
  int e;

  union {
    char buffer[4];
    uint32_t uint;
  } key;

  key.uint = ++reading;

  static rocksdb_read_t read;
  read.type = rocksdb_get;
  read.column_family = family;
  read.key = rocksdb_slice_init(key.buffer, sizeof(key));
  read.value = rocksdb_slice_empty();

  static rocksdb_read_batch_t batch;
  e = rocksdb_read(&db, &batch, &read, 1, NULL, on_read);
  assert(e == 0);
}

static void
loop_write(void);

static void
on_write(rocksdb_write_batch_t *req, int status) {
  int e;

  assert(status == 0);

  if (writing < ops) loop_write();
  else {
    int64_t elapsed = uv_hrtime() - start;

    printf("%f writes/s\n", ops / (elapsed / 1e9));

    start = uv_hrtime();

    loop_read();
  }
}

static void
loop_write(void) {
  int e;

  union {
    char buffer[4];
    uint32_t uint;
  } key;

  key.uint = ++writing;

  static char value[512];

  static rocksdb_write_t write;
  write.type = rocksdb_put;
  write.column_family = family;
  write.key = rocksdb_slice_init(key.buffer, sizeof(key));
  write.value = rocksdb_slice_init(value, sizeof(value));

  static rocksdb_write_batch_t batch;
  e = rocksdb_write(&db, &batch, &write, 1, NULL, on_write);
  assert(e == 0);
}

static void
on_open(rocksdb_open_t *req, int status) {
  int e;

  assert(status == 0);

  assert(req->error == NULL);

  start = uv_hrtime();

  loop_write();
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
  e = rocksdb_open(&db, &open, "bench/write-read.db", &options, &descriptor, &family, 1, on_open);
  assert(e == 0);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
