#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

static uint64_t
to_number(rocksdb_slice_t slice) {
  char buffer[32];

  assert(slice.len < sizeof(buffer));

  memcpy(buffer, slice.data, slice.len);

  buffer[slice.len] = '\0';

  return strtoull(buffer, NULL, 10);
}

int
main() {
  int e;

  uv_loop_t *loop = uv_default_loop();

  rocksdb_t db;
  rocksdb_column_family_t *families[2];

  rocksdb_options_t options;
  rocksdb_options_init(&options, 8);
  options.create_if_missing = true;
  options.create_missing_column_families = true;

  rocksdb_column_family_descriptor_t descriptors[] = {
    rocksdb_column_family_descriptor("default", NULL),
    rocksdb_column_family_descriptor("named", NULL),
  };

  rocksdb_open_t open;
  e = rocksdb_open(loop, &db, &open, "test/fixtures/column-family-property-get.db", &options, descriptors, families, 2, NULL, NULL);
  assert(e == 0);
  assert(open.error == NULL);
  rocksdb_open_cleanup(&open);

  rocksdb_write_t writes[5];

#define V(i, k) \
  writes[i].type = rocksdb_put; \
  writes[i].column_family = families[1]; \
  writes[i].key = rocksdb_slice_init(k, 2); \
  writes[i].value = rocksdb_slice_init(k, 2);

  V(0, "a")
  V(1, "b")
  V(2, "c")
  V(3, "d")
  V(4, "e")
#undef V

  rocksdb_write_batch_t write_batch;
  e = rocksdb_write(&db, &write_batch, writes, 5, NULL, NULL);
  assert(e == 0);
  assert(write_batch.error == NULL);
  rocksdb_write_cleanup(&write_batch);

  // Only a flushed memtable shows up in the file size properties.
  rocksdb_flush_t flush;
  e = rocksdb_flush(&db, &flush, families[1], NULL, NULL);
  assert(e == 0);
  assert(flush.error == NULL);
  rocksdb_flush_cleanup(&flush);

  rocksdb_slice_t value = rocksdb_slice_empty();

  e = rocksdb_column_family_property_get(&db, families[1], "rocksdb.total-sst-files-size", &value);
  assert(e == 0);
  assert(to_number(value) > 0);
  rocksdb_slice_destroy(&value);

  // The default family was never written to, so the unscoped getter reports
  // nothing for a database that keeps all of its data elsewhere.
  e = rocksdb_property_get(&db, "rocksdb.total-sst-files-size", &value);
  assert(e == 0);
  assert(to_number(value) == 0);
  rocksdb_slice_destroy(&value);

  e = rocksdb_column_family_property_get(&db, families[0], "rocksdb.total-sst-files-size", &value);
  assert(e == 0);
  assert(to_number(value) == 0);
  rocksdb_slice_destroy(&value);

  e = rocksdb_column_family_property_get(&db, families[1], "rocksdb.does-not-exist", &value);
  assert(e == UV_ENOENT);

  e = rocksdb_column_family_destroy(&db, families[0]);
  assert(e == 0);

  e = rocksdb_column_family_destroy(&db, families[1]);
  assert(e == 0);

  rocksdb_close_t close;
  e = rocksdb_close(&db, &close, NULL, NULL);
  assert(e == 0);
  assert(close.error == NULL);
  rocksdb_close_cleanup(&close);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
