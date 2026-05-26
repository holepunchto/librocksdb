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
  e = rocksdb_open(loop, &db, &open, "test/fixtures/iterate-sync.db", &options, &descriptor, &family, 1, NULL, NULL);
  assert(e == 0);
  assert(open.error == NULL);
  rocksdb_open_cleanup(&open);

  rocksdb_write_t writes[4];

#define V(i, k) \
  writes[i].type = rocksdb_put; \
  writes[i].column_family = family; \
  writes[i].key = rocksdb_slice_init(k, 3); \
  writes[i].value = rocksdb_slice_init(k, 3);

  V(0, "aa")
  V(1, "ab")
  V(2, "ac")
  V(3, "ad")
#undef V

  rocksdb_write_batch_t write_batch;
  e = rocksdb_write(&db, &write_batch, writes, 4, NULL, NULL);
  assert(e == 0);
  assert(write_batch.error == NULL);
  rocksdb_write_cleanup(&write_batch);

  rocksdb_range_t range = {
    .gte = rocksdb_slice_init("a", 2),
    .lt = rocksdb_slice_init("b", 2),
  };

  rocksdb_iterator_t iterator;
  e = rocksdb_iterator_open(&db, &iterator, family, range, NULL, NULL);
  assert(e == 0);
  assert(iterator.error == NULL);
  rocksdb_iterator_cleanup(&iterator);

  rocksdb_slice_t keys[4];
  rocksdb_slice_t values[4];
  e = rocksdb_iterator_read(&iterator, keys, values, 4, NULL);
  assert(e == 0);
  assert(iterator.error == NULL);
  assert(iterator.len == 4);

#define V(i, k) \
  assert(strcmp(iterator.keys[i].data, k) == 0); \
  assert(strcmp(iterator.values[i].data, k) == 0); \
  rocksdb_slice_destroy(&iterator.keys[i]); \
  rocksdb_slice_destroy(&iterator.values[i]);

  V(0, "aa")
  V(1, "ab")
  V(2, "ac")
  V(3, "ad")
#undef V

  rocksdb_iterator_cleanup(&iterator);

  e = rocksdb_iterator_close(&iterator, NULL);
  assert(e == 0);
  assert(iterator.error == NULL);
  rocksdb_iterator_cleanup(&iterator);

  e = rocksdb_column_family_destroy(&db, family);
  assert(e == 0);

  rocksdb_close_t close;
  e = rocksdb_close(&db, &close, NULL, NULL);
  assert(e == 0);
  assert(close.error == NULL);
  rocksdb_close_cleanup(&close);
}
