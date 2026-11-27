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

static void
test_shared_cache(const char *path, rocksdb_cache_t block_cache, uint64_t capacity) {
  int e;

  uv_loop_t *loop = uv_default_loop();

  rocksdb_t db;
  rocksdb_column_family_t *families[2];

  rocksdb_options_t options;
  rocksdb_options_init(&options, 9);
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.block_cache = block_cache;

  rocksdb_column_family_descriptor_t descriptors[] = {
    rocksdb_column_family_descriptor("default", NULL),
    rocksdb_column_family_descriptor("named", NULL),
  };

  rocksdb_open_t open;
  e = rocksdb_open(loop, &db, &open, path, &options, descriptors, families, 2, NULL, NULL);
  assert(e == 0);
  assert(open.error == NULL);
  rocksdb_open_cleanup(&open);

  rocksdb_slice_t value = rocksdb_slice_empty();

  // Both column families share the one cache, so both report its capacity.
  for (int i = 0; i < 2; i++) {
    e = rocksdb_column_family_property_get(&db, families[i], "rocksdb.block-cache-capacity", &value);
    assert(e == 0);
    assert(to_number(value) == capacity);
    rocksdb_slice_destroy(&value);

    e = rocksdb_column_family_destroy(&db, families[i]);
    assert(e == 0);
  }

  rocksdb_close_t close;
  e = rocksdb_close(&db, &close, NULL, NULL);
  assert(e == 0);
  assert(close.error == NULL);
  rocksdb_close_cleanup(&close);

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}

int
main() {
  test_shared_cache(
    "test/fixtures/block-cache-lru.db",
    (rocksdb_cache_t) {
      .type = rocksdb_lru_cache,
      .lru = {
        .version = 0,
        .capacity = 4 * 1024 * 1024,
        .num_shard_bits = -1,
        .strict_capacity_limit = false,
        .high_pri_pool_ratio = 0.5,
        .low_pri_pool_ratio = 0.0,
      },
    },
    4 * 1024 * 1024
  );

  test_shared_cache(
    "test/fixtures/block-cache-hyper-clock.db",
    (rocksdb_cache_t) {
      .type = rocksdb_hyper_clock_cache,
      .hyper_clock = {
        .version = 0,
        .capacity = 8 * 1024 * 1024,
        .estimated_entry_charge = 0,
        .num_shard_bits = -1,
        .strict_capacity_limit = false,
      },
    },
    8 * 1024 * 1024
  );
}
