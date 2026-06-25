#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <uv.h>

#include "../include/rocksdb.h"

int
main() {
  int e;

  uv_loop_t *loop = uv_default_loop();

  // Write two keys with distinct prefixes and leave them in the WAL by avoiding
  // the flush that a clean close would otherwise perform.
  {
    rocksdb_t db;
    rocksdb_column_family_t *family;

    rocksdb_options_t options = {
      .version = 8,
      .create_if_missing = true,
      .avoid_flush_during_shutdown = true,
    };

    rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", NULL);

    rocksdb_open_t open;
    e = rocksdb_open(loop, &db, &open, "test/fixtures/read-only-wal-filter.db", &options, &descriptor, &family, 1, NULL, NULL);
    assert(e == 0);
    assert(open.error == NULL);
    rocksdb_open_cleanup(&open);

    rocksdb_write_t writes[2];

    writes[0].type = rocksdb_put;
    writes[0].column_family = family;
    writes[0].key = rocksdb_slice_init("a:1", 3);
    writes[0].value = rocksdb_slice_init("A1", 3);

    writes[1].type = rocksdb_put;
    writes[1].column_family = family;
    writes[1].key = rocksdb_slice_init("b:1", 3);
    writes[1].value = rocksdb_slice_init("B1", 3);

    rocksdb_write_batch_t write_batch;
    e = rocksdb_write(&db, &write_batch, writes, 2, NULL, NULL);
    assert(e == 0);
    assert(write_batch.error == NULL);
    rocksdb_write_cleanup(&write_batch);

    e = rocksdb_column_family_destroy(&db, family);
    assert(e == 0);

    rocksdb_close_t close;
    e = rocksdb_close(&db, &close, NULL, NULL);
    assert(e == 0);
    assert(close.error == NULL);
    rocksdb_close_cleanup(&close);
  }

  // Reopen read-only, replaying only the WAL records whose keys begin with the
  // allowed prefix. The "a:" key must be visible; the filtered-out "b:" key
  // must be absent because it was never replayed into a memtable.
  {
    rocksdb_t db;
    rocksdb_column_family_t *family;

    rocksdb_slice_t prefixes[] = {
      rocksdb_slice_init("a:", 2),
    };

    rocksdb_options_t options = {
      .version = 8,
      .read_only = true,
      .wal_filter_prefixes = prefixes,
      .wal_filter_prefixes_len = 1,
    };

    rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", NULL);

    rocksdb_open_t open;
    e = rocksdb_open(loop, &db, &open, "test/fixtures/read-only-wal-filter.db", &options, &descriptor, &family, 1, NULL, NULL);
    assert(e == 0);
    assert(open.error == NULL);
    rocksdb_open_cleanup(&open);

    rocksdb_read_t reads[2];

    reads[0].type = rocksdb_get;
    reads[0].column_family = family;
    reads[0].key = rocksdb_slice_init("a:1", 3);
    reads[0].value = rocksdb_slice_empty();

    reads[1].type = rocksdb_get;
    reads[1].column_family = family;
    reads[1].key = rocksdb_slice_init("b:1", 3);
    reads[1].value = rocksdb_slice_empty();

    rocksdb_read_batch_t read_batch;
    e = rocksdb_read(&db, &read_batch, reads, 2, NULL, NULL);
    assert(e == 0);
    assert(read_batch.errors[0] == NULL);
    assert(read_batch.errors[1] == NULL);

    // The matching key survived WAL replay.
    assert(reads[0].value.data != NULL);
    assert(strcmp(reads[0].value.data, "A1") == 0);

    // The non-matching key was dropped during WAL replay.
    assert(reads[1].value.data == NULL);

    rocksdb_slice_destroy(&reads[0].value);
    rocksdb_read_cleanup(&read_batch);

    e = rocksdb_column_family_destroy(&db, family);
    assert(e == 0);

    rocksdb_close_t close;
    e = rocksdb_close(&db, &close, NULL, NULL);
    assert(e == 0);
    assert(close.error == NULL);
    rocksdb_close_cleanup(&close);
  }

  // Pairing a WAL prefix filter with a read-write open is rejected, since
  // flushing the filtered memtable would discard the unmatched records.
  {
    rocksdb_t db;
    rocksdb_column_family_t *family;

    rocksdb_slice_t prefixes[] = {
      rocksdb_slice_init("a:", 2),
    };

    rocksdb_options_t options = {
      .version = 8,
      .read_only = false,
      .wal_filter_prefixes = prefixes,
      .wal_filter_prefixes_len = 1,
    };

    rocksdb_column_family_descriptor_t descriptor = rocksdb_column_family_descriptor("default", NULL);

    rocksdb_open_t open;
    e = rocksdb_open(loop, &db, &open, "test/fixtures/read-only-wal-filter.db", &options, &descriptor, &family, 1, NULL, NULL);
    assert(e == 0);
    assert(open.error != NULL);
    assert(open.status == UV_EINVAL);
    rocksdb_open_cleanup(&open);
  }

  e = uv_run(loop, UV_RUN_DEFAULT);
  assert(e == 0);
}
