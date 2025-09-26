#include <chrono>
#include <vector>

#include <rocksdb/db.h>

using namespace rocksdb;

static auto ops = 100000;

int
main() {
  Status status;

  Options options;
  options.create_if_missing = true;

  std::unique_ptr<DB> db;
  status = DB::Open(options, "bench/raw.db", &db);
  assert(status.ok());

  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < ops; i++) {
    WriteBatch batch;
    WriteOptions options;

    union {
      char buffer[4];
      uint32_t uint;
    } key;

    key.uint = i + 1;

    static char value[512] = {0};

    batch.Put(Slice(key.buffer, sizeof(key)), Slice(value, sizeof(value)));

    status = db->Write(options, &batch);
    assert(status.ok());
  }

  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();

  printf("%f writes/s\n", ops / (elapsed / 1e9));

  start = std::chrono::steady_clock::now();

  for (int i = 0; i < ops; i++) {
    auto column_families = std::vector<ColumnFamilyHandle *>(1);

    column_families[0] = db->DefaultColumnFamily();

    union {
      char buffer[4];
      uint32_t uint;
    } key;

    key.uint = i + 1;

    auto keys = std::vector<Slice>(1);

    keys[0] = Slice(key.buffer, sizeof(key));

    auto values = std::vector<PinnableSlice>(1);

    auto statuses = std::vector<Status>(1);

    ReadOptions options;

    db->MultiGet(options, 1, column_families.data(), keys.data(), values.data(), statuses.data());
  }

  elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();

  printf("%f reads/s\n", ops / (elapsed / 1e9));
}
