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

  WriteBatch batch;

  union {
    char buffer[4];
    uint32_t uint;
  } key;

  static char value[512] = {0};

  for (int i = 0; i < ops; i++) {
    key.uint = i + 1;

    char *buffer = new char[4];

    std::copy(key.buffer, key.buffer + 4, buffer);

    batch.Put(Slice(buffer, sizeof(key)), Slice(value, sizeof(value)));
  }

  auto start = std::chrono::steady_clock::now();

  WriteOptions write_options;
  status = db->Write(write_options, &batch);
  assert(status.ok());

  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();

  printf("%f writes/s\n", ops / (elapsed / 1e9));

  auto column_families = std::vector<ColumnFamilyHandle *>(ops);

  auto keys = std::vector<Slice>(ops);

  auto values = std::vector<PinnableSlice>(ops);

  auto statuses = std::vector<Status>(ops);

  for (int i = 0; i < ops; i++) {
    column_families[i] = db->DefaultColumnFamily();

    key.uint = i + 1;

    char *buffer = new char[4];

    std::copy(key.buffer, key.buffer + 4, buffer);

    keys[0] = Slice(buffer, sizeof(key));
  }

  start = std::chrono::steady_clock::now();

  ReadOptions read_options;
  db->MultiGet(read_options, ops, column_families.data(), keys.data(), values.data(), statuses.data());

  for (auto status : statuses) {
    assert(status.ok());
  }

  elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();

  printf("%f reads/s\n", ops / (elapsed / 1e9));
}
