// Standalone test (no Drogon / Postgres needed):
//   g++ -std=c++17 -O2 -pthread -I include test/pipeline_test.cc -o pipeline_test
//   g++ -std=c++17 -O1 -g -fsanitize=thread -pthread -I include test/pipeline_test.cc -o pipeline_test_tsan
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ingest/batch_writer.hpp"

using namespace ingest;

static void test_queue_basic() {
  MpmcQueue<int> q(4);
  int out = 0;
  assert(!q.try_pop(out));
  for (int i = 0; i < 4; ++i) assert(q.try_push(int(i)));
  assert(!q.try_push(99));  // full
  for (int i = 0; i < 4; ++i) { assert(q.try_pop(out)); assert(out == i); }
  assert(!q.try_pop(out));
  std::puts("queue basic: ok");
}

static void test_pipeline(std::size_t producers, std::size_t perProducer, bool slowDb) {
  std::atomic<std::size_t> rowsSeen{0};
  std::atomic<std::size_t> badBatches{0};
  std::atomic<std::size_t> maxBatch{0};
  std::mutex mu;
  std::vector<std::thread> dbThreads;

  BatchWriter::Options opt;
  opt.queueCapacity = 1 << 12;  // small on purpose, to exercise backpressure
  opt.batchMax = 500;
  opt.writers = 2;
  opt.maxInflight = 4;

  BatchWriter w(opt, [&](std::string&& arr, std::size_t n, BatchWriter::DoneFn done) {
    // Mock "async DB": validate the batch shape, then ack from another thread.
    if (arr.front() != '[' || arr.back() != ']') ++badBatches;
    std::size_t commas = 0, depth = 0;
    for (char c : arr) { if (c == '{') ++depth; else if (c == '}') --depth; else if (c == ',' && depth == 0) ++commas; }
    if (commas + 1 != n) ++badBatches;
    if (n > opt.batchMax) ++badBatches;
    std::size_t m = maxBatch.load();
    while (n > m && !maxBatch.compare_exchange_weak(m, n)) {}
    std::lock_guard<std::mutex> g(mu);
    dbThreads.emplace_back([&rowsSeen, n, done = std::move(done), slowDb] {
      if (slowDb) std::this_thread::sleep_for(std::chrono::milliseconds(2));
      rowsSeen += n;
      done(true);
    });
  });
  w.start();

  std::atomic<std::size_t> pushed{0};
  std::vector<std::thread> ps;
  for (std::size_t p = 0; p < producers; ++p)
    ps.emplace_back([&, p] {
      for (std::size_t i = 0; i < perProducer; ++i) {
        std::string row = "{\"p\":" + std::to_string(p) + ",\"i\":" + std::to_string(i) + "}";
        // Retry on full: models a client that retries after a 503.
        while (!w.tryPush(std::string(row))) std::this_thread::yield();
        ++pushed;
      }
    });
  for (auto& t : ps) t.join();

  bool clean = w.stop();
  for (auto& t : dbThreads) t.join();

  auto s = w.stats();
  std::printf("producers=%zu rows=%zu slowDb=%d -> written=%zu failed=%zu batches=%zu maxBatch=%zu rejected(retried)=%zu clean=%d\n",
              producers, pushed.load(), int(slowDb), s.written, s.failed, s.batches, maxBatch.load(), s.rejected, int(clean));
  assert(clean);
  assert(badBatches == 0);
  assert(s.accepted == producers * perProducer);
  assert(s.written == s.accepted);   // nothing lost
  assert(rowsSeen == s.accepted);    // nothing duplicated
  assert(s.failed == 0 && s.inflight == 0 && s.queueDepth == 0);
}

int main() {
  test_queue_basic();
  test_pipeline(8, 50000, false);
  test_pipeline(8, 5000, true);
  std::puts("ALL OK");
}