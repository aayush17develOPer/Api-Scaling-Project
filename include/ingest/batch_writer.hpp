#pragma once
// Async batching writer.
//
//   HTTP handlers --try_push--> [lock-free MPMC queue] --> writer threads
//                                                           |  coalesce rows
//                                                           v
//                                            FlushFn(one JSON array, N rows)
//
// A writer flushes when it has `batchMax` rows or the oldest buffered row is
// `flushInterval` old, whichever comes first. FlushFn must be non-blocking:
// it starts the DB write and calls `done(ok)` from whatever thread finishes it.
// The number of unfinished flushes is capped (`maxInflight`); once the cap is
// hit the writers stop draining, the queue fills, and try_push() returns false
// so the HTTP layer can shed load with a 503 instead of growing memory.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "mpmc_queue.hpp"

namespace ingest {

class BatchWriter {
 public:
  using DoneFn = std::function<void(bool ok)>;
  // `jsonArray` is "[row1,row2,...]" where each row is an already-validated JSON value.
  using FlushFn = std::function<void(std::string&& jsonArray, std::size_t rows, DoneFn done)>;

  struct Options {
    std::size_t queueCapacity = 1 << 18;  // power of two
    std::size_t batchMax = 2000;
    std::size_t writers = 2;
    std::size_t maxInflight = 8;  // total concurrent unfinished flushes
    std::chrono::microseconds flushInterval{5000};
  };

  struct Stats {
    std::size_t accepted, rejected, written, failed, batches, queueDepth, inflight;
  };

  BatchWriter(Options opt, FlushFn flush)
      : opt_(opt), flush_(std::move(flush)), queue_(opt.queueCapacity) {}

  ~BatchWriter() { stop(); }

  void start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true)) return;
    for (std::size_t i = 0; i < opt_.writers; ++i)
      threads_.emplace_back([this] { writerLoop(); });
  }

  // Safe to call from any HTTP worker thread. False = queue full or shutting down.
  bool tryPush(std::string&& row) {
    if (!accepting_.load(std::memory_order_acquire) || !queue_.try_push(std::move(row))) {
      rejected_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    accepted_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  // Stop accepting, flush everything already queued, wait for the DB to ack.
  // Idempotent. Returns true if all in-flight writes finished before `timeout`.
  bool stop(std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
    accepting_.store(false, std::memory_order_release);
    stop_.store(true, std::memory_order_release);
    for (auto& t : threads_)
      if (t.joinable()) t.join();
    threads_.clear();
    drainRemainder();  // catch a push that raced with the shutdown flags
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (inflight_.load(std::memory_order_acquire) > 0) {
      if (std::chrono::steady_clock::now() > deadline) return false;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
  }

  Stats stats() const {
    return {accepted_.load(),  rejected_.load(), written_.load(),
            failed_.load(),    batches_.load(),  queue_.approx_size(),
            inflight_.load()};
  }

 private:
  using Clock = std::chrono::steady_clock;

  void writerLoop() {
    std::string buf;
    std::string item;
    std::size_t rows = 0;
    Clock::time_point firstAt;
    buf.reserve(opt_.batchMax * 256);

    for (;;) {
      // Read the stop flag BEFORE draining: if it was already set and the drain
      // below finds nothing, no producer can still be adding rows, so we may exit.
      const bool stopping = stop_.load(std::memory_order_acquire);

      bool got = false;
      while (rows < opt_.batchMax && queue_.try_pop(item)) {
        if (rows == 0) {
          buf.push_back('[');
          firstAt = Clock::now();
        } else {
          buf.push_back(',');
        }
        buf.append(item);
        ++rows;
        got = true;
      }

      if (rows == 0) {
        if (stopping) return;
        std::this_thread::sleep_for(std::chrono::microseconds(200));
        continue;
      }

      const bool full = rows >= opt_.batchMax;
      const bool aged = Clock::now() - firstAt >= opt_.flushInterval;
      if (!(full || aged || stopping)) {
        if (!got) std::this_thread::sleep_for(std::chrono::microseconds(100));
        continue;
      }

      submit(std::move(buf), rows);
      buf.clear();
      buf.reserve(opt_.batchMax * 256);
      rows = 0;
    }
  }

  // Closes the JSON array and hands it to FlushFn, waiting while too many flushes are open.
  void submit(std::string&& buf, std::size_t n) {
    while (inflight_.load(std::memory_order_acquire) >= opt_.maxInflight)
      std::this_thread::sleep_for(std::chrono::microseconds(200));
    buf.push_back(']');
    inflight_.fetch_add(1, std::memory_order_acq_rel);
    batches_.fetch_add(1, std::memory_order_relaxed);
    flush_(std::move(buf), n, [this, n](bool ok) {
      (ok ? written_ : failed_).fetch_add(n, std::memory_order_relaxed);
      inflight_.fetch_sub(1, std::memory_order_acq_rel);
    });
  }

  void drainRemainder() {
    std::string buf, item;
    std::size_t rows = 0;
    while (queue_.try_pop(item)) {
      buf.push_back(rows++ == 0 ? '[' : ',');
      buf.append(item);
      if (rows >= opt_.batchMax) {
        submit(std::move(buf), rows);
        buf.clear();
        rows = 0;
      }
    }
    if (rows > 0) submit(std::move(buf), rows);
  }

  Options opt_;
  FlushFn flush_;
  MpmcQueue<std::string> queue_;
  std::vector<std::thread> threads_;
  std::atomic<bool> started_{false};
  std::atomic<bool> accepting_{true};
  std::atomic<bool> stop_{false};
  std::atomic<std::size_t> inflight_{0};
  std::atomic<std::size_t> accepted_{0}, rejected_{0}, written_{0}, failed_{0}, batches_{0};
};

}  // namespace ingest