#pragma once
// Bounded lock-free multi-producer / multi-consumer queue.
// Based on Dmitry Vyukov's bounded MPMC queue: every cell carries a sequence
// number, so producers and consumers coordinate with one CAS on a shared
// index and never take a lock. try_push / try_pop never block: they return
// false when the queue is full / empty.

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>

namespace ingest {

template <typename T>
class MpmcQueue {
 public:
  // capacity must be a power of two (>= 2).
  explicit MpmcQueue(std::size_t capacity)
      : mask_(capacity - 1), buf_(new Cell[capacity]) {
    if (capacity < 2 || (capacity & (capacity - 1)) != 0)
      throw std::invalid_argument("MpmcQueue capacity must be a power of two >= 2");
    for (std::size_t i = 0; i < capacity; ++i)
      buf_[i].seq.store(i, std::memory_order_relaxed);
  }

  MpmcQueue(const MpmcQueue&) = delete;
  MpmcQueue& operator=(const MpmcQueue&) = delete;

  // On failure (queue full) `v` is left untouched.
  bool try_push(T&& v) {
    Cell* cell;
    std::size_t pos = tail_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &buf_[pos & mask_];
      const std::size_t seq = cell->seq.load(std::memory_order_acquire);
      const auto dif = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);
      if (dif == 0) {
        // Slot is free for this lap: try to claim it.
        if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
      } else if (dif < 0) {
        return false;  // slot still holds an unconsumed item from the previous lap: full
      } else {
        pos = tail_.load(std::memory_order_relaxed);  // another producer got here first
      }
    }
    cell->data = std::move(v);
    cell->seq.store(pos + 1, std::memory_order_release);  // publish to consumers
    return true;
  }

  bool try_pop(T& out) {
    Cell* cell;
    std::size_t pos = head_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &buf_[pos & mask_];
      const std::size_t seq = cell->seq.load(std::memory_order_acquire);
      const auto dif = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);
      if (dif == 0) {
        if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
      } else if (dif < 0) {
        return false;  // nothing published in this slot yet: empty
      } else {
        pos = head_.load(std::memory_order_relaxed);
      }
    }
    out = std::move(cell->data);
    cell->seq.store(pos + mask_ + 1, std::memory_order_release);  // hand slot back to producers
    return true;
  }

  // Racy snapshot, for metrics only.
  std::size_t approx_size() const {
    const auto t = tail_.load(std::memory_order_relaxed);
    const auto h = head_.load(std::memory_order_relaxed);
    return t >= h ? t - h : 0;
  }

  std::size_t capacity() const { return mask_ + 1; }

 private:
  struct Cell {
    std::atomic<std::size_t> seq;
    T data;
  };

  static constexpr std::size_t kCacheLine = 64;

  const std::size_t mask_;
  std::unique_ptr<Cell[]> buf_;
  // Keep the two hot indices on separate cache lines to avoid false sharing.
  alignas(kCacheLine) std::atomic<std::size_t> tail_{0};  // producers
  alignas(kCacheLine) std::atomic<std::size_t> head_{0};  // consumers
};

}  // namespace ingest