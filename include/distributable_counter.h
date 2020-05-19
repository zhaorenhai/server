/*****************************************************************************

Copyright (c) 2020 MariaDB Corporation.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA

*****************************************************************************/

#ifndef DISTRIBUTABLE_COUNTER_H
#define DISTRIBUTABLE_COUNTER_H

#include <cassert>
#include <cstddef>

#include <array>
#include <atomic>
#include <mutex>

#include "ilist.h"
#include "my_attribute.h"

namespace detail
{

template <typename Integral> class strong_bumper
{
public:
  strong_bumper(std::atomic<Integral> &value) noexcept : value_(value) {}

  void operator+=(Integral amount) noexcept { value_.fetch_add(amount); }
  void operator-=(Integral amount) noexcept { value_.fetch_sub(amount); }
  void operator++() noexcept { *this+= 1; }
  void operator++(int) noexcept { *this+= 1; }
  void operator--() noexcept { *this-= 1; }
  void operator--(int) noexcept { *this-= 1; }

private:
  std::atomic<Integral> &value_;
};

template <typename Integral> class weak_bumper
{
public:
  weak_bumper(std::atomic<Integral> &value) noexcept : value_(value) {}

  void operator+=(Integral amount) noexcept
  {
    value_.store(value_.load(std::memory_order_relaxed) + amount,
                 std::memory_order_relaxed);
  }
  void operator-=(Integral amount) noexcept
  {
    value_.store(value_.load(std::memory_order_relaxed) - amount,
                 std::memory_order_relaxed);
  }
  void operator++() noexcept { *this+= 1; }
  void operator++(int) noexcept { *this+= 1; }
  void operator--() noexcept { *this-= 1; }
  void operator--(int) noexcept { *this-= 1; }

private:
  std::atomic<Integral> &value_;
};

}; // namespace detail

template <typename Integral, size_t Size> class counter_broker_array;

template <typename Integral, size_t Size> class distributable_counter_array
{
  distributable_counter_array(const distributable_counter_array &)= delete;
  distributable_counter_array &
  operator=(const distributable_counter_array &)= delete;
  distributable_counter_array(distributable_counter_array &&)= delete;
  distributable_counter_array &
  operator=(distributable_counter_array &&)= delete;

public:
  using size_type= std::size_t;

  distributable_counter_array() noexcept
  {
    for (auto &counter : counters_)
      counter.store(0, std::memory_order_relaxed);
  }

  __attribute__((warn_unused_result)) detail::strong_bumper<Integral>
  operator[](size_type idx) noexcept
  {
    assert(idx < size());
    return detail::strong_bumper<Integral>(counters_[idx]);
  }

  __attribute__((warn_unused_result)) size_type size() const noexcept
  {
    return counters_.size();
  }
  __attribute__((warn_unused_result)) Integral load(size_type idx);
  Integral exchange(size_type idx, Integral to);

private:
  std::array<std::atomic<Integral>, Size> counters_;
  ilist<counter_broker_array<Integral, Size>> brokers_; // guarded by mutex_
  std::mutex mutex_;

  friend counter_broker_array<Integral, Size>;
};

template <typename Integral, size_t Size>
class counter_broker_array : public ilist_node<>
{
  counter_broker_array(const counter_broker_array &)= delete;
  counter_broker_array &operator=(const counter_broker_array &)= delete;
  counter_broker_array(counter_broker_array &&)= delete;
  counter_broker_array &operator=(counter_broker_array &&)= delete;

public:
  using size_type= std::size_t;

  counter_broker_array(distributable_counter_array<Integral, Size> &array)
      : base_(array)
  {
    for (auto &counter : counters_)
      counter.store(0, std::memory_order_relaxed);

    std::lock_guard<std::mutex> _(array.mutex_);
    array.brokers_.push_back(*this);
  }

  ~counter_broker_array() noexcept
  {
    // A reader of a distributable_counter_array may access this object while
    // it's being destroyed. To prevent a double sum of a counter we use an
    // exchange() here.
    for (size_type i= 0; i < size(); i++)
      base_[i]+= counters_[i].exchange(0, std::memory_order_relaxed);

    std::lock_guard<std::mutex> _(base_.mutex_);
    base_.brokers_.remove(*this);
  }

  __attribute__((warn_unused_result)) detail::weak_bumper<Integral>
  operator[](size_type idx) noexcept
  {
    assert(idx < size());
    return detail::weak_bumper<Integral>(counters_[idx]);
  }

  __attribute__((warn_unused_result)) size_type size() const noexcept
  {
    return counters_.size();
  }

private:
  std::array<std::atomic<Integral>, Size> counters_;
  distributable_counter_array<Integral, Size> &base_;

  friend class distributable_counter_array<Integral, Size>;
};

template <typename Integral, size_t Size>
Integral distributable_counter_array<Integral, Size>::load(size_type idx)
{
  assert(idx < size());

  Integral accumulator= 0;
  {
    std::lock_guard<std::mutex> _(mutex_);
    for (const auto &broker : brokers_)
      accumulator+= broker.counters_[idx].load(std::memory_order_relaxed);
  }
  return accumulator + counters_[idx].load(std::memory_order_relaxed);
}

template <typename Integral, size_t Size>
Integral distributable_counter_array<Integral, Size>::exchange(size_type idx,
                                                               Integral to)
{
  assert(idx < size());

  Integral accumulator= 0;
  {
    std::lock_guard<std::mutex> _(mutex_);
    for (const auto &broker : brokers_)
      accumulator+=
          broker.counters_[idx].exchange(0, std::memory_order_relaxed);
  }

  return accumulator + counters_[idx].exchange(to, std::memory_order_relaxed);
}

// Uses TLS to automatically distribute counter over any number of threads.
// Writing is a weakly atomical increment.
// Reading of values is O(N) where N is a number of threads.
// Thus, counter is optimized for writing and pessimized for reading.
template <typename Integral, size_t Size> class singleton_counter_array
{
public:
  __attribute__((warn_unused_result)) detail::weak_bumper<Integral>
  operator[](size_t idx)
  {
    assert(idx < Size);
    return local()[idx];
  }

  __attribute__((warn_unused_result)) Integral load(size_t idx)
  {
    return global_.load(idx);
  }
  Integral exchange(size_t idx, Integral to)
  {
    return global_.exchange(idx, to);
  }

private:
  counter_broker_array<Integral, Size> &local()
  {
    // Meyers' singleton ensures that the broker will be initialized on the
    // first access and thus will not slow down thread creation.
    thread_local counter_broker_array<Integral, Size> broker(global_);
    return broker;
  }

  distributable_counter_array<Integral, Size> global_;
};

#endif
