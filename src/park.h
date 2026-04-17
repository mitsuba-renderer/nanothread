/*
    src/park.h -- Worker park/wakeup primitive used by the task queue

    Copyright (c) 2026 Wenzel Jakob <wenzel.jakob@epfl.ch>

    All rights reserved. Use of this source code is governed by a BSD-style
    license that can be found in the LICENSE file.
*/

#pragma once

#include <atomic>
#include <cstdint>
#if defined(__APPLE__)
#  include <condition_variable>
#  include <mutex>
#endif

/**
 * \brief Park/wakeup primitive for idle queue workers.
 *
 * Task-queue workers spin on the queue while work is plentiful, but once
 * they have been idle for a while, the spin loop burns CPU cycles for no
 * gain and they should block until new work arrives. ``Parking`` handles
 * that transition: workers announce intent to sleep, suspend, and are
 * released en masse the moment a producer posts work.
 *
 * The implementation uses the OS-native compare-and-sleep facility
 * (``futex`` on Linux, ``WaitOnAddress`` on Windows,
 * ``os_sync_wait_on_address`` on macOS 14.4+, a
 * ``std::condition_variable`` fallback on older macOS).
 *
 * <b>Usage</b>:
 * \code
 *   // Producer
 *   enqueue(work);
 *   parking.wakeup();
 *
 *   // Worker
 *   uint32_t token = parking.enter();
 *   if (!try_dequeue() && !should_stop)
 *       parking.park(token);   // blocks until producer calls wakeup()
 *   parking.leave();
 * \endcode
 *
 * The handshake is lost-wake-free: either ``wakeup()`` sees the worker's
 * ``enter()`` and signals it, or the worker's re-check observes the
 * producer's enqueue and skips the park. This is the classic Dekker
 * pattern and requires a StoreLoad fence on both sides; ``enter()`` and
 * ``wakeup()`` therefore access the sleeper counter with
 * ``memory_order_seq_cst``. Plain release/acquire would leave a lost-wake
 * window on weakly ordered architectures such as AArch64.
 */
class Parking {
public:
    Parking();

    /// Wake every currently-parked worker. Cheap (single atomic load) when
    /// no workers are parked.
    void wakeup();

    /// Announce intent to park; returns a token to pass to \ref park().
    /// Must be balanced by a call to \ref leave().
    uint32_t enter();

    /// Block until \ref wakeup() is called. The caller must have first
    /// called \ref enter(), and must re-check its predicate after \ref
    /// park() returns -- spurious wake-ups are possible.
    void park(uint32_t token);

    /// Release the slot acquired by \ref enter().
    void leave();

private:
    /// Phase counter incremented by each \ref wakeup(). Workers park on
    /// this address using the OS-native compare-and-sleep primitive.
    std::atomic<uint32_t> phase;

    /// Number of workers currently parked or about to park. Consulted by
    /// \ref wakeup() to skip the syscall when nobody is listening.
    std::atomic<uint32_t> sleeper_count;

#if defined(__APPLE__)
    /// Fallback for macOS < 14.4
    std::mutex mutex;
    std::condition_variable cv;
#endif
};
