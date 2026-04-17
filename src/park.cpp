/*
    src/park.cpp -- Worker park/wakeup primitive used by the task queue

    Copyright (c) 2026 Wenzel Jakob <wenzel.jakob@epfl.ch>

    All rights reserved. Use of this source code is governed by a BSD-style
    license that can be found in the LICENSE file.
*/

#include "park.h"
#include "queue.h" // for NT_ASSERT
#include <climits>
#include <cerrno>
#include <cstdio>

// ``std::atomic<uint32_t>`` is passed by address to OS-native wait primitives
// that expect a plain ``uint32_t*``; this requires bit-compatible layout and
// a lock-free representation (otherwise the stored value may live behind a
// spinlock rather than at ``&atom``). All mainstream targets satisfy this.
static_assert(sizeof(std::atomic<uint32_t>) == sizeof(uint32_t) &&
              alignof(std::atomic<uint32_t>) == alignof(uint32_t) &&
              std::atomic<uint32_t>::is_always_lock_free,
              "Parking requires std::atomic<uint32_t> to be lock-free and "
              "bit-compatible with uint32_t");

#if defined(_WIN32)
#  include <windows.h>
#  include <synchapi.h>
#elif defined(__linux__)
#  include <linux/futex.h>
#  include <sys/syscall.h>
#  include <unistd.h>
#elif defined(__APPLE__)
// macOS 14.4+ public wait-on-address API. Weak-imported so a binary built
// against an older SDK still links, with a runtime probe selecting the
// primitive below. The flags parameter is an ``OS_OPTIONS`` enum backed by
// ``uint32_t`` in the public header -- match that ABI via a local typedef so
// the redeclaration is type-compatible with Apple's.
typedef uint32_t os_sync_wait_on_address_flags_t;
typedef uint32_t os_sync_wake_by_address_flags_t;

extern "C" int os_sync_wait_on_address(void *addr, uint64_t value,
                                       size_t size,
                                       os_sync_wait_on_address_flags_t flags)
    __attribute__((weak_import));
extern "C" int os_sync_wake_by_address_all(void *addr, size_t size,
                                           os_sync_wake_by_address_flags_t flags)
    __attribute__((weak_import));

static bool has_native_wait() {
    return os_sync_wait_on_address != nullptr &&
           os_sync_wake_by_address_all != nullptr;
}
#endif

static void atomic_wait_u32(std::atomic<uint32_t> *addr, uint32_t expected) {
#if defined(__linux__)
    long r = syscall(SYS_futex, addr, FUTEX_WAIT_PRIVATE, expected, nullptr,
                     nullptr, 0);
    // Success returns 0; EAGAIN (value mismatch) and EINTR (signal) are the
    // only other benign outcomes -- anything else indicates a bug.
    NT_ASSERT(r == 0 || errno == EAGAIN || errno == EINTR);
#elif defined(_WIN32)
    WaitOnAddress(addr, &expected, sizeof(expected), INFINITE);
#elif defined(__APPLE__)
    os_sync_wait_on_address(addr, (uint64_t) expected, sizeof(uint32_t), 0);
#else
#  error "Parking: no wait-on-address primitive available for this platform"
#endif
}

static void atomic_wake_all_u32(std::atomic<uint32_t> *addr) {
#if defined(__linux__)
    syscall(SYS_futex, addr, FUTEX_WAKE_PRIVATE, INT_MAX, nullptr, nullptr, 0);
#elif defined(_WIN32)
    WakeByAddressAll(addr);
#elif defined(__APPLE__)
    os_sync_wake_by_address_all(addr, sizeof(uint32_t), 0);
#else
#  error "Parking: no wait-on-address primitive available for this platform"
#endif
}

// ============================================================================
// Parking
// ============================================================================

Parking::Parking() : phase(0), sleeper_count(0) { }

void Parking::wakeup() {
    // Fast path: skip the wake entirely when nobody is parked.
    //
    // The SC load pairs with the SC ``fetch_add`` in ``enter()`` to provide
    // the StoreLoad fence required by the Dekker-style handshake: either
    // this load sees the worker's ``enter()`` (we wake it), or the worker's
    // re-check sees the enqueue that precedes this call (it never parks).
    // Plain release/acquire would leave a lost-wake window on weakly
    // ordered architectures (e.g. AArch64).
    if (sleeper_count.load(std::memory_order_seq_cst) == 0)
        return;

    phase.fetch_add(1, std::memory_order_release);

#if defined(__APPLE__)
    if (!has_native_wait()) {
        std::lock_guard<std::mutex> guard(mutex);
        cv.notify_all();
        return;
    }
#endif

    atomic_wake_all_u32(&phase);
}

uint32_t Parking::enter() {
    // SC fetch_add -- see ``wakeup()`` for the rationale.
    sleeper_count.fetch_add(1, std::memory_order_seq_cst);
    return phase.load(std::memory_order_acquire);
}

void Parking::park(uint32_t token) {
#if defined(__APPLE__)
    if (!has_native_wait()) {
        std::unique_lock<std::mutex> guard(mutex);
        while (phase.load(std::memory_order_acquire) == token)
            cv.wait(guard);
        return;
    }
#endif

    // The kernel re-checks ``phase`` atomically against ``token`` before
    // suspending, so a concurrent ``wakeup()`` cannot be missed. The loop
    // absorbs spurious returns.
    while (phase.load(std::memory_order_acquire) == token)
        atomic_wait_u32(&phase, token);
}

void Parking::leave() {
    sleeper_count.fetch_sub(1, std::memory_order_relaxed);
}
