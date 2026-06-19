/*
    src/queue.cpp -- Lock-free task queue implementation used by nanothread

    Copyright (c) 2021 Wenzel Jakob <wenzel.jakob@epfl.ch>

    All rights reserved. Use of this source code is governed by a BSD-style
    license that can be found in the LICENSE file.
*/

#include "queue.h"
#include <cstdio>
#include <cstdlib>
#include <ctime>

#if defined(_WIN32)
#  include <windows.h>
#endif

#if defined(_MSC_VER)
#  include <intrin.h>
#elif defined(__SSE2__)
#  include <emmintrin.h>
#endif

/// Put worker threads to sleep after this many milliseconds without work
#define NANOTHREAD_MAX_IDLE_MS 20.0

/// Consult the wall clock once every (mask + 1) iterations of the busy loop.
#define NANOTHREAD_IDLE_CHECK_MASK 0xFFFu

#if defined(_WIN32)
/// Scale factor converting QueryPerformanceCounter ticks to milliseconds.
/// The timer frequency is fixed for the lifetime of the process, so we cache
/// it at dynamic-initialization time and keep the hot path branch-free.
/// Declared ``extern`` in queue.h so ``task_time`` can reuse it.
extern const double timer_frequency_scale_ms = []() {
    LARGE_INTEGER freq;
    QueryPerformanceFrequency(&freq);
    return 1e3 / (double) freq.QuadPart;
}();
#endif

/// Monotonic wall-clock time in milliseconds. Does not require a context
/// switch on the target platforms.
static inline double time_milliseconds() {
#if defined(_WIN32)
    LARGE_INTEGER ticks;
    QueryPerformanceCounter(&ticks);
    return timer_frequency_scale_ms * (double) ticks.QuadPart;
#elif defined(__APPLE__)
    return clock_gettime_nsec_np(CLOCK_UPTIME_RAW) / 1000000.0;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1000000.0;
#endif
}

/// Monotonic wall-clock time in platform-native units (ns on Apple/Linux,
/// ``QueryPerformanceCounter`` ticks on Windows). ``task_time`` converts.
uint64_t get_time_raw() {
#if defined(_WIN32)
    LARGE_INTEGER ticks;
    QueryPerformanceCounter(&ticks);
    return (uint64_t) ticks.QuadPart;
#elif defined(__APPLE__)
    return clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ull + (uint64_t) ts.tv_nsec;
#endif
}

/// Reduce power usage in busy-wait CAS loops
static void cas_pause() {
#if defined(_M_X64) || defined(__SSE2__)
    _mm_pause();
#elif defined(_M_ARM64) || defined(_M_ARM)
    __yield();
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield" ::: "memory");
#endif
}

/// Atomic 16 byte compare-and-swap & release barrier on ARM
static bool cas(Task::Ptr &ptr, Task::Ptr &expected, Task::Ptr desired) {
#if defined(_MSC_VER)
    #if defined(_M_ARM64)
        return _InterlockedCompareExchange128_rel(
            (__int64 volatile *) &ptr, (__int64) desired.value,
            (__int64) desired.task, (__int64 *) &expected);
    #else
        return _InterlockedCompareExchange128(
            (__int64 volatile *) &ptr, (__int64) desired.value,
            (__int64) desired.task, (__int64 *) &expected);
    #endif
#else
    return __atomic_compare_exchange(&ptr, &expected, &desired, true,
                                     __ATOMIC_RELEASE, __ATOMIC_ACQUIRE);
#endif
}

// *Non-atomic* 16 byte load, acquire barrier on ARM
static Task::Ptr ldar(Task::Ptr &source) {
#if defined(_MSC_VER)
    using P = unsigned __int64 volatile *;
    #if defined(_M_ARM64)
        uint64_t value_1 = __ldar64((P) &source);
        uint64_t value_2 = __ldar64(((P) &source) + 1);
    #else
        uint64_t value_1 = *((P) &source);
        uint64_t value_2 = *(((P) &source) + 1);
    #endif
    return Task::Ptr{ (Task *) value_1, (uint64_t) value_2 };
#else
    uint64_t value_1 = __atomic_load_n((uint64_t *) &source, __ATOMIC_ACQUIRE);
    uint64_t value_2 = __atomic_load_n((((uint64_t *) &source) + 1), __ATOMIC_ACQUIRE);
    return Task::Ptr{ (Task *) value_1, value_2 };
#endif
}

// *Non-atomic* 16 byte store mirroring \ref ldar().
static void star(Task::Ptr &target, Task::Ptr value) {
#if defined(_MSC_VER)
    using P = unsigned __int64 volatile *;
    *((P) &target)       = (uint64_t) value.task;
    *(((P) &target) + 1) = (uint64_t) value.value;
#else
    __atomic_store_n((uint64_t *) &target, (uint64_t) value.task,
                     __ATOMIC_RELAXED);
    __atomic_store_n(((uint64_t *) &target) + 1, value.value, __ATOMIC_RELAXED);
#endif
}

TaskQueue::TaskQueue() : tasks_created(0), ready_units(0), worker_count(0) {
    head = Task::Ptr(alloc(0));
    tail = head;
}

TaskQueue::~TaskQueue() {
    uint32_t created = tasks_created.load(),
             deleted = 0, incomplete = 0,
             incomplete_size = 0;

    // Free jobs that are still in the queue
    Task::Ptr ptr = head;
    while (ptr.task) {
        Task *task = ptr.task;

        if (ptr.remain() != 0) {
            incomplete_size += ptr.remain();
            incomplete++;
        }

        for (Task *child : task->children) {
            uint32_t wait = child->wait_parents.fetch_sub(1);
            NT_ASSERT(wait != 0);
            if (wait == 1)
                push(child);
        }

        task->clear();
        deleted++;
        ptr = task->next;
        delete task;
    }

    // Free jobs on the free-job stack
    ptr = recycle;
    while (ptr.task) {
        Task *task = ptr.task;
        NT_ASSERT(task->payload == nullptr && task->children.empty());
        deleted++;
        ptr = task->next;
        delete task;
    }

    if (created != deleted)
        fprintf(stderr,
                "nanothread: %u/%u tasks were leaked! Did you forget to call "
                "task_release()?\n", created - deleted, created);

    if (incomplete > 0)
        fprintf(stderr, "nanothread: %u tasks with %u work units were not "
                "completed!\n", incomplete, incomplete_size);
}

Task *TaskQueue::alloc(uint32_t size) {
    Task::Ptr node = ldar(recycle);

    while (true) {
        // Stop if stack is empty
        if (!node)
            break;

        // Load the next node
        Task::Ptr next = ldar(node.task->next);

        // Next, try to move it to the stack head
        if (cas(recycle, node, node.update_task(next.task)))
            break;

        cas_pause();
    }

    Task *task;

    if (node.task) {
        task = node.task;
    } else {
        task = new Task();
        tasks_created++;
    }

    star(task->next, Task::Ptr());
    task->refcount.store(size + (size == 0 ? high_bit : (3 * high_bit)),
                         std::memory_order_relaxed);
    task->wait_parents.store(0, std::memory_order_relaxed);
    task->wait_count.store(0, std::memory_order_relaxed);
    task->size = size;
    task->time_start.store(0, std::memory_order_relaxed);
    task->time_end.store(0, std::memory_order_relaxed);

    NT_TRACE("%s task %p with size=%u",
             node.task ? "reusing recycled" : "created new", task, size);

    return task;
}

void TaskQueue::release(Task *task, bool high) {
    if (!high && task->profile)
        task->time_end.store(get_time_raw(), std::memory_order_relaxed);

    uint64_t result = task->refcount.fetch_sub(high ? high_bit : 1);
    uint32_t ref_lo = (uint32_t) result,
             ref_hi = (uint32_t) (result >> 32);

    NT_ASSERT((!high || ref_hi > 0) && (high || ref_lo > 0));
    ref_hi -= (uint32_t) high;
    ref_lo -= (uint32_t) !high;

    NT_TRACE("release(task=%p, %s) -> refcount=(hi=%u, lo=%u)", task,
             high ? "high" : "low", ref_hi, ref_lo);

    // If all work has completed: schedule children and free payload
    if (!high && ref_lo == 0) {
        NT_TRACE("all work associated with task %p has completed.", task);

        for (Task *child : task->children) {
            uint32_t wait = child->wait_parents.fetch_sub(1);

            NT_TRACE("notifying child %p of task %p: wait=%u", child, task,
                     wait - 1);

            NT_ASSERT(wait > 0);

            if (task->exception_used.load()) {
                bool expected = false;
                if (child->exception_used.compare_exchange_strong(expected, true)) {
                    NT_TRACE("propagating exception to child %p of task %p.",
                             child, task);
                    child->exception = task->exception;
                } else {
                    NT_TRACE("not propagating exception to child %p of "
                             "task %p (already stored).", child, task);
                }
            }

            if (wait == 1) {
                NT_TRACE("Child %p of task %p is ready for execution.", child,
                         task);
                push(child);
            }
        }

        task->clear();

        // Wake task_wait() helpers without disturbing idle workers.
        if (task->wait_count.load() > 0)
            wake_helpers();

        release(task, true);
    } else if (high && ref_hi == 0) {
        // Nobody holds any references at this point, recycle task

        NT_ASSERT(ref_lo == 0);
        NT_TRACE("all usage of task %p is done, recycling.", task);

        Task::Ptr node = ldar(recycle);
        while (true) {
            star(task->next, node);

            if (cas(recycle, node, node.update_task(task)))
                break;

            cas_pause();
        }
    }
}

void TaskQueue::add_dependency(Task *parent, Task *child) {
    if (!parent)
        return;

    /* Acquire load pairs with 'refcount.fetch_sub' in release() that a
       throwing worker performs *after* writing 'exception'. */
    uint64_t refcount =
        parent->refcount.load(std::memory_order_acquire);

    /* Increase the parent task's reference count to prevent the cleanup
       handler in release() from starting while the following executes. */
    while (true) {
        if ((uint32_t) refcount == 0) {
            // Parent task has already completed
            if (parent->exception_used.load()) {
                bool expected = false;
                if (child->exception_used.compare_exchange_strong(expected, true)) {
                    NT_TRACE("propagating exception to child %p of task %p.",
                             child, parent);
                    child->exception = parent->exception;
                } else {
                    NT_TRACE("not propagating exception to child %p of "
                             "task %p (already stored).", child, parent);
                }
            }
            return;
        }

        if (parent->refcount.compare_exchange_weak(refcount, refcount + 1,
                                                   std::memory_order_release,
                                                   std::memory_order_acquire))
            break;

        cas_pause();
    }

    // Otherwise, register the child task with the parent
    parent->children.push_back(child);
    uint32_t wait = ++child->wait_parents;
    (void) wait;

    NT_TRACE("registering dependency: parent=%p, child=%p, child->wait=%u",
             parent, child, wait);

    /* Undo the parent->refcount change. If the task completed in the
       meantime, child->wait_parents will also be decremented by
       this call. */
    release(parent);
}

void TaskQueue::retain(Task *task) {
    NT_TRACE("retain(task=%p)", task);
    task->refcount.fetch_add(high_bit);
}

void TaskQueue::push(Task *task) {
    uint32_t size = task->size;

    NT_TRACE("push(task=%p, size=%u)", task, size);

    // Relaxed: `ready_units` is advisory (wake/park heuristic only).
    ready_units.fetch_add(size, std::memory_order_relaxed);

    while (true) {
        // Lead tail and tail->next, and double-check, in this order
        Task::Ptr tail_c = ldar(tail);
        Task::Ptr &next = tail_c.task->next;
        Task::Ptr next_c = ldar(next);
        Task::Ptr tail_c_2 = ldar(tail);

        // Detect inconsistencies due to contention
        if (tail_c == tail_c_2) {
            if (!next_c.task) {
                // Tail was pointing to last node, try to insert here
                if (cas(next, next_c, Task::Ptr(task, size))) {
                    // Best-effort attempt to redirect tail to the added element
                    cas(tail, tail_c, tail_c.update_task(task));
                    break;
                }
            } else {
                // Tail wasn't pointing to the last node, try to update
                cas(tail, tail_c, tail_c.update_task(next_c.task));
            }
        }

        cas_pause();
    }

    wake_workers();
    helper_parking.wake_n(size);
}

std::pair<Task *, uint32_t> TaskQueue::pop() {
    uint32_t index;
    Task *task;

    while (true) {
        // Lead head, tail, and next element, and double-check, in this order
        Task::Ptr head_c = ldar(head);
        Task::Ptr tail_c = ldar(tail);
        Task::Ptr &next = head_c.task->next;
        Task::Ptr next_c = ldar(next);
        Task::Ptr head_c_2 = ldar(head);

        // Detect inconsistencies due to contention
        if (head_c == head_c_2) {
            if (head_c.task != tail_c.task) {
                uint32_t remain = next_c.remain();

                if (remain > 1) {
                    // More than 1 remaining work units, update work counter
                    if (cas(next, next_c, next_c.update_remain(remain - 1))) {
                        task = next_c.task;
                        index = task->size - remain;
                        break;
                    }
                } else {
                    NT_ASSERT(remain == 1);
                    // Head node is removed from the queue, reduce refcount
                    if (cas(head, head_c, head_c.update_task(next_c.task))) {
                        task = next_c.task;
                        index = task->size - 1;
                        // Account for the whole task in one shot at retirement
                        // instead of touching the shared `ready_units` cacheline
                        // on every unit claim.
                        ready_units.fetch_sub(task->size,
                                              std::memory_order_relaxed);
                        release(head_c.task, true);
                        break;
                    }
                }
            } else {
                // Task queue was empty
                if (!next_c.task) {
                    task = nullptr;
                    index = 0;
                    cas_pause();
                    break;
                } else {
                    // Advance the tail, it's falling behind
                    cas(tail, tail_c, tail_c.update_task(next_c.task));
                }
            }
        }

        cas_pause();
    }

    if (task) {
        NT_TRACE("pop(task=%p, index=%u)", task, index);
        // NB: `ready_units` is decremented per task at retirement (see the
        // head-removal branch above), not per unit, to keep the shared counter
        // off the hot claim path.

        if (index == 0 && task->profile)
            task->time_start.store(get_time_raw(), std::memory_order_relaxed);
    }

    return { task, index };
}

void TaskQueue::wake_everyone() {
    worker_parking.wake_n(UINT32_MAX);
    helper_parking.wake_n(UINT32_MAX);
}

void TaskQueue::wake_helpers() {
    helper_parking.wake_n(UINT32_MAX);
}

void TaskQueue::worker_started() {
    worker_count.fetch_add(1, std::memory_order_release);
}

void TaskQueue::worker_stopped() {
    worker_count.fetch_sub(1, std::memory_order_acq_rel);
}

uint32_t TaskQueue::worker_deficit() const {
    uint32_t workers = worker_count.load(std::memory_order_acquire);
    if (workers == 0)
        return 0;

    uint32_t ready = ready_units.load(std::memory_order_relaxed);
    if (ready == 0)
        return 0;

    uint32_t sleepers = worker_parking.sleepers();
    if (sleepers > workers)
        sleepers = workers;

    uint32_t awake = workers - sleepers,
             desired = ready < workers ? ready : workers,
             deficit = desired > awake ? desired - awake : 0;

    NT_TRACE("worker deficit=%u (ready=%u, awake=%u/%u)", deficit, ready, awake,
             workers);

    return deficit;
}

void TaskQueue::wake_workers() {
    // wake_n() broadcasts in a single syscall when the deficit covers every
    // parked worker, otherwise it releases exactly that many.
    uint32_t deficit = worker_deficit();
    if (deficit)
        worker_parking.wake_n(deficit);
}

std::pair<Task *, uint32_t>
TaskQueue::pop_or_sleep(bool (*stopping_criterion)(void *), void *payload,
                        bool may_sleep, SleepKind sleep_kind,
                        bool park_immediately) {
    std::pair<Task *, uint32_t> result(nullptr, 0);
    uint32_t attempts = 0;
    double start_ms = time_milliseconds();

    while (true) {
        result = pop();

        if (result.first || stopping_criterion(payload))
            break;

        if (!may_sleep)
            continue;

        /* Go to sleep once the worker has been unable to find work for at
           least NANOTHREAD_MAX_IDLE_MS of wall-clock time. The clock is only
           polled once every NANOTHREAD_IDLE_CHECK_MASK+1 attempts to keep
           the spin loop cheap. A freshly booted "start parked" worker skips
           this warm-up spin and parks on its first idle poll. */
        if (!park_immediately &&
            (((++attempts) & NANOTHREAD_IDLE_CHECK_MASK) != 0 ||
             time_milliseconds() - start_ms < NANOTHREAD_MAX_IDLE_MS))
            continue;
        park_immediately = false;

        // Park/wake handshake -- see Parking in park.h.
        Parking &parking =
            sleep_kind == SleepKind::Worker ? worker_parking : helper_parking;
        uint32_t token = parking.enter();
        result = pop();
        bool idle = !result.first && !stopping_criterion(payload);

        if (idle) {
            // Final sleep gate: re-check the deficit while counted as a
            // sleeper. If the queue visibly still needs this worker, back out
            // and keep polling instead of parking, so a stale producer-side
            // wake cannot strand work. We are about to resume polling and so
            // cover one unit of the deficit ourselves; wake peers for the rest.
            uint32_t deficit =
                sleep_kind == SleepKind::Worker ? worker_deficit() : 0;
            if (deficit > 0) {
                NT_TRACE("sleep gate: staying awake, waking %u peer(s)",
                         deficit - 1);
                parking.leave();
                worker_parking.wake_n(deficit - 1);
                start_ms = time_milliseconds();
                attempts = 0;
                continue;
            }

            NT_TRACE("park (%s, idle=%.1f ms)",
                     sleep_kind == SleepKind::Worker ? "worker" : "helper",
                     time_milliseconds() - start_ms);
            parking.park(token);
            NT_TRACE("unpark (%s)",
                     sleep_kind == SleepKind::Worker ? "worker" : "helper");
        }
        parking.leave();

        if (result.first || stopping_criterion(payload))
            break;

        // Returning from park() means new work just arrived; give the
        // worker a fresh spin window before potentially re-sleeping.
        start_ms = time_milliseconds();
        attempts = 0;
    }

    return result;
}
