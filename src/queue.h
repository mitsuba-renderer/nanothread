/*
    src/queue.h -- Lock-free task queue implementation used by nanothread

    Copyright (c) 2021 Wenzel Jakob <wenzel.jakob@epfl.ch>

    All rights reserved. Use of this source code is governed by a BSD-style
    license that can be found in the LICENSE file.
*/

#pragma once

#include "park.h"
#include <atomic>
#include <vector>
#include <type_traits>
#include <exception>
#include <cstring>
#include <cstdlib>

#if defined(_WIN32)
#  include <windows.h>
#  include <shared_mutex>
using Lock = std::shared_mutex; // Prefer (more efficient) shared_mutex on Windows
#else
#  include <mutex>
using Lock = std::mutex;
#endif


struct Pool;

enum class SleepKind : uint8_t {
    Worker,
    Helper
};

constexpr uint64_t high_bit  = (uint64_t) 0x0000000100000000ull;
constexpr uint64_t high_mask = (uint64_t) 0xFFFFFFFF00000000ull;

/// Monotonic wall-clock time in platform-native units (nanoseconds on
/// Apple/Linux, ``QueryPerformanceCounter`` ticks on Windows).
extern uint64_t get_time_raw();

#if defined(_WIN32)
/// Ticks-to-milliseconds scale, initialized at dynamic-init time in queue.cpp.
extern const double timer_frequency_scale_ms;
#endif

struct Task {
    /**
     * \brief Wide 16 byte pointer to a task in the worker pool. In addition to the
     * pointer itself, it encapsulates two more pieces of information:
     *
     * 1. The upper 32 bit of the \c value field contain a counter to prevent
     *    the ABA problem during atomic updates.
     *
     * 2. In queue link fields written by \ref TaskQueue::push(), the lower 32
     *    bit of the \c value field store the size of the pointed-to task.
     *    This lets \ref TaskQueue::pop() retire a drained node without
     *    dereferencing it, which would be unsafe since the node may already
     *    have been recycled for a new task. The head and tail fields leave
     *    these bits zero.
     */
    struct alignas(16) Ptr {
        Task *task;
        uint64_t value;

        Ptr(Task *task = nullptr, uint64_t value = 0) : task(task), value(value) { }

        Task::Ptr update_task(Task *new_task, uint32_t low = 0) const {
            return Ptr{ new_task, low | ((value & high_mask) + high_bit) };
        }

        operator bool() const { return task != nullptr; }

        uint32_t size() const { return (uint32_t) value; }

        bool operator==(const Task::Ptr &other) const {
            return task == other.task && value == other.value;
        }
    };

    /// Singly linked list, points to the next element
    Task::Ptr next;

    /**
     * \brief Reference count of this instance
     *
     * The reference count is arranged as a 2-tuple of 32 bit counters. When
     * submitting a work unit, its reference count is initially set to <tt>(3,
     * size)</tt>, where \c size is the number of associated work units. The
     * number '3' indicates three special references
     *
     *  - 1. A reference by the user code, which may e.g. wait for task completion
     *  - 2. A reference as part of the queue data structure
     *  - 3. A reference because the lower part is nonzero
     *
     * The function <tt>TaskQueue::release(task, high=true/false)</tt> can be
     * used to reduce the high and low parts separately.
     *
     * When the low part reaches zero, it assumed that all associated work
     * units have been completed, at which point child tasks are scheduled
     * and the task's payload is cleared. When both high and low parts reach
     * zero, it is assumed that no part of the system holds a reference to the
     * task, and it can be recycled.
     */
    std::atomic<uint64_t> refcount;

    /**
     * \brief Number of work units that have not yet been claimed by a thread
     *
     * This counter decides which thread executes which work unit. It is
     * armed in \ref TaskQueue::push() when the task becomes runnable and
     * must remain zero before that point. Otherwise, work units could be
     * claimed before the parent tasks have completed. Units are claimed by
     * \ref TaskQueue::pop() for the task at the head of the queue, and
     * directly through a task handle in \c task_wait_exclusive().
     */
    std::atomic<uint32_t> remain;

    /// Number of parent tasks that this task is waiting for
    std::atomic<uint32_t> wait_parents;

    /// Number of threads that are waiting for this task in task_wait()
    std::atomic<uint32_t> wait_count;

    /// Total number of work units in this task
    uint32_t size;

    /// Callback of the work unit
    void (*func)(uint32_t, void *);

    /// Pool that this tasks belongs to
    Pool *pool;

    /// Payload to be delivered to 'func'
    void *payload;

    /// Custom deleter used to free 'payload'
    void (*payload_deleter)(void *);

    /// Successor tasks that depend on this task
    std::vector<Task *> children;

    /// Pointer to an exception in case the task failed
    std::exception_ptr exception;

    /// Atomic flag stating whether the 'exception' field is already used
    std::atomic<bool> exception_used;

    /// Record the start/end time of this task?
    bool profile;

    /// Start/end timestamps in \ref get_time_raw units.
    std::atomic<uint64_t> time_start, time_end;

    /// Fixed-size payload storage region
    alignas(8) uint8_t payload_storage[256];

    void clear() {
        if (payload_deleter)
            payload_deleter(payload);
        payload_deleter = nullptr;
        payload = nullptr;
        children.clear();
#if !defined(NDEBUG)
        memset(payload_storage, 0xFF, sizeof(payload_storage));
#endif
    }
};

/**
 * Modified implementation of the lock-free queue presented in the paper
 *
 * "Simple, fast and practical non-blocking and blocking concurrent queue algorithms"
 * by Maged Michael and Michael Scott.
 *
 * The main difference compared to a Michael-Scott queue is that each queue
 * item (a task) represents \c N work units. The queue itself only tracks the
 * order of tasks. The number of unclaimed work units is stored in a per-task
 * counter (\ref Task::remain) that \ref push() arms when the task becomes
 * runnable. The \ref pop() operation claims one unit of the task at the head
 * of the queue and returns the item along with an index in the range
 * <tt>[0, N-1]</tt>. Nodes whose counter has reached zero are retired in
 * passing. A thread that holds a task handle can also claim units directly
 * without involving the queue (see \ref claim_or_sleep()). This is the basis
 * of \c task_wait_exclusive().
 *
 * Tasks can also have children. Following termination of a task, the queue
 * will push any children that don't depend on other unfinished work.
 *
 * The implementation here is designed to work on standard weakly ordered
 * memory architecture (e.g. AArch64), but likely would not not work an
 * completely weakly ordered architecture like the DEC Alpha.
 */
#if defined(_MSC_VER)
#  pragma warning(push)
// C4324: structure was padded due to alignment specifier.
#  pragma warning(disable : 4324)
#endif
struct TaskQueue {
public:
    /// Create an empty task queue
    TaskQueue();

    /// Free the queue and delete any remaining tasks
    ~TaskQueue();

    /**
     * \brief Allocate a new task record consisting of \c size work units
     *
     * The implementation tries to fetch an available task instance from a
     * pool of completed tasks, if possible. Otherwise, a new task is created.
     *
     * It is assumed that the caller will populate the remaining fields of the
     * returned task and then invoke \ref push() to submit the task to the
     * queue. The reference count of the returned task is initially set to
     * <tt>(2, size)</tt>, where \c size is the number of associated work
     * units. The number '2' indicates two special references by user code and
     * by the queue itself, which don't correspond to outstanding work.
     *
     * Initializes the Tasks' \c wait, \c size, \c refcount, \c remain, and
     * \c next fields.
     */
    Task *alloc(uint32_t size);

    /**
     * \brief Decrease the reference count of a task.
     *
     * The implementation moves the task into a pool of completed tasks once
     * the task is no longer referenced by any thread or data structure.
     */
    void release(Task *task, bool high = false);

    /// Increase the reference count of a task.
    void retain(Task *task);

    /// Append a task at the end of the queue
    void push(Task *task);

    /// Register an inter-task dependency
    void add_dependency(Task *task, Task *child);

    /**
     * \brief Pop a task from the queue
     *
     * When the queue is nonempty, this function returns a task instance and a
     * number in the range <tt>[0, size - 1]</tt>, where \c size is the number
     * of work units in the task. Otherwise, it returns \c nullptr and 0.
     */
    std::pair<Task *, uint32_t> pop();

    /**
     * \brief Fetch a task from the queue, or sleep
     *
     * This function repeatedly tries to fetch work from the queue and sleeps
     * if no work is available for an extended amount of time (~20 ms) and
     * the \c may_sleep parameter is set to \c true.
     *
     * The function stops trying to acquire work and returns <tt>(nullptr,
     * 0)</tt> when the supplied function <tt>stopping_criterion(payload)</tt>
     * evaluates to true.
     */
    std::pair<Task *, uint32_t> pop_or_sleep(bool (*stopping_criterion)(void *),
                                             void *payload, bool may_sleep,
                                             SleepKind sleep_kind,
                                             bool park_immediately);

    /**
     * \brief Claim a work unit of the given task, or sleep
     *
     * Counterpart of \ref pop_or_sleep() used by \c task_wait_exclusive().
     * It claims work units of \c task directly through its claim counter
     * without involving the queue. It therefore never hands out work that
     * belongs to other tasks. On success, the function returns \c true and
     * stores a number in the range <tt>[0, size - 1]</tt> in \c index. When
     * no unit is claimable, the function spins and eventually parks until
     * the task has completed. It then returns \c false.
     */
    bool claim_or_sleep(Task *task, uint32_t &index);

    /// Wake every sleeping participant. Used for shutdown and other global events.
    void wake_everyone();

    /// Wake helpers waiting in task_wait()/pool_work_until().
    void wake_helpers();

    /// Register/unregister a worker thread that can participate in execution.
    void worker_started();
    void worker_stopped();

private:
    /// Claim one work unit of \c task. Fails when all units are claimed.
    bool try_claim(Task *task, uint32_t &index);

    /// Wake sleeping workers if queued work exceeds awake workers.
    void wake_workers();

    /// Number of additional workers needed based on current queue/parking state.
    uint32_t worker_deficit() const;

    /// Cache line size used to separate independently-contended cursors.
    static constexpr size_t cacheline = 64;

    /// Head and tail of a lock-free list data structure
    alignas(cacheline) Task::Ptr head;
    alignas(cacheline) Task::Ptr tail;

    /// Head of a lock-free stack storing unused tasks
    alignas(cacheline) Task::Ptr recycle;

    /// Number of task instances created (for debugging)
    std::atomic<uint32_t> tasks_created;

    /// Number of queued work units that have not yet been claimed.
    std::atomic<uint32_t> ready_units;

    /// Number of live worker threads associated with this queue.
    std::atomic<uint32_t> worker_count;

    /// Idle worker park/wakeup state (see park.h).
    Parking worker_parking;

    /// Helper-thread park/wakeup state used by task_wait().
    Parking helper_parking;

    /// Park/wakeup state for threads inside task_wait_exclusive().
    Parking exclusive_parking;
};
#if defined(_MSC_VER)
#  pragma warning(pop)
#endif


extern "C" uint32_t pool_thread_id();

extern int profile_tasks;

#define NT_STR_2(x) #x
#define NT_STR(x)   NT_STR_2(x)

// NT_DEBUG is defined by the NANOTHREAD_ENABLE_TRACE CMake option.
#if defined(NT_DEBUG)
#  define NT_TRACE(fmt, ...)                                                  \
      fprintf(stderr, "%03u: " fmt "\n", pool_thread_id(), ##__VA_ARGS__)
#else
#  define NT_TRACE(fmt, ...) do { } while (0)
#endif

#define NT_ASSERT(x)                                                           \
    if (!(x)) {                                                                \
        fprintf(stderr, "Assertion failed in " __FILE__                        \
                        ":" NT_STR(__LINE__) ": " #x "\n");                    \
        abort();                                                               \
    }
