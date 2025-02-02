/*
    src/pool.cpp -- Simple thread pool with task-based API

    Copyright (c) 2021 Wenzel Jakob <wenzel.jakob@epfl.ch>

    All rights reserved. Use of this source code is governed by a BSD-style
    license that can be found in the LICENSE file.
*/

#include <nanothread/nanothread.h>
#include "queue.h"
#include <thread>
#include <memory>
#include <type_traits>

#if defined(__linux__)
#  include <unistd.h>
#elif defined(_WIN32)
#  include <windows.h>
#  include <processthreadsapi.h>
#endif

#if defined(_MSC_VER)
#  include <intrin.h>
#elif defined(__SSE2__)
#  include <pmmintrin.h>
#endif

struct Worker;

/// TLS variable storing an ID of each thread
#if defined(_MSC_VER)
    static __declspec(thread) uint32_t thread_id_tls = 0;
#else
    static __thread uint32_t thread_id_tls = 0;
#endif

/// Data structure describing a pool of workers
struct Pool {
    /// Queue of scheduled tasks
    TaskQueue queue;

    /// List of currently running worker threads
    std::vector<std::unique_ptr<Worker>> workers;

    /// Number of idle workers that have gone to sleep
    std::atomic<uint32_t> asleep;

    /// Should denormalized floating point numbers be flushed to zero?
    bool ftz = true;
};

struct Worker {
    Pool *pool;
    std::thread thread;
    uint32_t id;
    bool stop;
    bool ftz;

    Worker(Pool *pool, uint32_t id, bool ftz);
    ~Worker();
    void run();
};


static Pool *pool_default_inst = nullptr;
static Lock pool_default_lock;
static uint32_t cached_core_count = 0;

uint32_t core_count() {
    // assumes atomic word size memory access
    if (cached_core_count)
        return cached_core_count;

    // Determine the number of present cores
    uint32_t ncores = std::thread::hardware_concurrency();

#if defined(__linux__)
    // Don't try to query CPU affinity if running inside Valgrind
    if (getenv("VALGRIND_OPTS") == nullptr) {
        /* Some of the cores may not be available to the user
           (e.g. on certain cluster nodes) -- determine the number
           of actual available cores here. */
        uint32_t ncores_logical = ncores;
        size_t size = 0;
        cpu_set_t *cpuset = nullptr;
        int retval = 0;

        /* The kernel may expect a larger cpu_set_t than would
           be warranted by the physical core count. Keep querying
           with increasingly larger buffers if the
           pthread_getaffinity_np operation fails */
        for (uint32_t i = 0; i < 10; ++i) {
            size = CPU_ALLOC_SIZE(ncores_logical);
            cpuset = CPU_ALLOC(ncores_logical);
            if (!cpuset) {
                fprintf(stderr, "nanothread: core_count(): Could not allocate cpu_set_t.\n");
                return ncores;
            }
            CPU_ZERO_S(size, cpuset);

            int retval = pthread_getaffinity_np(pthread_self(), size, cpuset);
            if (retval == 0)
                break;
            CPU_FREE(cpuset);
            ncores_logical *= 2;
        }

        if (retval) {
            fprintf(stderr, "nanothread: core_count(): Could not read thread affinity map.\n");
            return ncores;
        }

        uint32_t ncores_avail = 0;
        for (uint32_t i = 0; i < ncores_logical; ++i)
            ncores_avail += CPU_ISSET_S(i, size, cpuset) ? 1 : 0;
        ncores = ncores_avail;
        CPU_FREE(cpuset);
    }
#endif
    cached_core_count = ncores;
    return ncores;
}


uint32_t pool_thread_id() {
    return thread_id_tls;
}

Pool *pool_default() {
    std::unique_lock<Lock> guard(pool_default_lock);

    if (!pool_default_inst)
        pool_default_inst = pool_create();

    return pool_default_inst;
}

Pool *pool_create(uint32_t size, int ftz) {
    Pool *pool = new Pool();
    pool->ftz = ftz != 0;
    if (size == (uint32_t) -1)
        size = core_count();
    NT_TRACE("pool_create(%p)", pool);
    pool_set_size(pool, size);
    return pool;
}


void pool_destroy(Pool *pool) {
    if (pool) {
        pool_set_size(pool, 0);
        delete pool;
    } else if (pool_default_inst) {
        pool_destroy(pool_default_inst);
        pool_default_inst = nullptr;
    }
}

uint32_t pool_size(Pool *pool) {
    if (!pool) {
        std::unique_lock<Lock> guard(pool_default_lock);
        pool = pool_default_inst;
    }

    if (pool)
        return (uint32_t) pool->workers.size();
    else
        return core_count();
}

void pool_set_size(Pool *pool, uint32_t size) {
    if (!pool) {
        std::unique_lock<Lock> guard(pool_default_lock);
        pool = pool_default_inst;

        if (!pool) {
            pool = pool_default_inst = new Pool();
            NT_TRACE("pool_create(%p)", pool);
        }
    }

    NT_TRACE("pool_set_size(%p, %u)", pool, size);

    int diff = (int) size - (int) pool->workers.size();
    if (diff > 0) {
        // Launch extra worker threads
        for (int i = 0; i < diff; ++i)
            pool->workers.push_back(std::unique_ptr<Worker>(
                new Worker(pool, (uint32_t) pool->workers.size() + 1, pool->ftz)));
    } else if (diff < 0) {
        // Remove worker threads (destructor calls join())
        for (int i = diff; i != 0; ++i)
            pool->workers[pool->workers.size() + i]->stop = true;
        pool->queue.wakeup();
        for (int i = diff; i != 0; ++i)
            pool->workers.pop_back();
    }
}

int profile_tasks = false;

int pool_profile() {
    return (int) profile_tasks;
}

void pool_set_profile(int value) {
    profile_tasks = (bool) value;
}

Task *task_submit_dep(Pool *pool, const Task *const *parent,
                      uint32_t parent_count, uint32_t size,
                      void (*func)(uint32_t, void *), void *payload,
                      uint32_t payload_size, void (*payload_deleter)(void *),
                      int async) {

    if (size == 0) {
        // There is no work, so the payload is irrelevant
        func = nullptr;

        // The queue requires task size >= 1
        size = 1;
    }

    // Does the task have parent tasks
    bool has_parent = false;
    for (uint32_t i = 0; i < parent_count; ++i)
        has_parent |= parent[i] != nullptr;

    // If this is a small work unit, execute it right away
    if (size == 1 && !has_parent && async == 0) {
        NT_TRACE("task_submit_dep(): task is small, executing right away");

        if (!profile_tasks) {
            if (func)
                func(0, payload);

            if (payload_deleter)
                payload_deleter(payload);

            // Don't even return a task..
            return nullptr;
        } else {
            if (!pool)
                pool = pool_default();

            Task *task = pool->queue.alloc(size);

            if (profile_tasks) {
                #if defined(_WIN32)
                    QueryPerformanceCounter(&task->time_start);
                #elif defined(__APPLE__)
                    task->time_start = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
                #else
                    clock_gettime(CLOCK_MONOTONIC, &task->time_start);
                #endif
            }

            if (func)
                func(0, payload);

            if (profile_tasks) {
                #if defined(_WIN32)
                    QueryPerformanceCounter(&task->time_end);
                #elif defined(__APPLE__)
                    task->time_end = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
                #else
                    clock_gettime(CLOCK_MONOTONIC, &task->time_end);
                #endif
            }

            if (payload_deleter)
                payload_deleter(payload);

            task->refcount.store(high_bit, std::memory_order_relaxed);
            task->exception_used.store(false, std::memory_order_relaxed);
            task->exception = nullptr;
            task->size = size;
            task->func = func;
            task->pool = pool;
            task->payload = nullptr;
            task->payload_deleter = nullptr;

            return task;
        }
    }

    // Size 0 is equivalent to size 1, but without the above optimization
    if (size == 0)
        size = 1;

    if (!pool)
        pool = pool_default();

    Task *task = pool->queue.alloc(size);
    task->exception_used.store(false, std::memory_order_relaxed);
    task->exception = nullptr;

    if (has_parent) {
        // Prevent early job submission due to completion of parents
        task->wait_parents.store(1, std::memory_order_release);

        // Register dependencies in queue, will further increase child->wait_parents
        for (uint32_t i = 0; i < parent_count; ++i)
            pool->queue.add_dependency((Task *) parent[i], task);
    }

    task->size = size;
    task->func = func;
    task->pool = pool;

    if (payload) {
        if (payload_deleter || payload_size == 0) {
            task->payload = payload;
            task->payload_deleter = payload_deleter;
        } else if (payload_size <= sizeof(Task::payload_storage)) {
            task->payload = task->payload_storage;
            memcpy(task->payload_storage, payload, payload_size);
            task->payload_deleter = nullptr;
        } else {
            /* Payload doesn't fit into temporary storage, and no
               custom deleter was provided. Make a temporary copy. */
            task->payload = malloc(payload_size);
            task->payload_deleter = free;
            NT_ASSERT(task->payload != nullptr);
            memcpy(task->payload, payload, payload_size);
        }
    } else {
        task->payload = nullptr;
        task->payload_deleter = nullptr;
    }

    bool push = true;
    if (has_parent) {
        /* Undo the earlier 'wait' increment. If the value is now zero, all
           parent tasks have completed and the job can be pushed. Otherwise,
           it's somebody else's job to carry out this step. */
        push = task->wait_parents.fetch_sub(1) == 1;
    }

    if (push)
        pool->queue.push(task);

    return task;
}

static void pool_execute_task(Pool *pool, bool (*stopping_criterion)(void *),
                              void *payload, bool may_sleep) {
    Task *task;
    uint32_t index;
    std::tie(task, index) =
        pool->queue.pop_or_sleep(stopping_criterion, payload, may_sleep);

    if (task) {
        if (task->func) {
            if (task->exception_used.load()) {
                NT_TRACE(
                    "not running callback (task=%p, index=%u) because another "
                    "work unit of this task generated an exception",
                    task, index);
            } else {
                try {
                    NT_TRACE("running callback (task=%p, index=%u, payload=%p)", task, index, task->payload);
                    task->func(index, task->payload);
                } catch (...) {
                    bool value = false;
                    if (task->exception_used.compare_exchange_strong(value, true)) {
                        NT_TRACE("exception caught, storing..");
                        task->exception = std::current_exception();
                    } else {
                        NT_TRACE("exception caught, ignoring (an exception was already stored)");
                    }
                }
            }
        }

        pool->queue.release(task);
    }
}

void pool_work_until(Pool *pool, bool (*stopping_criterion)(void *), void *payload) {
    if (!pool)
        pool = pool_default_inst;
    if (!pool)
        return;
    while (!stopping_criterion(payload))
        pool_execute_task(pool, stopping_criterion, payload, false);
}

#if defined(__SSE2__)
struct FTZGuard {
    FTZGuard(bool enable) : enable(enable) {
        if (enable) {
            csr = _mm_getcsr();
            _mm_setcsr(csr | (_MM_FLUSH_ZERO_ON | _MM_DENORMALS_ZERO_ON));
        }
    }

    ~FTZGuard() {
        if (enable)
            _mm_setcsr(csr);
    }

    bool enable;
    int csr;
};
#else
struct FTZGuard { FTZGuard(bool) { } };
#endif

void task_wait(Task *task) {
    if (task) {
        Pool *pool = task->pool;
        FTZGuard ftz_guard(pool->ftz);

        // Signal that we are waiting for this task
        task->wait_count++;

        auto stopping_criterion = [](void *ptr) -> bool {
            return (uint32_t)(((Task *) ptr)->refcount.load()) == 0;
        };

        NT_TRACE("task_wait(%p)", task);

        // Help executing work units in the meantime
        while (!stopping_criterion(task))
            pool_execute_task(pool, stopping_criterion, task, true);

        task->wait_count--;

        if (task->exception)
            std::rethrow_exception(task->exception);
    }
}

void task_retain(Task *task) {
    if (task)
        task->pool->queue.retain(task);
}

void task_release(Task *task) {
    if (task)
        task->pool->queue.release(task, true);
}

void task_wait_and_release(Task *task) NANOTHREAD_THROW {
    try {
        task_wait(task);
    } catch (...) {
        task_release(task);
        throw;
    }
    task_release(task);
}

#if defined(_WIN32)
static double timer_frequency_scale = 0.0;
#endif

NANOTHREAD_EXPORT double task_time(Task *task) NANOTHREAD_THROW {
    if (!task)
        return 0;

#if defined(__APPLE__)
    return (task->time_end - task->time_start) * 1e-6;
#elif !defined(_WIN32)
    return (task->time_end.tv_sec - task->time_start.tv_sec) * 1e3 +
           (task->time_end.tv_nsec - task->time_start.tv_nsec) * 1e-6;
#else
    if (timer_frequency_scale == 0.0) {
        LARGE_INTEGER timer_frequency;
        QueryPerformanceFrequency(&timer_frequency);
        timer_frequency_scale = 1e3 / timer_frequency.QuadPart;
    }

    return timer_frequency_scale *
           (task->time_end.QuadPart - task->time_start.QuadPart);
#endif
}

Worker::Worker(Pool *pool, uint32_t id, bool ftz)
    : pool(pool), id(id), stop(false), ftz(ftz) {
    thread = std::thread(&Worker::run, this);
}

Worker::~Worker() { thread.join(); }

void Worker::run() {
    thread_id_tls = id;

    NT_TRACE("worker started");

    #if defined(_WIN32)
        wchar_t buf[24];
        _snwprintf(buf, sizeof(buf) / sizeof(wchar_t), L"nanothread worker %u", id);
        SetThreadDescription(GetCurrentThread(), buf);
    #else
        char buf[24];
        snprintf(buf, sizeof(buf), "nanothread worker %u", id);
        #if defined(__APPLE__)
            pthread_setname_np(buf);
        #else
            pthread_setname_np(pthread_self(), buf);
        #endif
    #endif

    FTZGuard ftz_guard(ftz);
    while (!stop)
        pool_execute_task(
            pool, [](void *ptr) -> bool { return *((bool *) ptr); }, &stop,
            true);

    NT_TRACE("worker stopped");

    thread_id_tls = 0;
}

