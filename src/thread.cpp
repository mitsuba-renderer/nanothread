/*
    src/pool.cpp -- Simple thread pool with task-based API

    Copyright (c) 2021 Wenzel Jakob <wenzel.jakob@epfl.ch>

    All rights reserved. Use of this source code is governed by a BSD-style
    license that can be found in the LICENSE file.
*/

#include <enoki-thread/thread.h>
#include "queue.h"
#include <thread>
#include <memory>
#include <type_traits>

#if defined(_WIN32)
#  include <windows.h>
#  include <processthreadsapi.h>
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

};

struct Worker {
    Pool *pool;
    std::thread thread;
    uint32_t id;
    bool stop;

    Worker(Pool *pool, uint32_t id);
    ~Worker();
    void run();
};


static Pool *pool_default_inst = nullptr;
static std::mutex pool_default_lock;

uint32_t pool_thread_id() {
    return thread_id_tls;
}

Pool *pool_default() {
    std::unique_lock<std::mutex> guard(pool_default_lock);

    if (!pool_default_inst)
        pool_default_inst = pool_create();

    return pool_default_inst;
}

Pool *pool_create(uint32_t size) {
    Pool *pool = new Pool();
    if (size == (uint32_t) -1)
        size = std::thread::hardware_concurrency();
    EKT_TRACE("pool_create(%p)", pool);
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
    if (!pool)
        pool = pool_default();

    return pool->workers.size();
}

void pool_set_size(Pool *pool, uint32_t size) {
    if (!pool)
        pool = pool_default();

    EKT_TRACE("pool_set_size(%p, %u)", pool, size);

    int diff = (int) size - (int) pool->workers.size();
    if (diff > 0) {
        // Launch extra worker threads
        for (int i = 0; i < diff; ++i)
            pool->workers.push_back(std::unique_ptr<Worker>(
                new Worker(pool, (uint32_t) pool->workers.size() + 1)));
    } else if (diff < 0) {
        // Remove worker threads (destructor calls join())
        for (int i = diff; i != 0; ++i)
            pool->workers[pool->workers.size() + i]->stop = true;
        pool->queue.wakeup();
        for (int i = diff; i != 0; ++i)
            pool->workers.pop_back();
    }
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
        EKT_TRACE("task_submit_dep(): task is small, executing right away.");

        if (func)
            func(0, payload);
        if (payload_deleter)
            payload_deleter(payload);
        return nullptr;
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
            EKT_ASSERT(task->payload != nullptr);
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
                              void *payload) {
    Task *task;
    uint32_t index;
    std::tie(task, index) = pool->queue.pop_or_sleep(stopping_criterion, payload);

    if (task) {
        if (task->func) {
            if (task->exception_used.load()) {
                EKT_TRACE(
                    "Not running callback (task=%p, index=%u) because another "
                    "work unit of this task generated an exception.",
                    task, index);
            } else {
                try {
                    EKT_TRACE("Running callback (task=%p, index=%u, payload=%p)", task, index, task->payload);
                    task->func(index, task->payload);
                } catch (...) {
                    bool value = false;
                    if (task->exception_used.compare_exchange_strong(value, true)) {
                        EKT_TRACE("Exception caught, storing..");
                        task->exception = std::current_exception();
                    } else {
                        EKT_TRACE("Exception caught, ignoring (an exception was already stored).");
                    }
                }
            }
        }

        pool->queue.release(task);
    }
}

void task_wait(Task *task) {
    if (task) {
        Pool *pool = task->pool;

        // Signal that we are waiting for this task
        task->wait_count++;

        auto stopping_criterion = [](void *ptr) -> bool {
            return (uint32_t)(((Task *) ptr)->refcount.load()) == 0;
        };

        EKT_TRACE("task_wait(%p)", task);

        // Help executing work units in the meantime
        while (!stopping_criterion(task))
            pool_execute_task(pool, stopping_criterion, task);

        task->wait_count--;

        if (task->exception)
            std::rethrow_exception(task->exception);
    }
}

void task_release(Task *task) {
    if (task)
        task->pool->queue.release(task, true);
}

void task_wait_and_release(Task *task) ENOKI_THREAD_THROW {
    try {
        task_wait(task);
    } catch (...) {
        task_release(task);
        throw;
    }
    task_release(task);
}

Worker::Worker(Pool *pool, uint32_t id)
    : pool(pool), id(id), stop(false) {
    thread = std::thread(&Worker::run, this);
}

Worker::~Worker() { thread.join(); }

void Worker::run() {
    thread_id_tls = id;

    EKT_TRACE("worker started.");

    #if defined(_WIN32)
        wchar_t buf[24];
        _snwprintf(buf, sizeof(buf) / sizeof(wchar_t), L"Enoki worker %u", id);
        SetThreadDescription(GetCurrentThread(), buf);
    #else
        char buf[24];
        snprintf(buf, sizeof(buf), "Enoki worker %u", id);
        #if defined(__APPLE__)
            pthread_setname_np(buf);
        #else
            pthread_setname_np(pthread_self(), buf);
        #endif
    #endif

    while (!stop)
        pool_execute_task(
            pool, [](void *ptr) -> bool { return *((bool *) ptr); }, &stop);

    EKT_TRACE("worker stopped.");

    thread_id_tls = 0;
}

