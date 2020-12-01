<p align="center"><img src="https://github.com/mitsuba-renderer/enoki/raw/master/docs/enoki-logo.png" alt="Enoki logo" width="300"/></p>

# Enoki-Thread — Minimal thread pool for task parallelism

## Introduction

Enoki-Thread provides a minimal cross-platform interface for task parallelism.
Given a computation that is partitioned into a set of interdependent tasks, the
library efficiently distributes this work to a thread pool, while respecting
dependencies between tasks.

Each task is associated with a callback function that is potentially invoked
multiple times if the task consists of multiple *work units*. This whole
process is arbitrarily recursive: task callbacks can submit further jobs, wait
for their completion, etc. Parallel loops, reductions, and more complex
graph-based computations are easily realized using these abstractions.

This project is internally implemented in C++11, but exposes the main
functionality using a pure C99 API, along with a header-only C++11 convenience
wrapper. It has no dependencies other than CMake and a C++11-capable compiler.

Enoki is used by [Enoki-JIT](https://github.com/mitsuba-renderer/enoki-jit),
which is in turn part of the [Enoki](https://github.com/mitsuba-renderer/enoki)
project, hence the name. However, this project has no dependencies on these
parent projects and can be used in any other context.

## Why?

Many of my previous projects have built on Intel's Thread Building Blocks for
exactly this type of functionality. Unfortunately, large portions of TBB's task
interface were recently deprecated as part of the oneAPI / oneTBB transition.
Rather than struggling with this complex dependency, I decided to build
something minimal and stable that satisfies my requirements.

## Examples (C99)

The following code fragment submits a single task consisting of 100 work units
and waits for its completion.

```c
#include <enoki-thread/thread.h>
#include <stdio.h>
#include <unistd.h>

// Task callback function. Will be called with index = 0..99
void my_task(uint32_t index, void *payload) {
    printf("Worker thread %u is starting to process work unit %u\n",
           pool_thread_id(), index);

    // Sleep for a bit
    usleep(1000000);

    // Use payload to communicate some data to the caller
    ((uint32_t *) payload)[index] = index;
}

int main(int argc, char** argv) {
    uint32_t temp[100];

    // Create a worker per CPU thread
    Pool *pool = pool_create(ENOKI_THREAD_AUTO); 

    // Synchronous interface: submit a task and wait for it to complete
    task_submit_and_wait(
        pool,
        100,     // How many work units does this task contain?
        my_task, // Function to be executed 
        temp     // Optional payload, will be passed to function 
    );

    // .. contents of 'temp' are now ready .. 

    // Clean up used resources
    pool_destroy(pool);
}
```

Tasks can also be executed *asynchronously*, in which case extra steps must be
added to wait for tasks, and to release task handles.

```c
/// Heap-allocate scratch space for inter-task communication
uint32_t *payload = malloc(100 * sizeof(uint32_t));

/// Submit a task and return immediately
Task *task_1 = task_submit(
    pool,
    100,       // How many work units does this task contain?
    my_task_1, // Function to be executed 
    payload,   // Optional payload, will be passed to function 
    0,         // Size of the payload (only relevant if it should be copied)
    nullptr    // Payload deletion callback
);

/// Submit a task that is dependent on other tasks (specifically task_1)
Task *task_2 = task_submit_dep(
    pool,
    &task_1,   // Pointer to a list of parent tasks
    1,         // Number of parent tasks
    100,       // How many work units does this task contain?
    my_task_2, // Function to be executed 
    payload,   // Optional payload, will be passed to function 
    0,         // Size of the payload (only relevant if it should be copied)
    free       // Call free(payload) once this task completes
);

/* Now that the parent-child relationship is specified,
   the handle of task 1 can be released */
task_release(task_1);

// Wait for the completion of task 2 and also release its handle
task_wait_and_release(task_2);
```

## Examples (C++11)


## Documentation

The complete API is documented in the file
[enoki-thread/thread.h](https://github.com/mitsuba-renderer/enoki-thread/blob/master/include/enoki-thread/thread.h).

## Technical details

This library follows a lock-free design: tasks that are ready for execution are
stored in a [Michael-Scott
queue](https://www.cs.rochester.edu/u/scott/papers/1996_PODC_queues.pdf) that
is continuously polled by workers, and task submission/removal relies on atomic
compare-and-swap (CAS) operations. Workers that idle for more than roughly 50
milliseconds are put to sleep until more work becomes available.

The lock-free design is important: the central data structures of a task
submission system are heavily contended, and traditional abstractions (e.g.
``std::mutex``) will immediately put contending threads to sleep to defer lock
resolution to the OS kernel. The associated context switches produce an
extremely large overhead that can make a parallel program orders of magnitude
slower than a single-threaded version.

The implementation catches exception that occur while executing parallel work
and re-throws them the caller's thread. Worker threads can be pinned to CPU
cores on Linux and Windows.
