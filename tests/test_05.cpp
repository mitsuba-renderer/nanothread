/*
    tests/test_05.cpp -- After letting all workers park, submit batches of
    blocking work units and check that each batch runs fully in parallel.
    If the wake logic under-wakes, a batch serializes and overruns its budget.
*/

#include <nanothread/nanothread.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>

#if defined(_WIN32)
#  include <windows.h>
#else
#  include <unistd.h>
#endif

static void my_sleep(uint32_t ms) {
#if defined(_WIN32)
    Sleep(ms);
#else
    usleep(ms * 1000);
#endif
}

static const uint32_t DURATION_MS = 100;

/// Above nanothread's ~20 ms park threshold, so workers are asleep by then.
static const uint32_t IDLE_MS = 150;

/// Wake/scheduling slack; below DURATION_MS, so a single serialized unit
/// (>= 2 * DURATION_MS) is still caught.
static const double WAKE_BUDGET_MS = 80.0;

static void work_unit(uint32_t /* index */, void * /* payload */) {
    my_sleep(DURATION_MS);
}

int main(int, char **) {
    setvbuf(stdout, nullptr, _IONBF, 0); // survive abort() on failure

    Pool *pool = pool_create(NANOTHREAD_AUTO);

    // pool_size() counts the calling thread; it bounds achievable parallelism.
    uint32_t participants = pool_size(pool);

    fprintf(stderr, "test_05: %u participants (incl. calling thread), %u ms/unit\n",
           participants, DURATION_MS);

    const double limit_ms = DURATION_MS + WAKE_BUDGET_MS;

    bool ok = true;
    for (uint32_t k = 1;; k = (k * 2 <= participants) ? k * 2 : participants) {
        my_sleep(IDLE_MS); // let every worker park

        auto t0 = std::chrono::steady_clock::now();

        // always_async = 1 so k == 1 also goes through the queue/wake path.
        Task *task = task_submit(pool, k, work_unit, nullptr, 0, 0, 1);
        task_wait_and_release(task);

        auto t1 = std::chrono::steady_clock::now();
        double elapsed_ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();

        bool serialized = elapsed_ms > limit_ms;
        ok &= !serialized;

        fprintf(stderr, "  %4u unit(s): %8.1f ms%s\n", k, elapsed_ms,
               serialized ? "   <-- SERIALIZED!" : "");

        if (k == participants)
            break;
    }

    pool_destroy(pool);

    if (!ok) {
        fprintf(stderr, "test_05: FAILED -- one or more batches did not run in parallel\n");
        abort();
    }

    fprintf(stderr, "test_05: success\n");
    return 0;
}
