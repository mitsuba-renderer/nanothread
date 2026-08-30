/*
    tests/test_06.cpp -- Stress test for task_wait_exclusive()

    Checks that an exclusive wait executes every work unit of the target task
    exactly once, that the waiting thread never runs work units of other
    tasks, and that the mechanism holds up with worker-less pools, parked
    waiters, exceptions, and many concurrent waiters.
*/

#include <nanothread/nanothread.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

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

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "test_06: check failed in %s:%i: %s\n", __FILE__,  \
                    __LINE__, #cond);                                          \
            abort();                                                           \
        }                                                                      \
    } while (0)

/// Set on a thread while it sits in task_wait_exclusive(). Work units use it
/// to detect that a foreign unit ran on an exclusively waiting thread.
static thread_local const void *tl_exclusive_target = nullptr;

/// Total number of exclusivity violations (must stay zero)
static std::atomic<uint32_t> violations{0};

/// Number of target work units that ran on their exclusively waiting thread
static std::atomic<uint64_t> waiter_units{0};

struct WorkPayload {
    std::unique_ptr<std::atomic<uint32_t>[]> hits;
    uint32_t size = 0;
    uint32_t spin = 0;          // busy-loop iterations per unit
    uint32_t sleep_ms = 0;      // sleep per unit
    uint32_t throw_index = UINT32_MAX;

    WorkPayload(uint32_t size, uint32_t spin = 0, uint32_t sleep_ms = 0)
        : hits(new std::atomic<uint32_t>[size ? size : 1]), size(size),
          spin(spin), sleep_ms(sleep_ms) {
        for (uint32_t i = 0; i < (size ? size : 1); ++i)
            hits[i].store(0);
    }

    void check_all_ran_once() const {
        for (uint32_t i = 0; i < size; ++i)
            CHECK(hits[i].load() == 1);
    }

    void check_none_ran() const {
        for (uint32_t i = 0; i < size; ++i)
            CHECK(hits[i].load() == 0);
    }

    void check_ran_at_most_once() const {
        for (uint32_t i = 0; i < size; ++i)
            CHECK(hits[i].load() <= 1);
    }
};

static void work_cb(uint32_t index, void *payload) {
    WorkPayload *p = (WorkPayload *) payload;

    if (tl_exclusive_target) {
        if (tl_exclusive_target != payload)
            violations.fetch_add(1);
        else
            waiter_units.fetch_add(1);
    }

    if (p->spin) {
        volatile uint32_t sum = 0;
        for (uint32_t i = 0; i < p->spin; ++i)
            sum += i;
    }

    if (p->sleep_ms)
        my_sleep(p->sleep_ms);

    CHECK(index < p->size);
    p->hits[index].fetch_add(1);

    if (index == p->throw_index)
        throw std::runtime_error("intentional failure");
}

static Task *submit(Pool *pool, WorkPayload &p) {
    // always_async = 1 so that size 1 also goes through the queue
    return task_submit(pool, p.size, work_cb, &p, 0, nullptr, 1);
}

static void wait_exclusive(Task *task, WorkPayload &p) {
    tl_exclusive_target = &p;
    task_wait_exclusive(task);
    tl_exclusive_target = nullptr;
}

/// Worker-less pool: the exclusive waiter must run everything by itself and
/// must not touch a foreign task queued ahead of the target.
static void test_workerless(uint32_t iterations) {
    Pool *pool = pool_create(0);

    for (uint32_t i = 0; i < iterations; ++i) {
        WorkPayload foreign(1 + i % 17), target(1 + i % 67);

        Task *ft = submit(pool, foreign),
             *tt = submit(pool, target);

        uint64_t before = waiter_units.load();
        wait_exclusive(tt, target);
        task_release(tt);

        target.check_all_ran_once();
        foreign.check_none_ran();

        // Without workers, the waiting thread must have run every unit
        CHECK(waiter_units.load() - before == target.size);

        task_wait_and_release(ft);
        foreign.check_all_ran_once();
    }

    pool_destroy(pool);
    fprintf(stderr, "test_06: test_workerless passed\n");
}

/// Pool with workers: hammer the queue with foreign tasks while the main
/// thread waits exclusively on a target
static void test_exclusivity_under_load(uint32_t iterations) {
    Pool *pool = pool_create(NANOTHREAD_AUTO);
    std::mt19937 rng(42);

    for (uint32_t i = 0; i < iterations; ++i) {
        std::vector<std::unique_ptr<WorkPayload>> fp;
        std::vector<Task *> ft;

        uint32_t n_foreign = 1 + rng() % 8;
        for (uint32_t j = 0; j < n_foreign; ++j) {
            fp.emplace_back(new WorkPayload(1 + rng() % 32, 1000));
            ft.push_back(submit(pool, *fp.back()));
        }

        WorkPayload target(1 + rng() % 256, 500);
        Task *tt = submit(pool, target);

        wait_exclusive(tt, target);
        target.check_all_ran_once();
        task_release(tt);

        for (uint32_t j = 0; j < n_foreign; ++j) {
            task_wait_and_release(ft[j]);
            fp[j]->check_all_ran_once();
        }
    }

    pool_destroy(pool);
    fprintf(stderr, "test_06: test_exclusivity_under_load passed\n");
}

/// Force the waiter to park: workers grab all units and hold them for longer
/// than the 20 ms spin window
static void test_parked_waiter() {
    Pool *pool = pool_create(NANOTHREAD_AUTO);
    uint32_t participants = pool_size(pool);

    for (uint32_t i = 0; i < 3; ++i) {
        WorkPayload target(participants > 1 ? participants - 1 : 1, 0, 60);
        Task *tt = submit(pool, target);

        my_sleep(5); // let the workers claim every unit first
        wait_exclusive(tt, target);
        target.check_all_ran_once();
        task_release(tt);
    }

    pool_destroy(pool);
    fprintf(stderr, "test_06: test_parked_waiter passed\n");
}

/// Exceptions must propagate out of task_wait_exclusive()
static void test_exceptions() {
    for (uint32_t size : { 0u, 1u, 13u }) {
        Pool *pool = pool_create(size);

        WorkPayload target(64);
        target.throw_index = 17;
        Task *tt = submit(pool, target);

        bool caught = false;
        try {
            tl_exclusive_target = &target;
            task_wait_and_release_exclusive(tt);
        } catch (std::runtime_error &) {
            caught = true;
        }
        tl_exclusive_target = nullptr;

        CHECK(caught);
        target.check_ran_at_most_once();
        CHECK(target.hits[17].load() == 1);

        // Trivial cases: nullptr and an already completed task
        task_wait_exclusive(nullptr);

        WorkPayload target2(8);
        Task *tt2 = submit(pool, target2);
        task_wait(tt2);
        task_wait_exclusive(tt2);
        target2.check_all_ran_once();
        task_release(tt2);

        pool_destroy(pool);
    }
    fprintf(stderr, "test_06: test_exceptions passed\n");
}

/// Many threads wait on the same task, some exclusively, some not
static void test_concurrent_waiters(uint32_t iterations) {
    Pool *pool = pool_create(4);

    for (uint32_t i = 0; i < iterations; ++i) {
        WorkPayload target(512, 2000);
        Task *tt = submit(pool, target);

        std::vector<std::thread> threads;
        for (uint32_t j = 0; j < 6; ++j) {
            threads.emplace_back([&, j] {
                if (j % 2 == 0)
                    wait_exclusive(tt, target);
                else
                    task_wait(tt);
            });
        }

        for (auto &t : threads)
            t.join();

        target.check_all_ran_once();
        task_release(tt);
    }

    pool_destroy(pool);
    fprintf(stderr, "test_06: test_concurrent_waiters passed\n");
}

/// Randomized mix: several external threads submit foreign work and wait
/// exclusively on their own targets, with plain waits sprinkled in
static void test_random_stress(uint32_t pool_threads, double duration_sec) {
    Pool *pool = pool_create(pool_threads);
    uint32_t resolved_size = pool_size(pool);
    std::atomic<bool> stop{false};

    auto worker = [&](uint32_t seed) {
        std::mt19937 rng(seed);

        while (!stop.load()) {
            std::vector<std::unique_ptr<WorkPayload>> fp;
            std::vector<Task *> ft;

            uint32_t n_foreign = rng() % 4;
            for (uint32_t j = 0; j < n_foreign; ++j) {
                fp.emplace_back(new WorkPayload(1 + rng() % 16, rng() % 500));
                ft.push_back(submit(pool, *fp.back()));
            }

            WorkPayload target(1 + rng() % 64, rng() % 500);
            bool throws = rng() % 10 == 0;
            if (throws)
                target.throw_index = rng() % target.size;

            Task *tt = submit(pool, target);

            bool caught = false;
            try {
                if (rng() % 4 == 0) {
                    task_wait(tt);
                } else {
                    wait_exclusive(tt, target);
                }
            } catch (std::runtime_error &) {
                caught = true;
            }
            tl_exclusive_target = nullptr;
            task_release(tt);

            CHECK(caught == throws);
            if (throws)
                target.check_ran_at_most_once();
            else
                target.check_all_ran_once();

            for (uint32_t j = 0; j < n_foreign; ++j) {
                task_wait_and_release(ft[j]);
                fp[j]->check_all_ran_once();
            }
        }
    };

    std::vector<std::thread> threads;
    for (uint32_t j = 0; j < 4; ++j)
        threads.emplace_back(worker, j + 1);

    my_sleep((uint32_t) (duration_sec * 1000));
    stop.store(true);

    for (auto &t : threads)
        t.join();

    pool_destroy(pool);
    fprintf(stderr, "test_06: test_random_stress (pool size %u) passed\n",
            resolved_size);
}

int main(int argc, char **argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);
    bool quick = argc > 1 && strcmp(argv[1], "quick") == 0;

    uint32_t mult = quick ? 1 : 5;
    double stress_sec = quick ? 1.0 : 3.0;

    test_workerless(100 * mult);
    test_exclusivity_under_load(60 * mult);
    test_parked_waiter();
    test_exceptions();
    test_concurrent_waiters(20 * mult);
    test_random_stress(0, stress_sec);
    test_random_stress(2, stress_sec);
    test_random_stress(NANOTHREAD_AUTO, stress_sec);

    CHECK(violations.load() == 0);

    fprintf(stderr,
            "test_06: success (%llu work units ran on exclusive waiters)\n",
            (unsigned long long) waiter_units.load());
    return 0;
}
