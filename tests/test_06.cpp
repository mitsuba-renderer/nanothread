/*
    tests/test_06.cpp -- Stress test for task_wait_exclusive[_n]()

    Checks that an exclusive wait executes every work unit of the target
    tasks exactly once, that the waiting thread never runs work units of
    other tasks, and that the mechanism holds up with worker-less pools,
    dependencies, parked waiters, exceptions, and many concurrent waiters.
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

/// Payloads that a thread may execute while it sits in an exclusive wait.
/// Work units use it to detect that a foreign unit ran on such a thread.
static thread_local const void *const *tl_set = nullptr;
static thread_local size_t tl_set_size = 0;

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
    const char *throw_msg = "intentional failure";

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

    if (tl_set) {
        bool member = false;
        for (size_t i = 0; i < tl_set_size; ++i)
            member |= tl_set[i] == payload;
        if (member)
            waiter_units.fetch_add(1);
        else
            violations.fetch_add(1);
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
        throw std::runtime_error(p->throw_msg);
}

static Task *submit(Pool *pool, WorkPayload &p,
                    std::initializer_list<Task *> parents = {}) {
    // always_async = 1 so that size 1 also goes through the queue
    return task_submit_dep(pool, parents.begin(), (uint32_t) parents.size(),
                           p.size, work_cb, &p, 0, nullptr, 1);
}

/// Restricts the payloads that may run on this thread during an exclusive wait
struct ExclusiveGuard {
    ExclusiveGuard(const void *const *set, size_t size) {
        tl_set = set;
        tl_set_size = size;
    }

    ~ExclusiveGuard() {
        tl_set = nullptr;
        tl_set_size = 0;
    }
};

static void wait_exclusive(Task *task, WorkPayload &p) {
    const void *set[] = { &p };
    ExclusiveGuard guard(set, 1);
    task_wait_exclusive(task);
}

static void wait_exclusive_n(const std::vector<Task *> &tasks,
                             const std::vector<const void *> &set) {
    ExclusiveGuard guard(set.data(), set.size());
    task_wait_exclusive_n(tasks.size(), tasks.data());
}

/// Worker-less pool: the exclusive waiter must run everything by itself and
/// must not touch a foreign task queued ahead of the targets. The array
/// variant must additionally resolve dependencies among the listed tasks.
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

        // Without workers, the waiting thread must have run every unit
        CHECK(waiter_units.load() - before == target.size);

        // Dependency diamond a -> {b, c} -> d, listed parents-first
        WorkPayload a(1 + i % 7), b(1 + i % 13), c(1 + i % 5), d(1 + i % 31);

        Task *ta = submit(pool, a),
             *tb = submit(pool, b, { ta }),
             *tc = submit(pool, c, { ta }),
             *td = submit(pool, d, { tb, tc });

        before = waiter_units.load();
        wait_exclusive_n({ ta, tb, tc, td }, { &a, &b, &c, &d });

        a.check_all_ran_once(); b.check_all_ran_once();
        c.check_all_ran_once(); d.check_all_ran_once();
        CHECK(waiter_units.load() - before ==
              a.size + b.size + c.size + d.size);

        for (Task *t : { ta, tb, tc, td })
            task_release(t);

        foreign.check_none_ran();
        task_wait_and_release(ft);
        foreign.check_all_ran_once();
    }

    pool_destroy(pool);
    fprintf(stderr, "test_06: test_workerless passed\n");
}

/// Pool with workers: hammer the queue with foreign tasks while the main
/// thread waits exclusively on one or several targets
static void test_exclusivity_under_load(uint32_t iterations) {
    Pool *pool = pool_create(NANOTHREAD_AUTO);
    std::mt19937 rng(42);

    for (uint32_t i = 0; i < iterations; ++i) {
        std::vector<std::unique_ptr<WorkPayload>> fp, tp;
        std::vector<Task *> ft, tt;
        std::vector<const void *> set;

        uint32_t n_foreign = 1 + rng() % 8;
        for (uint32_t j = 0; j < n_foreign; ++j) {
            fp.emplace_back(new WorkPayload(1 + rng() % 32, 1000));
            ft.push_back(submit(pool, *fp.back()));
        }

        uint32_t n_targets = (i % 2) ? 1 + rng() % 6 : 1;
        for (uint32_t j = 0; j < n_targets; ++j) {
            tp.emplace_back(new WorkPayload(1 + rng() % 64, 500));
            tt.push_back(submit(pool, *tp.back()));
            set.push_back(tp.back().get());
        }

        if (i % 2)
            wait_exclusive_n(tt, set);
        else
            wait_exclusive(tt[0], *tp[0]);

        for (uint32_t j = 0; j < n_targets; ++j) {
            tp[j]->check_all_ran_once();
            task_release(tt[j]);
        }

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
    uint32_t n = participants > 1 ? participants - 1 : 1;

    for (uint32_t i = 0; i < 4; ++i) {
        if (i % 2) {
            WorkPayload target(n, 0, 60);
            Task *tt = submit(pool, target);

            my_sleep(5); // let the workers claim every unit first
            wait_exclusive(tt, target);
            target.check_all_ran_once();
            task_release(tt);
        } else {
            // Same, with the units spread over two tasks
            WorkPayload a((n + 1) / 2, 0, 60), b(n / 2 + 1, 0, 60);
            Task *ta = submit(pool, a), *tb = submit(pool, b);

            my_sleep(5);
            wait_exclusive_n({ ta, tb }, { &a, &b });
            a.check_all_ran_once();
            b.check_all_ran_once();
            task_release(ta);
            task_release(tb);
        }
    }

    pool_destroy(pool);
    fprintf(stderr, "test_06: test_parked_waiter passed\n");
}

/// A listed task with an unlisted parent: the waiter must wait passively
/// until the workers complete the parent, and must not execute its units
static void test_unlisted_parent() {
    Pool *pool = pool_create(NANOTHREAD_AUTO);

    for (uint32_t i = 0; i < 3; ++i) {
        WorkPayload parent(4, 0, 30), child(8);
        Task *tp = submit(pool, parent);
        Task *tc = submit(pool, child, { tp });

        wait_exclusive_n({ tc }, { &child });
        parent.check_all_ran_once();
        child.check_all_ran_once();

        task_release(tp);
        task_release(tc);
    }

    pool_destroy(pool);
    fprintf(stderr, "test_06: test_unlisted_parent passed\n");
}

/// Exceptions must propagate out of the exclusive waits. The array variant
/// re-raises the first exception in array order.
static void test_exceptions() {
    for (uint32_t size : { 0u, 1u, 13u }) {
        Pool *pool = pool_create(size);

        WorkPayload target(64);
        target.throw_index = 17;
        Task *tt = submit(pool, target);

        bool caught = false;
        try {
            const void *set[] = { &target };
            ExclusiveGuard guard(set, 1);
            task_wait_and_release_exclusive(tt);
        } catch (std::runtime_error &) {
            caught = true;
        }

        CHECK(caught);
        target.check_ran_at_most_once();
        CHECK(target.hits[17].load() == 1);

        WorkPayload a(32), b(16), c(32);
        b.throw_index = 3;  b.throw_msg = "first";
        c.throw_index = 11; c.throw_msg = "second";

        Task *ta = submit(pool, a), *tb = submit(pool, b),
             *tc = submit(pool, c);

        bool caught_first = false;
        try {
            wait_exclusive_n({ ta, tb, tc }, { &a, &b, &c });
        } catch (std::runtime_error &e) {
            caught_first = strcmp(e.what(), "first") == 0;
        }

        CHECK(caught_first);
        a.check_all_ran_once();
        b.check_ran_at_most_once();
        c.check_ran_at_most_once();
        CHECK(b.hits[3].load() == 1 && c.hits[11].load() == 1);

        for (Task *t : { ta, tb, tc })
            task_release(t);

        // Degenerate inputs: null and empty arrays, repeated entries, and
        // tasks that have already completed
        task_wait_exclusive(nullptr);
        task_wait_exclusive_n(0, nullptr);
        Task *nulls[3] = { nullptr, nullptr, nullptr };
        task_wait_exclusive_n(3, nulls);

        WorkPayload d(8);
        Task *td = submit(pool, d);
        wait_exclusive_n({ nullptr, td, td, nullptr, td }, { &d });
        d.check_all_ran_once();
        wait_exclusive_n({ td, td }, { &d });
        task_wait_exclusive(td);
        d.check_all_ran_once();
        task_release(td);

        pool_destroy(pool);
    }
    fprintf(stderr, "test_06: test_exceptions passed\n");
}

/// Many threads wait on the same tasks, some exclusively, some not
static void test_concurrent_waiters(uint32_t iterations) {
    Pool *pool = pool_create(4);

    for (uint32_t i = 0; i < iterations; ++i) {
        WorkPayload a(256, 2000), b(256, 2000);
        Task *ta = submit(pool, a), *tb = submit(pool, b);

        std::vector<std::thread> threads;
        for (uint32_t j = 0; j < 6; ++j) {
            threads.emplace_back([&, j] {
                switch (j % 3) {
                    case 0:
                        wait_exclusive_n({ ta, tb }, { &a, &b });
                        break;

                    case 1:
                        wait_exclusive(ta, a);
                        wait_exclusive(tb, b);
                        break;

                    default:
                        task_wait(ta);
                        task_wait(tb);
                        break;
                }
            });
        }

        for (auto &t : threads)
            t.join();

        a.check_all_ran_once();
        b.check_all_ran_once();
        task_release(ta);
        task_release(tb);
    }

    pool_destroy(pool);
    fprintf(stderr, "test_06: test_concurrent_waiters passed\n");
}

/// Randomized mix: several external threads submit foreign work and wait on
/// chains of their own targets, listed parents-first, with plain waits
/// sprinkled in
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

            uint32_t depth = 1 + rng() % 4;
            bool throws = rng() % 10 == 0;
            uint32_t throw_depth = throws ? rng() % depth : 0;

            std::vector<std::unique_ptr<WorkPayload>> tp;
            std::vector<Task *> tt;
            std::vector<const void *> set;

            for (uint32_t j = 0; j < depth; ++j) {
                tp.emplace_back(new WorkPayload(1 + rng() % 32, rng() % 500));
                if (throws && j == throw_depth)
                    tp.back()->throw_index = rng() % tp.back()->size;
                tt.push_back(j == 0 ? submit(pool, *tp.back())
                                    : submit(pool, *tp.back(), { tt.back() }));
                set.push_back(tp.back().get());
            }

            // The single-task waits only apply to a chain of length 1
            uint32_t mode = depth == 1 ? rng() % 3 : 2;

            bool caught = false;
            try {
                switch (mode) {
                    case 0:  task_wait(tt[0]); break;
                    case 1:  wait_exclusive(tt[0], *tp[0]); break;
                    default: wait_exclusive_n(tt, set); break;
                }
            } catch (std::runtime_error &) {
                caught = true;
            }

            CHECK(caught == throws);
            for (uint32_t j = 0; j < depth; ++j) {
                if (throws)
                    tp[j]->check_ran_at_most_once();
                else
                    tp[j]->check_all_ran_once();
                task_release(tt[j]);
            }

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
    test_unlisted_parent();
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
