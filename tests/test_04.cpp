#include <enoki-thread/thread.h>
#include <stdexcept>

namespace ek = enoki;

void test01() {
    try {
        ek::parallel_for(
            ek::blocked_range<uint32_t>(0, 1000, 5),
            [](ek::blocked_range<uint32_t> range) {
                throw std::runtime_error("Hello world!");
            }
        );
    } catch (std::exception &e) {
        printf("Test 1: success: %s\n", e.what());
        return;
    }
    abort();
}

void test02() {
    auto work1 = ek::parallel_for_async(
        ek::blocked_range<uint32_t>(0, 1000, 5),
        [](ek::blocked_range<uint32_t> range) {
            throw std::runtime_error("Hello world!");
        }
    );

    auto work2 = ek::parallel_for_async(
        ek::blocked_range<uint32_t>(0, 1000, 5),
        [](ek::blocked_range<uint32_t> range) {
            printf("Should never get here!\n");
            abort();
        },
        { work1 }
    );

    task_release(work1);

    try {
        task_wait_and_release(work2);
    } catch (std::exception &e) {
        printf("Test 2: success: %s\n", e.what());
        return;
    }
    abort();
}

int main(int arc, char** argv) {
    test01();
    test02();
}
