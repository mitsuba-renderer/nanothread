#include <enoki-thread/thread.h>
#include <stdexcept>

namespace ek = enoki;

int main(int arc, char** argv) {
    try {
        ek::parallel_for(
            ek::blocked_range<uint32_t>(0, 1000, 5),
            [](ek::blocked_range<uint32_t> range) {
                throw std::runtime_error("Hello world!");
            }
        );
    } catch (std::exception &e) {
        printf("Success: %s\n", e.what());
        return 0;
    }
    abort();
}
