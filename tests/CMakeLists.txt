add_executable(test_01 test_01.c)
target_link_libraries(test_01 PRIVATE enoki-thread)

add_executable(test_02 test_02.cpp)
target_link_libraries(test_02 PRIVATE enoki-thread)
target_compile_features(test_02 PRIVATE cxx_std_17)
