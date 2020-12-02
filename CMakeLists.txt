cmake_minimum_required(VERSION 3.13...3.18)

project(enoki-thread
  DESCRIPTION
    "Enoki thread library"
  LANGUAGES
    CXX C
)

option(ENOKI_THREAD_ENABLE_TESTS "Build test suite?" OFF)

include(cmake/EnokiDefaults.cmake)

add_library(
  enoki-thread SHARED
  include/enoki-thread/thread.h
  src/queue.cpp src/queue.h
  src/thread.cpp
)

target_compile_features(enoki-thread PRIVATE cxx_std_11)
target_include_directories(enoki-thread PRIVATE include)

if (CMAKE_SYSTEM_PROCESSOR MATCHES "amd64.*|x86_64.*|AMD64.*")
  if (CMAKE_CXX_COMPILER_ID MATCHES "GNU" OR CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(enoki-thread PRIVATE -mcx16)
  endif()
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  # GCC needs libatomic for 16 byte CSA
  find_library(LIBATOMIC NAMES libatomic.so libatomic.so.1)
  if (NOT LIBATOMIC)
    message(FATAL_ERROR "libatomic could not be found!")
  endif()
  target_link_libraries(enoki-thread PRIVATE ${LIBATOMIC})
endif()

target_include_directories(enoki-thread
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

if (NOT MSVC)
  target_compile_options(enoki-thread PRIVATE -Wall -Wextra)
  if (U_CMAKE_BUILD_TYPE MATCHES RELEASE OR U_CMAKE_BUILD_TYPE MATCHES MINSIZEREL)
    target_compile_options(enoki-thread PRIVATE -fvisibility=hidden)
  endif()
endif()

target_compile_definitions(enoki-thread PRIVATE -DENOKI_THREAD_BUILD=1)

if (ENOKI_THREAD_ENABLE_TESTS)
   add_subdirectory(tests)
endif()
