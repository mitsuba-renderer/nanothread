# ----------------------------------------------------------
# Top-level Enoki-Thread CMake file, needs a recent version
# ----------------------------------------------------------
cmake_minimum_required(VERSION 3.13...3.18)

project(enoki-thread
  DESCRIPTION
    "Enoki-Thread"
  LANGUAGES
    CXX C
)

# ----------------------------------------------------------
#  Optional features available to users
# ----------------------------------------------------------

option(ENOKI_THREAD_ENABLE_TESTS "Build test suite?" OFF)

# ----------------------------------------------------------
#  Check if submodules have been checked out, or fail early
# ----------------------------------------------------------

if (NOT IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/ext/cmake-defaults")
  message(FATAL_ERROR "The Enoki-JIT dependencies are missing! "
    "You probably did not clone the project with --recursive. It is possible to recover "
    "by invoking\n$ git submodule update --init --recursive")
endif()

# ----------------------------------------------------------
#  Build defaults for projects by the Realistic Graphics Lab
# ----------------------------------------------------------

include(ext/cmake-defaults/CMakeLists.txt)

# ----------------------------------------------------------
#  Compile the Enoki-Thread library
# ----------------------------------------------------------

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
  mark_as_advanced(LIBATOMIC)
endif()

target_include_directories(enoki-thread
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_compile_definitions(enoki-thread PRIVATE -DENOKI_THREAD_BUILD=1)
set_target_properties(enoki-thread PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)

if (ENOKI_THREAD_ENABLE_TESTS)
   add_subdirectory(tests)
endif()
