# ----------------------------------------------------------
# Top-level nanothread CMake file, needs a recent version
# ----------------------------------------------------------
cmake_minimum_required(VERSION 3.13...3.18)

project(nanothread
  DESCRIPTION
    "nanothread"
  LANGUAGES
    CXX C
)

# Is nanothread the top-level project, or embedded via add_subdirectory()?
if (CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(NANOTHREAD_MASTER_PROJECT ON)
else()
  set(NANOTHREAD_MASTER_PROJECT OFF)
endif()

# ----------------------------------------------------------
#  Optional features available to users
# ----------------------------------------------------------

option(NANOTHREAD_STATIC "Build as static library?" OFF)
option(NANOTHREAD_ENABLE_TESTS "Build test suite?" OFF)
option(NANOTHREAD_INSTALL "Install nanothread?" ${NANOTHREAD_MASTER_PROJECT})

# Trace logging is noisy and meant for debugging nanothread itself, so only
# expose it when nanothread is the top-level project.
include(CMakeDependentOption)
cmake_dependent_option(NANOTHREAD_ENABLE_TRACE
  "Print trace-level debug messages to stderr?" OFF
  "NANOTHREAD_MASTER_PROJECT" OFF)

# ----------------------------------------------------------
#  Check if submodules have been checked out, or fail early
# ----------------------------------------------------------

if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/ext/cmake-defaults/CMakeLists.txt")
  message(FATAL_ERROR "The nanothread dependencies are missing! "
    "You probably did not clone the project with --recursive. It is possible to recover "
    "by invoking\n$ git submodule update --init --recursive")
endif()

# ----------------------------------------------------------
#  Build defaults for projects by the Realistic Graphics Lab
# ----------------------------------------------------------

include(ext/cmake-defaults/CMakeLists.txt)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# ----------------------------------------------------------
#  Compile the nanothread library
# ----------------------------------------------------------

if(NANOTHREAD_STATIC)
  add_library(nanothread STATIC)
  target_compile_definitions(nanothread PUBLIC -DNANOTHREAD_STATIC)
else()
  add_library(nanothread SHARED)
endif()

target_sources(nanothread PRIVATE
  include/nanothread/nanothread.h
  src/queue.cpp src/queue.h
  src/park.cpp src/park.h
  src/nanothread.cpp
)

target_compile_features(nanothread PRIVATE cxx_std_17)
target_include_directories(nanothread PRIVATE include)
target_link_libraries(nanothread PRIVATE Threads::Threads)

if (CMAKE_SYSTEM_PROCESSOR MATCHES "amd64.*|x86_64.*|AMD64.*")
  if (CMAKE_CXX_COMPILER_ID MATCHES "GNU" OR CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(nanothread PRIVATE -mcx16)
  endif()
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  # GCC needs libatomic for 16 byte CSA
  find_library(LIBATOMIC NAMES atomic)
  if (NOT LIBATOMIC)
    message(FATAL_ERROR "libatomic could not be found!")
  endif()
  target_link_libraries(nanothread PRIVATE atomic)
  mark_as_advanced(LIBATOMIC)
endif()

if (WIN32)
  # WaitOnAddress / WakeByAddressAll live in Synchronization.lib (Win 8+).
  target_link_libraries(nanothread PRIVATE Synchronization)
endif()

target_include_directories(nanothread
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_compile_definitions(nanothread PRIVATE -DNANOTHREAD_BUILD=1)
set_target_properties(nanothread PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)

if (NANOTHREAD_ENABLE_TRACE)
  # Activates the NT_TRACE() messages defined in src/queue.h.
  target_compile_definitions(nanothread PRIVATE NT_DEBUG)
endif()

if (NANOTHREAD_ENABLE_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if (NANOTHREAD_INSTALL)
  set(NANOTHREAD_CMAKE_INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/nanothread")
  file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/nanothreadConfig.cmake" [=[
include(CMakeFindDependencyMacro)
find_dependency(Threads)
include("${CMAKE_CURRENT_LIST_DIR}/nanothreadTargets.cmake")
]=])
  install(
    FILES
    "${CMAKE_CURRENT_BINARY_DIR}/nanothreadConfig.cmake"
    DESTINATION
      "${NANOTHREAD_CMAKE_INSTALL_DIR}"
  )
  install(
    TARGETS
    nanothread
    EXPORT
      nanothreadTargets
    RUNTIME DESTINATION
      "${CMAKE_INSTALL_BINDIR}"
    LIBRARY DESTINATION
      "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION
      "${CMAKE_INSTALL_LIBDIR}"
  )
  install(
    DIRECTORY
    include/nanothread
    DESTINATION
      "${CMAKE_INSTALL_INCLUDEDIR}"
  )
  install(
    EXPORT
    nanothreadTargets
    FILE
      nanothreadTargets.cmake
    DESTINATION
      "${NANOTHREAD_CMAKE_INSTALL_DIR}"
  )
endif()
