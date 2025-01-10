cmake_minimum_required(VERSION 3.25)

find_package(cmake-fetch REQUIRED PATHS node_modules/cmake-fetch)

project(rocksdb C CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(PORTABLE ON CACHE BOOL "Make a portable RocksDB build" FORCE)

set(WITH_TESTS OFF CACHE BOOL "Build the RocksDB test suite" FORCE)

set(WITH_GFLAGS OFF CACHE BOOL "Build RocksDB with gflags support" FORCE)

set(FAIL_ON_WARNINGS OFF CACHE BOOL "Treat compile warnings as errors" FORCE)

set(ROCKSDB_SKIP_THIRDPARTY ON CACHE BOOL "Skip third-party RocksDB integrations" FORCE)

set(ARTIFACT_SUFFIX _facebook) # Ensure that upstream RocksDB targets don't clash

fetch_package("github:libuv/libuv@1.48.0")
fetch_package("github:facebook/rocksdb@9.10.0")
fetch_package("github:holepunchto/libpath")
fetch_package("github:holepunchto/libintrusive")

# Neuter the C bindings from RocksDB to ensure they don't collide with ours
set_source_files_properties(
  ${rocksdb_dir}/db/c.cc
  TARGET_DIRECTORY rocksdb_facebook
  PROPERTIES
  HEADER_FILE_ONLY ON
)

add_library(rocksdb OBJECT)

set_target_properties(
  rocksdb
  PROPERTIES
  C_STANDARD 99
  CXX_STANDARD 20
  POSITION_INDEPENDENT_CODE ON
)

target_sources(
  rocksdb
  INTERFACE
    include/rocksdb.h
  PRIVATE
    src/rocksdb.cc
)

target_include_directories(
  rocksdb
  INTERFACE
    include
  PUBLIC
    $<TARGET_PROPERTY:uv,INTERFACE_INCLUDE_DIRECTORIES>
)

target_link_libraries(
  rocksdb
  PRIVATE
    rocksdb_facebook
  PUBLIC
    path
    intrusive
)

add_library(rocksdb_shared SHARED)

set_target_properties(
  rocksdb_shared
  PROPERTIES
  OUTPUT_NAME rocksdb
  WINDOWS_EXPORT_ALL_SYMBOLS ON
)

target_link_libraries(
  rocksdb_shared
  PUBLIC
    uv
    rocksdb
  PRIVATE
    rocksdb_facebook
    path_shared
)

add_library(rocksdb_static STATIC)

set_target_properties(
  rocksdb_static
  PROPERTIES
  OUTPUT_NAME rocksdb
  PREFIX lib
)

target_link_libraries(
  rocksdb_static
  PUBLIC
    uv_a
    rocksdb
  PRIVATE
    rocksdb_facebook
    path_static
)

install(TARGETS rocksdb_shared rocksdb_static)

install(FILES include/rocksdb.h DESTINATION include)

if(PROJECT_IS_TOP_LEVEL)
  enable_testing()

  add_subdirectory(test)
endif()
