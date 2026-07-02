list(APPEND tests
  approximate-size
  compact
  compact-range
  compact-range-blobs
  create-folders-recursively
  current-wal-file
  delete-range
  delete-range-and-end
  iterate-prefix
  iterate-sync
  open-close
  open-close-immediately
  open-close-sync
  read-only-wal-filter
  write-flush
  write-read
  write-read-snapshot
  write-read-sync
  property-get
)

foreach(test IN LISTS tests)
  add_executable(${test} ${test}.c)

  target_link_libraries(
    ${test}
    PRIVATE
      rocksdb_static
  )

  add_test(
    NAME ${test}
    COMMAND ${test}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  )

  set_tests_properties(
    ${test}
    PROPERTIES
    TIMEOUT 30
  )
endforeach()
