list(APPEND tests
  delete-range
  delete-range-and-end
  iterate-prefix
  open-close
  write-read
  write-read-snapshot
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
