list(APPEND benches
  write-read-single-batch
  write-read-multiple-batches
)

foreach(bench IN LISTS benches)
  add_executable(${bench} ${bench}.c)

  set_target_properties(
    ${bench}
    PROPERTIES
    OUTPUT_NAME ${bench}
  )

  target_link_libraries(
    ${bench}
    PRIVATE
      rocksdb_static
  )
endforeach()
