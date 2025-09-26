list(APPEND benches
  write-read
)

foreach(bench IN LISTS benches)
  add_executable(bench-${bench} ${bench}.c)

  set_target_properties(
    bench-${bench}
    PROPERTIES
    OUTPUT_NAME ${bench}
  )

  target_link_libraries(
    bench-${bench}
    PRIVATE
      rocksdb_static
  )
endforeach()

