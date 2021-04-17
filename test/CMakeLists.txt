set(tests async_test_serial)

foreach(test ${tests})
  add_executable(${test} ${CMAKE_CURRENT_SOURCE_DIR}/${test}.c)
  target_link_libraries(${test} PUBLIC hdf5_vol_async)
endforeach ()
