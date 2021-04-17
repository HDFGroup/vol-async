set(tests async_test_serial)

foreach(test ${tests})
  target_include_directories(${test}../src)
  add_executable(${test} ${CMAKE_CURRENT_SOURCE_DIR}/${test}.c)
  target_link_libraries(${test})
endforeach ()
