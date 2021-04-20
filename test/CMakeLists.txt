set(tests async_test_multifile
	  async_test_multifile_env
	  async_test_parallel2
	  async_test_parallel3
	  async_test_parallel4
	  async_test_parallel5
	  async_test_parallel
	  async_test_serial 
	  async_test_serial2
	  async_test_serial_error_stack
	  async_test_serial_event_set
	  async_test_serial_event_set_error_stack)

foreach(test ${tests})
  add_executable(${test}.exe ${CMAKE_CURRENT_SOURCE_DIR}/${test}.c)
  target_link_libraries(${test}.exe PRIVATE hdf5_vol_async hdf5 abt)
  # Option 1: If you want to test individually, use the following.
  # add_test(${test} ${test}.exe)
endforeach ()

# Option2: Test all executables in one Python script.
add_test(check_serial ${CMAKE_CURRENT_SOURCE_DIR}/pytest.py)
add_test(check ${CMAKE_CURRENT_SOURCE_DIR}/pytest.py -p)
