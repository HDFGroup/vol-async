find_package(MPI)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
set(tests async_test_multifile
	  async_test_multifile_env
	  async_test_serial 
	  async_test_serial2
	  async_test_serial_error_stack
	  async_test_serial_event_set
	  async_test_serial_event_set_error_stack)

set(testp async_test_parallel2
	  async_test_parallel3
	  async_test_parallel4
	  async_test_parallel5
	  async_test_parallel)

foreach(test ${tests})
  add_executable(${test}.exe ${CMAKE_CURRENT_SOURCE_DIR}/${test}.c)
  target_link_libraries(${test}.exe PRIVATE hdf5_vol_async hdf5 abt ${MPI_C_LIBRARIES})
  # Option 1: If you want to test individually, use the following.
  add_test(${test} ${test}.exe -E env HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" )
endforeach ()

# Option2: Test all executables in one Python script.
# This will not work because pytest.py uses a fixed path.

# add_test(check_serial ${CMAKE_CURRENT_SOURCE_DIR}/pytest.py)
# add_test(check ${CMAKE_CURRENT_SOURCE_DIR}/pytest.py -p)
