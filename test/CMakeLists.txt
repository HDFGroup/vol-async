if(DEFINED ENV{HDF5_PLUGIN_PATH})
    message("HDF5_PLUGIN_PATH is " $ENV{HDF5_PLUGIN_PATH})
else()
    set(ENV{HDF5_PLUGIN_PATH} ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})
    message("HDF5_PLUGIN_PATH is set to $ENV{HDF5_PLUGIN_PATH}.")
endif()

if(DEFINED ENV{HDF5_VOL_CONNECTOR})
    message("HDF5_VOL_CONNECTOR is " $ENV{HDF5_VOL_CONNECTOR})
else()
    set(ENV{HDF5_VOL_CONNECTOR} "async under_vol=0;under_info={}")
    message("HDF5_VOL_CONNECTOR is set to $ENV{HDF5_VOL_CONNECTOR}.")
endif()

set(tests
    async_test_multifile
    async_test_serial 
    async_test_serial2
    async_test_serial_event_set
    async_test_serial_event_set_error_stack
)

if(MPI_FOUND)
    if(NOT DEFINED MPICMD)
        message("MPIEXEC is found - ${MPIEXEC}.")
        set(MPICMD ${MPIEXEC})
    endif()

    set(tests_p
        async_test_parallel2
        async_test_parallel3
        async_test_parallel4
        async_test_parallel5
        async_test_parallel
    )
endif()

include_directories(SYSTEM
    ${MPI_INCLUDE_PATH}
)
          
foreach(test ${tests} ${tests_p})
    add_executable(${test}.exe ${CMAKE_CURRENT_SOURCE_DIR}/${test}.c)
    target_link_libraries(${test}.exe
        PRIVATE
        ${MPI_LIBRARIES}
        ${ABT_LIBRARIES}
        ${HDF5_LIBRARIES}
        h5async
        asynchdf5
    )

    # Option 1: If you want to test individually, use the following.
    if(${test} MATCHES parallel)
        add_test(${test} ${MPICMD} -n 2 ./${test}.exe) 
    else()
        add_test(${test} ${test}.exe)
    endif()

    set_property(
        TEST
        ${test}
        APPEND PROPERTY
        ENVIRONMENT HDF5_PLUGIN_PATH=$ENV{HDF5_PLUGIN_PATH}
    )

    set_property(
        TEST
        ${test}
        APPEND PROPERTY 
        ENVIRONMENT "HDF5_VOL_CONNECTOR=async under_vol=0\;under_info={}"
    )
endforeach()

# Option2: Test all executables in one Python script.
# This will not work because pytest.py uses a fixed path.
# add_test(check_serial ${CMAKE_CURRENT_SOURCE_DIR}/pytest.py)
# add_test(check ${CMAKE_CURRENT_SOURCE_DIR}/pytest.py -p)
