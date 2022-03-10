set(HDF5_VOL_ASYNC_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/h5_async_lib.h
    ${CMAKE_CURRENT_SOURCE_DIR}/h5_async_vol.h
)

add_library(h5async
    SHARED
    ${CMAKE_CURRENT_SOURCE_DIR}/h5_async_vol.c
)
add_library(asynchdf5
    STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/h5_async_lib.c
)

set(ASYNC_INCLUDE_LIBRARIES
    PRIVATE
    ${ABT_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set(ASYNC_LINK_LIBRARIES
    PRIVATE
    ${MPI_LIBRARIES}
    ${HDF5_LIBRARIES}
    ${ABT_LIBRARIES}
)

target_include_directories(h5async 
    PRIVATE
    ${ASYNC_INCLUDE_LIBRARIES}
)
target_include_directories(h5async 
    PRIVATE
    ${ASYNC_INCLUDE_LIBRARIES}
)

target_link_libraries(h5async
    PRIVATE
    ${ASYNC_LINK_LIBRARIES}
)
target_link_libraries(asynchdf5
    PRIVATE
    ${ASYNC_LINK_LIBRARIES}
)

install(
    FILES
        ${HDF5_VOL_ASYNC_HEADERS}
    DESTINATION
        ${HDF5_VOL_ASYNC_INSTALL_INCLUDE_DIR}
    COMPONENT
        headers
)

install(
    TARGETS
        h5async
        asynchdf5
    EXPORT
        ${HDF5_VOL_ASYNC_EXPORTED_TARGETS}
    LIBRARY DESTINATION ${HDF5_VOL_ASYNC_INSTALL_LIB_DIR}
    ARCHIVE DESTINATION ${HDF5_VOL_ASYNC_INSTALL_LIB_DIR}
)
