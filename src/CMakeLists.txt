set(HDF5_VOL_ASYNC_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/h5_async_lib.h
    ${CMAKE_CURRENT_SOURCE_DIR}/h5_async_vol.h
)

add_library(libh5async
    SHARED
    ${CMAKE_CURRENT_SOURCE_DIR}/h5_async_vol.c
)
add_library(libasynchdf5
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

target_include_directories(libh5async 
    PRIVATE
    ${ASYNC_INCLUDE_LIBRARIES}
)
target_include_directories(libh5async 
    PRIVATE
    ${ASYNC_INCLUDE_LIBRARIES}
)

target_link_libraries(libh5async
    PRIVATE
    ${ASYNC_LINK_LIBRARIES}
)
target_link_libraries(libasynchdf5
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
        libh5async
        libasynchdf5
    EXPORT
        ${HDF5_VOL_ASYNC_EXPORTED_TARGETS}
    LIBRARY DESTINATION ${HDF5_VOL_ASYNC_INSTALL_LIB_DIR}
    ARCHIVE DESTINATION ${HDF5_VOL_ASYNC_INSTALL_LIB_DIR}
)
