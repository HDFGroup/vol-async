set(HDF5_VOL_ASYNC_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/h5_async_lib.h
  ${CMAKE_CURRENT_SOURCE_DIR}/h5_vol_external_async_native.h
)

set(HDF5_VOL_ASYNC_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/h5_vol_external_async_native.c
    ${CMAKE_CURRENT_SOURCE_DIR}/h5_async_lib.c)

add_library(hdf5_vol_async SHARED ${HDF5_VOL_ASYNC_SRCS})

target_include_directories(hdf5_vol_async 
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(hdf5_vol_async PRIVATE ${LINK_LIBS} abt mpi hdf5)

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
    hdf5_vol_async
  EXPORT
    ${HDF5_VOL_ASYNC_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${HDF5_VOL_ASYNC_INSTALL_LIB_DIR}
  ARCHIVE DESTINATION ${HDF5_VOL_ASYNC_INSTALL_LIB_DIR}
)
