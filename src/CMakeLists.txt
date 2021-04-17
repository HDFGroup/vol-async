set(HDF5_VOL_ASYNC_SRCS
    ${H5AV_SRC_SOURCE_DIR}/h5_vol_external_async_native.c
    ${H5AV_SRC_SOURCE_DIR}/h5_async_lib.c)

add_library(hdf5_vol_async MODULE ${HDF5_VOL_ASYNC_SRCS})
target_include_directories(hdf5_vol_async PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(hdf5_vol_async PUBLIC ${LINK_LIBS})
