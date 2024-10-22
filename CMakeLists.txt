cmake_minimum_required(VERSION 3.12)

project(HDF5_VOL_ASYNC VERSION 0.0.1 LANGUAGES C)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

set (CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(MPI REQUIRED)
find_package(ABT REQUIRED)
find_package(HDF5 REQUIRED COMPONENTS C)

include_directories(${MPI_INCLUDE_PATH})
include_directories(${HDF5_INCLUDE_DIRS})

include(CTest)
enable_testing()

if(NOT HDF5_VOL_ASYNC_INSTALL_BIN_DIR)
  set(HDF5_VOL_ASYNC_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
endif()
if(NOT HDF5_VOL_ASYNC_INSTALL_LIB_DIR)
  set(HDF5_VOL_ASYNC_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif()
if(NOT HDF5_VOL_ASYNC_INSTALL_INCLUDE_DIR)
  set(HDF5_VOL_ASYNC_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
endif()
if(NOT HDF5_VOL_ASYNC_INSTALL_DATA_DIR)
  set(HDF5_VOL_ASYNC_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif()
if(NOT HDF5_VOL_ASYNC_INSTALL_TEST_DIR)
  set(HDF5_VOL_ASYNC_INSTALL_TEST_DIR ${CMAKE_INSTALL_PREFIX}/test)
endif()

include(CheckSymbolExists)
list(APPEND CMAKE_REQUIRED_INCLUDES ${HDF5_INCLUDE_DIRS})
if(NOT HDF5_IS_PARALLEL)
  check_symbol_exists(H5_HAVE_PARALLEL "H5pubconf.h" HDF5_HAVE_PARALLEL)
  if(NOT HDF5_HAVE_PARALLEL)
    message(FATAL_ERROR "HDF5 library needs to enable parallel support.")
  endif()
endif()

# Comment out until H5_IS_THREADSAFE is added as a CMake variable. Otherwise, 
# this may fail when fetch content is used in VOL testing with HDF5 actions.
#check_symbol_exists(H5_HAVE_THREADSAFE "H5pubconf.h" HDF5_HAVE_THREADSAFE)
#if(NOT HDF5_HAVE_THREADSAFE)
#  message(FATAL_ERROR "HDF5 library needs to enable threadsafe support.")
#endif()

# Check if HDF5 version is 1.14 or greater
if(HDF5_FOUND)
  if (HDF5_VERSION VERSION_LESS "1.14.0")
     message (FATAL_ERROR "HDF5 ${HDF5_VERSION}: required HDF5 version is at least 1.14 ")
  endif ()
endif ()

set(HDF5_VOL_ASYNC_PACKAGE "hdf5_vol_async")
set(HDF5_VOL_ASYNC_PACKAGE_NAME "HDF5_VOL_ASYNC")
set(HDF5_VOL_ASYNC_PACKAGE_DESCRIPTION "HDF5 Asynchronous I/O VOL connector")
set(HDF5_VOL_ASYNC_PACKAGE_URL "https://github.com/HDFGroup/vol-async")
set(HDF5_VOL_ASYNC_PACKAGE_VENDOR "HDF Group")

#-----------------------------------------------------------------------------
# Options
#-----------------------------------------------------------------------------
option(ENABLE_WRITE_MEMCPY 
       "Make a copy of write buffer automatically, user's buffer can be safely reused or freed immediately." OFF)
option(ENABLE_DBG_MSG "Enable debug message." OFF)
option(PRINT_ERROR_STACK "Enable HDF5 error stack print out." OFF)

message(STATUS "Configuring ${HDF5_VOL_ASYNC_PACKAGE} ${PROJECT_VERSION}")

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
      ${PROJECT_BINARY_DIR}/lib CACHE PATH "Single Directory for all libraries."
)

#-----------------------------------------------------------------------------
# Source
#-----------------------------------------------------------------------------
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)

#-----------------------------------------------------------------------------
# Testing
#-----------------------------------------------------------------------------
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)
