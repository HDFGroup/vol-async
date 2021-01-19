HDF5_HOME="/Users/tonglin/nersc_dev_sync/hpc-io-hdf5/build/hdf5"
ASYNC_HOME="/Users/tonglin/nersc_dev_sync/vol-async"
TW_BUILD="/Users/tonglin/nersc_dev_sync/taskworks_build"
OPA_BUILD="/Users/tonglin/nersc_dev_sync/openpa/build"

export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}"
export HDF5_PLUGIN_PATH="$ASYNC_HOME/src/.libs"
export DYLD_LIBRARY_PATH="$HDF5_HOME/lib:$ASYNC_HOME/build/lib:$TW_BUILD/lib:$OPA_BUILD/lib"
