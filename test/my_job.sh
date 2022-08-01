#!/bin/bash
#SBATCH -N 1
#SBATCH -C haswell
#SBATCH -o my_job.o%j

export VOL_DIR=/global/homes/k/kamal/vol-async
export ASYNC_DIR=/global/homes/k/kamal/vol-async/src
export H5_DIR=/global/homes/k/kamal/hdf5
export HDF5_DIR=/global/homes/k/kamal/hdf5/install

export ABT_DIR=/global/homes/k/kamal/vol-async/argobots/install

export LD_LIBRARY_PATH=$VOL_DIR/src:$H5_DIR/install/lib:$ABT_DIR/lib:$LD_LIBRARY_PATH
export HDF5_PLUGIN_PATH="$VOL_DIR/src"
export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 
export MPICH_MAX_THREAD_SAFETY=multiple

export HDF5_USE_FILE_LOCKING=FALSE

srun -n 4 ./Hyperslab_by_row.exe


