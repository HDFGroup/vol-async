#!/bin/bash -l
#BSUB -P CSC300
#BSUB -W QTIME
#BSUB -nnodes NNODE
##BSUB -w ended(PREVJOBID)
#BSUB -J vpic_NNODEnode
#BSUB -o o%J.vpic_NNODEnode
#BSUB -e o%J.vpic_NNODEnode

REPEAT=REPEATTIME
NCORE=32
let NPROC=NNODE*$NCORE

ASYNC_HOME=/gpfs/alpine/csc300/world-shared/gnu_build/vol-async
HDF5_HOME=/gpfs/alpine/csc300/world-shared/gnu_build/hdf5/develop_build/hdf5
ABT_HOME=/gpfs/alpine/csc300/world-shared/gnu_build/argobots/install
H5BENCH_HOME=/gpfs/alpine/csc300/world-shared/gnu_build/h5bench/build/

EXEC=${H5BENCH_HOME}/h5bench_vpicio
SYNC_INPUT=${ASYNC_HOME}/h5bench_cfg/vpic_cc1d_sync.cfg
ASYNC_INPUT=${ASYNC_HOME}/h5bench_cfg/vpic_cc1d_async.cfg
OUTPUT=/gpfs/alpine/scratch/houjun/csc300/vpic_test_data

MPI_COMMAND="jsrun -n NNODE -r 1 -a ${NCORE} -c ${NCORE}"

export HDF5_PLUGIN_PATH=${ASYNC_HOME}/src
export LD_LIBRARY_PATH==${ASYNC_HOME}/src:${ABT_HOME}/lib:${HDF5_HOME}/lib:$LD_LIBRARY_PATH

# Darshan
module load darshan-runtime/3.2.1
export DARSHAN_DISABLE_SHARED_REDUCTION=1
export DXT_ENABLE_IO_TRACE=4

date

for (( i = 0; i < REPEAT; i++ )); do
    export HDF5_VOL_CONNECTOR=""
    $MPI_COMMAND $EXEC $SYNC_INPUT $OUTPUT/NNODE.h5
    cat perf_write_1d_sync.csv >> sync.log
    rm perf_write_1d_sync.csv

    export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}"
    $MPI_COMMAND $EXEC $ASYNC_INPUT $OUTPUT/NNODE.h5
    cat perf_write_1d_async.csv >> async.log
    rm perf_write_1d_async.csv

done

date
