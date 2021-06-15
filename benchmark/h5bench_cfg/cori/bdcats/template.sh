#!/bin/bash -l
#SBATCH -p QUEUE
#SBATCH -t QTIME
#ENABLEQOS#SBATCH --qos=premium
#SBATCH -N NNODE
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH -J FSTYPE_bdcats
#SBATCH -A m2621
#SBATCH -o o%j.bdcats_FSTYPE
#SBATCH -e o%j.bdcats_FSTYPE
#ENABLEBB#DW jobdw capacity=40960GB access_mode=striped type=scratch
#ENABLEBB#DW stage_in source=/global/cscratch1/sd/houhun/vpic_test_data/NNODE.h5 destination=$DW_JOB_STRIPED/NNODE.h5 type=file


REPEAT=REPEATTIME
let NPROC=NNODE*32

EXEC=/global/u1/h/houhun/h5bench/build/h5bench_bdcatsio
SYNC_INPUT=/global/u1/h/houhun/h5bench/scripts/bdcats_cc1d_sync.cfg
ASYNC_INPUT=/global/u1/h/houhun/h5bench/scripts/bdcats_cc1d_async.cfg
OUTPUT=/global/cscratch1/sd/houhun/vpic_test_data
#ENABLEBBOUTPUT=${DW_JOB_STRIPED}

MPI_COMMAND="srun -N NNODE -n ${NPROC} --cpu-bind=cores -c 2 "

module swap PrgEnv-intel PrgEnv-gnu

export MPICH_MAX_THREAD_SAFETY=multiple
export HDF5_PLUGIN_PATH=/global/u1/h/houhun/hdf5vol/vol-async/src
export LD_LIBRARY_PATH=/global/u1/h/houhun/hdf5vol/vol-async/src:$LD_LIBRARY_PATH

date

for (( i = 0; i < REPEAT; i++ )); do
    export HDF5_VOL_CONNECTOR=""
    $MPI_COMMAND $EXEC $SYNC_INPUT $OUTPUT/NNODE.h5
    echo "FSTYPE" >> sync.log
    cat perf_read_1d_sync.csv >> sync.log
    rm perf_read_1d_sync.csv

    export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}"
    $MPI_COMMAND $EXEC $ASYNC_INPUT $OUTPUT/NNODE.h5
    echo "FSTYPE" >> sync.log
    cat perf_read_1d_async.csv >> async.log
    rm perf_read_1d_async.csv

done

date
