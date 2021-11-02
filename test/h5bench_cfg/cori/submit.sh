#!/bin/bash

MIN_PROC=1
MAX_PROC=128

CASES=( "vpic" )
CASES=( "bdcats" )

if [ -z "$1" ]
then
    first_submit=1
else
    first_submit=0
    job=$1
fi

curdir=$(pwd)

for mycase in "${CASES[@]}"
do
    cd $curdir/${mycase}
    ../gen_script.sh
done

# filename=run_lustre.sh
filename=run_bb.sh

for (( i = $MIN_PROC; i <= $MAX_PROC ; i*=2 )); do

    for mycase in "${CASES[@]}"
    do
        cd $curdir/${mycase}/${i}
        echo "$curdir/${mycase}/${i}"
        
        if [[ $first_submit == 1 ]]; then
            # Submit first job w/o dependency
            echo "Submitting $filename"
            job=`sbatch $filename`
            first_submit=0
        else
            echo "Submitting $filename after ${job: -8}"
            job=`sbatch -d afterok:${job: -8} $filename`
            # job=`sbatch -d afterany:${job: -8} $filename`
        fi

        sleeptime=$[ ( $RANDOM % 10 ) ]
        sleep $sleeptime

    done
done
