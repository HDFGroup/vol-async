#!/bin/bash

MIN_PROC=4
MAX_PROC=128

CASES=( "vpic" "bdcats" )

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

filename=run_gpfs.sh

for (( i = $MIN_PROC; i <= $MAX_PROC ; i*=2 )); do

    for mycase in "${CASES[@]}"
    do
        cd $curdir/${mycase}/${i}
        echo "$curdir/${mycase}/${i}"
        
        if [[ $first_submit == 1 ]]; then
            # Submit first job w/o dependency
            echo "Submitting $filename"
            job=`bsub $filename`
            first_submit=0
        else
            last=`bjobs|grep $USER|tail -1|awk '{print $1}'`
            echo "Submitting $filename after ${last}"
            sed -i "s/##BSUB/#BSUB/g" ${filename}
            sed -i "s/PREVJOBID/${last}/g" ${filename}
            job=`bsub $filename`
        fi

        sleeptime=$[ ( $RANDOM % 10 ) ]
        sleep $sleeptime

    done
done
