#!/bin/bash
MIN_NODE=1
MAX_NODE=128

REPEAT_TIME=5

for (( i = $MIN_NODE; i <= $MAX_NODE; i*=2 )); do

    mkdir -p ./${i}/

    TARGET=./${i}/run_gpfs.sh
    cp template.sh $TARGET
    sed -i "s/NNODE/${i}/g"           $TARGET
    sed -i "s/REPEATTIME/$REPEAT_TIME/g"  $TARGET
    sed -i "s/QTIME/0:30/g"  $TARGET

done

