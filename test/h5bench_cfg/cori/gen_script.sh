#!/bin/bash
MIN_NODE=1
MAX_NODE=128

REPEAT_TIME=5

ndebugq=16

for (( i = $MIN_NODE; i <= $MAX_NODE; i*=2 )); do

    mkdir -p ./${i}/

    QUEUE_NAME=regular
    if (( $i <= $ndebugq )); then
        QUEUE_NAME=debug
    fi

    TARGET=./${i}/run_lustre.sh
    cp template.sh $TARGET
    # if (( $i > $ndebugq )); then
    #     sed -i "s/#ENABLEQOS//g"  $TARGET
    # fi
    sed -i "s/QUEUE/${QUEUE_NAME}/g"      $TARGET
    sed -i "s/NNODE/${i}/g"           $TARGET
    sed -i "s/REPEATTIME/$REPEAT_TIME/g"  $TARGET
    sed -i "s/FSTYPE/lst/g"  $TARGET
    sed -i "s/QTIME/0:30:00/g"  $TARGET

    TARGET=./${i}/run_bb.sh
    cp template.sh $TARGET
    # if (( $i > $ndebugq )); then
    #     sed -i "s/#ENABLEQOS//g"  $TARGET
    # fi
    sed -i "s/QUEUE/${QUEUE_NAME}/g"      $TARGET
    sed -i "s/NNODE/${i}/g"           $TARGET
    sed -i "s/REPEATTIME/$REPEAT_TIME/g"  $TARGET
    sed -i "s/#ENABLEBB//g"  $TARGET
    sed -i "s/FSTYPE/bb/g"  $TARGET
    sed -i "s/QTIME/0:30:00/g"  $TARGET


done

