#!/bin/bash
MIN_NODE=1
MAX_NODE=128

CASES=( "vpic" "bdcats" )
FILESYS=( "lst" "bb" )

curdir=$(pwd)
for fs in "${FILESYS[@]}"
do
    echo "$fs"

    for mycase in "${CASES[@]}"
    do
        echo "$mycase"

        for (( i = $MIN_NODE; i <= $MAX_NODE; i*=2 )); do
            echo "====Node $i"

            for file in "$curdir/${mycase}/${i}/"o*.${mycase}_${fs}
            do
                echo $file

                if [[ "$mycase" == "vpic" ]]; then
                    echo "Sync"
                    grep "Sync Observed write rate"  $file | cut -c27-36

                    echo "Async"
                    grep "Async Observed write rate" $file | cut -c29-37
                else
                    echo "Sync"
                    grep "Sync Observed read rate"  $file | cut -c26-35

                    echo "Async"
                    grep "Async Observed read rate" $file | cut -c28-36
                fi
            done
        done
    done
    echo "=========="
done
