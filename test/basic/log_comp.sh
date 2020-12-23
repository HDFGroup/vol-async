#!/bin/bash
# Copyright (C) 2020, Lawrence Berkeley National Laboratory.                
# All rights reserved.                                                      
#                                                                           
# This file is part of Taskworks. The full Taskworks copyright notice,    
# including terms governing use, modification, and redistribution, is       
# contained in the file COPYING at the root of the source code distribution 
# tree.                                                                     

SEQ_RUN=

#DRIVERS=${DRIVERS} # Set by AM_TESTS_ENVIRONMENT

NWORKERS=(2 4 8)
NTASKS=(16 64 256)

export TW_EVENT_BACKEND="NONE"

for DRIVER in ${DRIVERS[@]}
do
    export TW_BACKEND=${DRIVER}

    for NWORKER in ${NWORKERS[@]}
    do
        for NTASK in ${NTASKS[@]}
        do
            echo "========================== Testing $1 on ${NTASK} tasks and ${NWORKER} workers using the ${DRIVER} driver =========================="

            export TW_DEBUG_MSG_LVL=0           
            echo "${SEQ_RUN} $1 ${NWORKER} ${NTASK}"
            ${SEQ_RUN} $1 ${NWORKER} ${NTASK}

            if [ $? -ne "0" ]; then
                echo "Test failed. Rerun with full debug message."
                export TW_DEBUG_MSG_LVL=10
                echo "${SEQ_RUN} $1 ${NWORKER} ${NTASK}"
                ${SEQ_RUN} $1 ${NWORKER} ${NTASK}
                exit $?
            fi
        done
    done
done