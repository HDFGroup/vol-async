#!/usr/bin/env python3

# Arguments:
# -h: help, -v: verbose mode -m mpi-tasks

import os, sys, argparse, subprocess
import multiprocessing

#------------------------------------------------
def guess_mpi_cmd(mpi_tasks, cpu_allocation, verbose):
    if verbose: print('os.uname=', os.uname())
    node_name = os.uname()[1]
    if verbose: print('node_name=', node_name)
    sys_name = os.uname()[0]
    if verbose: print('sys_name=', sys_name)

    if mpi_tasks<=0: 
        mpi_tasks = 4
        max_task = multiprocessing.cpu_count()
        if max_task < 4:
            mpi_tasks = max_task
        print('\nParallel test run with %d MPI ranks' % (mpi_tasks))

    if 'quartz' in node_name:
        # the following setting is needed to combine h5py and subprocess.run on LC
        os.environ["PSM2_DEVICES"] = ""
        if cpu_allocation == "":
           mpirun_cmd="srun -ppdebug " + " -n " + str(mpi_tasks) + " -c "
        else:
           mpirun_cmd="srun -ppdebug " + " -A " + cpu_allocation + " -n " + str(mpi_tasks)
    elif 'cab' in node_name:
        mpirun_cmd="srun -ppdebug -n " + str(mpi_tasks)
    elif 'nid' in node_name: # the cori knl nodes are called nid
        mpirun_cmd="srun --cpu_bind=cores -n " + str(mpi_tasks)
    elif 'spock' in node_name:
        mpirun_cmd="srun --cpu_bind=cores -n " + str(mpi_tasks)
    elif 'fourier' in node_name:
        mpirun_cmd="mpirun -np " + str(mpi_tasks)
    elif 'batch' in node_name: # for summit
        mpirun_cmd="jsrun -a1 -c7 -l CPU-CPU -d packed -b packed:7 -n " + str(mpi_tasks)
    else:
        #default mpi command
        mpirun_cmd="mpirun -np " + str(mpi_tasks)

    return mpirun_cmd

#------------------------------------------------
def main_test(mpi_tasks=0, cpu_allocation="", run_parallel=False, verbose=False):

    assert sys.version_info >= (3,5) # named tuples in Python version >=3.3

    success = True;
    num_test=0
    num_pass=0
    num_fail=0

    serial_cases   = ['async_test_serial.exe', 'async_test_serial2.exe', 'async_test_multifile.exe', 'async_test_serial_event_set.exe', 'async_test_serial_event_set_error_stack.exe']
    parallel_cases = ['async_test_parallel.exe', 'async_test_parallel2.exe', 'async_test_parallel3.exe', 'async_test_parallel4.exe', 'async_test_parallel5.exe']
    
    print("Running serial tests")

    for qq in range(len(serial_cases)):
        num_test = num_test+1

        test_case = serial_cases[qq]
        run_cmd = './' + test_case

        stdout_file = open('async_vol_test.out', 'wt')
        stderr_file = open('async_vol_test.err', 'wt')

        status = subprocess.run(
                    run_cmd,
                    stdout=stdout_file,
                    stderr=stderr_file,
                 )

        stdout_file.close()
        stderr_file.close()

        if status.returncode!=0:
            print('ERROR: Test', test_case, ': returned non-zero exit status=', status.returncode, 'aborting test')
            print('run_cmd=', run_cmd)
            return False 

        if success:        
            print('Test #', num_test, ":", test_case, 'PASSED')
            num_pass += 1
        else:
            print('Test #', num_test, ":", test_case, 'FAILED')
            num_fail += 1
        
    if run_parallel:
        # guess the mpi run command from the uname info
        mpirun_cmd=guess_mpi_cmd(mpi_tasks, cpu_allocation, verbose)

        for qq in range(len(parallel_cases)):
            num_test = num_test+1

            test_case = parallel_cases[qq]
            run_cmd = mpirun_cmd + ' ./' + test_case

            stdout_file = open('async_vol_test.out', 'wt')
            stderr_file = open('async_vol_test.err', 'wt')

            status = subprocess.run(
                        run_cmd,
                        shell=True,
                        stdout=stdout_file,
                        stderr=stderr_file,
                     )

            stdout_file.close()
            stderr_file.close()

            if status.returncode!=0:
                print('ERROR: Test', test_case, ': returned non-zero exit status=', status.returncode, 'aborting test')
                print('run_cmd=', run_cmd)
                return False 

            if success:        
                print('Test #', num_test, ":", test_case, 'PASSED')
                num_pass += 1
            else:
                print('Test #', num_test, ":", test_case, 'FAILED')
                num_fail += 1
     
    # end for all cases in the test_dir
    print('Out of', num_test, 'tests,', num_fail, 'failed and ', num_pass, 'passed')
    # normal termination
    return True
    
#------------------------------------------------
if __name__ == "__main__":
    assert sys.version_info >= (3,5) # named tuples in Python version >=3.5
    # default arguments
    verbose=False
    run_parallel=False
    mpi_tasks=0 
    cpu_allocation=""

    parser=argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    parser.add_argument("-m", "--mpitasks", type=int, help="number of mpi tasks")
    parser.add_argument("-p", "--parallel", help="run parallel tests", action="store_true")
    parser.add_argument("-A", "--cpu_allocation", help="name of cpu bank/allocation",default="")

    args = parser.parse_args()
    if args.verbose:
        #print("verbose mode enabled")
        verbose=True
    if args.mpitasks:
        #print("MPI-tasks specified=", args.mpitasks)
        if args.mpitasks > 0: mpi_tasks=args.mpitasks
    if args.parallel:
        #print("parallel test enabled")
        run_parallel=True
    if args.cpu_allocation:
        #print("cpu_allocation specified=", args.cpu_allocation)
        cpu_allocation=args.cpu_allocation

    if not main_test(mpi_tasks, cpu_allocation, run_parallel, verbose):
        print("pytest was unsuccessful")

