# HDF5 Asynchronous I/O VOL Connector

## Background
Asynchronous I/O is becoming increasingly popular with the large amount of data access required by scientific applications. They can take advantage of an asynchronous interface by scheduling I/O as early as possible and overlap computation or communication with I/O operations, which hides the cost associated with I/O and improves the overall performance.


Some configuration parameters used in the instructions:

        VOL_DIR : directory of HDF5 Asynchronous I/O VOL connector repository
        ABT_DIR : directory of Argobots source code
        H5_DIR  : directory of HDF5 source code

## 1. Preparation

    1.1 Download the Asynchronous I/O VOL connector code (this repository) with Argobots git submodule 

       > git clone --recursive https://github.com/hpc-io/vol-async.git
       > # Latest Argobots can also be downloaded separately from https://github.com/pmodels/argobots

    1.2 Download the HDF5 source code

       > git clone https://github.com/HDFGroup/hdf5.git

    1.3 (Optional) Set the environment variables for the paths of the codes if the full path of VOL_DIR, ABT_DIR, and H5_DIR are not used in later setup
       > export H5_DIR=/path/to/hdf5/dir
       > export VOL_DIR=/path/to/async_vol/dir
       > export ABT_DIR=/path/to/argobots/dir

## 2. Installation

    2.1 Compile HDF5

        > cd $H5_DIR
        > ./autogen.sh  (may skip this step if the configure file exists)
        > ./configure --prefix=$H5_DIR/install --enable-parallel --enable-threadsafe --enable-unsupported #(may need to add CC=cc or CC=mpicc)
        > make && make install

    2.2 Compile Argobots

        > cd $ABT_DIR
        > ./autogen.sh  (may skip this step if the configure file exists)
        > ./configure --prefix=$ABT_DIR/install #(may need to add CC=cc or CC=mpicc)
        > make && make install

    2.3 Compile Asynchronous VOL connector
        > cd $VOL_DIR/src
        > Edit "Makefile"
            > Copy a sample Makefile (Makefile.cori, Makefile.summit, Makefile.macos), e.g. "cp Makefile.summit Makefile", which should work for most linux systems
            > Change the path of HDF5_DIR and ABT_DIR to $H5_DIR/install and $ABT_DIR/install (replace $H5_DIR and $ABT_DIR with their full path)
            > (Optional) update the compiler flag macros: DEBUG, CFLAGS, LIBS, ARFLAGS
            > (Optional) comment/uncomment the correct DYNLDFLAGS & DYNLIB macros
        > make

## 3. Set Environment Variables

    Will need to set the following environmental variable before running the asynchronous operation tests and your async application, e.g.:

    for Linux:
        > export LD_LIBRARY_PATH=$VOL_DIR/src:$H5_DIR/install/lib:$ABT_DIR/install/lib:$LD_LIBRARY_PATH
        > export HDF5_PLUGIN_PATH="$VOL_DIR/src"
        > export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 

    and on MacOS:
        > export DYLD_LIBRARY_PATH=$VOL_DIR/src:$H5_DIR/install/lib:$ABT_DIR/install/lib:$DYLD_LIBRARY_PATH
        > export HDF5_PLUGIN_PATH="$VOL_DIR/src"
        > export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 

## 4. Test

    > cd $VOL_DIR/test
    > Edit "Makefile":
        > Copy a sample Makefile (Makefile.cori, Makefile.summit, Makefile.macos), e.g. "cp Makefile.summit Makefile", Makefile.summit should work for most linux systems
        > Update H5_DIR, ABT_DIR and ASYNC_DIR to the correct paths of their installation directory
        > (Optional) update the compiler flag macros: DEBUG, CFLAGS, LIBS, ARFLAGS
        > (Optional) comment/uncomment the correct DYNLIB & LDFLAGS macro
    > make
    
    Running the automated tests requires Python3.
    (Optional) If the system is not using mpirun to launch MPI tasks, edit mpirun_cmd in pytest.py with the corresponding MPI launch command.
    
    Run both the serial and parallel tests

        > make check

    Run the serial tests only

        > make check_serial

    If any test fails, check async_vol_test.err in the $VOL_DIR/test directory for the error message. 
    With certain file systems where file locking is not supported, an error of "file create failed" may occur and can be fixed with "export HDF5_USE_FILE_LOCKING=FALSE", which disables the HDF5 file locking.

## 5. Using the Asynchronous I/O VOL connector with application code (Implicit mode)

    The implicit mode allows an application to enable asynchronous I/O through setting the following environemental variables and without any major code change. 
    By default, the HDF5 metadata operations are executed asynchronously, and the dataset operations are executed synchronously.

        > [Set environment variables, from step 3 above]
        > Run your application

## 6. Using the Asynchronous I/O VOL connector with application code (Explicit mode)

    Please refer to the Makefile and source code (async_test_serial_event_set*) under $VOL_DIR/test/ for example usage.

    6.1 Include header file

        > #include "h5_vol_external_async_native.h" 

    6.2 Use event set and new async API to manage asynchronous I/O operations
        > es_id = H5EScreate();                        // Create event set for tracking async operations
        > fid = H5Fopen_async(.., es_id);              // Asynchronous, can start immediately
        > gid = H5Gopen_async(fid, .., es_id);         // Asynchronous, starts when H5Fopen completes
        > did = H5Dopen_async(gid, .., es_id);         // Asynchronous, starts when H5Gopen completes
        > status = H5Dwrite_async(did, .., es_id);     // Asynchronous, starts when H5Dopen completes, may run concurrently with other H5Dwrite in event set
        > status = H5Dread_async(did, .., es_id);      // Asynchronous, starts when H5Dwrite completes, may run concurrently with other H5Dread in event set
        > H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed); 
        >                                              // Wait for operations in event set to complete, buffers used for H5Dwrite must only be changed after wait
        > H5ESclose(es_id);                            // Close the event set (must wait first)

    6.3 Error handling with event set
        > hbool_t es_err_status;
        > status = H5ESget_err_status(es_id, &es_err_status);   // Check if event set has failed operations (es_err_status is set to true)
        > size_t es_err_count;
        > status = H5ESget_err_count(es_id, &es_err_count);     // Retrieve the number of failed operations in this event set
        > size_t num_err_info;
        > H5ES_err_info_t err_info;
        > status = H5ESget_err_info(es_id, 1, &err_info, &es_err_cleared);   // Retrieve information about failed operations 
        > printf("API name: %s\nAPI args: %s\nAPI file name: %s\n API func name: %s\nAPI line number: %u\nOperation counter: %llu\nOperation timestamp: %llu\n",
        >        err_info.api_name, err_info.api_args, err_info.api_file_name, err_info.api_func_name, err_info.api_line_num, err_info.op_ins_count, err_info.op_ins_ts);    
        >        // Retrieve the faile operations's API name, arguments list, file name, function name, line number, operation counter (0-based), and operation timestamp
        > H5free_memory(err_info.api_name);
        > H5free_memory(err_info.api_args);
        > H5free_memory(err_info.app_file_name);
        > H5free_memory(err_info.app_func_name);

    6.4 Use MPI_THREAD_MULTIPLE
        The asynchronous tasks may involve MPI collecive operations, and can execute them concurrently with your application's MPI operations, 
        thus we require to initialize MPI with MPI_THREAD_MULTIPLE support. Change MPI_Init(argc, argv) in your application's code to the following:
        > MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);

    6.5 Run with async
        > [Set environment variables, from step 3 above]
        > Run your application

