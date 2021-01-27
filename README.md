# HDF5 Asynchronous I/O VOL Connector

## Background
Asynchronous I/O is becoming increasingly popular with the large amount of data access required by scientific applications. They can take advantage of an asynchronous interface by scheduling I/O as early as possible and overlap computation or communication with I/O operations, which hides the cost associated with I/O and improves the overall performance.


Some configuration parameters used in the instructions:

        VOL_DIR : directory of HDF5 Asynchronous I/O VOL connector repository
        ABT_DIR : directory of Argobots source code
        H5_DIR  : directory of HDF5 source code

1, Preparation

    1.1 Download the Asynchronous I/O VOL connector code (this repository) with Argobots git submodule 

       > git clone --recursive https://github.com/hpc-io/vol-async.git
       > Argobots can also be downloaded separately from https://github.com/pmodels/argobots

    1.2 Download the HDF5 code (hpc-io HDF5 async_vol_register_optional branch has the latest async-related features, they will be merged to the main HDF5 repo later)

       > git clone https://github.com/hpc-io/hdf5.git

2, Installation

    2.1 Compile HDF5 ( need to switch to the "async_vol_register_optional" branch )

        > cd $H5_DIR && git checkout async_vol_register_optional
        > ./autogen.sh  (may skip this step if the configure file exists)
        > ./configure --prefix=$H5_DIR/install --enable-parallel --enable-threadsafe --enable-unsupported (may need to add CC=cc or CC=mpicc)
        > make install

    2.2 Compile Argobots

        > cd $ABT_DIR
        > ./autogen.sh  (may skip this step if the configure file exists)
        > CC=cc ./configure --prefix=ABT_DIR/build
        > make install

    2.3 Compile Asynchronous VOL connector
        > cd $VOL_DIR/src
        > Edit "Makefile"
            > Update H5_DIR and ABT_DIR to the previously installed locations
            > Possibly update the compiler flag macros: DEBUG, CFLAGS, LIBS, ARFLAGS
            > Uncomment the correct DYNLDFLAGS & DYNLIB macros
        > make

3, Set Environment Variables

    Will need to set the following environmental variable before running the
    asynchronous operation tests and your async application, e.g.:

    for Linux:
        > export LD_LIBRARY_PATH=$VOL_DIR/src:$H5_DIR/lib:$LD_LIBRARY_PATH
        > export HDF5_PLUGIN_PATH="$VOL_DIR"
        > export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 

    and on MacOS:
        > export DYLD_LIBRARY_PATH=$VOL_DIR/src:$H5_DIR/lib:$DYLD_LIBRARY_PATH
        > export HDF5_PLUGIN_PATH="$VOL_DIR"
        > export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 

4, Test

    > cd $VOL_DIR/test
    > Edit "Makefile":
        > Update H5_DIR, ABT_DIR and ASYNC_DIR to the correct locations
        > Possibly update the compiler flag macros: DEBUG, CFLAGS, LIBS, ARFLAGS
        > Uncomment the correct DYNLIB & LDFLAGS macro
    > make

    Run both the serial and parallel tests

        > make check

    Run the serial tests only

        > make check_serial

4, Using the Asynchronous I/O VOL connector with application code (Implicit mode with environmental variable)

    The implicit mode allows an application to enable asynchronous I/O VOL connector through setting the following environemental variables and without any application code modification. By default, the HDF5 metadata operations are executed asynchronously, and the dataset operations are executed synchronously unless a cache VOL connector is used.

        > [Set environment variables, from step 3 above]
        > Run your application

5, Using the Asynchronous I/O VOL connector with application code (Explicit mode)

    Please refer to the Makefile and source code (async_test_serial_event_set*) under $VOL_DIR/test/ for example usage.

    5.1 Include header file

        > #include "h5_vol_external_async_native.h" 

    5.2 Use event set and new async API to manage asynchronous I/O operations
        > es_id = H5EScreate();                        // Create event set for tracking async operations
        > fid = H5Fopen_async(.., es_id);              // Asynchronous, can start immediately
        > gid = H5Gopen_async(fid, .., es_id);         // Asynchronous, starts when H5Fopen completes
        > did = H5Dopen_async(gid, .., es_id);         // Asynchronous, starts when H5Gopen completes
        > status = H5Dwrite_async(did, .., es_id);     // Asynchronous, starts when H5Dopen completes, may run concurrently with other H5Dwrite in event set
        > status = H5Dread_async(did, .., es_id);      // Asynchronous, starts when H5Dwrite completes, may run concurrently with other H5Dread in event set
        > H5ESwait(es_id);                             // Wait for operations in event set to complete, buffers used for H5Dwrite must only be changed after wait
        > H5ESclose(es_id);                            // Close the event set

    5.3 Error handling with event set
        > hbool_t es_err_status;
        > status = H5ESget_err_status(es_id, &es_err_status);   // Check if event set has failed operations (es_err_status is set to true)
        > size_t es_err_count;
        > status = H5ESget_err_count(es_id, &es_err_count);     // Retrieve the number of failed operations in this event set
        > size_t num_err_info;
        > H5ES_err_info_t err_info;
        > status = H5ESget_err_info(es_id, 1, &err_info, &es_err_cleared);   // Retrieve information about failed operations (the strings retrieved for each error info must be released with H5free_memory().)
        > printf("API name: %s\nAPI args: %s\nAPI file name: %s\n API func name: %s\nAPI line number: %u\nOperation counter: %llu\nOperation timestamp: %llu\n", err_info.api_name, err_info.api_args, err_info.api_file_name, err_info.api_func_name, err_info.api_line_num, err_info.op_ins_count, err_info.op_ins_ts);    // Retrieve the faile operations's API name, arguments list, file name, function name, line number, operation counter (0-based), and operation timestamp
        > H5free_memory(err_info.api_name);
        > H5free_memory(err_info.api_args);
        > H5free_memory(err_info.app_file_name);
        > H5free_memory(err_info.app_func_name);

    5.4 Run with async
        > [Set environment variables, from step 3 above]
        > Run your application

