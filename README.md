# HDF5 Asynchronous I/O VOL Connector

[**Full documentation**](https://hdf5-vol-async.readthedocs.io)

Asynchronous I/O is becoming increasingly popular with the large amount of data access required by scientific applications. They can take advantage of an asynchronous interface by scheduling I/O as early as possible and overlap computation or communication with I/O operations, which hides the cost associated with I/O and improves the overall performance.

Some configuration parameters used in the instructions:

        VOL_DIR : directory of HDF5 Asynchronous I/O VOL connector repository
        ABT_DIR : directory of Argobots source code
        H5_DIR  : directory of HDF5 source code
        
We have tested async VOL compiled with GNU(gcc 6.4+), Intel, and Cray compilers on Summit, Cori, and Theta supercomputers.

## 1. Preparation

1.1 Download the Asynchronous I/O VOL connector code (this repository) with Argobots git submodule 

    > git clone --recursive https://github.com/hpc-io/vol-async.git
    > # Latest Argobots can also be downloaded separately from https://github.com/pmodels/argobots

1.2 Download the HDF5 source code

    > git clone https://github.com/HDFGroup/hdf5.git

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
    # Note: using mpixlC on Summit will result in Argobots runtime error, use xlC or gcc instead.

2.3 Compile Asynchronous VOL connector

    > cd $VOL_DIR/src
    > # Edit "Makefile"
    > # Use a template Makefile: e.g. "cp Makefile.summit Makefile"
    > # Change the path of HDF5_DIR and ABT_DIR to $H5_DIR/install and $ABT_DIR/install
    > # (Optional) update the compiler flag macros: DEBUG, CFLAGS, LIBS, ARFLAGS
    > # (Optional) comment/uncomment the correct DYNLDFLAGS & DYNLIB macros
    > make

## 3. Set Environment Variables

Will need to set the following environmental variable before running the asynchronous operation tests and your async application, e.g.:

for Linux:

    > export LD_LIBRARY_PATH=$VOL_DIR/src:$H5_DIR/install/lib:$ABT_DIR/install/lib:$LD_LIBRARY_PATH
    > export HDF5_PLUGIN_PATH="$VOL_DIR/src"
    > export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 
    > (optional) export MPICH_MAX_THREAD_SAFETY=multiple # Some systems like Cori@NERSC need this to support MPI_THREAD_MULTIPLE 

MacOS:

    > export DYLD_LIBRARY_PATH=$VOL_DIR/src:$H5_DIR/install/lib:$ABT_DIR/install/lib:$DYLD_LIBRARY_PATH
    > export HDF5_PLUGIN_PATH="$VOL_DIR/src"
    > export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 

## 4. Test

    > cd $VOL_DIR/test
    > # Edit "Makefile":
    > # Use a template Makefile: e.g. "cp Makefile.summit Makefile"
    > # Update H5_DIR, ABT_DIR and ASYNC_DIR to the correct paths of their installation directory
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

## 5. Implicit mode

The implicit mode allows an application to enable asynchronous I/O through setting the following environemental variables and without any major code change. 
By default, the HDF5 metadata operations are executed asynchronously, and the dataset operations are executed synchronously.

    > # Set environment variables: HDF5_PLUGIN_PATH and HDF5_VOL_CONNECTOR
    > Run your application

## 6. Explicit mode

6.1 Use MPI_THREAD_MULTIPLE

The asynchronous tasks often involve MPI collecive operations from the HDF5 library, and they may be executee concurrently with your application's MPI operations, 
thus we require to initialize MPI with MPI_THREAD_MULTIPLE support. Change MPI_Init(argc, argv) in your application's code to the following:

    > MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
        
6.2 Use event set and new async API to manage asynchronous I/O operations

    > es_id = H5EScreate();                        // Create event set for tracking async operations
    > fid = H5Fopen_async(.., es_id);              // Asynchronous, can start immediately
    > gid = H5Gopen_async(fid, .., es_id);         // Asynchronous, starts when H5Fopen completes
    > did = H5Dopen_async(gid, .., es_id);         // Asynchronous, starts when H5Gopen completes
    > status = H5Dwrite_async(did, .., es_id);     // Asynchronous, starts when H5Dopen completes
    > status = H5Dread_async(did, .., es_id);      // Asynchronous, starts when H5Dwrite completes
    > H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed); 
    > # Wait for operations in event set to complete, buffers used for H5Dwrite must only be changed after
    > H5ESclose(es_id);                            // Close the event set (must wait first)

6.3 Error handling with event set

    > H5ESget_err_status(es_id, &es_err_status);   // Check if event set has failed operations
    > H5ESget_err_count(es_id, &es_err_count);     // Retrieve the number of failed operations in this event set
    > H5ESget_err_info(es_id, 1, &err_info, &es_err_cleared);   // Retrieve information about failed operations 
    > H5free_memory(err_info.api_name);
    > H5free_memory(...)

6.4 Run with async

    > # Set environment variables: HDF5_PLUGIN_PATH and HDF5_VOL_CONNECTOR
    > Run your application

## Know Issues
When an application has a large number of HDF5 function calls, an error like the following may occur:

    *** Process received signal ***
    Signal: Segmentation fault: 11 (11)
    Signal code: (0)
    Failing at address: 0x0
    [ 0] 0 libsystem_platform.dylib 0x00007fff20428d7d _sigtramp + 29
    [ 1] 0 ??? 0x0000000000000000 0x0 + 0
    [ 2] 0 libabt.1.dylib 0x0000000105bdbdc0 ABT_thread_create + 128
    [ 3] 0 libh5async.dylib 0x00000001064bde1f push_task_to_abt_pool + 559
   
This is due to the default Argobots thread stack size being too small (16384), and can be resovled by setting the environement variable:

    export ABT_THREAD_STACKSIZE=100000
