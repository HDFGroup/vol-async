Background
==========

Asynchronous I/O is becoming increasingly popular with the large amount of data access required by scientific applications. They can take advantage of an asynchronous interface by scheduling I/O as early as possible and overlap computation or communication with I/O operations, which hides the cost associated with I/O and improves the overall performance.


Preparation
-----------

Some configuration parameters used in the instructions:

.. code-block::

    VOL_DIR : directory of HDF5 Asynchronous I/O VOL connector repository
    ABT_DIR : directory of Argobots source code
    H5_DIR  : directory of HDF5 source code


#. Download the Asynchronous I/O VOL connector code (this repository) with Argobots git submodule Use system provided by HDF5.
Latest Argobots can also be downloaded separately from https://github.com/pmodels/argobots

.. code-block::

    git clone --recursive https://github.com/hpc-io/vol-async.git


#. Download the HDF5 source code

.. code-block::

    git clone https://github.com/HDFGroup/hdf5.git


#. (Optional) Set the environment variables for the paths of the codes if the full path of VOL_DIR, ABT_DIR, and H5_DIR are not used in later setup.

.. code-block::

    export H5_DIR=/path/to/hdf5/dir
    export VOL_DIR=/path/to/async_vol/dir
    export ABT_DIR=/path/to/argobots/dir


Installation
------------

#. Compile HDF5

.. code-block::

    cd $H5_DIR
    ./autogen.sh
    (Optional) CC=mpicc/gcc/cc
    ./configure --prefix=$H5_DIR/install --enable-parallel --enable-threadsafe --enable-unsupported 
    make && make install


#. Compile Argobots

.. code-block::

    cd $ABT_DIR
    ./autogen.sh  (may skip this step if the configure file exists)
    ./configure --prefix=$ABT_DIR/install #(may need to add CC=cc or CC=mpicc)
    make && make install


#. Compile Asynchronous VOL connector

.. code-block::

    cd $VOL_DIR/src
    Edit "Makefile"
        Copy a sample Makefile (Makefile.cori, Makefile.summit, Makefile.macos), e.g. "cp Makefile.summit Makefile", which should work for most linux systems
        Change the path of HDF5_DIR and ABT_DIR to $H5_DIR/install and $ABT_DIR/install (replace $H5_DIR and $ABT_DIR with their full path)
        (Optional) update the compiler flag macros: DEBUG, CFLAGS, LIBS, ARFLAGS
        (Optional) comment/uncomment the correct DYNLDFLAGS & DYNLIB macros
    make


Set Environmental Variables
---------------------------

Async VOL requires the setting of the following environmental variable to enable asynchronous I/O:

* Linux:

.. code-block::

    export LD_LIBRARY_PATH=$VOL_DIR/src:$H5_DIR/install/lib:$ABT_DIR/install/lib:$LD_LIBRARY_PATH
    export HDF5_PLUGIN_PATH="$VOL_DIR/src"
    export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 

* MacOS
.. code-block::

    export DYLD_LIBRARY_PATH=$VOL_DIR/src:$H5_DIR/install/lib:$ABT_DIR/install/lib:$DYLD_LIBRARY_PATH
    export HDF5_PLUGIN_PATH="$VOL_DIR/src"
    export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 


Test
----

#. Compile test codes.

.. code-block::

    cd $VOL_DIR/test
    Edit "Makefile":
        Copy a sample Makefile (Makefile.cori, Makefile.summit, Makefile.macos), e.g. "cp Makefile.summit Makefile", Makefile.summit should work for most linux systems
        Update H5_DIR, ABT_DIR and ASYNC_DIR to the correct paths of their installation directory
        (Optional) update the compiler flag macros: DEBUG, CFLAGS, LIBS, ARFLAGS
        (Optional) comment/uncomment the correct DYNLIB & LDFLAGS macro
    make


#. Run serial and parallel tests
.. note::
    Running the automated tests requires Python3.

(Optional) If the system is not using mpirun to launch MPI tasks, edit mpirun_cmd in pytest.py with the corresponding MPI launch command.

.. code-block::

    //Run serial and parallel tests
    make check

    //Run the serial tests only
    make check_serial

If any test fails, check async_vol_test.err in the test directory for the error message. 

(Optional) With certain file systems where file locking is not supported, an error of "file create failed" may occur and can be fixed with "export HDF5_USE_FILE_LOCKING=FALSE", which disables the HDF5 file locking.


Implicit mode
-------------

The implicit mode allows an application to enable asynchronous I/O through setting the following environemental variables and without any major code change. By default, the HDF5 metadata operations are executed asynchronously, and the dataset operations are executed synchronously.
.. note::
    Due to the limitations of the implicit mode, we highly recommend applications to use the explicit mode for the best I/O performance.

.. code-block::

    [Set environment variables, from step 3 above]
    Run your application


Explicit mode
-------------

Please refer to the Makefile and source code (async_test_serial_event_set*) under $VOL_DIR/test/ for example usage.

#. (Required) Set async VOL environment variables
See :ref:`(Optional) Async VOL double buffering`.

#. (Required) Init MPI with MPI_THREAD_MULTIPLE

Parallel HDF5 involve MPI collecive operations in many of its internal metadata operations, and they can be executed concurrently with the application's MPI operations, thus we require to initialize MPI with MPI_THREAD_MULTIPLE support. Change MPI_Init(argc, argv) in your application's code to the following:

.. code-block::

    MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);

#. (Required) Use event set and new async API to manage asynchronous I/O operations, see API section for a complete of APIs.

.. code-block::

    // Create event set for tracking async operations
    es_id = H5EScreate();
    fid = H5Fcreate_async(.., es_id);
    did = H5Dopen_async(fid, .., es_id);
    H5Dwrite_async(did, .., es_id);
    H5Dclose_async(did, .., es_id);
    H5Fclose_async(fid, .., es_id);
    // Wait for operations in event set to complete
    H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed); 
    // Close the event set (must wait first)
    H5ESclose(es_id);

.. warning::
    The buffers used for H5Dwrite can only be changed after H5ESwait unless async VOL double buffering is enabled, see :ref:`(Optional) Async VOL double buffering`.

#. (Optional) Error handling with event set
Although it is listed as optional, it is highly recommended to integrate the asynchronous I/O error checking into the application code.

.. code-block::

    // Check if event set has failed operations (es_err_status is set to true)
    status = H5ESget_err_status(es_id, &es_err_status);
    // Retrieve the number of failed operations in this event set
    H5ESget_err_count(es_id, &es_err_count);
    // Retrieve information about failed operations 
    H5ESget_err_info(es_id, 1, &err_info, &es_err_cleared);
    // Inspect and handle the error if there is any
    ...
    // Free memory
    H5free_memory(err_info.api_name);
    H5free_memory(err_info.api_args);
    H5free_memory(err_info.app_file_name);
    H5free_memory(err_info.app_func_name);


#. (Optional) Async VOL double buffering
Applications may choose to have async VOL to manage the write buffer consistency. When enabled, async VOL will automatically makes a memory copy of the buffer for data writes. This increases the runtime memory usage but relieves the burden for the application to manage the double buffering. The copy is automatically freed after the background asynchronous write completes.

.. code-block::
    Add -DENABLE_WRITE_MEMCPY=1 to the end of the CFLAGS line of src/Makefile before compiling.

#. (Optional) Include the header file if async VOL API is used (see Async API section)

.. code-block::

	#include "h5_async_vol.h" 

#. (Optional) Finer control of asynchronous I/O operation
When async VOL is enabled, each HDF5 operation is recorded and put into a task queue and returns without actually executing it. The async VOL detects whether the application is busy issuing HDF5 I/O calls or has moved on to other tasks (e.g. computation). If it finds no HDF5 function is called within a short period (600ms by default), it will start the background thread to execute the tasks in the queue. This is mainly due to the global mutex from the HDF5, allowing only one thread to execute the HDF5 operations at a given time to maintain its internal data consistency. The application status detection can avoid an effectively synchronous I/O when the application thread and the async VOL background thread acquire the mutex in an interleaved fashion. However, some applications may have larger time gaps between HDF5 function calls and experience partially asynchronous behavior. To mitigate this, we provide a way by setting an environment variable that informs async VOL to queue the operations and not start their execution until file/group/dataset close time. This is especially useful for applications that periodically output (write-only) data, e.g. checkpoint, and can take full advantage of the asynchronous I/O. 

.. warning::
    This option requires the application developer to ensure that no deadlock occurs.

.. code-block::
	// Start execution at file close time
	export HDF5_ASYNC_EXE_FCLOSE=1
	// Start execution at group close time
	export HDF5_ASYNC_EXE_GCLOSE=1
	// Start execution at dataset close time
	export HDF5_ASYNC_EXE_DCLOSE=1


