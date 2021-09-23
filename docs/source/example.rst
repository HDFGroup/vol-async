Using Async I/O VOL
===================
The HDF5 asynchronous I/O VOL connector (async I/O VOL) allows applications to fully or partially hide the I/O time by overlapping it with the computation. This page provides more information on how applications can take advantage of it.

The async I/O VOL can be used in ``explicit`` and ``implicit`` modes.

To use the ``implicit`` mode, see `Implicit Mode <https://hdf5-vol-async.readthedocs.io/en/latest/gettingstarted.html#implicit-mode>`_

The recommended mode for applications to use the async I/O VOL is ``explicit mode'', which is described below. 

Explicit Mode
=============
Using the async I/O VOL is ``explicit mode'' requires modifying the application code to use HDF5 asynchronous I/O APIs. 
This includes three steps:

1) adding the EventSet API calls, 
2) switching the existing HDF5 I/O calls to their asynchronous version
3) ensuring data consistency. 

EventSet API
------------
The EventSet APIs allows tracking and inspecting multiple asynchronous I/O operations and avoids the burden for the developer to manage the individual operation. An event set (ID) is an in-memory object that is created by the application and functions similar to a "bag" holding request tokens from one or more asynchronous I/O operations. Every asynchronous I/O operation must be associated with an event set, thus the application must create one before the first HDF5 operation:

.. code-block::

    es_id = H5EScreate();
	
The application can then use this event set ID (``es_id``) for all subsequent HDF5 I/O operations. One can also create multiple event set IDs for different operations. Because the HDF5 functions are non-blocking when async VOL is enabled, the application should also wait for all asynchronous operations to finish before exiting, with:

.. code-block::

    H5ESwait(es_id, H5ES_WAIT_FOREVER, &n_running, &op_failed);

The H5ESwait is also required before closing an event set, with: 

.. code-block::

    H5ESclose(es_id);

HDF5 Asynchronous I/O API
-------------------------
The HDF5-1.13 and later versions provide an asynchronous version of all the HDF5 I/O operations. A complete list of them can be found in the API section. Applications need to switch their existing HDF5 function calls to their asynchronous version, which can be done by adding "_async" to the end of the HDF5 function name and adding an event set ID as the last parameter to the function parameter list. One can also maintain both the original synchronous calls and  asynchronous with a MACRO and decides which to use at compile time, e.g.,:

.. code-block::

    #ifndef ENABLE_HDF5_ASYNC
    H5Fcreate(fname, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    #else
    H5Fcreate_async(fname, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT, es_id);
    #endif

	
Data Consistency
----------------
Once switched to use the event set and asynchronous APIs, the application developer must re-examine the code to ensure data consistency as all the HDF5 I/O calls are non-blocking. 

For data write, the application must not modify or free the write buffer until the write operation has been executed by the background thread. This can be done by calling ``H5ESwait`` before the buffer reuse or free, or by double buffering with alternating one buffer for the asynchronous write and another for new data. Users may also choose to have async VOL to perform the double buffering automatically by adding ``-DENABLE_WRITE_MEMCPY`` to the ``CFLAGS`` in the Makefile, however, this requires double the buffer size in memory while the I/O operation is queued, and should be used with caution (the extra memory copy is automatically freed after the operation completes). 

For data read, an ``H5ESwait`` function is needed before the data is accessed for the asynchronous operation to complete and fill the user's buffer. It is recommended to issue asynchronous read operations as early as possible and has their execution overlap with other tasks of the application to.


An Example
==========
An application may have the following HDF5 operations to write data:

.. code-block::

    // Synchronous file create
    fid = H5Fcreate(...);
    
    // Synchronous group create
    gid = H5Gcreate(fid, ...);
    
    // Synchronous dataset create
    did = H5Dcreate(gid, ..);
    
    // Synchronous dataset write
    status = H5Dwrite(did, ..);
    
    // Synchronous dataset read
    status = H5Dread(did, ..);
    ...
    
    // Synchronous file close
    H5Fclose(fid);
    
    // Continue to computation

which can be converted to use async VOL as the following:

.. code-block::

    // Create an event set to track async operations
    es_id = H5EScreate();
    
    // Asynchronous file create
    fid = H5Fcreate_async(.., es_id);
    
    // Asynchronous group create
    gid = H5Gcreate_async(fid, .., es_id);
    
    // Asynchronous dataset create
    did = H5Dcreate_async(gid, .., es_id);
    
    // Asynchronous dataset write
    status = H5Dwrite_async(did, .., es_id);
    
    // Asynchronous dataset read
    status = H5Dread_async(did, .., es_id);
    
    ...
    
    // Asynchronous file close
    status = H5Fclose_async(fid, .., es_id);
    
    // Continue to computation, overlapping with asynchronous operations
    ...
    
    // Finished computation, Wait for all previous operations in the event set to complete
    H5ESwait(es_id, H5ES_WAIT_FOREVER, &n_running, &op_failed);
    
    // Close the event set
    H5ESclose(es_id);
    ...

.. note::
    More details on using the ``explicit`` mode are available at `Explicit Mode <https://hdf5-vol-async.readthedocs.io/en/latest/gettingstarted.html#explicit-mode>`_
