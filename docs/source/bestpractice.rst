Best Practices
==============

Overlap time to hide I/O latency
--------------------------------
To take full advantage of the async I/O VOL connector, applications should have enough non-I/O time for the asynchronous operations to overlap with. This can often be achieved to separate the I/O phases of an application from the compute/communication phase. The compute/communication phases have to be long enough to hide the latency in an I/O phase. When there is not enough compute/communication time to overlap, async I/O may only hide partial I/O latency. 

Inform async VOL on access pattern
----------------------------------

By default, the async VOL detects whether the application is busy issuing HDF5 I/O calls or has moved on to other tasks (e.g., computation). If it finds no HDF5 function is called within a short wait period (600 ms by default), it will start the background thread to execute the tasks in the queue. Such status detection can avoid an effectively synchronous I/O when the application thread and the async VOL background thread acquire the HDF5 global mutex in an interleaved fashion. However, some applications may have larger time gaps than the default wait period between HDF5 function calls and experience partially asynchronous behavior. To avoid this, one can set the following environment variable to disable its active ''wait and check'' mechnism and inform async VOL when to start the async execution, this is especially useful for checkpointing data.

.. code-block::

    // Start execution at file close time
    export HDF5_ASYNC_EXE_FCLOSE=1
    // Start execution at group close time
    export HDF5_ASYNC_EXE_GCLOSE=1
    // Start execution at dataset close time
    export HDF5_ASYNC_EXE_DCLOSE=1

.. warning::
    This option requires the application developer to ensure that no deadlock occurs.

Mix sync and async operations
-----------------------------
It is generally discouraged to mix sync and async operations in an application, as deadlocks may occur unexpectedly. If it is unavoidable, we recommend to separate the sync and async operations as much as possible (ideally using different HDF5 file IDs, even they are opearting on the same file) and set the following FAPL property for the sync operations:

.. code-block::

    fapl_sync = H5Pcreate(H5P_FILE_ACCESS);
    hbool_t is_disable = true;
    if (H5Pinsert2(fapl_sync,  "gov.lbl.async.disable.implicit", sizeof(hbool_t),
                   &is_disable, NULL, NULL, NULL, NULL, NULL, NULL) < 0) {
        fprintf(stderr, "Disable hdf5 async failed!\n");
    }
    fid_sync = H5Fcreate(fname, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_sync);
    H5Pclose(fapl_sync)

Forced synchronous execution
----------------------------
Due to the nature of some HDF5 APIs such as H5\*exists, H5\*get\*, H5\*set\*, they must be executed synchronously and immediately in order to provide the correct return value back to the application, even asynchronous I/O VOL is used. This may lead to an effectively synchronous performance, especially when they are interleaved with other operations that can be executed asynchronously. It is recommended that applications either avoid these calls (by caching HDF5 IDs) or place them at the very beginning of the I/O phase.
