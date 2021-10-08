Known Issues
============

ABT_thread_create SegFault
--------------------------
When an application has a large number of HDF5 function calls, an error like the following may occur:

.. code-block::

    *** Process received signal ***
    Signal: Segmentation fault: 11 (11)
    Signal code: (0)
    Failing at address: 0x0
    [ 0] 0 libsystem_platform.dylib 0x00007fff20428d7d _sigtramp + 29
    [ 1] 0 ??? 0x0000000000000000 0x0 + 0
    [ 2] 0 libabt.1.dylib 0x0000000105bdbdc0 ABT_thread_create + 128
    [ 3] 0 libh5async.dylib 0x00000001064bde1f push_task_to_abt_pool + 559

This is due to the default Argobots thread stack size being too small, and can be resovled by setting the environement variable:

.. code-block::

    export ABT_THREAD_STACKSIZE=100000

Object Flush
------------
Currently due to an issue with the HDF5 library handling its internal object ID, asynchronous and synchronous flush operations will fail. 

EventSet ID with Multiple Files
-------------------------------
Currently each event set ID should only be associated with operations of one file, otherwise there can be unexpected errors from the HDF5 library.
This `patch <https://gist.github.com/houjun/4c556f5e5c5e64275c3f412eca395c4e>`_ (applies to the HDF5 develop branch) may be needed when an HDF5 iteration error occurs.
