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

Attribute Open After Close
--------------------------
Due to an issue in the HDF5 library handling HDF5 VOL objects, calling H5Aopen to the same attribute that was previously close may cause an error like the following:

.. code-block::
   
   ../../src/H5Fint.c:664: H5F__get_objects_cb: Assertion `obj_ptr' failed.
   
This `patch <https://gist.github.com/houjun/208903d8e6a64e2670754d8ca0f6b548>`_ (applies to the HDF5 develop branch) is a temporary fix for the issue.
