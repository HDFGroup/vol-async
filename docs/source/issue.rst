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

