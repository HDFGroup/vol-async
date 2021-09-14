Debug
=====
Async VOL provides a number of options for users to debug potential issues when using it.

Logging
-------
By default, async VOL operates in silent mode and does not print out any message indicating that the asynchronous I/O feature has been enabled. Users may not be certain whether their application is truly utilizing async VOL. It is also possible that the application is not fully converted to use the HDF5 async APIs, and with some operations executed synchronously, only a very limited I/O performance improvement can be observed.

To ensure that async VOL is properly enabled and that the application is using HDF5 async APIs correctly, developers can comment out the following line in h5_async_vol.c, recompile async VOL, and run their application.

.. code-block::

    #define ENABLE_LOG 1

The async VOL will then output logging messages to stderr such as the following:

.. code-block::

    [ASYNC VOL LOG] ASYNC VOL init
    [ASYNC VOL LOG] entering async_file_create
    [ASYNC ABT LOG] entering async_file_create_fn
    [ASYNC ABT LOG] Argobots execute async_file_create_fn success
    [ASYNC VOL LOG] entering async_file_optional
    [ASYNC ABT LOG] entering async_file_optional_fn
    [ASYNC ABT LOG] Argobots execute async_file_optional_fn success
    ...

If these messages were not seen, it is likely that the environment variables described in "Getting Started" are not set properly.

Additionally, if the application has an HDF5 I/O function call (e.g. H5Dwrite) that does not show up in the logging messages ("async_dataset_write_fn success"), it usually means that call was not converted to using async API and being executed synchronously.


Debug Message
-------------
The logging messages provide a simple way to verify that async VOL is working, however, getting more detailed information on how that I/O operations are executed asynchronously is often needed to get to the root cause when the performance improvement fell short of the expectation. More detailed debug messages can be enabled by commenting out the following line in h5_async_vol.c:

.. code-block::

    #ENABLE_DBG_MSG 1

Following is an example of the messages printed out. By default, when running in parallel, only rank 0 will print out the messages in order to avoid duplicated messages from all ranks. 

.. note::
    User can change the value of the ASYNC_DBG_MSG_RANK macro in h5_async_vol.c to let a specific rank output the messages to *stderr*. Setting the value to -1 results in each rank output their debug message to a *file* with "async.log.$rank" as the file name.

.. code-block::

    [ASYNC VOL DBG] set implicit mode to 0
    [ASYNC VOL DBG] create and append [0x2aaaab73fb2a] to new REGULAR task list
    [ASYNC VOL DBG] entering push_task_to_abt_pool
    [ASYNC VOL DBG] push task [0x2aaaab73fb2a] to Argobots pool
    [ASYNC VOL DBG] 0 tasks already in Argobots pool
    [ASYNC VOL DBG] leaving push_task_to_abt_pool
    [ASYNC VOL DBG] entering push_task_to_abt_pool
    [ASYNC VOL DBG] leaving push_task_to_abt_pool
    [ASYNC VOL DBG] async_file_create waiting to finish all previous tasks
    [ASYNC ABT DBG] async_file_create_fn: trying to aquire global lock
    [ASYNC ABT DBG] async_file_create_fn: global lock acquired

.. note::
    Messages with "[ASYNC VOL DBG]" are printed by the application thread entering async VOL, while "[ASYNC ABT DBG]" messages are printed by the background thread when executing the functions.


