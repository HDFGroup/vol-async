Async VOL APIs
==============
Besides the HDF5 EventSet and asynchronous I/O operation APIs, the async VOL connector also provides convinient functions for finer control of the asynchronous I/O operations. Application developers should be very careful with these APIs as they may cause unexpected behavior when not properly used. The "h5_async_lib.h" header file must be included in the application's source code and the static async VOL library (libasynchdf5.a) must be linked.


* Set the ``disable implicit`` flag to the property list, which will force all HDF5 I/O operations to be synchronous, even when the HDF5's explicit ``_async`` APIs are used.

.. code-block::

    herr_t H5Pset_fapl_disable_async_implicit(hid_t fapl, hbool_t is_disable);
    herr_t H5Pset_dxpl_disable_async_implicit(hid_t dxpl, hbool_t is_disable);

    // Retrieve the status of disable implicit mode (true when implicit is disabled).
    herr_t H5Pget_fapl_disable_async_implicit(hid_t fapl, hbool_t *is_disable);
    herr_t H5Pget_dxpl_disable_async_implicit(hid_t dxpl, hbool_t *is_disable);

.. note::
    The ``disable implicit`` flag only becomes effective when the corresponding ``fapl`` or ``dxpl`` is actually used by another HDF5 function call, e.g., with ``H5Fopen`` or ``H5Dwrite``. When a new ``fapl`` or ``dxpl`` is used by any HDF5 function without setting the ``disable implict`` flag, e.g., ``H5P_DEFAULT``, it will reset the mode back to asynchronous execution.

.. code-block::
    // Set the pause flag to property list that pauses all asynchronous I/O operations.
    // Note: similar to the disable implict flag setting, the operations are only paused when
    // the dxpl is used by another HDF5 function call.
    herr_t H5Pset_dxpl_pause(hid_t dxpl, hbool_t is_pause);

* Pause/restart all async operations.

.. code-block::

    // Retrieve the pause flag.
    herr_t H5Pget_dxpl_pause(hid_t dxpl, hbool_t *is_pause);

    // Restart all paused operations, takes effect immediately.
    herr_t H5Fstart(hid_t file_id, hid_t dxpl_id);
    herr_t H5Dstart(hid_t dset_id, hid_t dxpl_id);

* Set and get a delay time for all async operations.

.. code-block::

    // Set a delay time (microseconds) to property list that adds a delay for asynchronous I/O operations.
    herr_t H5Pset_dxpl_delay(hid_t dxpl, uint64_t time_us);

    // Retrieve the delay time.
    herr_t H5Pget_dxpl_delay(hid_t dxpl, uint64_t *time_us);

.. note::
    The operations are only delayed when the dxpl is used by another HDF5 function call.

* Convinient APIs for other stacked VOL connectors

.. warning:: 
    Following APIs are not intended for application use.

.. code-block::

    // Set a dependency parent to a request, should only be used by a another stacked VOL.
    herr_t H5async_set_request_dep(void *request, void *parent_request);

    // Start all paused operations, should only be used by a another stacked VOL.
    herr_t H5async_start(void *request);

