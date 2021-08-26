HDF5 Async APIs
===============
HDF5-1.13 and later versions added a number of new APIs that allow applications to take advantage of the asynchronous I/O feature provided by the asynchronous I/O VOL connector. They are part of the HDF5 public header, so users only need to include the HDF5 header file (hdf5.h) to use them.

EventSet APIs
-------------
The EventSet APIs can be used to create, manage, and retrieve information from an event set.

.. code-block::

    // H5EScreate creates a new event set and returns a corresponding event set identifier.
    hid_t H5EScreate(void);
    
    // H5ESwait waits for all operations in an event set (es_id) and timeout after (timeout) nanoseconds.
    // The timeout value is accumulated for all operations in the event set.
    // It will also return the number of operations (num_in_progress), and indicate whether any error
    // occurred (err_occurred).
    herr_t H5ESwait(hid_t es_id, uint64_t timeout, size_t *num_in_progress, hbool_t *err_occurred);
    
    // H5ESclose terminates access to an event set (es_id).
    herr_t H5ESclose(hid_t es_id);
    
    // H5EScancel attempt to cancel all operations in an eventset (es_id), some operations may be already
    // in progress and cannot be cancelled, which is recoreded (num_not_canceled), as well as if any error
    // occurred (err_occurred).
    herr_t H5EScancel(hid_t es_id, size_t *num_not_canceled, hbool_t *err_occurred);
    
    // H5ESget_count retrieves number of events in an event set (es_id).
    herr_t H5ESget_count(hid_t es_id, size_t *count);
    
    // H5ESget_err_status checks if an event set (es_id) has failed operations.
    herr_t H5ESget_err_status(hid_t es_id, hbool_t *err_occurred);
    
    // H5ESget_err_count retrieves the number of failed operations in an event set (es_id).
    H5_DLL herr_t H5ESget_err_count(hid_t es_id, size_t *num_errs);
    
    // H5ESget_err_info retrieves information about failed operations in an event set (es_id).  
    // The strings retrieved for each error info must be released by calling H5free_memory.
    herr_t H5ESget_err_info(hid_t es_id, size_t num_err_info, H5ES_err_info_t err_info[], size_t *err_cleared);
    
    // H5ESfree_err_info frees the error info
    herr_t H5ESfree_err_info(size_t num_err_info, H5ES_err_info_t err_info[]);

Explicit Asynchronous I/O APIs
------------------------------
The explicit asynchronous I/O APIs looks very similar to the original synchronous HDF5 I/O functions, they have "_async" as the suffix in the function names, and have an additional event set ID added as the last parameter.

---------------
File operations
---------------
.. code-block::

    hid_t H5Fcreate_async(const char *filename, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t es_id);
    
    hid_t H5Fopen_async(const char *filename, unsigned flags, hid_t access_plist, hid_t es_id);
    
    hid_t H5Freopen_async(hid_t file_id, hid_t es_id);
    
    herr_t H5Fflush_async(hid_t object_id, H5F_scope_t scope, hid_t es_id);
    
    herr_t H5Fclose_async(hid_t file_id, hid_t es_id)
    
----------------
Group operations
----------------
.. code-block::

    hid_t H5Gcreate_async(hid_t loc_id, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t es_id);
    
    hid_t H5Gopen_async(hid_t loc_id, const char *name, hid_t gapl_id, hid_t es_id);
    
    herr_t H5Gget_info_async(hid_t loc_id, H5G_info_t *ginfo, hid_t es_id);
    
    herr_t H5Gget_info_by_name_async(hid_t loc_id, const char *name, H5G_info_t *ginfo,
                                     hid_t lapl_id, hid_t es_id);
    
    herr_t H5Gget_info_by_idx_async(hid_t loc_id, const char *group_name, H5_index_t idx_type,
                                    H5_iter_order_t order, hsize_t n, H5G_info_t *ginfo,
                                    hid_t lapl_id, hid_t es_id);
    
    herr_t H5Gclose_async(hid_t group_id, hid_t es_id);

------------------
Dataset operations
------------------
.. code-block::

    hid_t H5Dcreate_async(hid_t loc_id, const char *name, hid_t type_id, hid_t space_id, 
                          hid_t lcpl_id, hid_t dcpl_id, hid_t dapl_id, hid_t es_id);
    
    hid_t H5Dopen_async(hid_t loc_id, const char *name, hid_t dapl_id, hid_t es_id);
    
    hid_t H5Dget_space_async(hid_t dset_id, hid_t es_id);
    
    herr_t H5Dread_async(hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, 
                         hid_t dxpl_id, void *buf, hid_t es_id);
    
    herr_t H5Dwrite_async(hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, 
                          hid_t dxpl_id, const void *buf, hid_t es_id);
    
    herr_t H5Dset_extent_async(hid_t dset_id, const hsize_t size[], hid_t es_id);
    
    herr_t H5Dclose_async(hid_t dset_id, hid_t es_id);

--------------------
Attribute operations
--------------------
.. code-block::

    herr_t H5Aclose_async(hid_t attr_id, hid_t es_id);
    
    hid_t H5Acreate_async(hid_t loc_id, const char *attr_name, hid_t type_id, hid_t space_id, 
                          hid_t acpl_id, hid_t aapl_id, hid_t es_id);
    
    hid_t H5Acreate_by_name_async(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t type_id,
                                  hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t lapl_id, hid_t es_id);
    
    herr_t H5Aexists_async(hid_t obj_id, const char *attr_name, hbool_t *exists, hid_t es_id);
    
    herr_t H5Aexists_by_name_async(hid_t loc_id, const char *obj_name, const char *attr_name,
                                   hbool_t *exists, hid_t lapl_id, hid_t es_id);
    
    hid_t H5Aopen_async(hid_t obj_id, const char *attr_name, hid_t aapl_id, hid_t es_id);
    
    hid_t H5Aopen_by_idx_async(hid_t loc_id, const char *obj_name, H5_index_t idx_type, H5_iter_order_t order, 
                               hsize_t n, hid_t aapl_id, hid_t lapl_id, hid_t es_id);
    
    hid_t H5Aopen_by_name_async(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t aapl_id,
                                hid_t lapl_id, hid_t es_id);
    
    herr_t H5Aread_async(hid_t attr_id, hid_t dtype_id, void *buf, hid_t es_id);
    
    herr_t H5Arename_async(hid_t loc_id, const char *old_name, const char *new_name, hid_t es_id);
    
    herr_t H5Arename_by_name_async(hid_t loc_id, const char *obj_name, const char *old_attr_name,
                                   const char *new_attr_name, hid_t lapl_id, hid_t es_id);
    
    herr_t H5Awrite_async(hid_t attr_id, hid_t type_id, const void *buf, hid_t es_id);

---------------
Link operations
---------------
.. code-block::

    herr_t H5Lcreate_hard_async(hid_t cur_loc_id, const char *cur_name, hid_t new_loc_id,
                                const char *new_name, hid_t lcpl_id, hid_t lapl_id, hid_t es_id);
    
    herr_t H5Lcreate_soft_async(const char *link_target, hid_t link_loc_id, const char *link_name,
                                hid_t lcpl_id, hid_t lapl_id, hid_t es_id);
    
    herr_t H5Ldelete_async(hid_t loc_id, const char *name, hid_t lapl_id, hid_t es_id);
    
    herr_t H5Ldelete_by_idx_async(hid_t loc_id, const char *group_name, H5_index_t idx_type,
                                  H5_iter_order_t order, hsize_t n, hid_t lapl_id, hid_t es_id);
    
    herr_t H5Lexists_async(hid_t loc_id, const char *name, hbool_t *exists, hid_t lapl_id, hid_t es_id);
    
    herr_t H5Literate_async(hid_t group_id, H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx_p, 
                            H5L_iterate2_t op, void *op_data, hid_t es_id);

-----------------
Object operations
-----------------
.. code-block::

    hid_t H5Oopen_async(hid_t loc_id, const char *name, hid_t lapl_id, hid_t es_id);
    
    hid_t H5Oopen_by_idx_async(hid_t loc_id, const char *group_name, H5_index_t idx_type, 
                               H5_iter_order_t order, hsize_t n, hid_t lapl_id, hid_t es_id);
    
    herr_t H5Oget_info_by_name_async(hid_t loc_id, const char *name, H5O_info2_t *oinfo,
                                     unsigned fields, hid_t lapl_id, hid_t es_id);
    
    herr_t H5Ocopy_async(hid_t src_loc_id, const char *src_name, hid_t dst_loc_id, const char *dst_name, 
                         hid_t ocpypl_id, hid_t lcpl_id, hid_t es_id);
    
    herr_t H5Oclose_async(hid_t object_id, hid_t es_id);
    
    herr_t H5Oflush_async(hid_t obj_id, hid_t es_id);
    
    herr_t H5Orefresh_async(hid_t oid, hid_t es_id);

