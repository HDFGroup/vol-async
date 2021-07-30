/*
 * Purpose:	The public header file for the async VOL connector.
 */

#ifndef __h5_async_lib_H
#define __h5_async_lib_H

/* Public headers needed by this file */
#include "hdf5.h" /* HDF5 */

#ifdef __cplusplus
extern "C" {
#endif

/* API routines for this package */
extern herr_t H5Pset_vol_async(hid_t fapl_id);
extern herr_t H5Pset_fapl_disable_async_implicit(hid_t fapl, hbool_t is_disable);
extern herr_t H5Pget_fapl_disable_async_implicit(hid_t fapl, hbool_t *is_disable);
extern herr_t H5Pset_dxpl_disable_async_implicit(hid_t dxpl, hbool_t is_disable);
extern herr_t H5Pget_dxpl_disable_async_implicit(hid_t dxpl, hbool_t *is_disable);
extern herr_t H5Pset_dxpl_pause(hid_t dxpl, hbool_t is_pause);
extern herr_t H5Pget_dxpl_pause(hid_t dxpl, hbool_t *is_pause);
extern herr_t H5Pset_dxpl_delay(hid_t dxpl, uint64_t time_us);
extern herr_t H5Pget_dxpl_delay(hid_t dxpl, uint64_t *time_us);
extern herr_t H5Fwait(hid_t file_id, hid_t dxpl_id);
extern herr_t H5Dwait(hid_t dset_id, hid_t dxpl_id);
extern herr_t H5Fstart(hid_t file_id, hid_t dxpl_id);
extern herr_t H5Dstart(hid_t dset_id, hid_t dxpl_id);
extern herr_t H5Fpause(hid_t file_id, hid_t dxpl_id);
extern herr_t H5Dpause(hid_t dset_id, hid_t dxpl_id);
extern herr_t H5Fset_delay_time(hid_t file_id, hid_t dxpl_id, uint64_t time_us);
extern herr_t H5Dset_delay_time(hid_t dset_id, hid_t dxpl_id, uint64_t time_us);
extern herr_t H5async_set_request_dep(void *request, void *parent_request);
extern herr_t H5async_start(void *request);

#ifdef __cplusplus
}
#endif

#endif /* __h5_async_lib_H */
