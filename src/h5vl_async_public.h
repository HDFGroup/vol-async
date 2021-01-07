/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Taskworks. The full Taskworks copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Async VOL APIs */

#pragma once

#include "H5VLpublic.h"

#ifdef __cplusplus
extern "C" {
#endif

H5_DLL hid_t H5VL_async_register (void);

herr_t H5Pset_vol_async (hid_t fapl_id);
herr_t H5Pget_dxpl_async (hid_t dxpl, hbool_t *is_async);
herr_t H5Pset_dxpl_async (hid_t dxpl, hbool_t is_async);
herr_t H5Pget_dxpl_async_cp_limit (hid_t dxpl, hsize_t *size);
herr_t H5Pset_dxpl_async_cp_limit (hid_t dxpl, hsize_t size);
void H5VLasync_waitall ();
void H5VLasync_finalize ();

#define H5VL_ASYNC_DYN_DATASET_WAIT     "gov.lbl.async.dataset.wait"
#define H5VL_ASYNC_DYN_FILE_WAIT        "gov.lbl.async.file.wait"

static int async_setup(void);
herr_t H5Fwait(hid_t file_id, hid_t dxpl_id);
herr_t H5Dwait(hid_t dset_id, hid_t dxpl_id);
#ifdef __cplusplus
}
#endif
