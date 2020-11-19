/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Public functions callbacks */

#include <assert.h>
#include <hdf5.h>
#include <stdio.h>
#include <stdlib.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_info.h"
#include "h5vl_async_public.h"
#include "h5vl_asynci.h"

herr_t H5Pset_vol_async (hid_t fapl_id) {
	H5VL_async_info_t async_vol_info;
	hid_t under_vol_id;
	void *under_vol_info;
	herr_t status;
	/* int n_thread = ASYNC_VOL_DEFAULT_NTHREAD; */

	/* /1* Initialize the Argobots I/O instance *1/ */
	/* if (NULL == async_instance_g) { */
	/*     if (async_instance_init(n_thread) < 0) { */
	/*         fprintf(stderr,"  [ASYNC VOL ERROR] with async_instance_init\n"); */
	/*         return -1; */
	/*     } */
	/* } */

	if (H5VLis_connector_registered_by_name (H5VL_ASYNC_NAME) == 0) {
		if (H5VL_async_register () < 0) {
			fprintf (stderr, "  [ASYNC VOL ERROR] H5Pset_vol_async: H5VL_async_register\n");
			goto done;
		}
	}

	status = H5Pget_vol_id (fapl_id, &under_vol_id);
	assert (status >= 0);
	assert (under_vol_id > 0);

	status = H5Pget_vol_info (fapl_id, &under_vol_info);
	assert (status >= 0);

	async_vol_info.under_vol_id	  = under_vol_id;
	async_vol_info.under_vol_info = under_vol_info;

	if (H5Pset_vol (fapl_id, H5VL_ASYNC_g, &async_vol_info) < 0) {
		fprintf (stderr, "  [ASYNC VOL ERROR] with H5Pset_vol\n");
		goto done;
	}

done:
	return 1;
}

void H5VL_async_start () {}

herr_t H5Pget_dxpl_async (hid_t dxpl, hbool_t *is_async) {
	herr_t err = 0;
	htri_t isdxpl, pexist;

	isdxpl = H5Pisa_class (dxpl, H5P_DATASET_XFER);
	CHECK_ID (isdxpl)
	if (isdxpl == 0)
		*is_async = false;	// Default property will not pass class check
	else {
		pexist = H5Pexist (dxpl, ASYNC_VOL_PROP_NAME);
		CHECK_ID (pexist)
		if (pexist) {
			err = H5Pget (dxpl, ASYNC_VOL_PROP_NAME, is_async);
			CHECK_ERR

		} else {
			*is_async = false;
		}
	}

err_out:;
	return err;
}

herr_t H5Pset_dxpl_async (hid_t dxpl, hbool_t is_async) {
	herr_t err = 0;
	htri_t isdxpl, pexist;

	isdxpl = H5Pisa_class (dxpl, H5P_DATASET_XFER);
	CHECK_ID (isdxpl)
	if (isdxpl == 0) RET_ERR ("Not dxplid")

	pexist = H5Pexist (dxpl, ASYNC_VOL_PROP_NAME);
	CHECK_ID (pexist)
	if (!pexist) {
		hbool_t f = false;
		err = H5Pinsert2 (dxpl, ASYNC_VOL_PROP_NAME, sizeof (hbool_t), &f, NULL, NULL, NULL, NULL,
						  NULL, NULL);
		CHECK_ERR
	}

	err = H5Pset (dxpl, ASYNC_VOL_PROP_NAME, &is_async);
	CHECK_ERR

err_out:;
	return err;
}

herr_t H5Pget_dxpl_async_cp_limit (hid_t dxpl, hsize_t *size) {
	herr_t ret;

	return ret;
}

herr_t H5Pset_dxpl_async_cp_limit (hid_t dxpl, hsize_t size) {
	herr_t ret;
	return ret;
}

void H5VLasync_waitall () { herr_t ret; }

void H5VLasync_finalize () {}
