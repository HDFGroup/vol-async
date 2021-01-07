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


static bool async_atclose_registered = false;
static int async_dataset_wait_op_g = -1;
static int async_file_wait_op_g = -1;
static hid_t H5VL_async_g = H5I_INVALID_HID;
static void async_reset(void *_ctx)
{
    /* Unregister the async VOL connector, if it's been registered */
    if(H5I_INVALID_HID != H5VL_async_g) {
        H5VLunregister_connector(H5VL_async_g);
        H5VL_async_g = H5I_INVALID_HID;
    } /* end if */

    /* Reset the operation values */
    async_dataset_wait_op_g = -1;
    async_file_wait_op_g = -1;

    /* Reset the 'atclose' callback status */
    async_atclose_registered = false;
}

static int async_setup(void)
{
    /* Singleton check for registering async VOL connector */
    if(H5I_VOL != H5Iget_type(H5VL_async_g)) {
        /* Register async VOL connector */
        H5VL_async_g = H5VLregister_connector_by_name(H5VL_ASYNC_NAME, H5P_DEFAULT);
        if (H5VL_async_g <= 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLregister_connector_by_name\n");
            return -1;
        }
    }

    /* Singleton check for dataset wait operation */
    if(-1 == async_dataset_wait_op_g) {
        /* Look up the operation values */
        if(H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_WAIT, &async_dataset_wait_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return(-1);
        }
        assert(async_dataset_wait_op_g > 0);
    }

    /* Singleton check for file wait operation */
    if(-1 == async_file_wait_op_g) {
        if(H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_WAIT, &async_file_wait_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return(-1);
        }
        assert(async_file_wait_op_g > 0);
    }

    /* Singleton check for registering 'atclose' callback */
    if(!async_atclose_registered) {
        /* Register callback for library shutdown, to release resources */
        if (H5atclose(async_reset, NULL) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5atclose\n");
            return -1;
        }
        async_atclose_registered = true;
    }

    return 0;
}
/*-------------------------------------------------------------------------
 * Function:    H5Fwait
 *
 * Purpose:     Wait for all operations on a file to complete.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fwait(hid_t file_id, hid_t dxpl_id)
{
    /* Look up operation value, if it's not already available */
    if(-1 == async_file_wait_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] H5Fwait: async_setup\n");
            return(-1);
        }
        assert(async_file_wait_op_g > 0);
    }

    /* Call the VOL file optional routine, requesting 'wait' occur */
    if(H5VLfile_optional_op(file_id, async_file_wait_op_g, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] H5Fwait: VOL connector file wait operation\n");
        return(-1);
    }

    return(0);
} /* H5Fwait() */

/*-------------------------------------------------------------------------
 * Function:    H5Dwait
 *
 * Purpose:     Wait for all operations on a dataset to complete.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dwait(hid_t dset_id, hid_t dxpl_id)
{
    /* Look up operation value, if it's not already available */
    if(-1 == async_dataset_wait_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] H5Dwait: async_setup\n");
            return(-1);
        }
        assert(async_dataset_wait_op_g > 0);
    }

    /* Call the VOL file optional routine, requesting 'wait' occur */
    if(H5VLdataset_optional_op(dset_id, async_dataset_wait_op_g, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] H5Dwait: VOL connector dataset wait operation\n");
        return(-1);
    }

    return(0);
} /* H5Dwait*/
