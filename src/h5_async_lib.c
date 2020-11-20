/*
 * Purpose:     These are public API routines to complement the "async" VOL
 *              connector, providing an application with the means to set
 *              properties to control async VOL connector behavior.
 *
 */

/* Header files needed */
#include <assert.h>
#include <string.h>

/* Public HDF5 file */
#include "hdf5.h"

/* The connector's header */
#include "h5_vol_external_async_native.h"

/* Header for async "extension" routines */
#include "h5_async_lib.h"

/**********/
/* Macros */
/**********/


/************/
/* Typedefs */
/************/


/*******************/
/* Global Variables*/
/*******************/


/********************* */
/* Function prototypes */
/********************* */


/*******************/
/* Local variables */
/*******************/

/* Track whether the 'atclose' callback is registered yet */
static bool async_atclose_registered = false;

/* The connector identification number, initialized at runtime */
static hid_t H5VL_async_g = H5I_INVALID_HID;

/* Operation values for new "API" routines */
/* These are initialized in the VOL connector's 'init' callback at runtime and
 *      looked up here, when an operation from this package is invoked..
 *      It's good practice to reset them back to -1 in the 'atclose' callback.
 */
static int async_dataset_wait_op_g = -1;
static int async_file_wait_op_g = -1;

static void
async_reset(void *_ctx)
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

static int
async_setup(void)
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

herr_t
H5Pset_vol_async(hid_t fapl_id)
{
    hid_t under_vol_id;
    herr_t status;

    /* Singleton wrapper library setup */
    if(H5I_INVALID_HID == H5VL_async_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] H5Pset_vol_async: async_setup\n");
            goto done;
        }
        assert(H5I_INVALID_HID != H5VL_async_g);
    }

    status = H5Pget_vol_id(fapl_id, &under_vol_id);
    assert(status >= 0);
    assert(under_vol_id > 0);

    if(under_vol_id != H5VL_async_g) {
        H5VL_async_info_t async_vol_info;
        void *under_vol_info;

        status = H5Pget_vol_info(fapl_id, &under_vol_info);
        assert(status >= 0);

        async_vol_info.under_vol_id   = under_vol_id;
        async_vol_info.under_vol_info = under_vol_info;

        if (H5Pset_vol(fapl_id, H5VL_async_g, &async_vol_info) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5Pset_vol\n");
            goto done;
        }
    }

    return 0;

done:
    return -1;
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

