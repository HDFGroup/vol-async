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

/* The connector's private header */
#include "h5_async_vol_private.h"

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
static int async_dataset_wait_op_g  = -1;
static int async_file_wait_op_g     = -1;
static int async_file_start_op_g    = -1;
static int async_dataset_start_op_g = -1;
static int async_file_pause_op_g    = -1;
static int async_dataset_pause_op_g = -1;
static int async_file_delay_op_g    = -1;
static int async_dataset_delay_op_g = -1;
static int async_request_start_op_g = -1;
static int async_request_dep_op_g   = -1;

static void
async_reset(void *_ctx)
{
    /* Unregister the async VOL connector, if it's been registered */
    if (H5I_INVALID_HID != H5VL_async_g) {
        H5VLunregister_connector(H5VL_async_g);
        H5VL_async_g = H5I_INVALID_HID;
    } /* end if */

    /* Reset the operation values */
    async_dataset_wait_op_g  = -1;
    async_file_wait_op_g     = -1;
    async_dataset_start_op_g = -1;
    async_file_start_op_g    = -1;
    async_dataset_pause_op_g = -1;
    async_file_pause_op_g    = -1;
    async_dataset_delay_op_g = -1;
    async_file_delay_op_g    = -1;
    async_request_start_op_g = -1;
    async_request_dep_op_g   = -1;

    /* Reset the 'atclose' callback status */
    async_atclose_registered = false;
}

static int
async_setup(void)
{
    /* Singleton check for registering async VOL connector */
    if (H5I_VOL != H5Iget_type(H5VL_async_g)) {
        /* Register async VOL connector */
        H5VL_async_g = H5VLregister_connector_by_name(H5VL_ASYNC_NAME, H5P_DEFAULT);
        if (H5VL_async_g <= 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLregister_connector_by_name\n");
            return -1;
        }
    }

    /* Singleton check for dataset wait operation */
    if (-1 == async_dataset_wait_op_g) {
        /* Look up the operation values */
        if (H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_WAIT,
                                   &async_dataset_wait_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_dataset_wait_op_g > 0);
    }

    /* Singleton check for file wait operation */
    if (-1 == async_file_wait_op_g) {
        if (H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_WAIT, &async_file_wait_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_file_wait_op_g > 0);
    }

    /* Singleton check for file start operation */
    if (-1 == async_file_start_op_g) {
        if (H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_START, &async_file_start_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_file_start_op_g > 0);
    }

    /* Singleton check for dataset start operation */
    if (-1 == async_dataset_start_op_g) {
        if (H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_START,
                                   &async_dataset_start_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_dataset_start_op_g > 0);
    }

    /* Singleton check for file pause operation */
    if (-1 == async_file_pause_op_g) {
        if (H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_PAUSE, &async_file_pause_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_file_pause_op_g > 0);
    }

    /* Singleton check for dataset pause operation */
    if (-1 == async_dataset_pause_op_g) {
        if (H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_PAUSE,
                                   &async_dataset_pause_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_dataset_pause_op_g > 0);
    }

    /* Singleton check for file delay operation */
    if (-1 == async_file_delay_op_g) {
        if (H5VLfind_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_DELAY, &async_file_delay_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_file_delay_op_g > 0);
    }

    /* Singleton check for dataset delay operation */
    if (-1 == async_dataset_delay_op_g) {
        if (H5VLfind_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_DELAY,
                                   &async_dataset_delay_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_dataset_delay_op_g > 0);
    }

    /* Singleton check for request start operation */
    if (-1 == async_request_start_op_g) {
        if (H5VLfind_opt_operation(H5VL_SUBCLS_REQUEST, H5VL_ASYNC_DYN_REQUEST_START,
                                   &async_request_start_op_g) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_request_start_op_g > 0);
    }

    /* Singleton check for file pause operation */
    if (-1 == async_request_dep_op_g) {
        if (H5VLfind_opt_operation(H5VL_SUBCLS_REQUEST, H5VL_ASYNC_DYN_REQUEST_DEP, &async_request_dep_op_g) <
            0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLfind_opt_operation\n");
            return (-1);
        }
        assert(async_request_dep_op_g > 0);
    }

    /* Singleton check for registering 'atclose' callback */
    if (!async_atclose_registered) {
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
    hid_t  under_vol_id;
    herr_t status;

    /* Singleton wrapper library setup */
    if (H5I_INVALID_HID == H5VL_async_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            goto done;
        }
        assert(H5I_INVALID_HID != H5VL_async_g);
    }

    status = H5Pget_vol_id(fapl_id, &under_vol_id);
    assert(status >= 0);
    assert(under_vol_id > 0);

    if (under_vol_id != H5VL_async_g) {
        H5VL_async_info_t async_vol_info;
        void *            under_vol_info;

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
    H5VL_optional_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Look up operation value, if it's not already available */
    if (-1 == async_file_wait_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_file_wait_op_g > 0);
    }

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = async_file_wait_op_g;
    vol_cb_args.args    = NULL;

    /* Call the VOL file optional routine, requesting 'wait' occur */
    if (H5VLfile_optional_op(file_id, &vol_cb_args, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] H5Fwait: VOL connector file wait operation\n");
        return (-1);
    }

    return (0);
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
    H5VL_optional_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Look up operation value, if it's not already available */
    if (-1 == async_dataset_wait_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_dataset_wait_op_g > 0);
    }

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = async_dataset_wait_op_g;
    vol_cb_args.args    = NULL;

    /* Call the VOL dataset optional routine, requesting 'wait' occur */
    if (H5VLdataset_optional_op(dset_id, &vol_cb_args, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] H5Dwait: VOL connector dataset wait operation\n");
        return (-1);
    }

    return (0);
} /* H5Dwait*/

/*-------------------------------------------------------------------------
 * Function:    H5Fstart
 *
 * Purpose:     Start executing the asynchronous tasks immediately
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fstart(hid_t file_id, hid_t dxpl_id)
{
    H5VL_optional_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Look up operation value, if it's not already available */
    if (-1 == async_file_start_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_file_start_op_g > 0);
    }

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = async_file_start_op_g;
    vol_cb_args.args    = NULL;

    /* Call the VOL file optional routine, requesting 'wait' occur */
    if (H5VLfile_optional_op(file_id, &vol_cb_args, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] H5Fstart: VOL connector file start operation failed!\n");
        return (-1);
    }

    return (0);
}

/*-------------------------------------------------------------------------
 * Function:    H5Dstart
 *
 * Purpose:     Start executing the asynchronous tasks immediately
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dstart(hid_t dset_id, hid_t dxpl_id)
{
    H5VL_optional_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Look up operation value, if it's not already available */
    if (-1 == async_dataset_start_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_dataset_start_op_g > 0);
    }

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = async_dataset_start_op_g;
    vol_cb_args.args    = NULL;

    /* Call the VOL dset optional routine, requesting 'wait' occur */
    if (H5VLdataset_optional_op(dset_id, &vol_cb_args, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] H5Dstart: VOL connector dset start operation failed!\n");
        return (-1);
    }

    return (0);
}

/*-------------------------------------------------------------------------
 * Function:    H5Dpause
 *
 * Purpose:     pause executing the asynchronous tasks immediately
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fpause(hid_t file_id, hid_t dxpl_id)
{
    H5VL_optional_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Look up operation value, if it's not already available */
    if (-1 == async_file_pause_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_file_pause_op_g > 0);
    }

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = async_file_pause_op_g;
    vol_cb_args.args    = NULL;

    /* Call the VOL file optional routine, requesting 'wait' occur */
    if (H5VLfile_optional_op(file_id, &vol_cb_args, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] H5Fpause: VOL connector file pause operation failed!\n");
        return (-1);
    }

    return (0);
}

/*-------------------------------------------------------------------------
 * Function:    H5Dpause
 *
 * Purpose:     pause executing the asynchronous tasks immediately
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dpause(hid_t dset_id, hid_t dxpl_id)
{
    H5VL_optional_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Look up operation value, if it's not already available */
    if (-1 == async_dataset_pause_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_dataset_pause_op_g > 0);
    }

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = async_dataset_pause_op_g;
    vol_cb_args.args    = NULL;

    /* Call the VOL dset optional routine, requesting 'wait' occur */
    if (H5VLdataset_optional_op(dset_id, &vol_cb_args, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] H5Dpause: VOL connector dset pause operation failed!\n");
        return (-1);
    }

    return (0);
}

/*-------------------------------------------------------------------------
 * Function:    H5Fset_delay_time
 *
 * Purpose:     Set the delay execution time for background thread
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fset_delay_time(hid_t file_id, hid_t dxpl_id, uint64_t time_us)
{
    H5VL_optional_args_t    vol_cb_args; /* Arguments to VOL callback */
    H5VL_async_delay_args_t delay_args;  /* Arguments for optional operation */

    /* Look up operation value, if it's not already available */
    /* Look up operation value, if it's not already available */
    if (-1 == async_file_delay_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_file_delay_op_g > 0);
    }

    /* Set up VOL callback arguments */
    delay_args.delay_time = time_us;
    vol_cb_args.op_type   = async_file_delay_op_g;
    vol_cb_args.args      = &delay_args;

    /* Call the VOL dset optional routine, requesting 'wait' occur */
    if (H5VLfile_optional_op(file_id, &vol_cb_args, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s: VOL connector set file delay operation failed!\n", __func__);
        return (-1);
    }

    return (0);
}

/*-------------------------------------------------------------------------
 * Function:    H5Dset_delay_time
 *
 * Purpose:     Set the delay execution time for background thread
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dset_delay_time(hid_t dset_id, hid_t dxpl_id, uint64_t time_us)
{
    H5VL_optional_args_t    vol_cb_args; /* Arguments to VOL callback */
    H5VL_async_delay_args_t delay_args;  /* Arguments for optional operation */

    /* Look up operation value, if it's not already available */
    if (-1 == async_dataset_delay_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_dataset_delay_op_g > 0);
    }

    /* Set up VOL callback arguments */
    delay_args.delay_time = time_us;
    vol_cb_args.op_type   = async_dataset_delay_op_g;
    vol_cb_args.args      = &delay_args;

    /* Call the VOL dset optional routine, requesting 'wait' occur */
    if (H5VLdataset_optional_op(dset_id, &vol_cb_args, dxpl_id, H5ES_NONE) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s: VOL connector set dset delay operation failed!\n", __func__);
        return (-1);
    }

    return (0);
}

/*-------------------------------------------------------------------------
 * Function:    H5Pset_dxpl_disable_async_implicit
 *
 * Purpose:     Set to disable async implicit mode with dxpl
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_dxpl_disable_async_implicit(hid_t dxpl, hbool_t is_disable)
{
    herr_t status;

    status = H5Pexist(dxpl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
        return -1;
    }
    else if (status == 0) {
        status = H5Pinsert2(dxpl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME, sizeof(hbool_t), &is_disable, NULL, NULL,
                            NULL, NULL, NULL, NULL);
        if (status < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pinsert2 failed!\n", __func__);
            return (-1);
        }
    }

    status = H5Pset(dxpl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME, &is_disable);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pset failed!\n", __func__);
        return (-1);
    }

    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5Pget_dxpl_disable_async_implicit
 *
 * Purpose:     Get the async implicit mode from dxpl
 *
 * Return:      SUCCEED/NOTEXIST/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_dxpl_disable_async_implicit(hid_t dxpl, hbool_t *is_disable)
{
    herr_t status;
    assert(is_disable);

    *is_disable = false;

    status = H5Pexist(dxpl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
        return -1;
    }
    else if (status > 0) {
        status = H5Pget(dxpl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME, is_disable);
        if (status < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pget failed!\n", __func__);
            return (-1);
        }
    }
    else
        return 0;

    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5Pset_dxpl_pause
 *
 * Purpose:     Set to pause async operations with dxpl
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_dxpl_pause(hid_t dxpl, hbool_t is_pause)
{
    herr_t status;

    status = H5Pexist(dxpl, H5VL_ASYNC_PAUSE_NAME);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
        return -1;
    }
    else if (status == 0) {
        status = H5Pinsert2(dxpl, H5VL_ASYNC_PAUSE_NAME, sizeof(hbool_t), &is_pause, NULL, NULL, NULL, NULL,
                            NULL, NULL);
        if (status < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pinsert2 failed!\n", __func__);
            return (-1);
        }
    }

    status = H5Pset(dxpl, H5VL_ASYNC_PAUSE_NAME, &is_pause);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pset failed!\n", __func__);
        return (-1);
    }

    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5Pget_dxpl_pause
 *
 * Purpose:     Get the async implicit mode from dxpl
 *
 * Return:      SUCCEED/NOTEXIST/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_dxpl_pause(hid_t dxpl, hbool_t *is_pause)
{
    herr_t status;
    assert(is_pause);

    *is_pause = false;

    status = H5Pexist(dxpl, H5VL_ASYNC_PAUSE_NAME);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
        return -1;
    }
    else if (status > 0) {
        status = H5Pget(dxpl, H5VL_ASYNC_PAUSE_NAME, is_pause);
        if (status < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pget failed!\n", __func__);
            return (-1);
        }
    }
    else
        return 0;

    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5async_start
 *
 * Purpose:     Start (un-pause) async operations with request
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5async_start(void *request)
{
    H5VL_optional_args_t vol_cb_args; /* Arguments to VOL callback */
    herr_t               status;

    assert(request);

    /* Look up operation value, if it's not already available */
    if (-1 == async_request_start_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_request_start_op_g > 0);
    }

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = async_request_start_op_g;
    vol_cb_args.args    = NULL;

    /* Call the VOL file optional routine, requesting 'wait' occur */
    if ((status = H5VLrequest_optional_op(request, H5VL_async_g, &vol_cb_args)) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] H5async_start: VOL connector request start operation failed!\n");
        return (-1);
    }

    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5async_set_request_dep
 *
 * Purpose:     Set the request's dependent parent request
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5async_set_request_dep(void *request, void *parent_request)
{
    H5VL_optional_args_t      vol_cb_args; /* Arguments to VOL callback */
    H5VL_async_req_dep_args_t dep_args;    /* Arguments for optional operation */
    herr_t                    status;

    assert(request);
    assert(parent_request);

    /* Look up operation value, if it's not already available */
    if (-1 == async_request_dep_op_g) {
        if (async_setup() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: async_setup error!\n", __func__);
            return (-1);
        }
        assert(async_request_dep_op_g > 0);
    }

    /* Set up VOL callback arguments */
    dep_args.parent_req = parent_request;
    vol_cb_args.op_type = async_request_dep_op_g;
    vol_cb_args.args    = &dep_args;

    /* Call the VOL file optional routine, requesting 'wait' occur */
    if ((status = H5VLrequest_optional_op(request, H5VL_async_g, &vol_cb_args)) < 0) {
        fprintf(
            stderr,
            "  [ASYNC VOL ERROR] H5async_set_request_dep: VOL connector request depend operation failed!\n");
        return (-1);
    }

    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5Pset_dxpl_delay
 *
 * Purpose:     Set to delay async operations
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_dxpl_delay(hid_t dxpl, uint64_t time_us)
{
    herr_t status;

    status = H5Pexist(dxpl, H5VL_ASYNC_DELAY_NAME);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
        return -1;
    }
    else if (status == 0) {
        status = H5Pinsert2(dxpl, H5VL_ASYNC_DELAY_NAME, sizeof(uint64_t), &time_us, NULL, NULL, NULL, NULL,
                            NULL, NULL);
        if (status < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pinsert2 failed!\n", __func__);
            return (-1);
        }
    }

    status = H5Pset(dxpl, H5VL_ASYNC_DELAY_NAME, &time_us);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pset failed!\n", __func__);
        return (-1);
    }

    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5Pget_dxpl_delay
 *
 * Purpose:     Get the delay async operations
 *
 * Return:      SUCCEED/NOTEXIST/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_dxpl_delay(hid_t dxpl, uint64_t *time_us)
{
    herr_t status;
    assert(time_us);

    *time_us = 0;

    status = H5Pexist(dxpl, H5VL_ASYNC_DELAY_NAME);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
        return -1;
    }
    else if (status > 0) {
        status = H5Pget(dxpl, H5VL_ASYNC_DELAY_NAME, time_us);
        if (status < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pget failed!\n", __func__);
            return (-1);
        }
    }
    else
        return 0;

    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_disable_async_implicit
 *
 * Purpose:     Set to disable async implicit mode with fapl
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_disable_async_implicit(hid_t fapl, hbool_t is_disable)
{
    herr_t status;

    status = H5Pexist(fapl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
        return -1;
    }
    else if (status == 0) {
        status = H5Pinsert2(fapl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME, sizeof(hbool_t), &is_disable, NULL, NULL,
                            NULL, NULL, NULL, NULL);
        if (status < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pinsert2 failed!\n", __func__);
            return (-1);
        }
    }

    status = H5Pset(fapl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME, &is_disable);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pset failed!\n", __func__);
        return (-1);
    }

    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fapl_disable_async_implicit
 *
 * Purpose:     Get the async implicit mode from fapl
 *
 * Return:      SUCCEED/NOTEXIST/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_disable_async_implicit(hid_t fapl, hbool_t *is_disable)
{
    herr_t status;
    assert(is_disable);

    *is_disable = false;

    status = H5Pexist(fapl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME);
    if (status < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
        return -1;
    }
    else if (status > 0) {
        status = H5Pget(fapl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME, is_disable);
        if (status < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s: H5Pget failed!\n", __func__);
            return (-1);
        }
    }
    else
        return 0;

    return status;
}
