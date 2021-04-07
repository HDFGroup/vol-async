/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Request callbacks */

#include <assert.h>
#include <stdlib.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_req.h"
#include "h5vl_asynci.h"

H5VL_async_req_t* H5VL_async_new_req(void *under_obj, hid_t under_vol_id) {
    printf("%s: vol_id = %lu\n", __func__, under_vol_id);
    H5VL_async_req_t* new_req = (H5VL_async_req_t*)calloc(1, sizeof(H5VL_async_req_t));
    new_req->async_obj = H5VL_async_new_obj (NULL, under_vol_id);
    new_req->isReq = 1;
    new_req->req_stat = REQ_IN_PROGRESS;
    new_req->req_task = TW_HANDLE_NULL;
    new_req->async_obj->stat = H5VL_async_stat_init;
    return new_req;
}

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_wait
 *
 * Purpose:     Wait (with a timeout) for an async operation to complete
 *
 * Note:        Releases the request if the operation has completed and the
 *              connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_request_wait (void *obj, uint64_t timeout, H5VL_request_status_t *status) {
	herr_t err			   = 0;
	terr_t twerr		   = TW_SUCCESS;
	H5VL_async_req_t *reqp = (H5VL_async_req_t *)obj;
DEBUG_PRINT
#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Wait\n");
#endif

	if (reqp->req_stat == REQ_IN_PROGRESS) {
		twerr = TW_Task_wait (reqp->req_task, timeout * 1000);
		if (twerr == TW_ERR_TIMEOUT) {
			if (status) {
			    *status = H5VL_REQUEST_STATUS_IN_PROGRESS;
			    goto err_out;
			}
		}
		CHK_TWERR
	}

	if (status) {
		if (reqp->req_stat == REQ_FAIL) {
			*status = H5VL_REQUEST_STATUS_FAIL;
		} else if (reqp->req_stat == REQ_CANCELLED) {
			*status = H5VL_REQUEST_STATUS_CANCELED;
		} else {
			*status = H5VL_REQUEST_STATUS_SUCCEED;
		}
	}

err_out:;
	return err;
} /* end H5VL_async_request_wait() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_notify
 *
 * Purpose:     Registers a user callback to be invoked when an asynchronous
 *              operation completes
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_request_notify (void *obj, H5VL_request_notify_t cb, void *ctx) {
	herr_t err			   = 0;
	terr_t twerr		   = TW_SUCCESS;
	H5VL_async_req_t *reqp = (H5VL_async_req_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Notify\n");
#endif

err_out:;
	return err;
} /* end H5VL_async_request_notify() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_cancel
 *
 * Purpose:     Cancels an asynchronous operation
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_request_cancel (void *obj) {
	herr_t err			   = 0;
	terr_t twerr		   = TW_SUCCESS;
	H5VL_async_req_t *reqp = (H5VL_async_req_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Cancel\n");
#endif

	twerr = TW_Task_retract (reqp->req_task);
	CHK_TWERR

	reqp->req_stat = REQ_CANCELLED;

err_out:;
	return err;
} /* end H5VL_async_request_cancel() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_specific
 *
 * Purpose:     Specific operation on a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_request_specific (void *obj,
									H5VL_request_specific_t specific_type,
									va_list arguments) {
	herr_t err			   = 0;
	terr_t twerr		   = TW_SUCCESS;
	H5VL_async_req_t *req = (H5VL_async_req_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Specific\n");
#endif
//TODO:
//	- get error stack: look at Tang's code
//	- H5VL_REQUEST_GET_EXEC_TIME: return to ouput params.

    if(H5VL_REQUEST_GET_ERR_STACK == specific_type) {
        hid_t *err_stack_id_ptr;

        TW_Task_handle_t task = req->req_task;

        if (task == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object\n", __func__);
            return -1;
        }

        /* Retrieve pointer to error stack ID */
        err_stack_id_ptr = va_arg(arguments, hid_t *);
        assert(err_stack_id_ptr);

        /* Increment refcount on task's error stack, if it has one */
        if(H5I_INVALID_HID != req->error_stack)
            H5Iinc_ref(req->error_stack);

        /* Return the task's error stack (including H5I_INVALID_HID) */
        *err_stack_id_ptr = req->error_stack;


    } /* end if */
    else if(H5VL_REQUEST_GET_EXEC_TIME == specific_type) {
        uint64_t* op_exec_ts;
        uint64_t* op_exec_time;

        op_exec_ts = va_arg(arguments, uint64_t*);
        op_exec_time = va_arg(arguments, uint64_t*);

        *op_exec_ts = req->op_exec_ts;
        *op_exec_time = req->op_exec_time;
    }
    else
        assert(0 && "Unknown 'specific' operation");

err_out:;
	return err;
} /* end H5VL_async_request_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_optional
 *
 * Purpose:     Perform a connector-specific operation for a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_request_optional (void *obj,
									H5VL_request_optional_t opt_type,
									va_list arguments) {
	herr_t err			   = 0;
	terr_t twerr		   = TW_SUCCESS;
	H5VL_async_req_t *reqp = (H5VL_async_req_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Optional\n");
#endif

err_out:;
	return err;
} /* end H5VL_async_request_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_free
 *
 * Purpose:     Releases a request, allowing the operation to complete without
 *              application tracking
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_request_free (void *obj) {
	herr_t err			   = 0;
	terr_t twerr		   = TW_SUCCESS;
	H5VL_async_req_t *reqp = (H5VL_async_req_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Wait\n");
#endif

	twerr = TW_Task_free (reqp->req_task);
	CHK_TWERR

err_out:;
	free (reqp);

	return err;
} /* end H5VL_async_request_free() */
