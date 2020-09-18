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
herr_t H5VL_async_request_wait (void *obj, uint64_t timeout, H5ES_status_t *status) {
	herr_t err			   = 0;
	terr_t twerr		   = TW_SUCCESS;
	H5VL_async_req_t *reqp = (H5VL_async_req_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Wait\n");
#endif

	if (reqp->ret <= 0) {
		twerr = TW_Task_wait (reqp->task, timeout * 1000);
		if (twerr == TW_ERR_TIMEOUT) {
			if (status) { *status = H5ES_STATUS_IN_PROGRESS; }
		}
		CHK_TWERR
	}

	if (status) {
		if (reqp->ret < 0) {
			*status = H5ES_STATUS_FAIL;
		} else if (reqp->ret > 0) {
			*status = H5ES_STATUS_CANCELED;
		} else {
			*status = H5ES_STATUS_SUCCEED;
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

	twerr = TW_Task_retract (reqp->task);
	CHK_TWERR

	reqp->ret = 1;

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
	H5VL_async_req_t *reqp = (H5VL_async_req_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Specific\n");
#endif

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

	twerr = TW_Task_free (reqp->task);
	CHK_TWERR

err_out:;
	free (reqp);

	return err;
} /* end H5VL_async_request_free() */