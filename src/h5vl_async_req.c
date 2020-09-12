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
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Wait\n");
#endif

	ret_value = H5VLrequest_wait (o->under_object, o->under_vol_id, timeout, status);

	if (ret_value >= 0 && *status != H5ES_STATUS_IN_PROGRESS) H5VL_async_free_obj (o);

	return ret_value;
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
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Notify\n");
#endif

	ret_value = H5VLrequest_notify (o->under_object, o->under_vol_id, cb, ctx);

	if (ret_value >= 0) H5VL_async_free_obj (o);

	return ret_value;
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
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Cancel\n");
#endif

	ret_value = H5VLrequest_cancel (o->under_object, o->under_vol_id);

	if (ret_value >= 0) H5VL_async_free_obj (o);

	return ret_value;
} /* end H5VL_async_request_cancel() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_specific_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              request specific callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_request_specific_reissue (void *obj,
											hid_t connector_id,
											H5VL_request_specific_t specific_type,
											...) {
	va_list arguments;
	herr_t ret_value;

	va_start (arguments, specific_type);
	ret_value = H5VLrequest_specific (obj, connector_id, specific_type, arguments);
	va_end (arguments);

	return ret_value;
} /* end H5VL_async_request_specific_reissue() */

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
	herr_t ret_value = -1;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Specific\n");
#endif

	if (H5VL_REQUEST_WAITANY == specific_type || H5VL_REQUEST_WAITSOME == specific_type ||
		H5VL_REQUEST_WAITALL == specific_type) {
		va_list tmp_arguments;
		size_t req_count;

		/* Sanity check */
		assert (obj == NULL);

		/* Get enough info to call the underlying connector */
		va_copy (tmp_arguments, arguments);
		req_count = va_arg (tmp_arguments, size_t);

		/* Can only use a request to invoke the underlying VOL connector when
		 * there's >0 requests */
		if (req_count > 0) {
			void **req_array;
			void **under_req_array;
			uint64_t timeout;
			H5VL_async_t *o;
			size_t u; /* Local index variable */

			/* Get the request array */
			req_array = va_arg (tmp_arguments, void **);

			/* Get a request to use for determining the underlying VOL connector */
			o = (H5VL_async_t *)req_array[0];

			/* Create array of underlying VOL requests */
			under_req_array = (void **)malloc (req_count * sizeof (void **));
			for (u = 0; u < req_count; u++)
				under_req_array[u] = ((H5VL_async_t *)req_array[u])->under_object;

			/* Remove the timeout value from the vararg list (it's used in all the
			 * calls below) */
			timeout = va_arg (tmp_arguments, uint64_t);

			/* Release requests that have completed */
			if (H5VL_REQUEST_WAITANY == specific_type) {
				size_t *idx;		   /* Pointer to the index of completed request */
				H5ES_status_t *status; /* Pointer to the request's status */

				/* Retrieve the remaining arguments */
				idx = va_arg (tmp_arguments, size_t *);
				assert (*idx <= req_count);
				status = va_arg (tmp_arguments, H5ES_status_t *);

				/* Reissue the WAITANY 'request specific' call */
				ret_value = H5VL_async_request_specific_reissue (
					o->under_object, o->under_vol_id, specific_type, req_count, under_req_array,
					timeout, idx, status);

				/* Release the completed request, if it completed */
				if (ret_value >= 0 && *status != H5ES_STATUS_IN_PROGRESS) {
					H5VL_async_t *tmp_o;

					tmp_o = (H5VL_async_t *)req_array[*idx];
					H5VL_async_free_obj (tmp_o);
				} /* end if */
			}	  /* end if */
			else if (H5VL_REQUEST_WAITSOME == specific_type) {
				size_t *outcount;				  /* # of completed requests */
				unsigned *array_of_indices;		  /* Array of indices for completed requests */
				H5ES_status_t *array_of_statuses; /* Array of statuses for completed requests */

				/* Retrieve the remaining arguments */
				outcount = va_arg (tmp_arguments, size_t *);
				assert (*outcount <= req_count);
				array_of_indices  = va_arg (tmp_arguments, unsigned *);
				array_of_statuses = va_arg (tmp_arguments, H5ES_status_t *);

				/* Reissue the WAITSOME 'request specific' call */
				ret_value = H5VL_async_request_specific_reissue (
					o->under_object, o->under_vol_id, specific_type, req_count, under_req_array,
					timeout, outcount, array_of_indices, array_of_statuses);

				/* If any requests completed, release them */
				if (ret_value >= 0 && *outcount > 0) {
					unsigned *idx_array; /* Array of indices of completed requests */

					/* Retrieve the array of completed request indices */
					idx_array = va_arg (tmp_arguments, unsigned *);

					/* Release the completed requests */
					for (u = 0; u < *outcount; u++) {
						H5VL_async_t *tmp_o;

						tmp_o = (H5VL_async_t *)req_array[idx_array[u]];
						H5VL_async_free_obj (tmp_o);
					}							  /* end for */
				}								  /* end if */
			}									  /* end else-if */
			else {								  /* H5VL_REQUEST_WAITALL == specific_type */
				H5ES_status_t *array_of_statuses; /* Array of statuses for completed requests */

				/* Retrieve the remaining arguments */
				array_of_statuses = va_arg (tmp_arguments, H5ES_status_t *);

				/* Reissue the WAITALL 'request specific' call */
				ret_value = H5VL_async_request_specific_reissue (
					o->under_object, o->under_vol_id, specific_type, req_count, under_req_array,
					timeout, array_of_statuses);

				/* Release the completed requests */
				if (ret_value >= 0) {
					for (u = 0; u < req_count; u++) {
						if (array_of_statuses[u] != H5ES_STATUS_IN_PROGRESS) {
							H5VL_async_t *tmp_o;

							tmp_o = (H5VL_async_t *)req_array[u];
							H5VL_async_free_obj (tmp_o);
						} /* end if */
					}	  /* end for */
				}		  /* end if */
			}			  /* end else */

			/* Release array of requests for underlying connector */
			free (under_req_array);
		} /* end if */

		/* Finish use of copied vararg list */
		va_end (tmp_arguments);
	} /* end if */
	else
		assert (0 && "Unknown 'specific' operation");

	return ret_value;
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
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Optional\n");
#endif

	ret_value = H5VLrequest_optional (o->under_object, o->under_vol_id, opt_type, arguments);

	return ret_value;
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
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL REQUEST Free\n");
#endif

	ret_value = H5VLrequest_free (o->under_object, o->under_vol_id);

	if (ret_value >= 0) H5VL_async_free_obj (o);

	return ret_value;
} /* end H5VL_async_request_free() */