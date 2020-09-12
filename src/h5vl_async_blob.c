/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Blob callbacks */

/* Async VOL headers */
#include "h5vl_async_blob.h"

#include "h5vl_async.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_blob_put
 *
 * Purpose:     Handles the blob 'put' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_blob_put (void *obj, const void *buf, size_t size, void *blob_id, void *ctx) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL BLOB Put\n");
#endif

	ret_value = H5VLblob_put (o->under_object, o->under_vol_id, buf, size, blob_id, ctx);

	return ret_value;
} /* end H5VL_async_blob_put() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_blob_get
 *
 * Purpose:     Handles the blob 'get' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_blob_get (void *obj, const void *blob_id, void *buf, size_t size, void *ctx) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL BLOB Get\n");
#endif

	ret_value = H5VLblob_get (o->under_object, o->under_vol_id, blob_id, buf, size, ctx);

	return ret_value;
} /* end H5VL_async_blob_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_blob_specific
 *
 * Purpose:     Handles the blob 'specific' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_blob_specific (void *obj,
								 void *blob_id,
								 H5VL_blob_specific_t specific_type,
								 va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL BLOB Specific\n");
#endif

	ret_value =
		H5VLblob_specific (o->under_object, o->under_vol_id, blob_id, specific_type, arguments);

	return ret_value;
} /* end H5VL_async_blob_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_blob_optional
 *
 * Purpose:     Handles the blob 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_blob_optional (void *obj,
								 void *blob_id,
								 H5VL_blob_optional_t opt_type,
								 va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL BLOB Optional\n");
#endif

	ret_value = H5VLblob_optional (o->under_object, o->under_vol_id, blob_id, opt_type, arguments);

	return ret_value;
} /* end H5VL_async_blob_optional() */
