/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Wrap callbacks */

#include <assert.h>
#include <hdf5.h>
#include <stdlib.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_wrap.h"

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_get_object
 *
 * Purpose:     Retrieve the 'data' for a VOL object.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
void *H5VL_async_get_object (const void *obj) {
	const H5VL_async_t *o = (const H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL Get object\n");
#endif

	return H5VLget_object (o->under_object, o->under_vol_id);
} /* end H5VL_async_get_object() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_get_wrap_ctx
 *
 * Purpose:     Retrieve a "wrapper context" for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t H5VL_async_get_wrap_ctx (const void *obj, void **wrap_ctx) {
	const H5VL_async_t *o = (const H5VL_async_t *)obj;
	H5VL_async_wrap_ctx_t *new_wrap_ctx;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL WRAP CTX Get\n");
#endif

	/* Allocate new VOL object wrapping context for the async connector */
	new_wrap_ctx = (H5VL_async_wrap_ctx_t *)calloc (1, sizeof (H5VL_async_wrap_ctx_t));

	/* Increment reference count on underlying VOL ID, and copy the VOL info */
	new_wrap_ctx->under_vol_id = o->under_vol_id;
	H5Iinc_ref (new_wrap_ctx->under_vol_id);
	H5VLget_wrap_ctx (o->under_object, o->under_vol_id, &new_wrap_ctx->under_wrap_ctx);

	/* Set wrap context to return */
	*wrap_ctx = new_wrap_ctx;

	return 0;
} /* end H5VL_async_get_wrap_ctx() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_wrap_object
 *
 * Purpose:     Use a "wrapper context" to wrap a data object
 *
 * Return:      Success:    Pointer to wrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
void *H5VL_async_wrap_object (void *obj, H5I_type_t obj_type, void *_wrap_ctx) {
	H5VL_async_wrap_ctx_t *wrap_ctx = (H5VL_async_wrap_ctx_t *)_wrap_ctx;
	H5VL_async_t *new_obj;
	void *under;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL WRAP Object\n");
#endif

	/* Wrap the object with the underlying VOL */
	under = H5VLwrap_object (obj, obj_type, wrap_ctx->under_vol_id, wrap_ctx->under_wrap_ctx);
	if (under){
		new_obj = H5VL_async_new_obj ();
		new_obj->under_object = under;
		new_obj->under_vol_id = wrap_ctx->under_vol_id;
	}
	else
		new_obj = NULL;

	return new_obj;
} /* end H5VL_async_wrap_object() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_unwrap_object
 *
 * Purpose:     Unwrap a wrapped object, discarding the wrapper, but returning
 *		underlying object.
 *
 * Return:      Success:    Pointer to unwrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
void *H5VL_async_unwrap_object (void *obj) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	void *under;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL UNWRAP Object\n");
#endif

	/* Unrap the object with the underlying VOL */
	under = H5VLunwrap_object (o->under_object, o->under_vol_id);

	if (under) H5VL_async_free_obj (o);

	return under;
} /* end H5VL_async_unwrap_object() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_free_wrap_ctx
 *
 * Purpose:     Release a "wrapper context" for an object
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t H5VL_async_free_wrap_ctx (void *_wrap_ctx) {
	H5VL_async_wrap_ctx_t *wrap_ctx = (H5VL_async_wrap_ctx_t *)_wrap_ctx;
	hid_t err_id;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL WRAP CTX Free\n");
#endif

	err_id = H5Eget_current_stack ();

	/* Release underlying VOL ID and wrap context */
	if (wrap_ctx->under_wrap_ctx)
		H5VLfree_wrap_ctx (wrap_ctx->under_wrap_ctx, wrap_ctx->under_vol_id);
	H5Idec_ref (wrap_ctx->under_vol_id);

	H5Eset_current_stack (err_id);

	/* Free async wrap context object itself */
	free (wrap_ctx);

	return 0;
} /* end H5VL_async_free_wrap_ctx() */
