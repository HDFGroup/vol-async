/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Datatype callbacks */

#include <assert.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_dtype.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_commit
 *
 * Purpose:     Commits a datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_datatype_commit (void *obj,
								  const H5VL_loc_params_t *loc_params,
								  const char *name,
								  hid_t type_id,
								  hid_t lcpl_id,
								  hid_t tcpl_id,
								  hid_t tapl_id,
								  hid_t dxpl_id,
								  void **req) {
	H5VL_async_t *dt;
	H5VL_async_t *o = (H5VL_async_t *)obj;
	void *under;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL DATATYPE Commit\n");
#endif

	under = H5VLdatatype_commit (o->under_object, loc_params, o->under_vol_id, name, type_id,
								 lcpl_id, tcpl_id, tapl_id, dxpl_id, req);
	if (under) {
		dt = H5VL_async_new_obj (under, o->under_vol_id);

		/* Check for async request */
		if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);
	} /* end if */
	else
		dt = NULL;

	return (void *)dt;
} /* end H5VL_async_datatype_commit() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_open
 *
 * Purpose:     Opens a named datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_datatype_open (void *obj,
								const H5VL_loc_params_t *loc_params,
								const char *name,
								hid_t tapl_id,
								hid_t dxpl_id,
								void **req) {
	H5VL_async_t *dt;
	H5VL_async_t *o = (H5VL_async_t *)obj;
	void *under;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL DATATYPE Open\n");
#endif

	under = H5VLdatatype_open (o->under_object, loc_params, o->under_vol_id, name, tapl_id, dxpl_id,
							   req);
	if (under) {
		dt = H5VL_async_new_obj (under, o->under_vol_id);

		/* Check for async request */
		if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);
	} /* end if */
	else
		dt = NULL;

	return (void *)dt;
} /* end H5VL_async_datatype_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_get
 *
 * Purpose:     Get information about a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_datatype_get (
	void *dt, H5VL_datatype_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)dt;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL DATATYPE Get\n");
#endif

	ret_value =
		H5VLdatatype_get (o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_datatype_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_specific
 *
 * Purpose:     Specific operations for datatypes
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_datatype_specific (void *obj,
									 H5VL_datatype_specific_t specific_type,
									 hid_t dxpl_id,
									 void **req,
									 va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	hid_t under_vol_id;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL DATATYPE Specific\n");
#endif

	// Save copy of underlying VOL connector ID and prov helper, in case of
	// refresh destroying the current object
	under_vol_id = o->under_vol_id;

	ret_value = H5VLdatatype_specific (o->under_object, o->under_vol_id, specific_type, dxpl_id,
									   req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, under_vol_id);

	return ret_value;
} /* end H5VL_async_datatype_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_optional
 *
 * Purpose:     Perform a connector-specific operation on a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_datatype_optional (
	void *obj, H5VL_datatype_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL DATATYPE Optional\n");
#endif

	ret_value =
		H5VLdatatype_optional (o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_datatype_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_close
 *
 * Purpose:     Closes a datatype.
 *
 * Return:      Success:    0
 *              Failure:    -1, datatype not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_datatype_close (void *dt, hid_t dxpl_id, void **req) {
	H5VL_async_t *o = (H5VL_async_t *)dt;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL DATATYPE Close\n");
#endif

	assert (o->under_object);

	ret_value = H5VLdatatype_close (o->under_object, o->under_vol_id, dxpl_id, req);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	/* Release our wrapper, if underlying datatype was closed */
	if (ret_value >= 0) H5VL_async_free_obj (o);

	return ret_value;
} /* end H5VL_async_datatype_close() */
