/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Attribute callbacks */

/* Async VOL headers */
#include "h5vl_async_attr.h"

#include "h5vl_async.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_create
 *
 * Purpose:     Creates an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_attr_create (void *obj,
							  const H5VL_loc_params_t *loc_params,
							  const char *name,
							  hid_t type_id,
							  hid_t space_id,
							  hid_t acpl_id,
							  hid_t aapl_id,
							  hid_t dxpl_id,
							  void **req) {
	H5VL_async_t *attr;
	H5VL_async_t *o = (H5VL_async_t *)obj;
	void *under;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL ATTRIBUTE Create\n");
#endif

	under = H5VLattr_create (o->under_object, loc_params, o->under_vol_id, name, type_id, space_id,
							 acpl_id, aapl_id, dxpl_id, req);
	if (under) {
		attr = H5VL_async_new_obj (under, o->under_vol_id);

		/* Check for async request */
		if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);
	} /* end if */
	else
		attr = NULL;

	return (void *)attr;
} /* end H5VL_async_attr_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_open
 *
 * Purpose:     Opens an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_attr_open (void *obj,
							const H5VL_loc_params_t *loc_params,
							const char *name,
							hid_t aapl_id,
							hid_t dxpl_id,
							void **req) {
	H5VL_async_t *attr;
	H5VL_async_t *o = (H5VL_async_t *)obj;
	void *under;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL ATTRIBUTE Open\n");
#endif

	under =
		H5VLattr_open (o->under_object, loc_params, o->under_vol_id, name, aapl_id, dxpl_id, req);
	if (under) {
		attr = H5VL_async_new_obj (under, o->under_vol_id);

		/* Check for async request */
		if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);
	} /* end if */
	else
		attr = NULL;

	return (void *)attr;
} /* end H5VL_async_attr_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_read
 *
 * Purpose:     Reads data from attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_read (void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req) {
	H5VL_async_t *o = (H5VL_async_t *)attr;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL ATTRIBUTE Read\n");
#endif

	ret_value = H5VLattr_read (o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_attr_read() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_write
 *
 * Purpose:     Writes data to attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_write (
	void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req) {
	H5VL_async_t *o = (H5VL_async_t *)attr;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL ATTRIBUTE Write\n");
#endif

	ret_value = H5VLattr_write (o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_attr_write() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_get
 *
 * Purpose:     Gets information about an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_get (
	void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL ATTRIBUTE Get\n");
#endif

	ret_value = H5VLattr_get (o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_attr_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_specific
 *
 * Purpose:     Specific operation on attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_specific (void *obj,
								 const H5VL_loc_params_t *loc_params,
								 H5VL_attr_specific_t specific_type,
								 hid_t dxpl_id,
								 void **req,
								 va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL ATTRIBUTE Specific\n");
#endif

	ret_value = H5VLattr_specific (o->under_object, loc_params, o->under_vol_id, specific_type,
								   dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_attr_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_optional
 *
 * Purpose:     Perform a connector-specific operation on an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_optional (
	void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL ATTRIBUTE Optional\n");
#endif

	ret_value =
		H5VLattr_optional (o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_attr_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_close
 *
 * Purpose:     Closes an attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1, attr not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_close (void *attr, hid_t dxpl_id, void **req) {
	H5VL_async_t *o = (H5VL_async_t *)attr;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL ATTRIBUTE Close\n");
#endif

	ret_value = H5VLattr_close (o->under_object, o->under_vol_id, dxpl_id, req);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	/* Release our wrapper, if underlying attribute was closed */
	if (ret_value >= 0) H5VL_async_free_obj (o);

	return ret_value;
} /* end H5VL_async_attr_close() */
