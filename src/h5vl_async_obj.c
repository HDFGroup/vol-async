/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Object callbacks */

/* Async VOL headers */
#include "h5vl_async_obj.h"

#include "h5vl_async.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_open
 *
 * Purpose:     Opens an object inside a container.
 *
 * Return:      Success:    Pointer to object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_object_open (void *obj,
							  const H5VL_loc_params_t *loc_params,
							  H5I_type_t *opened_type,
							  hid_t dxpl_id,
							  void **req) {
	H5VL_async_t *new_obj;
	H5VL_async_t *o = (H5VL_async_t *)obj;
	void *under;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL OBJECT Open\n");
#endif

	under =
		H5VLobject_open (o->under_object, loc_params, o->under_vol_id, opened_type, dxpl_id, req);
	if (under) {
		new_obj = H5VL_async_new_obj (under, o->under_vol_id);

		/* Check for async request */
		if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);
	} /* end if */
	else
		new_obj = NULL;

	return (void *)new_obj;
} /* end H5VL_async_object_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_copy
 *
 * Purpose:     Copies an object inside a container.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_object_copy (void *src_obj,
							   const H5VL_loc_params_t *src_loc_params,
							   const char *src_name,
							   void *dst_obj,
							   const H5VL_loc_params_t *dst_loc_params,
							   const char *dst_name,
							   hid_t ocpypl_id,
							   hid_t lcpl_id,
							   hid_t dxpl_id,
							   void **req) {
	H5VL_async_t *o_src = (H5VL_async_t *)src_obj;
	H5VL_async_t *o_dst = (H5VL_async_t *)dst_obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL OBJECT Copy\n");
#endif

	ret_value = H5VLobject_copy (o_src->under_object, src_loc_params, src_name, o_dst->under_object,
								 dst_loc_params, dst_name, o_src->under_vol_id, ocpypl_id, lcpl_id,
								 dxpl_id, req);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o_src->under_vol_id);

	return ret_value;
} /* end H5VL_async_object_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_get
 *
 * Purpose:     Get info about an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_object_get (void *obj,
							  const H5VL_loc_params_t *loc_params,
							  H5VL_object_get_t get_type,
							  hid_t dxpl_id,
							  void **req,
							  va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL OBJECT Get\n");
#endif

	ret_value = H5VLobject_get (o->under_object, loc_params, o->under_vol_id, get_type, dxpl_id,
								req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_object_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_specific
 *
 * Purpose:     Specific operation on an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_object_specific (void *obj,
								   const H5VL_loc_params_t *loc_params,
								   H5VL_object_specific_t specific_type,
								   hid_t dxpl_id,
								   void **req,
								   va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	hid_t under_vol_id;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL OBJECT Specific\n");
#endif

	// Save copy of underlying VOL connector ID and prov helper, in case of
	// refresh destroying the current object
	under_vol_id = o->under_vol_id;

	ret_value = H5VLobject_specific (o->under_object, loc_params, o->under_vol_id, specific_type,
									 dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, under_vol_id);

	return ret_value;
} /* end H5VL_async_object_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_optional
 *
 * Purpose:     Perform a connector-specific operation for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_object_optional (
	void *obj, H5VL_object_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL OBJECT Optional\n");
#endif

	ret_value =
		H5VLobject_optional (o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_object_optional() */
