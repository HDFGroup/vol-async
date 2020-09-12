/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Link callbacks */

#include <assert.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_link.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_create_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              link create callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_create_reissue (H5VL_link_create_type_t create_type,
									   void *obj,
									   const H5VL_loc_params_t *loc_params,
									   hid_t connector_id,
									   hid_t lcpl_id,
									   hid_t lapl_id,
									   hid_t dxpl_id,
									   void **req,
									   ...) {
	va_list arguments;
	herr_t ret_value;

	va_start (arguments, req);
	ret_value = H5VLlink_create (create_type, obj, loc_params, connector_id, lcpl_id, lapl_id,
								 dxpl_id, req, arguments);
	va_end (arguments);

	return ret_value;
} /* end H5VL_async_link_create_reissue() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_create
 *
 * Purpose:     Creates a hard / soft / UD / external link.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_create (H5VL_link_create_type_t create_type,
							   void *obj,
							   const H5VL_loc_params_t *loc_params,
							   hid_t lcpl_id,
							   hid_t lapl_id,
							   hid_t dxpl_id,
							   void **req,
							   va_list arguments) {
	H5VL_async_t *o	   = (H5VL_async_t *)obj;
	hid_t under_vol_id = -1;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL LINK Create\n");
#endif

	/* Try to retrieve the "under" VOL id */
	if (o) under_vol_id = o->under_vol_id;

	/* Fix up the link target object for hard link creation */
	if (H5VL_LINK_CREATE_HARD == create_type) {
		void *cur_obj;
		H5VL_loc_params_t *cur_params;

		/* Retrieve the object & loc params for the link target */
		cur_obj	   = va_arg (arguments, void *);
		cur_params = va_arg (arguments, H5VL_loc_params_t *);

		/* If it's a non-NULL pointer, find the 'under object' and re-set the
		 * property */
		if (cur_obj) {
			/* Check if we still need the "under" VOL ID */
			if (under_vol_id < 0) under_vol_id = ((H5VL_async_t *)cur_obj)->under_vol_id;

			/* Set the object for the link target */
			cur_obj = ((H5VL_async_t *)cur_obj)->under_object;
		} /* end if */

		/* Re-issue 'link create' call, using the unwrapped pieces */
		ret_value = H5VL_async_link_create_reissue (create_type, (o ? o->under_object : NULL),
													loc_params, under_vol_id, lcpl_id, lapl_id,
													dxpl_id, req, cur_obj, cur_params);
	} /* end if */
	else
		ret_value = H5VLlink_create (create_type, (o ? o->under_object : NULL), loc_params,
									 under_vol_id, lcpl_id, lapl_id, dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, under_vol_id);

	return ret_value;
} /* end H5VL_async_link_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_copy
 *
 * Purpose:     Renames an object within an HDF5 container and copies it to a
 *new group.  The original name SRC is unlinked from the group graph and then
 *inserted with the new name DST (which can specify a new path for the object)
 *as an atomic operation. The names are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_copy (void *src_obj,
							 const H5VL_loc_params_t *loc_params1,
							 void *dst_obj,
							 const H5VL_loc_params_t *loc_params2,
							 hid_t lcpl_id,
							 hid_t lapl_id,
							 hid_t dxpl_id,
							 void **req) {
	H5VL_async_t *o_src = (H5VL_async_t *)src_obj;
	H5VL_async_t *o_dst = (H5VL_async_t *)dst_obj;
	hid_t under_vol_id	= -1;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL LINK Copy\n");
#endif

	/* Retrieve the "under" VOL id */
	if (o_src)
		under_vol_id = o_src->under_vol_id;
	else if (o_dst)
		under_vol_id = o_dst->under_vol_id;
	assert (under_vol_id > 0);

	ret_value = H5VLlink_copy ((o_src ? o_src->under_object : NULL), loc_params1,
							   (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id,
							   lcpl_id, lapl_id, dxpl_id, req);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, under_vol_id);

	return ret_value;
} /* end H5VL_async_link_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_move
 *
 * Purpose:     Moves a link within an HDF5 file to a new group.  The original
 *              name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_move (void *src_obj,
							 const H5VL_loc_params_t *loc_params1,
							 void *dst_obj,
							 const H5VL_loc_params_t *loc_params2,
							 hid_t lcpl_id,
							 hid_t lapl_id,
							 hid_t dxpl_id,
							 void **req) {
	H5VL_async_t *o_src = (H5VL_async_t *)src_obj;
	H5VL_async_t *o_dst = (H5VL_async_t *)dst_obj;
	hid_t under_vol_id	= -1;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL LINK Move\n");
#endif

	/* Retrieve the "under" VOL id */
	if (o_src)
		under_vol_id = o_src->under_vol_id;
	else if (o_dst)
		under_vol_id = o_dst->under_vol_id;
	assert (under_vol_id > 0);

	ret_value = H5VLlink_move ((o_src ? o_src->under_object : NULL), loc_params1,
							   (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id,
							   lcpl_id, lapl_id, dxpl_id, req);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, under_vol_id);

	return ret_value;
} /* end H5VL_async_link_move() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_get
 *
 * Purpose:     Get info about a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_get (void *obj,
							const H5VL_loc_params_t *loc_params,
							H5VL_link_get_t get_type,
							hid_t dxpl_id,
							void **req,
							va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL LINK Get\n");
#endif

	ret_value = H5VLlink_get (o->under_object, loc_params, o->under_vol_id, get_type, dxpl_id, req,
							  arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_link_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_specific
 *
 * Purpose:     Specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_specific (void *obj,
								 const H5VL_loc_params_t *loc_params,
								 H5VL_link_specific_t specific_type,
								 hid_t dxpl_id,
								 void **req,
								 va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL LINK Specific\n");
#endif

	ret_value = H5VLlink_specific (o->under_object, loc_params, o->under_vol_id, specific_type,
								   dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_link_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_optional
 *
 * Purpose:     Perform a connector-specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_optional (
	void *obj, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL LINK Optional\n");
#endif

	ret_value =
		H5VLlink_optional (o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	return ret_value;
} /* end H5VL_async_link_optional() */
