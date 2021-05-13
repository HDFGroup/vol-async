/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This object is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the object COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* object callbacks */

#include <hdf5.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_info.h"
#include "h5vl_async_link.h"
#include "h5vl_async_linki.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_debug.h"

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
herr_t H5VL_async_link_create_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					  = TW_SUCCESS;
	H5VL_async_link_create_args *argp = (H5VL_async_link_create_args *)data;
	H5VL_async_t *o					  = (H5VL_async_t *)argp->target_obj;
	hid_t under_vol_id				  = -1;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL LINK Create\n");
#endif

	H5VL_ASYNC_HANDLER_BEGIN

	/* Try to retrieve the "under" VOL id */
	if (o) under_vol_id = o->under_vol_id;

	/* Fix up the link target object for hard link creation */
	if (H5VL_LINK_CREATE_HARD == argp->create_type) {
		void *cur_obj;
		H5VL_loc_params_t *cur_params;

		/* Retrieve the object & loc params for the link target */
		cur_obj	   = va_arg (argp->arguments, void *);
		cur_params = va_arg (argp->arguments, H5VL_loc_params_t *);

		/* If it's a non-NULL pointer, find the 'under object' and re-set the
		 * property */
		if (cur_obj) {
			/* Check if we still need the "under" VOL ID */
			if (under_vol_id < 0) under_vol_id = ((H5VL_async_t *)cur_obj)->under_vol_id;

			/* Set the object for the link target */
			cur_obj = ((H5VL_async_t *)cur_obj)->under_object;
		} /* end if */

		/* Re-issue 'link create' call, using the unwrapped pieces */
		ret_value = H5VL_async_link_create_reissue (
			argp->create_type, (o ? o->under_object : NULL), argp->loc_params, under_vol_id,
			argp->lcpl_id, argp->lapl_id, argp->dxpl_id, NULL, cur_obj, cur_params);
	} /* end if */
	else
		ret_value = H5VLlink_create (argp->create_type, (o ? o->under_object : NULL),
									 argp->loc_params, under_vol_id, argp->lcpl_id, argp->lapl_id,
									 argp->dxpl_id, NULL, argp->arguments);

err_out:;
	H5VL_ASYNC_HANDLER_END

	/* Mark task as finished */
	H5VL_asynci_mutex_lock (argp->op->lock);
	if (argp->op->prev_task == argp->task) { argp->op->prev_task = TW_HANDLE_NULL; }
	H5VL_asynci_mutex_unlock (argp->op->lock);

	H5Pclose (argp->lcpl_id);
	H5Pclose (argp->lapl_id);
	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
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
herr_t H5VL_async_link_copy_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					= TW_SUCCESS;
	H5VL_async_link_copy_args *argp = (H5VL_async_link_copy_args *)data;
	H5VL_async_t *o_src				= (H5VL_async_t *)argp->target_obj;
	H5VL_async_t *o_dst				= (H5VL_async_t *)argp->dst_obj;
	hid_t under_vol_id				= -1;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL LINK Copy\n");
#endif

	H5VL_ASYNC_HANDLER_BEGIN

	/* Retrieve the "under" VOL id */
	if (o_src)
		under_vol_id = o_src->under_vol_id;
	else if (o_dst)
		under_vol_id = o_dst->under_vol_id;
	if (under_vol_id <= 0) RET_ERR ("invalid under_vol_id");

	err = H5VLlink_copy ((o_src ? o_src->under_object : NULL), argp->loc_params1,
						 (o_dst ? o_dst->under_object : NULL), argp->loc_params2, under_vol_id,
						 argp->lcpl_id, argp->lapl_id, argp->dxpl_id, NULL);

err_out:;
	/* Decrease reference count of dependent objects */
	if (argp->dst_obj) {
		H5VL_asynci_mutex_lock (argp->dst_obj->lock);
		if (argp->dst_obj->prev_task == argp->task) { argp->dst_obj->prev_task = TW_HANDLE_NULL; }
		H5VL_asynci_mutex_unlock (argp->dst_obj->lock);
	}
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->lcpl_id);
	H5Pclose (argp->lapl_id);
	H5Pclose (argp->dxpl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
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
herr_t H5VL_async_link_move_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					= TW_SUCCESS;
	H5VL_async_link_move_args *argp = (H5VL_async_link_move_args *)data;
	H5VL_async_t *o_src				= (H5VL_async_t *)argp->target_obj;
	H5VL_async_t *o_dst				= (H5VL_async_t *)argp->dst_obj;
	hid_t under_vol_id				= -1;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL LINK Move\n");
#endif

	/* Retrieve the "under" VOL id */
	if (o_src)
		under_vol_id = o_src->under_vol_id;
	else if (o_dst)
		under_vol_id = o_dst->under_vol_id;
	if (under_vol_id <= 0) RET_ERR ("invalid under_vol_id");

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLlink_move ((o_src ? o_src->under_object : NULL), argp->loc_params1,
						 (o_dst ? o_dst->under_object : NULL), argp->loc_params2, under_vol_id,
						 argp->lcpl_id, argp->lapl_id, argp->dxpl_id, NULL);

err_out:;
	/* Decrease reference count of dependent objects */
	if (argp->dst_obj) {
		H5VL_asynci_mutex_lock (argp->dst_obj->lock);
		if (argp->dst_obj->prev_task == argp->task) { argp->dst_obj->prev_task = TW_HANDLE_NULL; }
		H5VL_asynci_mutex_unlock (argp->dst_obj->lock);
	}
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->lcpl_id);
	H5Pclose (argp->lapl_id);
	H5Pclose (argp->dxpl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
} /* end H5VL_async_link_move() */

int H5VL_async_link_get_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr				   = TW_SUCCESS;
	H5VL_async_link_get_args *argp = (H5VL_async_link_get_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLlink_get (argp->target_obj->under_object, argp->loc_params,
						argp->target_obj->under_vol_id, argp->get_type, argp->dxpl_id, NULL,
						argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}
int H5VL_async_link_specific_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	hid_t under_vol_id					= -1;
	H5VL_async_link_specific_args *argp = (H5VL_async_link_specific_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLlink_specific (argp->target_obj->under_object, argp->loc_params,
							 argp->target_obj->under_vol_id, argp->specific_type, argp->dxpl_id,
							 NULL, argp->arguments);

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_link_optional_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_link_optional_args *argp = (H5VL_async_link_optional_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLlink_optional (argp->target_obj->under_object, argp->loc_params, argp->target_obj->under_vol_id,
							 argp->opt_type, argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}
