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
#include "h5vl_async_obj.h"
#include "h5vl_async_obji.h"
#include "h5vl_asynci.h"

int H5VL_async_object_open_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_object_open_args *argp = (H5VL_async_object_open_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the object with the underlying VOL connector */
	argp->op->under_vol_id = argp->pp->under_vol_id;
	H5Iinc_ref (argp->pp->under_vol_id);
	argp->op->under_object =
		H5VLobject_open (argp->pp->under_object, argp->loc_params, argp->pp->under_vol_id,
						 argp->opened_type, argp->dxpl_id, NULL);
	CHECK_PTR (argp->pp->under_object)

err_out:;
	if (err) {
		argp->op->stat = H5VL_async_stat_err;
	} else {
		argp->op->stat = H5VL_async_stat_ready;
	}

	H5VL_asynci_mutex_lock (argp->op->lock);
	H5VL_async_dec_ref (argp->op);
	H5VL_asynci_mutex_unlock (argp->op->lock);

	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_object_copy_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_object_copy_args *argp = (H5VL_async_object_copy_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the pp with the underlying VOL connector */
	err = H5VLobject_copy (argp->pp->under_object, argp->src_loc_params, argp->src_name,
						   argp->dst_obj->under_object, argp->dst_loc_params, argp->dst_name,
						   argp->pp->under_vol_id, argp->ocpypl_id, argp->lcpl_id, argp->dxpl_id,
						   NULL);
	CHECK_ERR

err_out:;
	H5VL_asynci_mutex_lock (argp->dst_obj->lock);
	H5VL_async_dec_ref (argp->dst_obj);
	H5VL_asynci_mutex_unlock (argp->dst_obj->lock);

	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->lcpl_id);
	H5Pclose (argp->ocpypl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_object_get_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					 = TW_SUCCESS;
	H5VL_async_object_get_args *argp = (H5VL_async_object_get_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLobject_get (argp->pp->under_object, argp->loc_params, argp->pp->under_vol_id,
						  argp->get_type, argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}
int H5VL_async_object_specific_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	hid_t under_vol_id					  = -1;
	H5VL_async_object_specific_args *argp = (H5VL_async_object_specific_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLobject_specific (argp->pp->under_object, argp->loc_params, argp->pp->under_vol_id,
							   argp->specific_type, argp->dxpl_id, NULL, argp->arguments);

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_object_optional_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_object_optional_args *argp = (H5VL_async_object_optional_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLobject_optional (argp->pp->under_object, argp->pp->under_vol_id, argp->opt_type,
							   argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}
