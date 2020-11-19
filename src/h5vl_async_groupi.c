/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This group is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the group COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* group callbacks */

#include <hdf5.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_group.h"
#include "h5vl_async_groupi.h"
#include "h5vl_async_info.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_debug.h"

int H5VL_async_group_create_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_group_create_args *argp = (H5VL_async_group_create_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the gp with the underlying VOL connector */
	argp->gp->under_vol_id = argp->pp->under_vol_id;
	H5Iinc_ref (argp->gp->under_vol_id);
	argp->gp->under_object = H5VLgroup_create (argp->pp->under_object, argp->loc_params,
											   argp->gp->under_vol_id, argp->name, argp->lcpl_id,
											   argp->gcpl_id, argp->gapl_id, argp->dxpl_id, NULL);
	CHECK_PTR (argp->gp->under_object)

err_out:;
	if (err) {
		argp->gp->stat = H5VL_async_stat_err;
	} else {
		argp->gp->stat = H5VL_async_stat_ready;
	}

	H5VL_asynci_mutex_lock (argp->gp->lock);
	argp->gp->init_task = NULL;
	H5VL_async_dec_ref (argp->gp);
	H5VL_asynci_mutex_unlock (argp->gp->lock);

	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->gapl_id);
	H5Pclose (argp->gcpl_id);
	H5Pclose (argp->lcpl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_group_open_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_group_open_args *argp = (H5VL_async_group_open_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the group with the underlying VOL connector */
	argp->gp->under_vol_id = argp->pp->under_vol_id;
	H5Iinc_ref (argp->gp->under_vol_id);
	argp->gp->under_object =
		H5VLgroup_open (argp->pp->under_object, argp->loc_params, argp->gp->under_vol_id,
						argp->name, argp->gapl_id, argp->dxpl_id, NULL);
	CHECK_PTR (argp->gp->under_object)

err_out:;
	if (err) {
		argp->gp->stat = H5VL_async_stat_err;
	} else {
		argp->gp->stat = H5VL_async_stat_ready;
	}

	H5VL_asynci_mutex_lock (argp->gp->lock);
	argp->gp->init_task = NULL;
	H5VL_async_dec_ref (argp->gp);
	H5VL_asynci_mutex_unlock (argp->gp->lock);

	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->gapl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_group_get_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					= TW_SUCCESS;
	H5VL_async_group_get_args *argp = (H5VL_async_group_get_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLgroup_get (argp->pp->under_object, argp->pp->under_vol_id, argp->get_type,
						 argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_specific_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              group specific callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_group_specific_reissue (void *obj,
										  hid_t connector_id,
										  H5VL_group_specific_t specific_type,
										  hid_t dxpl_id,
										  void **req,
										  ...) {
	va_list arguments;
	herr_t ret_value;

	va_start (arguments, req);
	ret_value = H5VLgroup_specific (obj, connector_id, specific_type, dxpl_id, req, arguments);
	va_end (arguments);

	return ret_value;
} /* end H5VL_async_group_specific_reissue() */

int H5VL_async_group_specific_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	hid_t under_vol_id					 = -1;
	H5VL_async_group_specific_args *argp = (H5VL_async_group_specific_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLgroup_specific (argp->pp->under_object, argp->pp->under_vol_id, argp->specific_type,
							  argp->dxpl_id, NULL, argp->arguments);

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_group_optional_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_group_optional_args *argp = (H5VL_async_group_optional_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLgroup_optional (argp->pp->under_object, argp->pp->under_vol_id, argp->opt_type,
							  argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_group_close_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_group_close_args *argp = (H5VL_async_group_close_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLgroup_close (argp->pp->under_object, argp->pp->under_vol_id, argp->dxpl_id, NULL);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	err = H5VL_async_free_obj (argp->pp);
	CHECK_ERR2
	H5Pclose (argp->dxpl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}