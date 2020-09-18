/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This attr is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the attr COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* attr callbacks */

#include <hdf5.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_attr.h"
#include "h5vl_async_attri.h"
#include "h5vl_async_info.h"
#include "h5vl_asynci.h"

int H5VL_async_attr_create_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_attr_create_args *argp = (H5VL_async_attr_create_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the ap with the underlying VOL connector */
	argp->ap->under_vol_id = argp->pp->under_vol_id;
	H5Iinc_ref (argp->ap->under_vol_id);
	argp->ap->under_object = H5VLattr_create (
		argp->pp->under_object, argp->loc_params, argp->ap->under_vol_id, argp->name, argp->type_id,
		argp->space_id, argp->acpl_id, argp->acpl_id, argp->dxpl_id, NULL);
	CHECK_PTR (argp->ap->under_object)

err_out:;
	if (err) {
		argp->ap->stat = H5VL_async_stat_err;
	} else {
		argp->ap->stat = H5VL_async_stat_ready;
	}

	H5VL_ASYNC_HANDLER_END

	H5VL_asynci_mutex_lock (argp->ap->lock);
	H5VL_async_dec_ref (argp->ap);
	H5VL_asynci_mutex_unlock (argp->ap->lock);

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->acpl_id);
	H5Pclose (argp->aapl_id);
	H5Sclose (argp->space_id);
	H5Tclose (argp->type_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_attr_open_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_attr_open_args *argp = (H5VL_async_attr_open_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the attr with the underlying VOL connector */
	argp->ap->under_vol_id = argp->pp->under_vol_id;
	H5Iinc_ref (argp->ap->under_vol_id);
	argp->ap->under_object =
		H5VLattr_open (argp->pp->under_object, argp->loc_params, argp->ap->under_vol_id, argp->name,
					   argp->aapl_id, argp->dxpl_id, NULL);
	CHECK_PTR (argp->ap->under_object)

err_out:;
	if (err) {
		argp->ap->stat = H5VL_async_stat_err;
	} else {
		argp->ap->stat = H5VL_async_stat_ready;
	}

	H5VL_ASYNC_HANDLER_END

	H5VL_asynci_mutex_lock (argp->ap->lock);
	H5VL_async_dec_ref (argp->ap);
	H5VL_asynci_mutex_unlock (argp->ap->lock);

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->aapl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_attr_read_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					= TW_SUCCESS;
	H5VL_async_attr_read_args *argp = (H5VL_async_attr_read_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLattr_read (argp->pp->under_object, argp->pp->under_vol_id, argp->mem_type_id,
						 argp->buf, argp->dxpl_id, NULL);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5Tclose (argp->mem_type_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_attr_write_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					 = TW_SUCCESS;
	H5VL_async_attr_write_args *argp = (H5VL_async_attr_write_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLattr_write (argp->pp->under_object, argp->pp->under_vol_id, argp->mem_type_id,
						  argp->buf, argp->dxpl_id, NULL);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5Tclose (argp->mem_type_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_attr_get_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr				   = TW_SUCCESS;
	H5VL_async_attr_get_args *argp = (H5VL_async_attr_get_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLattr_get (argp->pp->under_object, argp->pp->under_vol_id, argp->get_type,
						argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_attr_specific_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	hid_t under_vol_id					= -1;
	H5VL_async_attr_specific_args *argp = (H5VL_async_attr_specific_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLattr_specific (argp->pp->under_object, argp->loc_params, argp->pp->under_vol_id,
							 argp->specific_type, argp->dxpl_id, NULL, argp->arguments);

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_attr_optional_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_attr_optional_args *argp = (H5VL_async_attr_optional_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLattr_optional (argp->pp->under_object, argp->pp->under_vol_id, argp->opt_type,
							 argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_attr_close_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_attr_close_args *argp = (H5VL_async_attr_close_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLattr_close (argp->pp->under_object, argp->pp->under_vol_id, argp->dxpl_id, NULL);
	CHECK_ERR

	err = H5VL_async_free_obj (argp->pp);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}