/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This datatype is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the datatype COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* datatype callbacks */

#include <hdf5.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_dtype.h"
#include "h5vl_async_dtypei.h"
#include "h5vl_async_info.h"
#include "h5vl_asynci.h"

int H5VL_async_datatype_open_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_datatype_open_args *argp = (H5VL_async_datatype_open_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the datatype with the underlying VOL connector */
	argp->tp->under_vol_id = argp->pp->under_vol_id;
	H5Iinc_ref (argp->pp->under_vol_id);
	argp->tp->under_object =
		H5VLdatatype_open (argp->pp->under_object, argp->loc_params, argp->pp->under_vol_id,
						   argp->name, argp->tapl_id, argp->dxpl_id, NULL);
	CHECK_PTR (argp->pp->under_object)

err_out:;
	if (err) {
		argp->tp->stat = H5VL_async_stat_err;
	} else {
		argp->tp->stat = H5VL_async_stat_ready;
	}

	H5VL_asynci_mutex_lock (argp->tp->lock);
	H5VL_async_dec_ref (argp->tp);
	H5VL_asynci_mutex_unlock (argp->tp->lock);

	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->tapl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_datatype_commit_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_datatype_commit_args *argp = (H5VL_async_datatype_commit_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the pp with the underlying VOL connector */
	err = H5VLdatatype_commit (argp->pp->under_object, argp->loc_params, argp->pp->under_vol_id,
							   argp->name, argp->type_id, argp->lcpl_id, argp->tcpl_id,
							   argp->tapl_id, argp->dxpl_id, NULL);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->lcpl_id);
	H5Pclose (argp->tcpl_id);
	H5Pclose (argp->tapl_id);
	H5Tclose (argp->type_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_datatype_get_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					   = TW_SUCCESS;
	H5VL_async_datatype_get_args *argp = (H5VL_async_datatype_get_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLdatatype_get (argp->pp->under_object, argp->pp->under_vol_id, argp->get_type,
							argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}
int H5VL_async_datatype_specific_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	hid_t under_vol_id						= -1;
	H5VL_async_datatype_specific_args *argp = (H5VL_async_datatype_specific_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLdatatype_specific (argp->pp->under_object, argp->pp->under_vol_id,
								 argp->specific_type, argp->dxpl_id, NULL, argp->arguments);

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_datatype_optional_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_datatype_optional_args *argp = (H5VL_async_datatype_optional_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLdatatype_optional (argp->pp->under_object, argp->pp->under_vol_id, argp->opt_type,
								 argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_datatype_close_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_datatype_close_args *argp = (H5VL_async_datatype_close_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLdatatype_close (argp->pp->under_object, argp->pp->under_vol_id, argp->dxpl_id, NULL);
	CHECK_ERR

	err = H5VL_async_free_obj (argp->pp);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}