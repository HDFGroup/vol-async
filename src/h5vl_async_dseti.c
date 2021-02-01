/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This dataset is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the dataset COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* dataset callbacks */

#include <hdf5.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_dset.h"
#include "h5vl_async_dseti.h"
#include "h5vl_async_info.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_debug.h"

int H5VL_async_dataset_create_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_dataset_create_args *argp = (H5VL_async_dataset_create_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the op with the underlying VOL connector */
	argp->op->under_vol_id = argp->target_obj->under_vol_id;
	H5Iinc_ref (argp->op->under_vol_id);
    void* obj;
	H5E_BEGIN_TRY {
        obj = H5VLdataset_create (argp->target_obj->under_object, argp->loc_params,
                    argp->op->under_vol_id, argp->name, argp->lcpl_id, argp->type_id,
                    argp->space_id, argp->dcpl_id, argp->dapl_id, argp->dxpl_id, NULL);
    } H5E_END_TRY

    if (NULL == obj) {
        if ((argp->op->error_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto err_out;
    }
    argp->op->under_object = obj;
	CHECK_PTR (argp->op->under_object)

err_out:;
	if (err) {
		argp->op->stat = H5VL_async_stat_err;
	} else {
		argp->op->stat = H5VL_async_stat_ready;
	}

	H5VL_ASYNC_HANDLER_END

	/* Mark task as finished */
	H5VL_asynci_mutex_lock (argp->op->lock);
	if (argp->op->prev_task == argp->task) { argp->op->prev_task = TW_HANDLE_NULL; }
	H5VL_asynci_mutex_unlock (argp->op->lock);

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->dapl_id);
	H5Pclose (argp->dcpl_id);
	H5Pclose (argp->lcpl_id);
	H5Sclose (argp->space_id);
	H5Tclose (argp->type_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_dataset_open_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_dataset_open_args *argp = (H5VL_async_dataset_open_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	/* Open the dataset with the underlying VOL connector */
	argp->op->under_vol_id = argp->target_obj->under_vol_id;
	H5Iinc_ref (argp->op->under_vol_id);
	argp->op->under_object =
		H5VLdataset_open (argp->target_obj->under_object, argp->loc_params, argp->op->under_vol_id,
						  argp->name, argp->dapl_id, argp->dxpl_id, NULL);
	CHECK_PTR (argp->op->under_object)

err_out:;
	if (err) {
		argp->op->stat = H5VL_async_stat_err;
	} else {
		argp->op->stat = H5VL_async_stat_ready;
	}

	/* Mark task as finished */
	H5VL_asynci_mutex_lock (argp->op->lock);
	if (argp->op->prev_task == argp->task) { argp->op->prev_task = TW_HANDLE_NULL; }
	H5VL_asynci_mutex_unlock (argp->op->lock);

	H5VL_ASYNC_HANDLER_END

	/* Mark task as finished */
	H5VL_asynci_mutex_lock (argp->op->lock);
	if (argp->op->prev_task == argp->task) { argp->op->prev_task = TW_HANDLE_NULL; }
	H5VL_asynci_mutex_unlock (argp->op->lock);

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->dapl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_dataset_read_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					   = TW_SUCCESS;
	H5VL_async_dataset_read_args *argp = (H5VL_async_dataset_read_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLdataset_read (argp->target_obj->under_object, argp->target_obj->under_vol_id,
							argp->mem_type_id, argp->mem_space_id, argp->file_space_id,
							argp->dxpl_id, argp->buf, NULL);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	if (argp->file_space_id != H5S_ALL) H5Sclose (argp->file_space_id);
	if (argp->mem_space_id != H5S_ALL) H5Sclose (argp->mem_space_id);
	H5Tclose (argp->mem_type_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_dataset_write_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr						= TW_SUCCESS;
	H5VL_async_dataset_write_args *argp = (H5VL_async_dataset_write_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN
	H5E_BEGIN_TRY {
	err = H5VLdataset_write (argp->target_obj->under_object, argp->target_obj->under_vol_id,
							 argp->mem_type_id, argp->mem_space_id, argp->file_space_id,
							 argp->dxpl_id, argp->buf, NULL);
    } H5E_END_TRY

    if (err < 0) {
        if ((argp->op->error_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto err_out;
    }

	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	if (argp->file_space_id != H5S_ALL) H5Sclose (argp->file_space_id);
	if (argp->mem_space_id != H5S_ALL) H5Sclose (argp->mem_space_id);
	H5Tclose (argp->mem_type_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_dataset_get_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					  = TW_SUCCESS;
	H5VL_async_dataset_get_args *argp = (H5VL_async_dataset_get_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLdataset_get (argp->target_obj->under_object, argp->target_obj->under_vol_id,
						   argp->get_type, argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_dataset_specific_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	hid_t under_vol_id					   = -1;
	H5VL_async_dataset_specific_args *argp = (H5VL_async_dataset_specific_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLdataset_specific (argp->target_obj->under_object, argp->target_obj->under_vol_id,
								argp->specific_type, argp->dxpl_id, NULL, argp->arguments);

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_dataset_optional_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_dataset_optional_args *argp = (H5VL_async_dataset_optional_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

	err = H5VLdataset_optional (argp->target_obj->under_object, argp->target_obj->under_vol_id,
								argp->opt_type, argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_dataset_close_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_dataset_close_args *argp = (H5VL_async_dataset_close_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN
	H5E_BEGIN_TRY {
        err = H5VLdataset_close (argp->target_obj->under_object, argp->target_obj->under_vol_id,
                                 argp->dxpl_id, NULL);
	} H5E_END_TRY
    if (err < 0) {
        if ((argp->op->error_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto err_out;
    }

	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	err = H5VL_async_free_obj (argp->target_obj);
	CHECK_ERR2
	H5Pclose (argp->dxpl_id);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}
