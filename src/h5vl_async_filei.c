/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* File callbacks */

#include <hdf5.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_file.h"
#include "h5vl_async_filei.h"
#include "h5vl_async_info.h"
#include "h5vl_asynci.h"

int H5VL_async_file_create_handler (void *data) {
	herr_t err = 0;
	H5VL_async_info_t *info, *under_vol_info;
	hid_t under_fapl_id, under_vol_id;
	void *under;
	H5VL_async_file_create_args *argp = (H5VL_async_file_create_args *)data;

	/* Get copy of our VOL info from FAPL */
	H5Pget_vol_info (argp->fapl_id, (void **)&info);

	if (info) {
		under_vol_id   = info->under_vol_id;
		under_vol_info = info->under_vol_info;
	} else {  // If no under VOL specified, use the native VOL
		htri_t ret;
		ret = H5VLis_connector_registered_by_name ("native");
		if (ret != 1) { RET_ERR ("Native VOL not found") }
		under_vol_id = H5VLpeek_connector_id_by_name ("native");
		CHECK_ID (under_vol_id)
		under_vol_info = NULL;
	}

	/* Copy the FAPL */
	under_fapl_id = H5Pcopy (argp->fapl_id);

	/* Set the VOL ID and info for the underlying FAPL */
	H5Pset_vol (under_fapl_id, under_vol_id, under_vol_info);

	/* Open the fp with the underlying VOL connector */
	argp->fp->under_object = H5VLfile_create (argp->name, argp->flags, argp->fcpl_id, under_fapl_id,
											  argp->dxpl_id, NULL);
	CHECK_PTR (argp->fp->under_object)

	argp->fp->under_object = under;
	argp->fp->under_vol_id = under_vol_id;

err_out:;
	if (err) {
		argp->fp->stat = H5VL_async_stat_err;
	} else {
		argp->fp->stat = H5VL_async_stat_ready;
	}

	H5VL_asynci_mutex_lock (argp->fp->lock);

	if (argp->ret) {
		*argp->ret = err;
	} else {
		TW_Task_free (argp->fp->init_task);
	}
	argp->fp->init_task = TW_HANDLE_NULL;
	H5VL_async_dec_ref (argp->fp);

	H5VL_asynci_mutex_unlock (argp->fp->lock);

	/* Close underlying FAPL */
	H5Pclose (under_fapl_id);

	/* Release copy of our VOL info */
	H5VL_async_info_free (info);

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->fapl_id);
	H5Pclose (argp->fcpl_id);
	free (argp->name);
	free (argp);

	return 0;
}

int H5VL_async_file_open_handler (void *data) {
	herr_t err = 0;
	H5VL_async_info_t *info, *under_vol_info;
	hid_t under_fapl_id, under_vol_id;
	void *under;
	H5VL_async_file_open_args *argp = (H5VL_async_file_open_args *)data;

	/* Get copy of our VOL info from FAPL */
	H5Pget_vol_info (argp->fapl_id, (void **)&info);

	if (info) {
		under_vol_id   = info->under_vol_id;
		under_vol_info = info->under_vol_info;
	} else {  // If no under VOL specified, use the native VOL
		htri_t ret;
		ret = H5VLis_connector_registered_by_name ("native");
		if (ret != 1) { RET_ERR ("Native VOL not found") }
		under_vol_id = H5VLpeek_connector_id_by_name ("native");
		CHECK_ID (under_vol_id)
		under_vol_info = NULL;
	}

	/* Copy the FAPL */
	under_fapl_id = H5Pcopy (argp->fapl_id);

	/* Set the VOL ID and info for the underlying FAPL */
	H5Pset_vol (under_fapl_id, under_vol_id, under_vol_info);

	/* Open the fp with the underlying VOL connector */
	argp->fp->under_object =
		H5VLfile_open (argp->name, argp->flags, under_fapl_id, argp->dxpl_id, NULL);
	CHECK_PTR (argp->fp->under_object)

	argp->fp->under_object = under;
	argp->fp->under_vol_id = under_vol_id;

err_out:;

	if (err) {
		argp->fp->stat = H5VL_async_stat_err;
	} else {
		argp->fp->stat = H5VL_async_stat_ready;
	}

	H5VL_asynci_mutex_lock (argp->fp->lock);

	if (argp->ret) {
		*argp->ret = err;
	} else {
		TW_Task_free (argp->fp->init_task);
	}
	argp->fp->init_task = TW_HANDLE_NULL;
	H5VL_async_dec_ref (argp->fp);

	H5VL_async_dec_ref (argp->fp);

	H5VL_asynci_mutex_unlock (argp->fp->lock);

	/* Close underlying FAPL */
	H5Pclose (under_fapl_id);

	/* Release copy of our VOL info */
	H5VL_async_info_free (info);

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->fapl_id);
	free (argp->name);
	free (argp);

	return 0;
}

int H5VL_async_file_get_handler (void *data) {
	herr_t err					   = 0;
	terr_t twerr				   = TW_SUCCESS;
	H5VL_async_file_get_args *argp = (H5VL_async_file_get_args *)data;

	err = H5VLfile_get (argp->fp->under_object, argp->fp->under_vol_id, argp->get_type,
						argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;

	H5VL_asynci_mutex_lock (argp->fp->lock);

	if (argp->ret) {
		*argp->ret = err;
	} else {
		TW_Task_free (argp->task);
	}
	H5VL_async_dec_ref (argp->fp);

	H5VL_asynci_mutex_unlock (argp->fp->lock);

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	free (argp);

	return 0;
}

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_specific_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              file specific callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_file_specific_reissue (void *obj,
										 hid_t connector_id,
										 H5VL_file_specific_t specific_type,
										 hid_t dxpl_id,
										 void **req,
										 ...) {
	va_list arguments;
	herr_t ret_value;

	va_start (arguments, req);
	ret_value = H5VLfile_specific (obj, connector_id, specific_type, dxpl_id, req, arguments);
	va_end (arguments);

	return ret_value;
} /* end H5VL_async_file_specific_reissue() */

int H5VL_async_file_specific_handler (void *data) {
	herr_t err							= 0;
	hid_t under_vol_id					= -1;
	H5VL_async_file_specific_args *argp = (H5VL_async_file_specific_args *)data;

	/* Unpack arguments to get at the child file pointer when mounting a file */
	if (argp->specific_type == H5VL_FILE_MOUNT) {
		H5I_type_t loc_type;
		const char *name;
		H5VL_async_t *child_file;
		hid_t plist_id;

		/* Retrieve parameters for 'mount' operation, so we can unwrap the child
		 * file */
		loc_type   = (H5I_type_t)va_arg (argp->arguments, int); /* enum work-around */
		name	   = va_arg (argp->arguments, const char *);
		child_file = (H5VL_async_t *)va_arg (argp->arguments, void *);
		plist_id   = va_arg (argp->arguments, hid_t);

		/* Keep the correct underlying VOL ID for possible async request token */
		under_vol_id = argp->fp->under_vol_id;

		/* Re-issue 'file specific' call, using the unwrapped pieces */
		err = H5VL_async_file_specific_reissue (
			argp->fp->under_object, argp->fp->under_vol_id, argp->specific_type, argp->dxpl_id,
			NULL, (int)loc_type, name, child_file->under_object, plist_id);
	} /* end if */
	else if (argp->specific_type == H5VL_FILE_IS_ACCESSIBLE ||
			 argp->specific_type == H5VL_FILE_DELETE) {
		H5VL_async_info_t *info;
		hid_t fapl_id, under_fapl_id;
		const char *name;
		htri_t *ret;

		/* Get the arguments for the 'is accessible' check */
		fapl_id = va_arg (argp->arguments, hid_t);
		name	= va_arg (argp->arguments, const char *);
		ret		= va_arg (argp->arguments, htri_t *);

		/* Get copy of our VOL info from FAPL */
		H5Pget_vol_info (fapl_id, (void **)&info);

		/* Copy the FAPL */
		under_fapl_id = H5Pcopy (fapl_id);

		/* Set the VOL ID and info for the underlying FAPL */
		H5Pset_vol (under_fapl_id, info->under_vol_id, info->under_vol_info);

		/* Keep the correct underlying VOL ID for possible async request token */
		under_vol_id = info->under_vol_id;

		/* Re-issue 'file specific' call */
		err = H5VL_async_file_specific_reissue (NULL, info->under_vol_id, argp->specific_type,
												argp->dxpl_id, NULL, under_fapl_id, name, ret);

		/* Close underlying FAPL */
		H5Pclose (under_fapl_id);

		/* Release copy of our VOL info */
		H5VL_async_info_free (info);
	} /* end else-if */
	else {
		va_list my_arguments;

		/* Make a copy of the argument list for later, if reopening */
		if (argp->specific_type == H5VL_FILE_REOPEN) va_copy (my_arguments, argp->arguments);

		/* Keep the correct underlying VOL ID for possible async request token */
		under_vol_id = argp->fp->under_vol_id;

		err = H5VLfile_specific (argp->fp->under_object, argp->fp->under_vol_id,
								 argp->specific_type, argp->dxpl_id, NULL, argp->arguments);

		/* Wrap file struct pointer, if we reopened one */
		if (argp->specific_type == H5VL_FILE_REOPEN) {
			if (err >= 0) {
				void **ret = va_arg (my_arguments, void **);

				if (ret && *ret) *ret = H5VL_async_new_obj (*ret, argp->fp->under_vol_id);
			} /* end if */

			/* Finish use of copied vararg list */
			va_end (my_arguments);
		} /* end if */
	}	  /* end else */

err_out:;
	H5VL_asynci_mutex_lock (argp->fp->lock);

	if (argp->ret) {
		*argp->ret = err;
	} else {
		TW_Task_free (argp->task);
	}
	H5VL_async_dec_ref (argp->fp);

	H5VL_asynci_mutex_unlock (argp->fp->lock);

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	free (argp);

	return 0;
}

int H5VL_async_file_optional_handler (void *data) {
	herr_t err							= 0;
	terr_t twerr						= TW_SUCCESS;
	H5VL_async_file_specific_args *argp = (H5VL_async_file_specific_args *)data;

	err = H5VLfile_optional (argp->fp->under_object, argp->fp->under_vol_id, argp->opt_type,
							 argp->dxpl_id, NULL, argp->arguments);
	CHECK_ERR

err_out:;
	TW_Task_free (argp->task);
	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	free (argp);

	return 0;
}

int H5VL_async_file_close_handler (void *data) {
	ret_value = H5VLfile_close (o->under_object, o->under_vol_id, dxpl_id, req);

	/* Check for async request */
	if (req && *req) *req = H5VL_async_new_obj (*req, o->under_vol_id);

	/* Release our wrapper, if underlying file was closed */
	if (ret_value >= 0) H5VL_async_free_obj (o);
}