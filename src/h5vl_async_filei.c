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
#include "h5vl_asynci_debug.h"

int H5VL_async_file_create_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_info_t *info, *under_vol_info;
	hid_t under_fapl_id, under_vol_id;
	H5VL_async_file_create_args *argp = (H5VL_async_file_create_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

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
		// under_vol_id = H5VLget_connector_id_by_value (0);
		CHECK_ID (under_vol_id)
		under_vol_info = NULL;
	}

	/* Copy the FAPL */
	under_fapl_id = H5Pcopy (argp->fapl_id);

	/* Set the VOL ID and info for the underlying FAPL */
	H5Pset_vol (under_fapl_id, under_vol_id, under_vol_info);

	/* Open the file with the underlying VOL connector */
	argp->op->under_vol_id = under_vol_id;
	H5Iinc_ref (argp->op->under_vol_id);
	void* obj = NULL;
    H5E_BEGIN_TRY {
	    REQ_TIMER_START
	    printf("%s: argp->op->under_vol_id %llu\n", __func__, argp->op->under_vol_id);
	    obj = H5VLfile_create (argp->name, argp->flags, argp->fcpl_id, under_fapl_id,
						argp->dxpl_id, NULL);
	    REQ_TIMER_END

	    argp->op->under_object = obj;
    } H5E_END_TRY

    if (NULL == obj) {
        if ((argp->op->error_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
    }

    CHECK_PTR (argp->op->under_object)
    /* Check for 'post open' callback */
    uint64_t supported = 0;
    if(H5VLintrospect_opt_query(obj, under_vol_id, H5VL_SUBCLS_FILE, H5VL_NATIVE_FILE_POST_OPEN, &supported) < 0) {
        fprintf(stderr," %s H5VLintrospect_opt_query failed\n", __func__);
        goto err_out;
    }
    if(supported & H5VL_OPT_QUERY_SUPPORTED) {
        /* Make the 'post open' callback */
        /* Try executing operation, without default error stack handling */
        herr_t status;
        H5E_BEGIN_TRY {
            status = H5VLfile_optional_vararg(obj, under_vol_id, H5VL_NATIVE_FILE_POST_OPEN, argp->dxpl_id, NULL);
            err = status;
        } H5E_END_TRY
        if ( status < 0 ) {
            if ((argp->op->error_stack = H5Eget_current_stack()) < 0)
                fprintf(stderr,"  %s H5Eget_current_stack failed\n", __func__);
            goto err_out;
        }
    } /* end if */

err_out:;
if (err) {
    argp->op->stat = H5VL_async_stat_err;
    if(argp->req)
        ((H5VL_async_req_t*)(argp->req))->req_stat = REQ_FAIL;
} else {
    argp->op->stat = H5VL_async_stat_ready;
    if(argp->req)
        ((H5VL_async_req_t*)(argp->req))->req_stat = REQ_SUCCEED;
}

	H5VL_ASYNC_HANDLER_END

	/* Mark task as finished */
	H5VL_asynci_mutex_lock (argp->op->lock);
	if (argp->op->prev_task == argp->task) { argp->op->prev_task = TW_HANDLE_NULL; }
	H5VL_asynci_mutex_unlock (argp->op->lock);

	/* Close underlying FAPL */
	H5Pclose (under_fapl_id);

	/* Release copy of our VOL info */
	H5VL_async_info_free (info);

	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->fapl_id);
	H5Pclose (argp->fcpl_id);
	free (argp->name);

	//H5VL_ASYNC_HANDLER_FREE
    err = H5VL_asynci_handler_free (
                (H5VL_asynci_debug_args *)argp);
    CHECK_ERR
    //*(argp->ret_arg) = err;
     free(argp);
	return 0;
}

int H5VL_async_file_open_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_info_t *info, *under_vol_info;
	hid_t under_fapl_id, under_vol_id;
	H5VL_async_file_open_args *argp = (H5VL_async_file_open_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

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

	/* Open the file with the underlying VOL connector */
	argp->op->under_vol_id = under_vol_id;
	H5Iinc_ref (argp->op->under_vol_id);

	REQ_TIMER_START
	argp->op->under_object = H5VLfile_open (argp->name, argp->flags, under_fapl_id, argp->dxpl_id, NULL);
	REQ_TIMER_END

	CHECK_PTR (argp->op->under_object)

err_out:;
    if (err) {
        argp->op->stat = H5VL_async_stat_err;
        if(argp->req)
            ((H5VL_async_req_t*)(argp->req))->req_stat = REQ_FAIL;
    } else {
        argp->op->stat = H5VL_async_stat_ready;
        if(argp->req)
            ((H5VL_async_req_t*)(argp->req))->req_stat = REQ_SUCCEED;
    }

	H5VL_ASYNC_HANDLER_END

	/* Mark task as finished */
	H5VL_asynci_mutex_lock (argp->op->lock);
	if (argp->op->prev_task == argp->task) { argp->op->prev_task = TW_HANDLE_NULL; }
	H5VL_asynci_mutex_unlock (argp->op->lock);

	/* Close underlying FAPL */
	H5Pclose (under_fapl_id);

	/* Release copy of our VOL info */
	H5VL_async_info_free (info);
	H5Pclose (argp->dxpl_id);
	H5Pclose (argp->fapl_id);
	free (argp->name);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_file_get_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	H5VL_async_file_get_args *argp = (H5VL_async_file_get_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN
	REQ_TIMER_START
	err = H5VLfile_get (argp->target_obj->under_object, argp->target_obj->under_vol_id,
						argp->get_type, argp->dxpl_id, NULL, argp->arguments);
	REQ_TIMER_END
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

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
	H5VL_ASYNC_HANDLER_VARS
	hid_t under_vol_id					= -1;
	H5VL_async_file_specific_args *argp = (H5VL_async_file_specific_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN

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
		under_vol_id = argp->target_obj->under_vol_id;

		/* Re-issue 'file specific' call, using the unwrapped pieces */
		err = H5VL_async_file_specific_reissue (
			argp->target_obj->under_object, argp->target_obj->under_vol_id, argp->specific_type,
			argp->dxpl_id, NULL, (int)loc_type, name, child_file->under_object, plist_id);
	} /* end if */
	else if (argp->specific_type == H5VL_FILE_IS_ACCESSIBLE ||
			 argp->specific_type == H5VL_FILE_DELETE) {
		H5VL_async_info_t *info;
		hid_t fapl_id, under_fapl_id;
		const char *name;
		htri_t *local_ret;

		/* Get the arguments for the 'is accessible' check */
		fapl_id = va_arg (argp->arguments, hid_t);
		name	= va_arg (argp->arguments, const char *);
		local_ret		= va_arg (argp->arguments, htri_t *);

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
												argp->dxpl_id, NULL, under_fapl_id, name, local_ret);

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
		under_vol_id = argp->target_obj->under_vol_id;

		err = H5VLfile_specific (argp->target_obj->under_object, argp->target_obj->under_vol_id,
								 argp->specific_type, argp->dxpl_id, NULL, argp->arguments);

		/* Wrap file struct pointer, if we reopened one */
		if (argp->specific_type == H5VL_FILE_REOPEN) {
			if (err >= 0) {
				void **ret = va_arg (my_arguments, void **);

				if (ret && *ret) *ret = H5VL_async_new_obj (*ret, argp->target_obj->under_vol_id);
			} /* end if */

			/* Finish use of copied vararg list */
			va_end (my_arguments);
		} /* end if */
	}	  /* end else */

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_file_optional_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr						= TW_SUCCESS;
	H5VL_async_file_optional_args *argp = (H5VL_async_file_optional_args *)data;

	H5VL_ASYNC_HANDLER_BEGIN
	REQ_TIMER_START
	err = H5VLfile_optional (argp->target_obj->under_object, argp->target_obj->under_vol_id,
							 argp->opt_type, argp->dxpl_id, NULL, argp->arguments);
	REQ_TIMER_END
	CHECK_ERR

err_out:;
	H5VL_ASYNC_HANDLER_END

	H5Pclose (argp->dxpl_id);
	va_end (argp->arguments);
	H5VL_ASYNC_HANDLER_FREE

	return 0;
}

int H5VL_async_file_close_handler (void *data) {
	H5VL_ASYNC_HANDLER_VARS
	terr_t twerr					 = TW_SUCCESS;
	H5VL_async_file_close_args *argp = (H5VL_async_file_close_args *)data;
	printf("%s: argp->target_obj->under_vol_id = %llu\n", __func__, argp->target_obj->under_vol_id);
	H5VL_ASYNC_HANDLER_BEGIN
DEBUG_PRINT
    H5E_BEGIN_TRY {
	    REQ_TIMER_START
	    err = H5VLfile_close (argp->target_obj->under_object, argp->target_obj->under_vol_id,
						  argp->dxpl_id, NULL);
	    //argp->target_obj->under_object???
	    REQ_TIMER_END
DEBUG_PRINT
    } H5E_END_TRY
    if (err < 0) {
        DEBUG_PRINT
        if ((argp->op->error_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
    }
	CHECK_ERR
DEBUG_PRINT
err_out:;
    if (err) {
DEBUG_PRINT
        argp->op->stat = H5VL_async_stat_err;
        if(argp->req){
DEBUG_PRINT
            ((H5VL_async_req_t*)(argp->req))->req_stat = REQ_FAIL;
            ((H5VL_async_req_t*)(argp->req))->error_stack = argp->op->error_stack;
        }
    } else {
DEBUG_PRINT
        argp->op->stat = H5VL_async_stat_ready;
        if(argp->req)
            ((H5VL_async_req_t*)(argp->req))->req_stat = REQ_SUCCEED;
    }
DEBUG_PRINT
	H5VL_ASYNC_HANDLER_END
	printf("%s: obj = %p, closing lock = %p\n", __func__, argp->target_obj, argp->target_obj->lock); fflush(stdout); fflush(stderr);
DEBUG_PRINT
    err = H5VL_async_free_obj (argp->op);//argp->target_obj
	CHECK_ERR2
DEBUG_PRINT
	H5Pclose (argp->dxpl_id);
	H5VL_ASYNC_HANDLER_FREE
DEBUG_PRINT
	return 0;
}
