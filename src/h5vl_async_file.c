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
#include "h5vl_async_public.h"
#include "h5vl_async_req.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_debug.h"
#include "h5vl_asynci_mutex.h"
#include "h5vl_asynci_vector.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_file_create (
	const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_file_create_args *argp = NULL;
	H5VL_async_t *op				  = NULL;
	size_t name_len;
	H5VL_async_t *target_obj = NULL;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Create\n");
#endif

	op = H5VL_async_new_obj ();
	CHECK_PTR (op)

	argp = (H5VL_async_file_create_args *)malloc (sizeof (H5VL_async_file_create_args));
	CHECK_PTR (argp)
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->fapl_id	 = H5Pcopy (fapl_id);
	argp->fcpl_id	 = H5Pcopy (fcpl_id);
	argp->op		 = op;
	argp->target_obj = NULL;
	argp->flags		 = flags;
	name_len		 = strlen (name);
	argp->name		 = (char *)malloc (name_len + 1);
	strncpy (argp->name, name, name_len + 1);

	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_file_create_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	op->prev_task = task;

	H5VL_ASYNC_CB_TASK_COMMIT
	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->fapl_id);
			H5Pclose (argp->fcpl_id);
			free (argp->name);
			free (argp);
		}

		free (reqp);

		free (target_obj);
		target_obj = NULL;
	}

	return (void *)target_obj;
} /* end H5VL_async_file_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_file_open (
	const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_file_open_args *argp = NULL;
	H5VL_async_t *op				= NULL;
	size_t name_len;
	H5VL_async_t *target_obj = NULL;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Open\n");
#endif

	op = H5VL_async_new_obj ();
	CHECK_PTR (op)

	argp = (H5VL_async_file_open_args *)malloc (sizeof (H5VL_async_file_open_args));
	CHECK_PTR (argp)
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->fapl_id	 = H5Pcopy (fapl_id);
	argp->op		 = op;
	argp->target_obj = NULL;
	argp->flags		 = flags;
	name_len		 = strlen (name);
	argp->name		 = (char *)malloc (name_len + 1);
	strncpy (argp->name, name, name_len + 1);

	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_file_open_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	op->prev_task = task;

	H5VL_ASYNC_CB_TASK_COMMIT
	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->fapl_id);
			free (argp->name);
			free (argp);
		}

		free (reqp);

		free (target_obj);
		target_obj = NULL;
	}

	return (void *)target_obj;
} /* end H5VL_async_file_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_get
 *
 * Purpose:     Get info about a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_file_get (
	void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_file_get_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)file;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Get\n");
#endif

	argp = (H5VL_async_file_get_args *)malloc (sizeof (H5VL_async_file_get_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->get_type	 = get_type;
	va_copy (argp->arguments, arguments);

	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_file_get_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_ASYNC_CB_TASK_COMMIT
	H5VL_ASYNC_CB_TASK_WAIT
err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			va_end (argp->arguments);
			free (argp);
		}

		free (reqp);
	}
	return err;
} /* end H5VL_async_file_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_specific
 *
 * Purpose:     Specific operation on file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_file_specific (
	void *file, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_file_specific_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)file;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Specific\n");
#endif

	//if (specific_type == H5VL_FILE_WAIT) { return (H5VL_asynci_obj_wait (file)); }

	argp = (H5VL_async_file_specific_args *)malloc (sizeof (H5VL_async_file_specific_args));
	CHECK_PTR (argp)
	argp->target_obj	= target_obj;
	argp->dxpl_id		= H5Pcopy (dxpl_id);
	argp->specific_type = specific_type;
	va_copy (argp->arguments, arguments);

	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_file_specific_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_ASYNC_CB_TASK_COMMIT
	H5VL_ASYNC_CB_TASK_WAIT
err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			va_end (argp->arguments);
			free (argp);
		}
	}

	return err;
} /* end H5VL_async_file_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_optional
 *
 * Purpose:     Perform a connector-specific operation on a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_file_optional (
	void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_file_optional_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)file;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL File Optional\n");
#endif

	argp = (H5VL_async_file_optional_args *)malloc (sizeof (H5VL_async_file_optional_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->opt_type	 = opt_type;
	va_copy (argp->arguments, arguments);

	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_file_optional_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_ASYNC_CB_TASK_COMMIT
	H5VL_ASYNC_CB_TASK_WAIT
err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			va_end (argp->arguments);
			free (argp);
		}
	}

	return err;
} /* end H5VL_async_file_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_close
 *
 * Purpose:     Closes a file.
 *
 * Return:      Success:    0
 *              Failure:    -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_file_close (void *file, hid_t dxpl_id, void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_file_close_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)file;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Close\n");
#endif

	/* Mark as closed so no operation can be performed */
	H5VL_asynci_mutex_lock (target_obj->lock);
	target_obj->stat == H5VL_async_stat_close;
	H5VL_asynci_mutex_unlock (target_obj->lock);

	argp = (H5VL_async_file_close_args *)malloc (sizeof (H5VL_async_file_close_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_file_close_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_ASYNC_CB_TASK_COMMIT
	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			free (argp);
		}
	}

	return err;
} /* end H5VL_async_file_close() */
