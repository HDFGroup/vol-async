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
	herr_t err						  = 0;
	terr_t twerr					  = TW_SUCCESS;
	H5VL_async_file_create_args *argp = NULL;
	H5VL_async_t *fp				  = NULL;
	H5VL_async_req_t *reqp			  = NULL;
	size_t name_len;
	hbool_t is_async;
	herr_t ret;
	TW_Task_handle_t task;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Create\n");
#endif

	fp = H5VL_async_new_obj ();
	CHECK_PTR (fp)
	argp = (H5VL_async_file_create_args *)malloc (sizeof (H5VL_async_file_create_args));
	CHECK_PTR (argp)

	err = H5Pget_dxpl_async (dxpl_id, &is_async);
	CHECK_ERR

	argp->dxpl_id = H5Pcopy (dxpl_id);
	argp->fapl_id = H5Pcopy (fapl_id);
	argp->fcpl_id = H5Pcopy (fcpl_id);
	argp->fp	  = fp;
	argp->flags	  = flags;
	name_len	  = strlen (name);
	argp->name	  = (char *)malloc (name_len + 1);
	strncpy (argp->name, name, name_len);
	if (is_async) {
		if (req) {
			reqp = (H5VL_async_req_t *)malloc (sizeof (H5VL_async_req_t));
			CHECK_PTR (reqp)

			argp->ret = &(reqp->ret);
		} else {
			argp->ret = NULL;
		}
	} else {
		argp->ret = &ret;
	}

	twerr = TW_Task_create (H5VL_async_file_create_handler, argp, TW_TASK_DEP_NULL, 0, &task);
	CHK_TWERR
	fp->init_task = task;
	if (reqp) { reqp->task = task; }

	H5VL_async_inc_ref (fp);
	twerr = TW_Task_commit (task, H5VL_async_engine);
	CHK_TWERR

	if (!is_async) {
		// Release the lock so worker thread can acquire
		H5TSmutex_release ();

		twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);
		CHK_TWERR

		err = H5VL_asynci_h5ts_mutex_lock ();
		CHECK_ERR

		twerr = TW_Task_free (task);
		CHK_TWERR
		fp->init_task = TW_HANDLE_NULL;

		err = ret;
		CHECK_ERR
	}

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

		free (fp);
		fp = NULL;
	}

	return (void *)fp;
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
	herr_t err						= 0;
	terr_t twerr					= TW_SUCCESS;
	H5VL_async_file_open_args *argp = NULL;
	H5VL_async_t *fp				= NULL;
	H5VL_async_req_t *reqp			= NULL;
	size_t name_len;
	hbool_t is_async;
	herr_t ret;
	TW_Task_handle_t task;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Open\n");
#endif

	err = H5Pget_dxpl_async (dxpl_id, &is_async);
	CHECK_ERR

	fp = H5VL_async_new_obj ();
	CHECK_PTR (fp)
	argp = (H5VL_async_file_open_args *)malloc (sizeof (H5VL_async_file_open_args));
	CHECK_PTR (argp)

	argp->dxpl_id = H5Pcopy (dxpl_id);
	argp->fapl_id = H5Pcopy (fapl_id);
	argp->fp	  = fp;
	argp->flags	  = flags;
	name_len	  = strlen (name);
	argp->name	  = (char *)malloc (name_len + 1);
	strncpy (argp->name, name, name_len);
	if (is_async) {
		if (req) {
			reqp = (H5VL_async_req_t *)malloc (sizeof (H5VL_async_req_t));
			CHECK_PTR (reqp)

			argp->ret = &(reqp->ret);

			*req = reqp;
		} else {
			argp->ret = NULL;
		}
	} else {
		argp->ret = &ret;
	}

	twerr = TW_Task_create (H5VL_async_file_open_handler, argp, TW_TASK_DEP_NULL, 0, &task);
	CHK_TWERR
	fp->init_task = task;
	if (reqp) { reqp->task = task; }

	H5VL_async_inc_ref (fp);
	twerr = TW_Task_commit (task, H5VL_async_engine);
	CHK_TWERR

	if (!is_async) {
		// Release the lock so worker thread can acquire
		H5TSmutex_release ();

		twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);
		CHK_TWERR

		err = H5VL_asynci_h5ts_mutex_lock ();
		CHECK_ERR

		twerr = TW_Task_free (task);
		CHK_TWERR
		task = TW_HANDLE_NULL;

		err = ret;
		CHECK_ERR
	}

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

		free (fp);
		fp = NULL;
	}

	return (void *)fp;
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
	herr_t err	 = 0;
	terr_t twerr = TW_SUCCESS;
	H5VL_async_file_get_args *argp;
	H5VL_async_t *fp	   = (H5VL_async_t *)file;
	H5VL_async_req_t *reqp = NULL;
	hbool_t is_async;
	herr_t ret;
	TW_Task_handle_t task;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Get\n");
#endif

	if ((fp->stat == H5VL_async_stat_err) || (fp->stat == H5VL_async_stat_close)) {
		RET_ERR ("Parent object in wrong status");
	}

	err = H5Pget_dxpl_async (dxpl_id, &is_async);
	CHECK_ERR

	argp = (H5VL_async_file_get_args *)malloc (sizeof (H5VL_async_file_get_args));
	CHECK_PTR (argp)

	argp->fp	   = fp;
	argp->dxpl_id  = H5Pcopy (dxpl_id);
	argp->get_type = get_type;
	va_copy (argp->arguments, arguments);
	if (is_async) {
		if (req) {
			reqp = (H5VL_async_req_t *)malloc (sizeof (H5VL_async_req_t));
			CHECK_PTR (reqp)

			argp->ret = &(reqp->ret);

			*req = reqp;
		} else {
			argp->ret = NULL;
		}
	} else {
		argp->ret = &ret;
	}

	argp->task = TW_HANDLE_NULL;
	twerr	   = TW_Task_create (H5VL_async_file_get_handler, argp, TW_TASK_DEP_NULL, 0, &task);
	CHK_TWERR
	argp->task = task;
	if (reqp) { reqp->task = argp->task; }

	H5VL_asynci_mutex_lock (fp->lock);
	if (fp->init_task) {
		twerr = TW_Task_add_dep (task, fp->init_task);
		CHK_TWERR
	}
	H5VL_async_inc_ref (fp);
	twerr = TW_Task_commit (argp->task, H5VL_async_engine);
	CHK_TWERR
	H5VL_asynci_mutex_unlock (fp->lock);

	if (!is_async) {
		// Release the lock so worker thread can acquire
		H5TSmutex_release ();

		twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);
		CHK_TWERR

		err = H5VL_asynci_h5ts_mutex_lock ();
		CHECK_ERR

		twerr = TW_Task_free (task);
		CHK_TWERR

		err = ret;
		CHECK_ERR
	}

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
	herr_t err	 = 0;
	terr_t twerr = TW_SUCCESS;
	H5VL_async_file_specific_args *argp;
	H5VL_async_t *fp	   = (H5VL_async_t *)file;
	H5VL_async_req_t *reqp = NULL;
	hbool_t is_async;
	herr_t ret;
	TW_Task_handle_t task;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Specific\n");
#endif

	if ((fp->stat == H5VL_async_stat_err) || (fp->stat == H5VL_async_stat_close)) {
		RET_ERR ("Parent object in wrong status");
	}

	err = H5Pget_dxpl_async (dxpl_id, &is_async);
	CHECK_ERR

	argp = (H5VL_async_file_specific_args *)malloc (sizeof (H5VL_async_file_specific_args));
	CHECK_PTR (argp)

	argp->fp			= fp;
	argp->dxpl_id		= H5Pcopy (dxpl_id);
	argp->specific_type = specific_type;
	va_copy (argp->arguments, arguments);
	if (is_async) {
		if (req) {
			reqp = (H5VL_async_req_t *)malloc (sizeof (H5VL_async_req_t));
			CHECK_PTR (reqp)

			argp->ret = &(reqp->ret);

			*req = reqp;
		} else {
			argp->ret = NULL;
		}
	} else {
		argp->ret = &ret;
	}

	argp->task = TW_HANDLE_NULL;
	twerr = TW_Task_create (H5VL_async_file_specific_handler, argp, TW_TASK_DEP_NULL, 0, &task);
	CHK_TWERR
	argp->task = task;
	if (reqp) { reqp->task = argp->task; }

	H5VL_asynci_mutex_lock (fp->lock);
	if (fp->init_task) {
		twerr = TW_Task_add_dep (task, fp->init_task);
		CHK_TWERR
	}
	H5VL_async_inc_ref (fp);
	twerr = TW_Task_commit (argp->task, H5VL_async_engine);
	CHK_TWERR
	H5VL_asynci_mutex_unlock (fp->lock);

	if (!is_async) {
		// Release the lock so worker thread can acquire
		H5TSmutex_release ();

		twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);
		CHK_TWERR

		err = H5VL_asynci_h5ts_mutex_lock ();
		CHECK_ERR

		twerr = TW_Task_free (task);
		CHK_TWERR

		err = ret;
		CHECK_ERR
	}
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
	herr_t err	 = 0;
	terr_t twerr = TW_SUCCESS;
	H5VL_async_file_optional_args *argp;
	H5VL_async_t *fp	   = (H5VL_async_t *)file;
	H5VL_async_req_t *reqp = NULL;
	hbool_t is_async;
	herr_t ret;
	TW_Task_handle_t task;
#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL File Optional\n");
#endif

	if ((fp->stat == H5VL_async_stat_err) || (fp->stat == H5VL_async_stat_close)) {
		RET_ERR ("Parent object in wrong status");
	}

	err = H5Pget_dxpl_async (dxpl_id, &is_async);
	CHECK_ERR

	argp = (H5VL_async_file_optional_args *)malloc (sizeof (H5VL_async_file_optional_args));
	CHECK_PTR (argp)

	argp->fp	   = fp;
	argp->dxpl_id  = H5Pcopy (dxpl_id);
	argp->opt_type = opt_type;
	va_copy (argp->arguments, arguments);
	if (is_async) {
		if (req) {
			reqp = (H5VL_async_req_t *)malloc (sizeof (H5VL_async_req_t));
			CHECK_PTR (reqp)

			argp->ret = &(reqp->ret);

			*req = reqp;
		} else {
			argp->ret = NULL;
		}
	} else {
		argp->ret = &ret;
	}

	argp->task = TW_HANDLE_NULL;
	twerr =
		TW_Task_create (H5VL_async_file_optional_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	argp->task = task;
	if (reqp) { reqp->task = task; }

	H5VL_asynci_mutex_lock (fp->lock);
	if (fp->init_task) {
		twerr = TW_Task_add_dep (task, fp->init_task);
		CHK_TWERR
	}
	H5VL_async_inc_ref (fp);
	twerr = TW_Task_commit (task, H5VL_async_engine);
	CHK_TWERR
	H5VL_asynci_mutex_unlock (fp->lock);

	if (!is_async) {
		// Release the lock so worker thread can acquire
		H5TSmutex_release ();

		twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);
		CHK_TWERR

		err = H5VL_asynci_h5ts_mutex_lock ();
		CHECK_ERR

		twerr = TW_Task_free (task);
		CHK_TWERR

		err = ret;
		CHECK_ERR
	}

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
	herr_t err	 = 0;
	terr_t twerr = TW_SUCCESS;
	H5VL_async_file_close_args *argp;
	H5VL_async_t *fp	   = (H5VL_async_t *)file;
	H5VL_async_req_t *reqp = NULL;
	hbool_t is_async;
	herr_t ret;
	TW_Task_handle_t task;
#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL FILE Close\n");
#endif

	if ((fp->stat == H5VL_async_stat_err) || (fp->stat == H5VL_async_stat_close)) {
		RET_ERR ("Parent object in wrong status");
	}
	fp->stat == H5VL_async_stat_close;

	err = H5Pget_dxpl_async (dxpl_id, &is_async);
	CHECK_ERR

	argp = (H5VL_async_file_close_args *)malloc (sizeof (H5VL_async_file_close_args));
	CHECK_PTR (argp)

	argp->fp	  = fp;
	argp->dxpl_id = H5Pcopy (dxpl_id);
	if (is_async) {
		if (req) {
			reqp = (H5VL_async_req_t *)malloc (sizeof (H5VL_async_req_t));
			CHECK_PTR (reqp)

			argp->ret = &(reqp->ret);

			*req = reqp;
		} else {
			argp->ret = NULL;
		}
	} else {
		argp->ret = &ret;
	}

	argp->task = TW_HANDLE_NULL;
	twerr = TW_Task_create (H5VL_async_file_close_handler, argp, TW_TASK_DEP_NULL, fp->cnt, &task);
	CHK_TWERR
	argp->task = task;
	if (reqp) { reqp->task = argp->task; }

	H5VL_asynci_mutex_lock (fp->lock);
	if (fp->ref) {
		fp->close_task = task;
	} else {
		twerr = TW_Task_commit (task, H5VL_async_engine);
		CHK_TWERR
	}
	H5VL_asynci_mutex_unlock (fp->lock);

	if (!is_async) {
		// Release the lock so worker thread can acquire
		H5TSmutex_release ();

		twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);
		CHK_TWERR

		err = H5VL_asynci_h5ts_mutex_lock ();
		CHECK_ERR

		twerr = TW_Task_free (task);
		CHK_TWERR

		err = ret;
		CHECK_ERR
	}

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