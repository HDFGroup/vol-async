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
#include "h5vl_async_public.h"
#include "h5vl_async_req.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_debug.h"
#include "h5vl_asynci_mutex.h"
#include "h5vl_asynci_vector.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a attr object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_attr_create (void *obj,
							  const H5VL_loc_params_t *loc_params,
							  const char *name,
							  hid_t type_id,
							  hid_t space_id,
							  hid_t acpl_id,
							  hid_t aapl_id,
							  hid_t dxpl_id,
							  void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_attr_create_args *argp = NULL;
	size_t name_len;
	H5VL_async_t *op		 = NULL;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;//parent obj

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL attr Create\n");
#endif

	op = H5VL_async_new_obj ();
	CHECK_PTR (op)

	name_len = strlen (name);
	argp	 = (H5VL_async_attr_create_args *)malloc (sizeof (H5VL_async_attr_create_args) +
												  sizeof (H5VL_loc_params_t) + name_len + 1);
	CHECK_PTR (argp)
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->aapl_id	 = H5Pcopy (aapl_id);
	argp->acpl_id	 = H5Pcopy (acpl_id);
	argp->space_id	 = H5Scopy (space_id);
	argp->type_id	 = H5Tcopy (type_id);
	argp->target_obj = target_obj;
	argp->op		 = op;
	argp->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(argp->loc_params, loc_params);

	argp->name = (char *)argp->loc_params + sizeof (H5VL_loc_params_t);
	strncpy (argp->name, name, name_len + 1);
	H5VL_ASYNC_CB_TASK_INIT
DEBUG_PRINT
	twerr =
		TW_Task_create (H5VL_async_attr_create_handler,
		        argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	op->prev_task = task;
DEBUG_PRINT
	H5VL_ASYNC_CB_TASK_COMMIT
DEBUG_PRINT
	H5VL_ASYNC_CB_TASK_WAIT
DEBUG_PRINT
err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->aapl_id);
			H5Pclose (argp->acpl_id);
			H5Sclose (argp->space_id);
			H5Tclose (argp->type_id);
			free (argp);
		}

		free (reqp);

		free (op);
		op = NULL;
	}

	return (void *)op;
} /* end H5VL_async_attr_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a attr object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_attr_open (void *obj,
							const H5VL_loc_params_t *loc_params,
							const char *name,
							hid_t aapl_id,
							hid_t dxpl_id,
							void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_attr_open_args *argp = NULL;
	size_t name_len;
	H5VL_async_t *op		 = NULL;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL attr Create\n");
#endif

	op = H5VL_async_new_obj ();
	CHECK_PTR (op)

	name_len = strlen (name);
	argp	 = (H5VL_async_attr_open_args *)malloc (sizeof (H5VL_async_attr_open_args) +
												sizeof (H5VL_loc_params_t) + name_len + 1);
	CHECK_PTR (argp)
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->aapl_id	 = H5Pcopy (aapl_id);
	argp->op		 = op;
	argp->target_obj = target_obj;
    argp->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(argp->loc_params, loc_params);
	argp->name = (char *)argp->loc_params + sizeof (H5VL_loc_params_t);
	strncpy (argp->name, name, name_len + 1);

	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_attr_open_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	op->prev_task = task;

	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->aapl_id);
			free (argp);
		}

		free (reqp);

		free (op);
		op = NULL;
	}

	return (void *)op;
} /* end H5VL_async_attr_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_read
 *
 * Purpose:     Reads data elements from a attr into a buffer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_read (void *dset, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_attr_read_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)dset;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL attr Get\n");
#endif

	argp = (H5VL_async_attr_read_args *)malloc (sizeof (H5VL_async_attr_read_args));
	CHECK_PTR (argp)
	argp->target_obj  = target_obj;
	argp->mem_type_id = H5Tcopy (mem_type_id);
	argp->dxpl_id	  = H5Pcopy (dxpl_id);
	argp->buf		  = buf;
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_attr_read_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Tclose (argp->mem_type_id);
			H5Pclose (argp->dxpl_id);
			free (argp);
		}

		free (reqp);
	}
	return err;
} /* end H5VL_async_attr_read() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_write
 *
 * Purpose:     Writes data elements from a buffer into a attr.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_write (
	void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_attr_write_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)attr;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL attr Get\n");
#endif

	argp = (H5VL_async_attr_write_args *)malloc (sizeof (H5VL_async_attr_write_args));
	CHECK_PTR (argp)
	argp->target_obj  = target_obj;
	argp->mem_type_id = H5Tcopy (mem_type_id);
	argp->dxpl_id	  = H5Pcopy (dxpl_id);
	argp->buf		  = buf;
	H5VL_ASYNC_CB_TASK_INIT
DEBUG_PRINT
	twerr =
		TW_Task_create (H5VL_async_attr_write_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
DEBUG_PRINT
	H5VL_ASYNC_CB_TASK_COMMIT
DEBUG_PRINT
	H5VL_ASYNC_CB_TASK_WAIT
DEBUG_PRINT
err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			H5Tclose (argp->mem_type_id);
			free (argp);
		}

		free (reqp);
	}
	return err;
} /* end H5VL_async_attr_write() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_get
 *
 * Purpose:     Get info about a attr
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_get (
	void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_attr_get_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL attr Get\n");
#endif

	argp = (H5VL_async_attr_get_args *)malloc (sizeof (H5VL_async_attr_get_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->get_type	 = get_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_attr_get_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_attr_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_specific
 *
 * Purpose:     Specific operation on attr
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_specific (void *obj,
								 const H5VL_loc_params_t *loc_params,
								 H5VL_attr_specific_t specific_type,
								 hid_t dxpl_id,
								 void **req,
								 va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_attr_specific_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL attr Specific\n");
#endif

	if ((target_obj->stat == H5VL_async_stat_err) || (target_obj->stat == H5VL_async_stat_close)) {
		RET_ERR ("target_obj object in wrong status");
	}

	argp = (H5VL_async_attr_specific_args *)malloc (sizeof (H5VL_async_attr_specific_args) +
													sizeof (H5VL_loc_params_t));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
    argp->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(argp->loc_params, loc_params);
	argp->specific_type = specific_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_attr_specific_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_attr_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_optional
 *
 * Purpose:     Perform a connector-specific operation on a attr
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_optional (
	void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_attr_optional_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL attr Optional\n");
#endif

	if ((target_obj->stat == H5VL_async_stat_err) || (target_obj->stat == H5VL_async_stat_close)) {
		RET_ERR ("target_obj object in wrong status");
	}

	argp = (H5VL_async_attr_optional_args *)malloc (sizeof (H5VL_async_attr_optional_args));
	CHECK_PTR (argp)

	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->opt_type	 = opt_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_attr_optional_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_attr_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_close
 *
 * Purpose:     Closes a attr.
 *
 * Return:      Success:    0
 *              Failure:    -1, attr not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_attr_close (void *grp, hid_t dxpl_id, void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_attr_close_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)grp;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL attr Close\n");
#endif

	/* Mark as closed so no operation can be performed */
	H5VL_asynci_mutex_lock (target_obj->lock);
	target_obj->stat == H5VL_async_stat_close;
	H5VL_asynci_mutex_unlock (target_obj->lock);

	argp = (H5VL_async_attr_close_args *)malloc (sizeof (H5VL_async_attr_close_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_attr_close_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	argp->task = task;

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
} /* end H5VL_async_attr_close() */
