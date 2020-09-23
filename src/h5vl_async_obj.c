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
#include "h5vl_async_public.h"
#include "h5vl_async_req.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_mutex.h"
#include "h5vl_asynci_vector.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a object object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_object_open (void *obj,
							  const H5VL_loc_params_t *loc_params,
							  H5I_type_t *opened_type,
							  hid_t dxpl_id,
							  void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_object_open_args *argp = NULL;
	H5VL_async_t *op				  = NULL;
	H5VL_async_t *pp				  = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL object Create\n");
#endif

	op = H5VL_async_new_obj ();
	CHECK_PTR (op)

	argp = (H5VL_async_object_open_args *)malloc (sizeof (H5VL_async_object_open_args) +
												  sizeof (H5VL_loc_params_t));
	CHECK_PTR (argp)
	argp->dxpl_id	  = H5Pcopy (dxpl_id);
	argp->opened_type = opened_type;
	argp->op		  = op;
	argp->pp		  = pp;
	argp->loc_params  = (H5VL_loc_params_t *)((char *)argp + sizeof (H5VL_async_object_open_args));
	memcpy (argp->loc_params, loc_params, sizeof (H5VL_loc_params_t));
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_object_open_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	op->init_task = task;

	H5VL_async_inc_ref (argp->op);
	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			free (argp);
		}

		free (reqp);

		free (op);
		op = NULL;
	}

	return (void *)pp;
} /* end H5VL_async_object_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_copy
 *
 * Purpose:     Copies an object inside a container.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_object_copy (void *src_obj,
							   const H5VL_loc_params_t *src_loc_params,
							   const char *src_name,
							   void *dst_obj,
							   const H5VL_loc_params_t *dst_loc_params,
							   const char *dst_name,
							   hid_t ocpypl_id,
							   hid_t lcpl_id,
							   hid_t dxpl_id,
							   void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_object_copy_args *argp = NULL;
	size_t src_name_len, dst_name_len;
	H5VL_async_t *pp = (H5VL_async_t *)src_obj;
	H5VL_async_t *cp = (H5VL_async_t *)dst_obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL object Copy\n");
#endif

	src_name_len = strlen (src_name);
	dst_name_len = strlen (dst_name);
	argp		 = (H5VL_async_object_copy_args *)malloc (sizeof (H5VL_async_object_copy_args) +
												  sizeof (H5VL_loc_params_t) * 2 + src_name_len +
												  dst_name_len + 2);
	CHECK_PTR (argp)
	argp->dxpl_id	= H5Pcopy (dxpl_id);
	argp->ocpypl_id = H5Pcopy (ocpypl_id);
	argp->lcpl_id	= H5Pcopy (lcpl_id);
	argp->pp		= pp;
	argp->dst_obj	= (H5VL_async_t *)cp;
	argp->src_loc_params =
		(H5VL_loc_params_t *)((char *)argp + sizeof (H5VL_async_object_copy_args));
	memcpy (argp->src_loc_params, src_loc_params, sizeof (H5VL_loc_params_t));
	argp->src_name = (char *)argp->src_loc_params + sizeof (H5VL_loc_params_t);
	strncpy (argp->src_name, src_name, src_name_len + 1);
	argp->dst_loc_params = (H5VL_loc_params_t *)((char *)argp->src_name + src_name_len + 1);
	memcpy (argp->dst_loc_params, dst_loc_params, sizeof (H5VL_loc_params_t));
	argp->dst_name = (char *)argp->dst_loc_params + sizeof (H5VL_loc_params_t);
	strncpy (argp->dst_name, dst_name, dst_name_len + 1);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_object_copy_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_asynci_mutex_lock (cp->lock);
	H5VL_async_inc_ref (cp);
	if (cp->init_task) {
		twerr = TW_Task_add_dep (task, cp->init_task);
		CHK_TWERR
	}
	H5VL_asynci_mutex_unlock (cp->lock);
	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->ocpypl_id);
			H5Pclose (argp->lcpl_id);
			free (argp);
		}

		free (reqp);
	}

	return err;
} /* end H5VL_async_object_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_get
 *
 * Purpose:     Get info about a object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_object_get (void *obj,
							  const H5VL_loc_params_t *loc_params,
							  H5VL_object_get_t get_type,
							  hid_t dxpl_id,
							  void **req,
							  va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_object_get_args *argp;
	H5VL_async_t *pp = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL object Get\n");
#endif

	argp = (H5VL_async_object_get_args *)malloc (sizeof (H5VL_async_object_get_args) +
												 sizeof (H5VL_loc_params_t));
	CHECK_PTR (argp)
	argp->pp		 = pp;
	argp->loc_params = (H5VL_loc_params_t *)((char *)argp + sizeof (H5VL_async_object_get_args));
	memcpy (argp->loc_params, loc_params, sizeof (H5VL_loc_params_t));
	argp->dxpl_id  = H5Pcopy (dxpl_id);
	argp->get_type = get_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_object_get_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_object_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_specific
 *
 * Purpose:     Specific operation on object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_object_specific (void *obj,
								   const H5VL_loc_params_t *loc_params,
								   H5VL_object_specific_t specific_type,
								   hid_t dxpl_id,
								   void **req,
								   va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_object_specific_args *argp;
	H5VL_async_t *pp = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL object Specific\n");
#endif

	argp = (H5VL_async_object_specific_args *)malloc (sizeof (H5VL_async_object_specific_args) +
													  sizeof (H5VL_loc_params_t));
	CHECK_PTR (argp)
	argp->pp = pp;
	argp->loc_params =
		(H5VL_loc_params_t *)((char *)argp + sizeof (H5VL_async_object_specific_args));
	memcpy (argp->loc_params, loc_params, sizeof (H5VL_loc_params_t));
	argp->dxpl_id		= H5Pcopy (dxpl_id);
	argp->specific_type = specific_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_object_specific_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0,
							&task);
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
} /* end H5VL_async_object_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_optional
 *
 * Purpose:     Perform a connector-specific operation on a object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_object_optional (
	void *obj, H5VL_object_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_object_optional_args *argp;
	H5VL_async_t *pp = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL object Optional\n");
#endif

	argp = (H5VL_async_object_optional_args *)malloc (sizeof (H5VL_async_object_optional_args));
	CHECK_PTR (argp)
	argp->pp	   = pp;
	argp->dxpl_id  = H5Pcopy (dxpl_id);
	argp->opt_type = opt_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_object_optional_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0,
							&task);
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
} /* end H5VL_async_object_optional() */
