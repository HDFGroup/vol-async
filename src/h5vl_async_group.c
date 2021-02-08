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
#include "h5vl_async_public.h"
#include "h5vl_async_req.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_debug.h"
#include "h5vl_asynci_mutex.h"
#include "h5vl_asynci_vector.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_group_create (void *obj,
							   const H5VL_loc_params_t *loc_params,
							   const char *name,
							   hid_t lcpl_id,
							   hid_t gcpl_id,
							   hid_t gapl_id,
							   hid_t dxpl_id,
							   void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_group_create_args *argp = NULL;
	size_t name_len;
	H5VL_async_t *op		 = NULL;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL group Create\n");
#endif

	op = H5VL_async_new_obj (NULL, target_obj->under_vol_id);
	CHECK_PTR (op)

	name_len = strlen (name);
	argp	 = (H5VL_async_group_create_args *)malloc (sizeof (H5VL_async_group_create_args) +
												   sizeof (H5VL_loc_params_t) + name_len + 1);
	CHECK_PTR (argp)
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->gapl_id	 = H5Pcopy (gapl_id);
	argp->gcpl_id	 = H5Pcopy (gcpl_id);
	argp->lcpl_id	 = H5Pcopy (lcpl_id);
	argp->target_obj = target_obj;
	argp->op		 = op;

	argp->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(argp->loc_params, loc_params);

	argp->name = (char *)argp->loc_params + sizeof (H5VL_loc_params_t);
	argp->req = NULL;
	strncpy (argp->name, name, name_len + 1);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_group_create_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	op->prev_task = task;

	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->gapl_id);
			H5Pclose (argp->gcpl_id);
			H5Pclose (argp->lcpl_id);
			free (argp);
		}

		free (reqp);

		free (op);
		op = NULL;
	}

	return (void *)op;
} /* end H5VL_async_group_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_group_open (void *obj,
							 const H5VL_loc_params_t *loc_params,
							 const char *name,
							 hid_t gapl_id,
							 hid_t dxpl_id,
							 void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_group_open_args *argp = NULL;
	size_t name_len;
	H5VL_async_t *op		 = NULL;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL group Create\n");
#endif

    op = H5VL_async_new_obj (NULL, target_obj->under_vol_id);
	CHECK_PTR (op)

	name_len = strlen (name);
	argp	 = (H5VL_async_group_open_args *)malloc (sizeof (H5VL_async_group_open_args) +
												 sizeof (H5VL_loc_params_t) + name_len + 1);
	CHECK_PTR (argp)
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->gapl_id	 = H5Pcopy (gapl_id);
	argp->op		 = op;
	argp->target_obj = target_obj;
	argp->loc_params = (H5VL_loc_params_t *)((char *)argp + sizeof (H5VL_async_group_open_args));
	memcpy (argp->loc_params, loc_params, sizeof (H5VL_loc_params_t));
	argp->name = (char *)argp->loc_params + sizeof (H5VL_loc_params_t);
	strncpy (argp->name, name, name_len + 1);

	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_group_open_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	op->prev_task = task;

	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->gapl_id);
			free (argp);
		}

		free (reqp);

		free (op);
		op = NULL;
	}

	return (void *)op;
} /* end H5VL_async_group_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_get
 *
 * Purpose:     Get info about a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_group_get (
	void *obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_group_get_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL group Get\n");
#endif

	argp = (H5VL_async_group_get_args *)malloc (sizeof (H5VL_async_group_get_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->get_type	 = get_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_group_get_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_group_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_specific
 *
 * Purpose:     Specific operation on group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_group_specific (
	void *obj, H5VL_group_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_group_specific_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL group Specific\n");
#endif

	if ((target_obj->stat == H5VL_async_stat_err) || (target_obj->stat == H5VL_async_stat_close)) {
		RET_ERR ("target_obj object in wrong status");
	}

	argp = (H5VL_async_group_specific_args *)malloc (sizeof (H5VL_async_group_specific_args));
	CHECK_PTR (argp)

	argp->target_obj	= target_obj;
	argp->dxpl_id		= H5Pcopy (dxpl_id);
	argp->specific_type = specific_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_group_specific_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0,
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
} /* end H5VL_async_group_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_optional
 *
 * Purpose:     Perform a connector-specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_group_optional (
	void *obj, H5VL_group_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_group_optional_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL group Optional\n");
#endif

	if ((target_obj->stat == H5VL_async_stat_err) || (target_obj->stat == H5VL_async_stat_close)) {
		RET_ERR ("target_obj object in wrong status");
	}

	argp = (H5VL_async_group_optional_args *)malloc (sizeof (H5VL_async_group_optional_args));
	CHECK_PTR (argp)

	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->opt_type	 = opt_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_group_optional_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0,
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
} /* end H5VL_async_group_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_close
 *
 * Purpose:     Closes a group.
 *
 * Return:      Success:    0
 *              Failure:    -1, group not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_group_close (void *grp, hid_t dxpl_id, void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_group_close_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)grp;
    H5VL_async_t * op = H5VL_async_new_obj (NULL, target_obj->under_vol_id);
#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL group Close\n");
#endif

	/* Mark as closed so no operation can be performed */
	H5VL_asynci_mutex_lock (target_obj->lock);
	target_obj->stat == H5VL_async_stat_close;
	H5VL_asynci_mutex_unlock (target_obj->lock);

	argp = (H5VL_async_group_close_args *)malloc (sizeof (H5VL_async_group_close_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
    argp->op = op;
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_group_close_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_ASYNC_CB_TASK_COMMIT
	H5VL_ASYNC_CB_TASK_WAIT
err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			free (argp);
			free (op);
		}
	}

	return err;
} /* end H5VL_async_group_close() */
