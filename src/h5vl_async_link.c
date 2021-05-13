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
#include "h5vl_async_link.h"
#include "h5vl_async_linki.h"
#include "h5vl_async_public.h"
#include "h5vl_async_req.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_debug.h"
#include "h5vl_asynci_mutex.h"
#include "h5vl_asynci_vector.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_create
 *
 * Purpose:     Creates a hard / soft / UD / external link.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_create (H5VL_link_create_type_t create_type,
							   void *obj,
							   const H5VL_loc_params_t *loc_params,
							   hid_t lcpl_id,
							   hid_t lapl_id,
							   hid_t dxpl_id,
							   void **req,
							   va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_link_create_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;
	H5VL_async_t *op		 = NULL;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL link Create\n");
#endif

	argp = (H5VL_async_link_create_args *)malloc (sizeof (H5VL_async_link_create_args) +
												  sizeof (H5VL_loc_params_t));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
    argp->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(argp->loc_params, loc_params);
	argp->dxpl_id	  = H5Pcopy (dxpl_id);
	argp->lcpl_id	  = H5Pcopy (lcpl_id);
	argp->lapl_id	  = H5Pcopy (lapl_id);
	argp->create_type = create_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_link_create_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->lapl_id);
			H5Pclose (argp->lcpl_id);
			va_end (argp->arguments);
			free (argp);
		}

		free (reqp);
	}
	return err;
} /* end H5VL_async_link_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_copy
 *
 * Purpose:     Renames an object within an HDF5 container and copies it to a
 *new group.  The original name SRC is unlinked from the group graph and then
 *inserted with the new name DST (which can specify a new path for the object)
 *as an atomic operation. The names are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_copy (void *src_obj,
							 const H5VL_loc_params_t *loc_params1,
							 void *dst_obj,
							 const H5VL_loc_params_t *loc_params2,
							 hid_t lcpl_id,
							 hid_t lapl_id,
							 hid_t dxpl_id,
							 void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_link_copy_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)src_obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL link Copy\n");
#endif

	argp = (H5VL_async_link_copy_args *)malloc (sizeof (H5VL_async_link_copy_args) +
												sizeof (H5VL_loc_params_t) * 2);
	CHECK_PTR (argp)
	argp->target_obj  = target_obj;

    argp->loc_params1 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params1));
    dup_loc_param(argp->loc_params1, loc_params1);

    argp->loc_params2 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params2));
    dup_loc_param(argp->loc_params2, loc_params2);

	argp->dxpl_id = H5Pcopy (dxpl_id);
	argp->lcpl_id = H5Pcopy (lcpl_id);
	argp->lapl_id = H5Pcopy (lapl_id);
	argp->dst_obj = dst_obj;
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_link_move_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_asynci_mutex_lock (argp->dst_obj->lock);
	/* Add dependency to init task */
	if (argp->dst_obj->prev_task == TW_HANDLE_NULL) {
		twerr = TW_Task_add_dep (task, argp->dst_obj->prev_task);
		CHK_TWERR
	}

	/* Release object lock */
	H5VL_asynci_mutex_unlock (argp->dst_obj->lock);
	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->lapl_id);
			H5Pclose (argp->lcpl_id);
			free (argp);
		}

		free (reqp);
	}
	return err;
} /* end H5VL_async_link_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_move
 *
 * Purpose:     Moves a link within an HDF5 file to a new group.  The original
 *              name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_move (void *src_obj,
							 const H5VL_loc_params_t *loc_params1,
							 void *dst_obj,
							 const H5VL_loc_params_t *loc_params2,
							 hid_t lcpl_id,
							 hid_t lapl_id,
							 hid_t dxpl_id,
							 void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_link_move_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)src_obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL link Move\n");
#endif

	argp = (H5VL_async_link_move_args *)malloc (sizeof (H5VL_async_link_move_args) +
												sizeof (H5VL_loc_params_t) * 2);
	CHECK_PTR (argp)
	argp->target_obj  = target_obj;

	argp->loc_params1 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params1));
    dup_loc_param(argp->loc_params1, loc_params1);

    argp->loc_params2 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params2));
    dup_loc_param(argp->loc_params2, loc_params2);

	argp->dxpl_id = H5Pcopy (dxpl_id);
	argp->lcpl_id = H5Pcopy (lcpl_id);
	argp->lapl_id = H5Pcopy (lapl_id);
	argp->dst_obj = dst_obj;
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_link_move_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_asynci_mutex_lock (argp->dst_obj->lock);
	/* Add dependency to init task */
	if (argp->dst_obj->prev_task == TW_HANDLE_NULL) {
		twerr = TW_Task_add_dep (task, argp->dst_obj->prev_task);
		CHK_TWERR
	}
	/* Release object lock */
	H5VL_asynci_mutex_unlock (argp->dst_obj->lock);
	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->lapl_id);
			H5Pclose (argp->lcpl_id);
			free (argp);
		}

		free (reqp);
	}
	return err;
} /* end H5VL_async_link_move() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_get
 *
 * Purpose:     Get info about a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_get (void *obj,
							const H5VL_loc_params_t *loc_params,
							H5VL_link_get_t get_type,
							hid_t dxpl_id,
							void **req,
							va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_link_get_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL link Get\n");
#endif

	argp = (H5VL_async_link_get_args *)malloc (sizeof (H5VL_async_link_get_args) +
											   sizeof (H5VL_loc_params_t));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;

    argp->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(argp->loc_params, loc_params);

	argp->dxpl_id  = H5Pcopy (dxpl_id);
	argp->get_type = get_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_link_get_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_link_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_specific
 *
 * Purpose:     Specific operation on link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_specific (void *obj,
								 const H5VL_loc_params_t *loc_params,
								 H5VL_link_specific_t specific_type,
								 hid_t dxpl_id,
								 void **req,
								 va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_link_specific_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL link Specific\n");
#endif

	argp = (H5VL_async_link_specific_args *)malloc (sizeof (H5VL_async_link_specific_args) +
													sizeof (H5VL_link_get_t));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;

//	argp->loc_params = (H5VL_loc_params_t *)((char *)argp + sizeof (H5VL_async_link_specific_args));
//	memcpy (argp->loc_params, loc_params, sizeof (H5VL_loc_params_t));

    argp->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(argp->loc_params, loc_params);

	argp->dxpl_id		= H5Pcopy (dxpl_id);
	argp->specific_type = specific_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_link_specific_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_link_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_optional
 *
 * Purpose:     Perform a connector-specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_link_optional (
	void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_link_optional_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL link Optional\n");
#endif

	argp = (H5VL_async_link_optional_args *)malloc (sizeof (H5VL_async_link_optional_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->opt_type	 = opt_type;

	argp->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(argp->loc_params, loc_params);


	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_link_optional_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_link_optional() */
