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

#include <H5VLpublic.h>
#include <hdf5.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_dset.h"
#include "h5vl_async_dseti.h"
#include "h5vl_async_info.h"
#include "h5vl_async_public.h"
#include "h5vl_async_req.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_debug.h"
#include "h5vl_asynci_mutex.h"
#include "h5vl_asynci_vector.h"

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_dataset_create (void *obj,
								 const H5VL_loc_params_t *loc_params,
								 const char *name,
								 hid_t lcpl_id,
								 hid_t type_id,
								 hid_t space_id,
								 hid_t dcpl_id,
								 hid_t dapl_id,
								 hid_t dxpl_id,
								 void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_dataset_create_args *argp = NULL;
	size_t name_len;
	H5VL_async_t *op		 = NULL;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL dataset Create\n");
#endif

	op = H5VL_async_new_obj ();
	CHECK_PTR (op)

	name_len = strlen (name);
	argp	 = (H5VL_async_dataset_create_args *)malloc (sizeof (H5VL_async_dataset_create_args) +
													 sizeof (H5VL_loc_params_t) + name_len + 1);
	CHECK_PTR (argp)
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->dapl_id	 = H5Pcopy (dapl_id);
	argp->dcpl_id	 = H5Pcopy (dcpl_id);
	argp->lcpl_id	 = H5Pcopy (lcpl_id);
	argp->space_id	 = H5Scopy (space_id);
	argp->type_id	 = H5Tcopy (type_id);
	argp->target_obj = target_obj;
	argp->op		 = op;
	argp->loc_params =
		(H5VL_loc_params_t *)((char *)argp + sizeof (H5VL_async_dataset_create_args));
	memcpy (argp->loc_params, loc_params, sizeof (H5VL_loc_params_t));
	argp->name = (char *)argp->loc_params + sizeof (H5VL_loc_params_t);
	strncpy (argp->name, name, name_len + 1);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_dataset_create_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0,
							&task);
	CHK_TWERR
	op->prev_task = task;

	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->dapl_id);
			H5Pclose (argp->dcpl_id);
			H5Pclose (argp->lcpl_id);
			H5Sclose (argp->space_id);
			H5Tclose (argp->type_id);
			free (argp);
		}

		free (reqp);

		free (op);
		op = NULL;
	}

	return (void *)op;
} /* end H5VL_async_dataset_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *H5VL_async_dataset_open (void *obj,
							   const H5VL_loc_params_t *loc_params,
							   const char *name,
							   hid_t dapl_id,
							   hid_t dxpl_id,
							   void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_dataset_open_args *argp = NULL;
	size_t name_len;
	H5VL_async_t *op		 = NULL;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL dataset Create\n");
#endif

	op = H5VL_async_new_obj ();
	CHECK_PTR (op)

	name_len = strlen (name);
	argp	 = (H5VL_async_dataset_open_args *)malloc (sizeof (H5VL_async_dataset_open_args) +
												   sizeof (H5VL_loc_params_t) + name_len + 1);
	CHECK_PTR (argp)
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->dapl_id	 = H5Pcopy (dapl_id);
	argp->op		 = op;
	argp->target_obj = target_obj;
	argp->loc_params = (H5VL_loc_params_t *)((char *)argp + sizeof (H5VL_async_dataset_open_args));
	memcpy (argp->loc_params, loc_params, sizeof (H5VL_loc_params_t));
	argp->name = (char *)argp->loc_params + sizeof (H5VL_loc_params_t);
	strncpy (argp->name, name, name_len + 1);

	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_dataset_open_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR
	op->prev_task = task;

	H5VL_ASYNC_CB_TASK_COMMIT
	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (task != TW_HANDLE_NULL) { TW_Task_free (task); }

		if (argp) {
			H5Pclose (argp->dxpl_id);
			H5Pclose (argp->dapl_id);
			free (argp);
		}

		free (reqp);

		free (op);
		op = NULL;
	}

	return (void *)op;
} /* end H5VL_async_dataset_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_read
 *
 * Purpose:     Reads data elements from a dataset into a buffer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_dataset_read (void *dset,
								hid_t mem_type_id,
								hid_t mem_space_id,
								hid_t file_space_id,
								hid_t dxpl_id,
								void *buf,
								void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_dataset_read_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)dset;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL dataset Get\n");
#endif

	argp = (H5VL_async_dataset_read_args *)malloc (sizeof (H5VL_async_dataset_read_args));
	CHECK_PTR (argp)
	argp->target_obj  = target_obj;
	argp->mem_type_id = H5Tcopy (mem_type_id);
	if (mem_space_id == H5S_ALL)
		argp->mem_space_id = H5S_ALL;
	else
		argp->mem_space_id = H5Scopy (mem_space_id);
	if (file_space_id == H5S_ALL)
		argp->file_space_id = H5S_ALL;
	else
		argp->file_space_id = H5Scopy (file_space_id);
	argp->dxpl_id = H5Pcopy (dxpl_id);
	argp->buf	  = buf;
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_dataset_read_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Sclose (argp->file_space_id);
			H5Sclose (argp->mem_space_id);
			H5Tclose (argp->mem_type_id);
			H5Pclose (argp->dxpl_id);
			free (argp);
		}

		free (reqp);
	}
	return err;
} /* end H5VL_async_dataset_read() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_write
 *
 * Purpose:     Writes data elements from a buffer into a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_dataset_write (void *dset,
								 hid_t mem_type_id,
								 hid_t mem_space_id,
								 hid_t file_space_id,
								 hid_t dxpl_id,
								 const void *buf,
								 void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_dataset_write_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)dset;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL dataset Get\n");
#endif

	argp = (H5VL_async_dataset_write_args *)malloc (sizeof (H5VL_async_dataset_write_args));
	CHECK_PTR (argp)
	argp->target_obj  = target_obj;
	argp->mem_type_id = H5Tcopy (mem_type_id);
	if (mem_space_id == H5S_ALL)
		argp->mem_space_id = H5S_ALL;
	else
		argp->mem_space_id = H5Scopy (mem_space_id);
	if (file_space_id == H5S_ALL)
		argp->file_space_id = H5S_ALL;
	else
		argp->file_space_id = H5Scopy (file_space_id);
	argp->dxpl_id = H5Pcopy (dxpl_id);
	argp->buf	  = buf;

	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_dataset_write_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
	CHK_TWERR

	H5VL_ASYNC_CB_TASK_COMMIT

	H5VL_ASYNC_CB_TASK_WAIT

err_out:;
	if (err) {
		if (argp) {
			if (argp->task != TW_HANDLE_NULL) { TW_Task_free (argp->task); }
			H5Pclose (argp->dxpl_id);
			H5Sclose (argp->file_space_id);
			H5Sclose (argp->mem_space_id);
			H5Tclose (argp->mem_type_id);
			free (argp);
		}

		free (reqp);
	}
	return err;
} /* end H5VL_async_dataset_write() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_get
 *
 * Purpose:     Get info about a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_dataset_get (
	void *obj, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_dataset_get_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL dataset Get\n");
#endif

	argp = (H5VL_async_dataset_get_args *)malloc (sizeof (H5VL_async_dataset_get_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->get_type	 = get_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_dataset_get_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_dataset_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_specific
 *
 * Purpose:     Specific operation on dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_dataset_specific (void *obj,
									H5VL_dataset_specific_t specific_type,
									hid_t dxpl_id,
									void **req,
									va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_dataset_specific_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL dataset Specific\n");
#endif

	if ((target_obj->stat == H5VL_async_stat_err) || (target_obj->stat == H5VL_async_stat_close)) {
		RET_ERR ("target_obj object in wrong status");
	}

	if (specific_type == H5VL_DATASET_WAIT) { return (H5VL_asynci_obj_wait (obj)); }

	argp = (H5VL_async_dataset_specific_args *)malloc (sizeof (H5VL_async_dataset_specific_args));
	CHECK_PTR (argp)

	argp->target_obj	= target_obj;
	argp->dxpl_id		= H5Pcopy (dxpl_id);
	argp->specific_type = specific_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_dataset_specific_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0,
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
} /* end H5VL_async_dataset_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_optional
 *
 * Purpose:     Perform a connector-specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_dataset_optional (
	void *obj, H5VL_dataset_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_dataset_optional_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL dataset Optional\n");
#endif

	if ((target_obj->stat == H5VL_async_stat_err) || (target_obj->stat == H5VL_async_stat_close)) {
		RET_ERR ("target_obj object in wrong status");
	}

	argp = (H5VL_async_dataset_optional_args *)malloc (sizeof (H5VL_async_dataset_optional_args));
	CHECK_PTR (argp)

	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	argp->opt_type	 = opt_type;
	va_copy (argp->arguments, arguments);
	H5VL_ASYNC_CB_TASK_INIT

	twerr = TW_Task_create (H5VL_async_dataset_optional_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0,
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
} /* end H5VL_async_dataset_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_close
 *
 * Purpose:     Closes a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1, dataset not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_dataset_close (void *grp, hid_t dxpl_id, void **req) {
	H5VL_ASYNC_CB_VARS
	H5VL_async_dataset_close_args *argp;
	H5VL_async_t *target_obj = (H5VL_async_t *)grp;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL dataset Close\n");
#endif

	/* Mark as closed so no operation can be performed */
	H5VL_asynci_mutex_lock (target_obj->lock);
	target_obj->stat == H5VL_async_stat_close;
	H5VL_asynci_mutex_unlock (target_obj->lock);

	argp = (H5VL_async_dataset_close_args *)malloc (sizeof (H5VL_async_dataset_close_args));
	CHECK_PTR (argp)
	argp->target_obj = target_obj;
	argp->dxpl_id	 = H5Pcopy (dxpl_id);
	H5VL_ASYNC_CB_TASK_INIT

	twerr =
		TW_Task_create (H5VL_async_dataset_close_handler, argp, TW_TASK_DEP_ALL_COMPLETE, 0, &task);
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
} /* end H5VL_async_dataset_close() */
