/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Debug functions */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

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

#ifdef ASYNCVOL_DEBUG
herr_t H5VL_asynci_handler_begin (H5VL_asynci_debug_args *argp, hbool_t *stat_restored) {
	herr_t err = 0;

	/* Acquire global lock */
	err = H5VL_asynci_h5ts_mutex_lock ();
	CHECK_ERR_EX ("H5VL_asynci_h5ts_mutex_lock failed")
	/* Apply HDF5 state copied in the VOL call so the handler is recognized as VOL functions
	 */
    err = H5VLstart_lib_state ();
    CHECK_ERR_EX ("H5VLstart_lib_state failed")                                            
	err = H5VLrestore_lib_state (argp->stat);
	CHECK_ERR_EX ("H5VLrestore_lib_state failed")
	*stat_restored = true;

err_out:;
	return err;
}

herr_t H5VL_asynci_handler_end (H5VL_asynci_debug_args *argp, hbool_t stat_restored) {
	herr_t err = 0;

	/* Restore HDF5 status */
	if (stat_restored) { H5VLfinish_lib_state (); }
	err = H5VLfree_lib_state (argp->stat);
	if (err) {
		printf ("Error at line %d in %sn", __LINE__, __FILE__);
		H5Eprint2 (H5E_DEFAULT, stdout);
		DEBUG_ABORT
	}

	/* Clear task flag */
	if (argp->target_obj) {
		/* Acquire object lock */
		H5VL_asynci_mutex_lock (argp->target_obj->lock);
		if (argp->target_obj->prev_task == argp->task) {
			argp->target_obj->prev_task = TW_HANDLE_NULL;
		}
		/* Release object lock */
		H5VL_asynci_mutex_unlock (argp->target_obj->lock);
	}

err_out:;
	return err;
}

herr_t H5VL_asynci_handler_free (H5VL_asynci_debug_args *argp) {
	herr_t err = 0;

	/* Release global lock */
	err = H5TSmutex_release (&LOCK_COUNT_GLOBAL);
	CHECK_ERR_EX ("H5TSmutex_release failed")

	/* Record return val in request handle */
	//*(argp->ret_arg) = err;

	/* Free arguments */
//	    free (argp);

err_out:;
	return err;
}

herr_t H5VL_asynci_cb_task_init (
	hid_t dxpl_id, H5VL_asynci_debug_args *argp, H5VL_async_req_t *reqp) {

	herr_t err = 0;

	/* Check if the operations is async */
    argp->req = reqp;
	/* Retrieve current library state */
	err = H5VLretrieve_lib_state (&argp->stat);
	CHECK_ERR_EX ("H5VLretrieve_lib_state failed")

err_out:;
	return err;
}

herr_t H5VL_asynci_cb_task_commit (H5VL_asynci_debug_args *argp,
								   H5VL_async_req_t *reqp,
								   H5VL_async_t *op,
								   TW_Task_handle_t task) {
	herr_t err = 0;
	int twerr  = TW_SUCCESS;

	/* Copy task handle in args */
	argp->task = task;
	if (reqp) { reqp->req_task = task; }

	if (op) {
		/* Acquire object lock */
		H5VL_asynci_mutex_lock (op->lock);
		/* Check status */
//		if ((op->stat == H5VL_async_stat_err) || (op->stat == H5VL_async_stat_close)) {
//			H5VL_asynci_mutex_unlock (op->lock);
//			RET_ERR ("Parent object in wrong status");
//		}
		/* Add dependency to prev task */
		if (op->prev_task != TW_HANDLE_NULL) {
			twerr = TW_Task_add_dep (task, op->prev_task);
			CHK_TWERR
		}
		/* Record current task */
		op->prev_task = task;

		/* Commit task*/
		// H5VL_async_inc_ref (op);
		twerr = TW_Task_commit (task, H5VL_async_engine);
		CHK_TWERR
		/* Release object lock */
		H5VL_asynci_mutex_unlock (op->lock);
	} else {
		twerr = TW_Task_commit (task, H5VL_async_engine);
		CHK_TWERR
	}

err_out:;
	return err;
}

herr_t H5VL_asynci_cb_task_wait (void **req, TW_Task_handle_t task) {
	herr_t err = 0;
	int twerr  = TW_SUCCESS;
DEBUG_PRINT
	/* Wait for task if the operation is sync */
	if (!req) {DEBUG_PRINT
		/* Release the lock so worker thread can acquire*/
		H5TSmutex_release (&LOCK_COUNT_GLOBAL);

		twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);
		CHK_TWERR

		err = H5VL_asynci_h5ts_mutex_lock ();
		CHECK_ERR

		// twerr = TW_Task_free (task);
		// CHK_TWERR

		CHECK_ERR_EX ("Async operation failed")
	}

err_out:;
	return err;
}
#endif
