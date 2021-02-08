/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Async VOL declarations */

#pragma once

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <assert.h>
#include <hdf5.h>
#include <stdio.h>
#include <stdlib.h>

#include "h5vl_asynci_mutex.h"

static unsigned int LOCK_COUNT_GLOBAL = 1;
#define H5VL_ASYNC_UNUSED		__attribute__ ((unused))
#define H5VL_ASYNC_FALL_THROUGH __attribute__ ((fallthrough));

#ifdef ASYNCVOL_DEBUG
#define DEBUG_ABORT                             \
	{                                           \
		char *env;                              \
		env = getenv ("ASYNC_VOL_DEBUG_ABORT"); \
		if (env) { abort (); }                  \
	}

#define PRINT_ERR_MSG(E, M) \
	{ printf ("Error at line %d in %s: %s (%d)\n", __LINE__, __FILE__, M, E); }
#else
#define DEBUG_ABORT \
	{}
#define PRINT_ERR_MSG(E, M) \
	{}
#endif

#define CHECK_ERR_EX(M)                                                        \
	{                                                                          \
		{                                                                      \
			if (err < 0) {                                                     \
				printf ("Error at line %d in %s:%s\n", __LINE__, __FILE__, M); \
				H5Eprint2 (H5E_DEFAULT, stdout);                               \
				DEBUG_ABORT                                                    \
				err = -1;                                                      \
				goto err_out;                                                  \
			}                                                                  \
		}                                                                      \
	}
#define CHECK_ERR CHECK_ERR_EX ("")

#define CHECK_ERR2_EX(M)                                                       \
	{                                                                          \
		{                                                                      \
			if (err < 0) {                                                     \
				printf ("Error at line %d in %s:%s\n", __LINE__, __FILE__, M); \
				H5Eprint2 (H5E_DEFAULT, stdout);                               \
				DEBUG_ABORT                                                    \
				err = -1;                                                      \
			}                                                                  \
		}                                                                      \
	}
#define CHECK_ERR2 CHECK_ERR2_EX ("")

#define CHECK_MPIERR                                                             \
	{                                                                            \
		if (mpierr != MPI_SUCCESS) {                                             \
			int el = 256;                                                        \
			char errstr[256];                                                    \
			MPI_Error_string (mpierr, errstr, &el);                              \
			printf ("Error at line %d in %s: %s\n", __LINE__, __FILE__, errstr); \
			err = -1;                                                            \
			DEBUG_ABORT                                                          \
			goto err_out;                                                        \
		}                                                                        \
	}

#define CHECK_ID(A)                                                                             \
	{                                                                                           \
		if (A < 0) {                                                                            \
			printf ("Error at line %d in %s: %s is not valid hid\n", __LINE__, __FILE__, "#A"); \
			H5Eprint2 (H5E_DEFAULT, stdout);                                                    \
			DEBUG_ABORT                                                                         \
			err = -1;                                                                           \
			goto err_out;                                                                       \
		}                                                                                       \
	}

#define CHECK_PTR(A)                                                                   \
	{                                                                                  \
		if (A == NULL) {                                                               \
			printf ("Error at line %d in %s: %s is NULL\n", __LINE__, __FILE__, "#A"); \
			H5Eprint2 (H5E_DEFAULT, stdout);                                           \
			DEBUG_ABORT                                                                \
			err = -1;                                                                  \
			goto err_out;                                                              \
		}                                                                              \
	}

#define RET_ERR(A)                                                      \
	{                                                                   \
		printf ("Error at line %d in %s: %s\n", __LINE__, __FILE__, A); \
		DEBUG_ABORT                                                     \
		err = -1;                                                       \
		goto err_out;                                                   \
	}

#define CHK_TWRET(R)                                            \
	{                                                           \
		if (R != TW_SUCCESS) {                                  \
			char msg[256];                                      \
			sprintf (msg, "TaskWorks: %s", TW_Get_err_msg (R)); \
			PRINT_ERR_MSG (R, msg);                             \
			DEBUG_ABORT;                                        \
			err = -1;                                           \
			goto err_out;                                       \
		}                                                       \
	}
#define CHK_TWERR CHK_TWRET (twerr)

#define H5VL_ASYNC_LOCK_POLL_PEROID 1000

#define H5VL_ASYNC_HANDLER_VARS \
	herr_t err			  = 0;  \
	hbool_t stat_restored = false;

#ifdef ASYNCVOL_DEBUG
#define H5VL_ASYNC_HANDLER_BEGIN                                                          \
	{                                                                                     \
		err = H5VL_asynci_handler_begin ((H5VL_asynci_debug_args *)argp, &stat_restored); \
		CHECK_ERR                                                                         \
	}

#define H5VL_ASYNC_HANDLER_END                                                         \
	{                                                                                  \
		err = H5VL_asynci_handler_end ((H5VL_asynci_debug_args *)argp, stat_restored); \
		CHECK_ERR                                                                      \
	}

#define H5VL_ASYNC_HANDLER_FREE                                          \
	{                                                                    \
		err = H5VL_asynci_handler_free ((H5VL_asynci_debug_args *)argp); \
		CHECK_ERR                                                        \
	}

#define H5VL_ASYNC_CB_TASK_INIT                                                                   \
	{                                                                                             \
		err =                                                                                     \
			H5VL_asynci_cb_task_init (dxpl_id, req, &ret, (H5VL_asynci_debug_args *)argp, &reqp); \
		CHECK_ERR                                                                                 \
	}

#define H5VL_ASYNC_CB_TASK_COMMIT                                                                  \
	{                                                                                              \
		err = H5VL_asynci_cb_task_commit ((H5VL_asynci_debug_args *)argp, reqp, target_obj, task); \
		CHECK_ERR                                                                                  \
	}

#define H5VL_ASYNC_CB_TASK_WAIT                           \
	{                                                     \
		err = H5VL_asynci_cb_task_wait (req, task, &ret); \
		CHECK_ERR                                         \
	}
#else
#define H5VL_ASYNC_HANDLER_BEGIN                                                                 \
	{                                                                                            \
		/* Acquire global lock */                                                                \
		err = H5VL_asynci_h5ts_mutex_lock ();                                                    \
		CHECK_ERR_EX ("H5VL_asynci_h5ts_mutex_lock failed")                                      \
		/* Apply HDF5 state copied in the VOL call so the handler is recognized as VOL functions \
		 */                                                                                      \
        err = H5VLstart_lib_state ();		                                                     \
        CHECK_ERR_EX ("H5VLstart_lib_state failed")                                            \
		err = H5VLrestore_lib_state (argp->stat);                                                \
		CHECK_ERR_EX ("H5VLrestore_lib_state failed")                                            \
		*stat_restored = true;                                                                   \
	}

#define H5VL_ASYNC_HANDLER_END                                      \
	{                                                               \
		/* Restore HDF5 status */                                   \
		if (stat_restored) { H5VLfinish_lib_state (); }              \
		err = H5VLfree_lib_state (argp->stat);                      \
		if (err) {                                                  \
			printf ("Error at line %d in %sn", __LINE__, __FILE__); \
			H5Eprint2 (H5E_DEFAULT, stdout);                        \
			DEBUG_ABORT                                             \
		}                                                           \
                                                                    \
		/* Clear task flag */                                       \
		if (argp->target_obj) {                                     \
			/* Acquire object lock */                               \
			H5VL_asynci_mutex_lock (argp->target_obj->lock);        \
			if (argp->target_obj->prev_task == argp->task) {        \
				argp->target_obj->prev_task = TW_HANDLE_NULL;       \
			}                                                       \
			/* Release object lock */                               \
			H5VL_asynci_mutex_unlock (argp->target_obj->lock);      \
		}                                                           \
	}

#define H5VL_ASYNC_HANDLER_FREE                   \
	{                                             \
		/* Release global lock */                 \
		err = H5TSmutex_release ();               \
		CHECK_ERR_EX ("H5TSmutex_release failed") \
                                                  \
		/* Record return val in request handle */ \
		*argp->ret = err;                         \
                                                  \
		/* Free arguments */                      \
		free (argp);                              \
	}

#define H5VL_ASYNC_CB_TASK_INIT                                             \
	{                                                                       \
		/* Check if the operations is async */                              \
		                                                   \
		if (req) {                                                          \
			reqp = (H5VL_async_req_t *)malloc (sizeof (H5VL_async_req_t)); \
			CHECK_PTR (reqp)                                               \
			/* argp->ret = &(reqp->ret); ??? */                                   \
			*req = (void*)reqp;                                            \
		} else {                                                            \
			argp->ret = ret;                                                \
			argp->req = NULL;                                               \
		}                                                                   \
                                                                            \
		/* Retrieve current library state */                                \
		err = H5VLretrieve_lib_state (&argp->stat);                         \
		CHECK_ERR_EX ("H5VLretrieve_lib_state failed")                      \
	}

#define H5VL_ASYNC_CB_TASK_COMMIT                                                           \
	{                                                                                       \
		/* Copy task handle in args */                                                      \
		argp->task = task;                                                                  \
		if (reqp) { reqp->task = task; }                                                    \
                                                                                            \
		if (op) {                                                                           \
			/* Acquire object lock */                                                       \
			H5VL_asynci_mutex_lock (op->lock);                                              \
			/* Check status */                                                              \
			if ((op->stat == H5VL_async_stat_err) || (op->stat == H5VL_async_stat_close)) { \
				H5VL_asynci_mutex_unlock (op->lock);                                        \
				RET_ERR ("Parent object in wrong status");                                  \
			}                                                                               \
			/* Add dependency to prev task */                                               \
			if (op->prev_task != TW_HANDLE_NULL) {                                          \
				twerr = TW_Task_add_dep (task, op->prev_task);                              \
				CHK_TWERR                                                                   \
			}                                                                               \
			/* Record current task */                                                       \
			op->prev_task = task;                                                           \
                                                                                            \
			/* Commit task*/                                                                \
			twerr = TW_Task_commit (task, H5VL_async_engine);                               \
			CHK_TWERR                                                                       \
			/* Release object lock */                                                       \
			H5VL_asynci_mutex_unlock (op->lock);                                            \
		} else {                                                                            \
			twerr = TW_Task_commit (task, H5VL_async_engine);                               \
			CHK_TWERR                                                                       \
		}                                                                                   \
	}

#define H5VL_ASYNC_CB_TASK_WAIT                                \
	{                                                          \
		/* Wait for task if the operation is sync */           \
		if (!req) {                                            \
			/* Release the lock so worker thread can acquire*/ \
			H5TSmutex_release ();                              \
                                                               \
			twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);     \
			CHK_TWERR                                          \
                                                               \
			err = H5VL_asynci_h5ts_mutex_lock ();              \
			CHECK_ERR                                          \
                                                               \
			err = *ret;                                        \
			CHECK_ERR_EX ("Async operation failed")            \
		}                                                      \
	}
#endif

#define H5VL_ASYNC_CB_VARS         \
	herr_t err	 = 0;              \
	terr_t twerr = TW_SUCCESS;     \
	herr_t ret;                    \
	H5VL_async_req_t *reqp = NULL; \
	TW_Task_handle_t task;

#define H5VL_ASYNC_ARG_VARS   \
	H5VL_async_t *target_obj; \
	TW_Task_handle_t task;    \
	void** req;             \
	herr_t *ret;              \
	void *stat;             \
    H5VL_async_t *op;

herr_t H5VL_asynci_h5ts_mutex_lock ();
herr_t H5VL_asynci_obj_wait (void *target_obj);
