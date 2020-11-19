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

#define H5VL_ASYNC_CB_TASK_INIT                                                 \
	{                                                                           \
		err = H5VL_asynci_cb_task_init (dxpl_id, &is_async, req, &ret,          \
										(H5VL_asynci_debug_args *)argp, &reqp); \
		CHECK_ERR                                                               \
	}

#define H5VL_ASYNC_CB_TASK_COMMIT                                                          \
	{                                                                                      \
		err = H5VL_asynci_cb_task_commit ((H5VL_asynci_debug_args *)argp, reqp, pp, task); \
		CHECK_ERR                                                                          \
	}

#define H5VL_ASYNC_CB_TASK_WAIT                                \
	{                                                          \
		err = H5VL_asynci_cb_task_wait (is_async, task, &ret); \
		CHECK_ERR                                              \
	}

#define H5VL_ASYNC_CB_CLOSE_TASK_WAIT                                    \
	{                                                                    \
		err = H5VL_asynci_cb_close_task_wait (is_async, pp, task, &ret); \
		CHECK_ERR                                                        \
	}
#else
#define H5VL_ASYNC_HANDLER_BEGIN                                                                 \
	{                                                                                            \
		/* Acquire global lock */                                                                \
		err = H5VL_asynci_h5ts_mutex_lock ();                                                    \
		CHECK_ERR_EX ("H5VL_asynci_h5ts_mutex_lock failed")                                      \
		/* Apply HDF5 state copied in the VOL call so the handler is recognized as VOL functions \
		 */                                                                                      \
		err = H5VLrestore_lib_state (argp->stat);                                                \
		CHECK_ERR_EX ("H5VLrestore_lib_state failed")                                            \
		stat_restored = true;                                                                    \
	}

#define H5VL_ASYNC_HANDLER_END                                       \
	{                                                                \
		/* Restore HDF5 status */                                    \
		if (stat_restored) { H5VLreset_lib_state (); }               \
		err = H5VLfree_lib_state (argp->stat);                       \
		if (err) {                                                   \
			printf ("Error at line %d in %s\n", __LINE__, __FILE__); \
			H5Eprint2 (H5E_DEFAULT, stdout);                         \
			DEBUG_ABORT                                              \
		}                                                            \
		/* Update reference count */                                 \
		if (argp->pp) {                                              \
			H5VL_asynci_mutex_lock (argp->pp->lock);                 \
			H5VL_async_dec_ref (argp->pp);                           \
			H5VL_asynci_mutex_unlock (argp->pp->lock);               \
		}                                                            \
	}

#define H5VL_ASYNC_HANDLER_FREE                                         \
	{                                                                   \
		/* Release global lock */                                       \
		err = H5TSmutex_release ();                                     \
		CHECK_ERR_EX ("H5TSmutex_release failed")                       \
		/* Free task */                                                 \
		if (argp->ret) {                                                \
			*argp->ret = err;                                           \
		} else {                                                        \
			int twerr;                                                  \
			twerr = TW_Task_free (argp->task);                          \
			if (twerr != TW_SUCCESS) {                                  \
				char msg[256];                                          \
				sprintf (msg, "TaskWorks: %s", TW_Get_err_msg (twerr)); \
				PRINT_ERR_MSG (twerr, msg);                             \
				DEBUG_ABORT;                                            \
			}                                                           \
		}                                                               \
		/* Free arguments */                                            \
		free (argp);                                                    \
	}

#define H5VL_ASYNC_CB_TASK_INIT                                                \
	{                                                                          \
		/* Check if the operations is async */                                 \
		err = H5Pget_dxpl_async (dxpl_id, &is_async);                          \
		CHECK_ERR_EX ("H5Pget_dxpl_async failed")                              \
		if (is_async) {                                                        \
			if (req) {                                                         \
				reqp = (H5VL_async_req_t *)malloc (sizeof (H5VL_async_req_t)); \
				CHECK_PTR (reqp)                                               \
				argp->ret = &(reqp->ret);                                      \
			} else {                                                           \
				argp->ret = NULL;                                              \
			}                                                                  \
		} else {                                                               \
			argp->ret = &ret;                                                  \
			assert (argp->ret);                                                \
		}                                                                      \
		if (!is_async) { assert (argp->ret && 1); }                            \
		/* Retrieve current library state */                                   \
		err = H5VLretrieve_lib_state (&argp->stat);                            \
		CHECK_ERR_EX ("H5VLretrieve_lib_state failed")                         \
		if (!is_async) { assert (argp->ret); }                                 \
	}

#define H5VL_ASYNC_CB_TASK_COMMIT                                                           \
	{                                                                                       \
		/* Copy task handle in args */                                                      \
		argp->task = task;                                                                  \
		if (reqp) { reqp->task = task; }                                                    \
		/* Acquire object lock */                                                           \
		if (pp) {                                                                           \
			H5VL_asynci_mutex_lock (pp->lock);                                              \
			/* Check status */                                                              \
			if ((pp->stat == H5VL_async_stat_err) || (pp->stat == H5VL_async_stat_close)) { \
				H5VL_asynci_mutex_unlock (pp->lock);                                        \
				RET_ERR ("Parent object in wrong status");                                  \
			}                                                                               \
			/* Add dependency to init task */                                               \
			if (pp->init_task) {                                                            \
				twerr = TW_Task_add_dep (task, pp->init_task);                              \
				CHK_TWERR                                                                   \
			}                                                                               \
			/* Increase reference count and commit task*/                                   \
			H5VL_async_inc_ref (pp);                                                        \
			twerr = TW_Task_commit (task, H5VL_async_engine);                               \
			CHK_TWERR                                                                       \
			/* Release object lock */                                                       \
			H5VL_asynci_mutex_unlock (pp->lock);                                            \
		} else {                                                                            \
			twerr = TW_Task_commit (task, H5VL_async_engine);                               \
			CHK_TWERR                                                                       \
		}                                                                                   \
	}

#define H5VL_ASYNC_CB_TASK_WAIT                                \
	{                                                          \
		/* Wait for task if the operation is sync */           \
		if (!is_async) {                                       \
			/* Release the lock so worker thread can acquire*/ \
			H5TSmutex_release ();                              \
                                                               \
			twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);     \
			CHK_TWERR                                          \
                                                               \
			err = H5VL_asynci_h5ts_mutex_lock ();              \
			CHECK_ERR                                          \
                                                               \
			twerr = TW_Task_free (task);                       \
			CHK_TWERR                                          \
                                                               \
			err = ret;                                         \
			CHECK_ERR_EX ("Async operation failed")            \
		}                                                      \
	}

#define H5VL_ASYNC_CB_CLOSE_TASK_WAIT                           \
	{                                                           \
		/* Wait for task if the operation is sync */            \
		if (!is_async) {                                        \
			/* Release the lock so worker thread can acquire*/  \
			H5TSmutex_release ();                               \
                                                                \
			while (pp->ref) {                                   \
				twerr = TW_Engine_progress (H5VL_async_engine); \
				CHK_TWERR                                       \
			}                                                   \
			twerr = TW_Task_commit (task, H5VL_async_engine);   \
			CHK_TWERR                                           \
                                                                \
			twerr = TW_Task_wait (task, TW_TIMEOUT_NEVER);      \
			CHK_TWERR                                           \
                                                                \
			err = H5VL_asynci_h5ts_mutex_lock ();               \
			CHECK_ERR                                           \
                                                                \
			twerr = TW_Task_free (task);                        \
			CHK_TWERR                                           \
                                                                \
			err = ret;                                          \
			CHECK_ERR_EX ("Async operation failed")             \
		}                                                       \
	}
#endif

#define H5VL_ASYNC_CB_VARS         \
	herr_t err	 = 0;              \
	terr_t twerr = TW_SUCCESS;     \
	hbool_t is_async;              \
	herr_t ret;                    \
	H5VL_async_req_t *reqp = NULL; \
	TW_Task_handle_t task;

#define H5VL_ASYNC_ARG_VARS \
	H5VL_async_t *pp;       \
	TW_Task_handle_t task;  \
	herr_t *ret;            \
	void *stat;

herr_t H5VL_asynci_h5ts_mutex_lock ();
herr_t H5VL_asynci_obj_wait (void *pp);