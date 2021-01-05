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

#include "h5vl_async_req.h"
#include "h5vl_asynci.h"

#ifdef ASYNCVOL_DEBUG

typedef struct H5VL_asynci_debug_args {
	H5VL_ASYNC_ARG_VARS
} H5VL_asynci_debug_args;

herr_t H5VL_asynci_handler_begin (H5VL_asynci_debug_args *argp, hbool_t *stat_restored);
herr_t H5VL_asynci_handler_end (H5VL_asynci_debug_args *argp, hbool_t stat_restored);
herr_t H5VL_asynci_handler_free (H5VL_asynci_debug_args *argp);
herr_t H5VL_asynci_cb_task_init (
	hid_t dxpl_id, void **req, herr_t *ret, H5VL_asynci_debug_args *argp, H5VL_async_req_t **reqp);
herr_t H5VL_asynci_cb_task_commit (H5VL_asynci_debug_args *argp,
								   H5VL_async_req_t *reqp,
								   H5VL_async_t *op,
								   TW_Task_handle_t task);
herr_t H5VL_asynci_cb_task_wait (void **req, TW_Task_handle_t task, herr_t *ret);
#endif