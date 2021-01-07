/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Async VOL internal functions */

#include <H5VLpublic.h>
#include <stdio.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_asynci.h"

static inline void H5VL_asynci_sleep (int ms) {
#ifdef _WIN32
	Sleep (ms);
#else
	usleep (ms * 1000);
#endif
}

herr_t H5VL_asynci_h5ts_mutex_lock () {
	herr_t err = 0;
	hbool_t locked;

	err = H5TSmutex_acquire (LOCK_COUNT_GLOBAL, &locked);
	CHECK_ERR
	while (!locked) {
		H5VL_asynci_sleep (H5VL_ASYNC_LOCK_POLL_PEROID);
		err = H5TSmutex_acquire (LOCK_COUNT_GLOBAL, &locked);
		CHECK_ERR
	}

err_out:;
	return err;
}

herr_t H5VL_asynci_obj_wait (void *obj) {
	herr_t err = 0;
	int twerr;
	H5VL_async_t *op = (H5VL_async_t *)obj;

	if (op->prev_task) {
		twerr = TW_Task_wait (op->prev_task, TW_TIMEOUT_NEVER);
		CHK_TWERR
	}

err_out:;
	return err;
}
