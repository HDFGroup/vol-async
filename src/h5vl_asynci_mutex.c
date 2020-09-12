/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Taskworks. The full Taskworks copyright notice,
 *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* mutex wrapper for win32 and posix */

#include <assert.h>
#include <errno.h>
#include <stdio.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

#include "h5vl_asynci.h"
#include "h5vl_asynci_mutex.h"

#ifdef H5VL_asynci_DEBUG
#ifdef _WIN32
#define CHECK_MERR                                                          \
	{                                                                       \
		if (ret == WAIT_FAILED || ret == 0) PRINT_OS_ERR (GetLastError ()); \
		abort ();                                                           \
	}
#else

#define CHECK_MERR               \
	{                            \
		if (perr) {              \
			PRINT_OS_ERR (perr); \
			abort ();            \
		}                        \
	}
#endif
#else
#define CHECK_MERR \
	{}
#endif

H5VL_asynci_mutex_handle_t H5VL_asynci_mutex_create (void) {
	herr_t err					  = 0;
	H5VL_asynci_mutex_handle_t mp = NULL;

	mp = (H5VL_asynci_mutex_handle_t)malloc (sizeof (H5VL_asynci_mutex_t));
	CHECK_PTR (mp);

	err = H5VL_asynci_mutex_init (mp);
	CHECK_ERR

err_out:;
	if (err) {
		free (mp);
		mp = NULL;
	}
	return mp;
}

void H5VL_asynci_mutex_free (H5VL_asynci_mutex_handle_t m) {
	H5VL_asynci_mutex_finalize (m);
	free (m);
}

#ifdef _WIN32
herr_t H5VL_asynci_mutex_init (H5VL_asynci_mutex_handle_t m) {
	herr_t err = 0;
	*m		   = Createmutex (NULL, FALSE, NULL);
	if (*m == NULL) RET_ERR ("Createmutex failed")
err_out:;
	return err;
}
void H5VL_asynci_mutex_finalize (H5VL_asynci_mutex_handle_t m) { CloseHandle (*m); }
void H5VL_asynci_mutex_lock (H5VL_asynci_mutex_handle_t m) {
	herr_t err = 0;
	DWORD ret;

	ret = WaitForSingleObject (*m, INFINITE);
	CHECK_MERR
}
void H5VL_asynci_mutex_trylock (H5VL_asynci_mutex_handle_t m, int *success) {
	herr_t err = 0;
	DWORD ret;

	ret = WaitForSingleObject (*m, INFINITE);
	if (ret == WAIT_TIMEOUT)
		*success = 0;
	else {
		CHECK_MERR
		*success = 1;
	}
}
void H5VL_asynci_mutex_unlock (H5VL_asynci_mutex_handle_t m) {
	herr_t err = 0;
	BOOL ret;

	ret = Releasemutex (*m);
	CHECK_MERR
}
#else
herr_t H5VL_asynci_mutex_init (H5VL_asynci_mutex_handle_t m) {
	herr_t err = 0;
	int perr;

	perr = pthread_mutex_init (m, NULL);
	if (perr) { RET_ERR ("pthread_mutex_init failed") }

err_out:;
	return err;
}
void H5VL_asynci_mutex_finalize (H5VL_asynci_mutex_handle_t m) {
	int perr;

	perr = pthread_mutex_destroy (m);
	CHECK_MERR
}
void H5VL_asynci_mutex_lock (H5VL_asynci_mutex_handle_t m) {
	int perr;

	perr = pthread_mutex_lock (m);
	CHECK_MERR
}
void H5VL_asynci_mutex_trylock (H5VL_asynci_mutex_handle_t m, int *success) {
	int perr;

	perr = pthread_mutex_trylock (m);
	if (perr == EBUSY)
		*success = 0;
	else {
		CHECK_MERR
		*success = 1;
	}
}
void H5VL_asynci_mutex_unlock (H5VL_asynci_mutex_handle_t m) {
	int perr;

	perr = pthread_mutex_unlock (m);
	CHECK_MERR
}
#endif
