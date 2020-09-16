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

#include <hdf5.h>
#include <stdio.h>

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

#define CHECK_ERR                                                     \
	{                                                                 \
		if (err < 0) {                                                \
			printf ("Error at line %d in %s:\n", __LINE__, __FILE__); \
			H5Eprint2 (H5E_DEFAULT, stdout);                          \
			DEBUG_ABORT                                               \
			err = -1;                                                 \
			goto err_out;                                             \
		}                                                             \
	}

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

#define CHECK_ID(A)                                                   \
	{                                                                 \
		if (A < 0) {                                                  \
			printf ("Error at line %d in %s:\n", __LINE__, __FILE__); \
			H5Eprint2 (H5E_DEFAULT, stdout);                          \
			DEBUG_ABORT                                               \
			err = -1;                                                 \
			goto err_out;                                             \
		}                                                             \
	}

#define CHECK_PTR(A)                                                  \
	{                                                                 \
		if (A == NULL) {                                              \
			printf ("Error at line %d in %s:\n", __LINE__, __FILE__); \
			H5Eprint2 (H5E_DEFAULT, stdout);                          \
			DEBUG_ABORT                                               \
			err = -1;                                                 \
			goto err_out;                                             \
		}                                                             \
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

herr_t H5VL_asynci_h5ts_mutex_lock ();
