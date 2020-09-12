/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Taskworks. The full Taskworks copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Cross-platform read write lock wrapper */

#pragma once

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

#include <H5VLpublic.h>

#ifdef _WIN32
typedef HANDLE H5VL_asynci_mutex_t;
#else
typedef pthread_mutex_t H5VL_asynci_mutex_t;
#endif
typedef H5VL_asynci_mutex_t *H5VL_asynci_mutex_handle_t;

H5VL_asynci_mutex_handle_t H5VL_asynci_mutex_create (void);
herr_t H5VL_asynci_mutex_init (H5VL_asynci_mutex_handle_t m);
void H5VL_asynci_mutex_finalize (H5VL_asynci_mutex_handle_t m);
void H5VL_asynci_mutex_free (H5VL_asynci_mutex_handle_t m);
void H5VL_asynci_mutex_lock (H5VL_asynci_mutex_handle_t m);
void H5VL_asynci_mutex_trylock (H5VL_asynci_mutex_handle_t m, int *success);
void H5VL_asynci_mutex_unlock (H5VL_asynci_mutex_handle_t m);
