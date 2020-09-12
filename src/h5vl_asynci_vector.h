/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Taskworks. The full Taskworks copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Thread safe queue */

#pragma once

#include <H5VLpublic.h>

/* Async VOL headers */
#include "h5vl_asynci_mutex.h"

typedef struct H5VL_asynci_vector_t {
	void **ptr;
	void **end;
	void **base;
} H5VL_asynci_vector_t;

typedef H5VL_asynci_vector_t *H5VL_asynci_vector_handle_t;

#define H5VL_ASYNC_VECTOR_INIT_SIZE	   0x20	 // 32 elements
#define H5VL_ASYNC_VECTOR_SHIFT_AMOUNT 2	 // x 4 each time

#define H5VL_asynci_vector_begin(v) (v->base)
#define H5VL_asynci_vector_end(v)	(v->ptr)

H5VL_asynci_vector_handle_t H5VL_asynci_vector_create (void);
herr_t H5VL_asynci_vector_init (H5VL_asynci_vector_handle_t s);
void H5VL_asynci_vector_finalize (H5VL_asynci_vector_handle_t s);
void H5VL_asynci_vector_free (H5VL_asynci_vector_handle_t s);
size_t H5VL_asynci_vector_size (H5VL_asynci_vector_handle_t s);
herr_t H5VL_asynci_vector_resize (H5VL_asynci_vector_handle_t s, size_t size);
herr_t H5VL_asynci_vector_push_back (H5VL_asynci_vector_handle_t s, void *data);
herr_t H5VL_asynci_vector_pop_back (H5VL_asynci_vector_handle_t s, void **data);