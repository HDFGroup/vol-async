/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserled.                                                      *
 *                                                                           *
 * This file is part of Taskworks. The full Taskworks copyright notice,      *
 * including terms golerning use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* No wait lqueue */

#include <H5VLpublic.h>
#include <stdlib.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_asynci.h"
#include "h5vl_asynci_vector.h"

H5VL_asynci_vector_handle_t H5VL_asynci_vector_create (void) {
	herr_t err					  = 0;
	H5VL_asynci_vector_handle_t s = NULL;

	s = (H5VL_asynci_vector_handle_t)malloc (sizeof (H5VL_asynci_vector_t));
	CHECK_PTR (s);

	err = H5VL_asynci_vector_init (s);
	CHECK_ERR

err_out:;
	if (err) {
		free (s);
		s = NULL;
	}
	return s;
}
herr_t H5VL_asynci_vector_init (H5VL_asynci_vector_handle_t s) {
	herr_t err = 0;

	s->base = (void **)malloc (sizeof (void *) * H5VL_ASYNC_VECTOR_INIT_SIZE);
	CHECK_PTR (s->base);
	s->end = s->base + H5VL_ASYNC_VECTOR_INIT_SIZE;
	s->ptr = s->base;

err_out:;
	return err;
}
void H5VL_asynci_vector_finalize (H5VL_asynci_vector_handle_t s) {
	free (s->base);
	s->end = s->base;
	s->ptr = s->base;
}
void H5VL_asynci_vector_free (H5VL_asynci_vector_handle_t s) {
	H5VL_asynci_vector_finalize (s);
	free (s);
}
size_t H5VL_asynci_vector_size (H5VL_asynci_vector_handle_t s) {
	return (size_t) (s->ptr - s->base);
}
static inline herr_t H5VL_asynci_vector_extend (H5VL_asynci_vector_handle_t s, size_t size) {
	herr_t err = 0;
	size_t nalloc;
	void *tmp;

	nalloc = (size_t) (s->end - s->base);
	if (size > nalloc) {
		do { nalloc <<= H5VL_ASYNC_VECTOR_SHIFT_AMOUNT; } while (size > nalloc);

		tmp = realloc (s->base, sizeof (void *) * nalloc);
		CHECK_PTR (tmp)
		s->base = tmp;
		s->end	= s->base + nalloc;
	}

err_out:;
	return err;
}
herr_t H5VL_asynci_vector_resize (H5VL_asynci_vector_handle_t s, size_t size) {
	herr_t err = 0;

	s->ptr = s->base + size;
	if (s->ptr > s->end) {
		err = H5VL_asynci_vector_extend (s, size);
		CHECK_ERR
	}

err_out:;
	return err;
}

herr_t H5VL_asynci_vector_push_back (H5VL_asynci_vector_handle_t s, void *data) {
	herr_t err = 0;

	// Increase size if needed
	if (s->ptr == s->end) {
		err = H5VL_asynci_vector_extend (s, (size_t) (s->end - s->base + 1));
		CHECK_ERR
	}

	// Push to stack
	*(s->ptr) = data;
	(s->ptr)++;

err_out:;
	return err;
}

herr_t H5VL_asynci_vector_pop_back (H5VL_asynci_vector_handle_t s, void **data) {
	herr_t err = 0;

	if (s->ptr == s->base)
		RET_ERR ("Vector is empty")
	else {
		(s->ptr)--;
		*data = *(s->ptr);
	}

err_out:;
	return 0;
}
