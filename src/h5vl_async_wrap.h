/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* VOL object wrap / retrieval callbacks */

#pragma once

#include <H5VLpublic.h>

/* The async VOL wrapper context */
typedef struct H5VL_async_wrap_ctx_t {
	hid_t under_vol_id;	  /* VOL ID for under VOL */
	void *under_wrap_ctx; /* Object wrapping context for under VOL */
} H5VL_async_wrap_ctx_t;

void *H5VL_async_get_object (const void *obj);
herr_t H5VL_async_get_wrap_ctx (const void *obj, void **wrap_ctx);
void *H5VL_async_wrap_object (void *obj, H5I_type_t obj_type, void *wrap_ctx);
void *H5VL_async_unwrap_object (void *obj);
herr_t H5VL_async_free_wrap_ctx (void *obj);
