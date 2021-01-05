/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This object is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the object COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* object callbacks */

#pragma once

#include <H5VLpublic.h>
#include <taskworks.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_asynci.h"

typedef struct H5VL_async_object_open_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *op;
	H5VL_loc_params_t *loc_params;
	H5I_type_t *opened_type;
	hid_t dxpl_id;
} H5VL_async_object_open_args;

typedef struct H5VL_async_object_copy_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_loc_params_t *src_loc_params;
	char *src_name;
	H5VL_async_t *dst_obj;
	H5VL_loc_params_t *dst_loc_params;
	char *dst_name;
	hid_t ocpypl_id;
	hid_t lcpl_id;
	hid_t dxpl_id;
} H5VL_async_object_copy_args;

typedef struct H5VL_async_object_get_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_loc_params_t *loc_params;
	H5VL_object_get_t get_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_object_get_args;

typedef struct H5VL_async_object_specific_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_loc_params_t *loc_params;
	H5VL_object_specific_t specific_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_object_specific_args;

typedef struct H5VL_async_object_optional_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_object_optional_t opt_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_object_optional_args;

int H5VL_async_object_copy_handler (void *data);
int H5VL_async_object_open_handler (void *data);
int H5VL_async_object_get_handler (void *data);
int H5VL_async_object_specific_handler (void *data);
int H5VL_async_object_optional_handler (void *data);