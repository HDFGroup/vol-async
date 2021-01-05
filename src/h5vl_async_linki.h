/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This link is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the link COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* link callbacks */

#pragma once

#include <H5VLpublic.h>
#include <taskworks.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_asynci.h"

typedef struct H5VL_async_link_create_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *op;
	H5VL_link_create_type_t create_type;
	H5VL_loc_params_t *loc_params;
	hid_t lcpl_id;
	hid_t lapl_id;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_link_create_args;

typedef struct H5VL_async_link_copy_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_loc_params_t *loc_params1;
	H5VL_async_t *dst_obj;
	H5VL_loc_params_t *loc_params2;
	hid_t lcpl_id;
	hid_t lapl_id;
	hid_t dxpl_id;
} H5VL_async_link_copy_args;

typedef struct H5VL_async_link_move_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_loc_params_t *loc_params1;
	H5VL_async_t *dst_obj;
	H5VL_loc_params_t *loc_params2;
	hid_t lcpl_id;
	hid_t lapl_id;
	hid_t dxpl_id;
} H5VL_async_link_move_args;

typedef struct H5VL_async_link_get_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_loc_params_t *loc_params;
	H5VL_link_get_t get_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_link_get_args;

typedef struct H5VL_async_link_specific_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_loc_params_t *loc_params;
	H5VL_link_specific_t specific_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_link_specific_args;

typedef struct H5VL_async_link_optional_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_link_optional_t opt_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_link_optional_args;

int H5VL_async_link_create_handler (void *data);
int H5VL_async_link_copy_handler (void *data);
int H5VL_async_link_move_handler (void *data);
int H5VL_async_link_get_handler (void *data);
int H5VL_async_link_specific_handler (void *data);
int H5VL_async_link_optional_handler (void *data);