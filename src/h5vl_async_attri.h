/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This attr is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the attr COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* attr callbacks */

#pragma once

#include <H5VLpublic.h>
#include <taskworks.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_asynci.h"

typedef struct H5VL_async_attr_create_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *op;
	H5VL_loc_params_t *loc_params;
	char *name;
	unsigned flags;
	hid_t type_id;
	hid_t space_id;
	hid_t acpl_id;
	hid_t aapl_id;
	hid_t dxpl_id;
} H5VL_async_attr_create_args;

typedef struct H5VL_async_attr_open_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *op;
	H5VL_loc_params_t *loc_params;
	char *name;
	unsigned flags;
	hid_t type_id;
	hid_t space_id;
	hid_t aapl_id;
	hid_t dxpl_id;
} H5VL_async_attr_open_args;

typedef struct H5VL_async_attr_read_args {
	H5VL_ASYNC_ARG_VARS
	hid_t mem_type_id;
	hid_t dxpl_id;
	void *buf;
} H5VL_async_attr_read_args;

typedef struct H5VL_async_attr_write_args {
	H5VL_ASYNC_ARG_VARS
	void* attr;
	hid_t mem_type_id;
	hid_t dxpl_id;
	const void *buf;
} H5VL_async_attr_write_args;

typedef struct H5VL_async_attr_get_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_attr_get_t get_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_attr_get_args;

typedef struct H5VL_async_attr_specific_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_loc_params_t *loc_params;
	H5VL_attr_specific_t specific_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_attr_specific_args;

typedef struct H5VL_async_attr_optional_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_attr_optional_t opt_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_attr_optional_args;

typedef struct H5VL_async_attr_close_args {
	H5VL_ASYNC_ARG_VARS
	hid_t dxpl_id;
} H5VL_async_attr_close_args;

int H5VL_async_attr_create_handler (void *data);
int H5VL_async_attr_open_handler (void *data);
int H5VL_async_attr_read_handler (void *data);
int H5VL_async_attr_write_handler (void *data);
int H5VL_async_attr_get_handler (void *data);
int H5VL_async_attr_specific_handler (void *data);
int H5VL_async_attr_optional_handler (void *data);
int H5VL_async_attr_close_handler (void *data);
