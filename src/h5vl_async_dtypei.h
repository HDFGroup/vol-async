/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This datatype is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the datatype COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* datatype callbacks */

#pragma once

#include <H5VLpublic.h>
#include <taskworks.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_asynci.h"

typedef struct H5VL_async_datatype_commit_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *tp;
	H5VL_loc_params_t *loc_params;
	char *name;
	hid_t type_id;
	hid_t lcpl_id;
	hid_t tcpl_id;
	hid_t tapl_id;
	hid_t dxpl_id;
} H5VL_async_datatype_commit_args;

typedef struct H5VL_async_datatype_open_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *tp;
	H5VL_loc_params_t *loc_params;
	char *name;
	hid_t tapl_id;
	hid_t dxpl_id;
} H5VL_async_datatype_open_args;

typedef struct H5VL_async_datatype_get_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_datatype_get_t get_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_datatype_get_args;

typedef struct H5VL_async_datatype_specific_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_loc_params_t *loc_params;
	H5VL_datatype_specific_t specific_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_datatype_specific_args;

typedef struct H5VL_async_datatype_optional_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_datatype_optional_t opt_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_datatype_optional_args;

typedef struct H5VL_async_datatype_close_args {
	H5VL_ASYNC_ARG_VARS
	hid_t dxpl_id;
} H5VL_async_datatype_close_args;

int H5VL_async_datatype_commit_handler (void *data);
int H5VL_async_datatype_open_handler (void *data);
int H5VL_async_datatype_get_handler (void *data);
int H5VL_async_datatype_specific_handler (void *data);
int H5VL_async_datatype_optional_handler (void *data);
int H5VL_async_datatype_close_handler (void *data);