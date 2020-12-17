/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* File callbacks */

#pragma once

#include <H5VLpublic.h>
#include <taskworks.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_asynci.h"

typedef struct H5VL_async_file_create_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *pp;
	char *name;
	unsigned flags;
	hid_t fcpl_id;
	hid_t fapl_id;
	hid_t dxpl_id;
} H5VL_async_file_create_args;

typedef struct H5VL_async_file_open_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *pp;
	char *name;
	unsigned flags;
	hid_t fapl_id;
	hid_t dxpl_id;
} H5VL_async_file_open_args;

typedef struct H5VL_async_file_get_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_file_get_t get_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_file_get_args;

typedef struct H5VL_async_file_specific_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_file_specific_t specific_type;
	hid_t dxpl_id;
	va_list arguments;

} H5VL_async_file_specific_args;

typedef struct H5VL_async_file_optional_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_file_optional_t opt_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_file_optional_args;

typedef struct H5VL_async_file_close_args {
	H5VL_ASYNC_ARG_VARS
	hid_t dxpl_id;
} H5VL_async_file_close_args;

int H5VL_async_file_create_handler (void *data);
int H5VL_async_file_open_handler (void *data);
int H5VL_async_file_get_handler (void *data);
int H5VL_async_file_specific_handler (void *data);
int H5VL_async_file_optional_handler (void *data);
int H5VL_async_file_close_handler (void *data);