/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This dataset is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the dataset COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* dataset callbacks */

#pragma once

#include <H5VLpublic.h>
#include <taskworks.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_asynci.h"

typedef struct H5VL_async_dataset_create_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *gp;
	H5VL_loc_params_t *loc_params;
	char *name;
	unsigned flags;
	hid_t type_id;
	hid_t space_id;
	hid_t lcpl_id;
	hid_t dcpl_id;
	hid_t dapl_id;
	hid_t dxpl_id;
} H5VL_async_dataset_create_args;

typedef struct H5VL_async_dataset_open_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_async_t *gp;
	H5VL_loc_params_t *loc_params;
	char *name;
	unsigned flags;
	hid_t type_id;
	hid_t space_id;
	hid_t dapl_id;
	hid_t dxpl_id;
} H5VL_async_dataset_open_args;

typedef struct H5VL_async_dataset_read_args {
	H5VL_ASYNC_ARG_VARS
	hid_t mem_type_id;
	hid_t mem_space_id;
	hid_t file_space_id;
	hid_t dxpl_id;
	void *buf;
} H5VL_async_dataset_read_args;

typedef struct H5VL_async_dataset_write_args {
	H5VL_ASYNC_ARG_VARS
	hid_t mem_type_id;
	hid_t mem_space_id;
	hid_t file_space_id;
	hid_t dxpl_id;
	const void *buf;
} H5VL_async_dataset_write_args;

typedef struct H5VL_async_dataset_get_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_dataset_get_t get_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_dataset_get_args;

typedef struct H5VL_async_dataset_specific_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_dataset_specific_t specific_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_dataset_specific_args;

typedef struct H5VL_async_dataset_optional_args {
	H5VL_ASYNC_ARG_VARS
	H5VL_dataset_optional_t opt_type;
	hid_t dxpl_id;
	va_list arguments;
} H5VL_async_dataset_optional_args;

typedef struct H5VL_async_dataset_close_args {
	H5VL_ASYNC_ARG_VARS
	hid_t dxpl_id;
} H5VL_async_dataset_close_args;

int H5VL_async_dataset_create_handler (void *data);
int H5VL_async_dataset_open_handler (void *data);
int H5VL_async_dataset_read_handler (void *data);
int H5VL_async_dataset_write_handler (void *data);
int H5VL_async_dataset_get_handler (void *data);
int H5VL_async_dataset_specific_handler (void *data);
int H5VL_async_dataset_optional_handler (void *data);
int H5VL_async_dataset_close_handler (void *data);