/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This group is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the group COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* group callbacks */

#pragma once

#include <H5VLpublic.h>
#include <taskworks.h>

/* Async VOL headers */
#include "h5vl_async.h"

typedef struct H5VL_async_group_create_args {
	H5VL_async_t *parent;
	H5VL_loc_params_t *loc_params;
	char *name;
	unsigned flags;
	hid_t lcpl_id;
	hid_t gcpl_id;
	hid_t gapl_id;
	hid_t dxpl_id;
	H5VL_async_t *gp;
	herr_t *ret;
} H5VL_async_group_create_args;

typedef struct H5VL_async_group_open_args {
	H5VL_async_t *parent;
	H5VL_loc_params_t *loc_params;
	char *name;
	unsigned flags;
	hid_t gapl_id;
	hid_t dxpl_id;
	H5VL_async_t *gp;
	herr_t *ret;
} H5VL_async_group_open_args;

typedef struct H5VL_async_group_get_args {
	H5VL_async_t *gp;
	H5VL_group_get_t get_type;
	hid_t dxpl_id;
	va_list arguments;
	TW_Task_handle_t task;
	herr_t *ret;
} H5VL_async_group_get_args;

typedef struct H5VL_async_group_specific_args {
	H5VL_async_t *gp;
	H5VL_group_specific_t specific_type;
	hid_t dxpl_id;
	va_list arguments;
	TW_Task_handle_t task;
	herr_t *ret;
} H5VL_async_group_specific_args;

typedef struct H5VL_async_group_optional_args {
	H5VL_async_t *gp;
	H5VL_group_optional_t opt_type;
	hid_t dxpl_id;
	va_list arguments;
	TW_Task_handle_t task;
	herr_t *ret;
} H5VL_async_group_optional_args;

typedef struct H5VL_async_group_close_args {
	H5VL_async_t *gp;
	hid_t dxpl_id;
	TW_Task_handle_t task;
	herr_t *ret;
} H5VL_async_group_close_args;

int H5VL_async_group_create_handler (void *data);
int H5VL_async_group_open_handler (void *data);
int H5VL_async_group_get_handler (void *data);
int H5VL_async_group_specific_handler (void *data);
int H5VL_async_group_optional_handler (void *data);
int H5VL_async_group_close_handler (void *data);