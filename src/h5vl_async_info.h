/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* VOL info callbacks */

#pragma once

#include <H5VLpublic.h>

/* Async VOL connector info */
typedef struct H5VL_async_info_t {
	hid_t under_vol_id;	  /* VOL ID for under VOL */
	void *under_vol_info; /* VOL info for under VOL */
} H5VL_async_info_t;

void *H5VL_async_info_copy (const void *info);
herr_t H5VL_async_info_cmp (int *cmp_value, const void *info1, const void *info2);
herr_t H5VL_async_info_free (void *info);
herr_t H5VL_async_info_to_str (const void *info, char **str);
herr_t H5VL_async_str_to_info (const char *str, void **info);