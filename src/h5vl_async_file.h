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

void *H5VL_async_file_create (
	const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
void *H5VL_async_file_open (
	const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
herr_t H5VL_async_file_get (
	void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_file_specific (
	void *file, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_file_optional (
	void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_file_close (void *file, hid_t dxpl_id, void **req);