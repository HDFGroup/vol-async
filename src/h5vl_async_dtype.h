/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Datatype callbacks */

#pragma once

#include <H5VLpublic.h>

herr_t H5VL_async_datatype_commit (void *obj,
								   const H5VL_loc_params_t *loc_params,
								   const char *name,
								   hid_t type_id,
								   hid_t lcpl_id,
								   hid_t tcpl_id,
								   hid_t tapl_id,
								   hid_t dxpl_id,
								   void **req);
void *H5VL_async_datatype_open (void *obj,
								const H5VL_loc_params_t *loc_params,
								const char *name,
								hid_t tapl_id,
								hid_t dxpl_id,
								void **req);
herr_t H5VL_async_datatype_get (
	void *dt, H5VL_datatype_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_datatype_specific (void *obj,
									 H5VL_datatype_specific_t specific_type,
									 hid_t dxpl_id,
									 void **req,
									 va_list arguments);
herr_t H5VL_async_datatype_optional (
	void *obj, H5VL_datatype_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_datatype_close (void *dt, hid_t dxpl_id, void **req);