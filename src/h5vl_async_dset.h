/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Dataset callbacks */

#pragma once

#include <H5VLpublic.h>

void *H5VL_async_dataset_create (void *obj,
								 const H5VL_loc_params_t *loc_params,
								 const char *name,
								 hid_t lcpl_id,
								 hid_t type_id,
								 hid_t space_id,
								 hid_t dcpl_id,
								 hid_t dapl_id,
								 hid_t dxpl_id,
								 void **req);
void *H5VL_async_dataset_open (void *obj,
							   const H5VL_loc_params_t *loc_params,
							   const char *name,
							   hid_t dapl_id,
							   hid_t dxpl_id,
							   void **req);
herr_t H5VL_async_dataset_read (void *dset,
								hid_t mem_type_id,
								hid_t mem_space_id,
								hid_t file_space_id,
								hid_t plist_id,
								void *buf,
								void **req);
herr_t H5VL_async_dataset_write (void *dset,
								 hid_t mem_type_id,
								 hid_t mem_space_id,
								 hid_t file_space_id,
								 hid_t plist_id,
								 const void *buf,
								 void **req);
herr_t H5VL_async_dataset_get (
	void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_dataset_specific (
	void *obj, H5VL_dataset_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_dataset_optional (
	void *obj, H5VL_dataset_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_dataset_close (void *dset, hid_t dxpl_id, void **req);