/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Object callbacks */

#pragma once

#include <H5VLpublic.h>

void *H5VL_async_object_open (void *obj,
							  const H5VL_loc_params_t *loc_params,
							  H5I_type_t *opened_type,
							  hid_t dxpl_id,
							  void **req);
herr_t H5VL_async_object_copy (void *src_obj,
							   const H5VL_loc_params_t *src_loc_params,
							   const char *src_name,
							   void *dst_obj,
							   const H5VL_loc_params_t *dst_loc_params,
							   const char *dst_name,
							   hid_t ocpypl_id,
							   hid_t lcpl_id,
							   hid_t dxpl_id,
							   void **req);
herr_t H5VL_async_object_get (void *obj,
							  const H5VL_loc_params_t *loc_params,
							  H5VL_object_get_t get_type,
							  hid_t dxpl_id,
							  void **req,
							  va_list arguments);
herr_t H5VL_async_object_specific (void *obj,
								   const H5VL_loc_params_t *loc_params,
								   H5VL_object_specific_t specific_type,
								   hid_t dxpl_id,
								   void **req,
								   va_list arguments);
herr_t H5VL_async_object_optional (
	void *obj,  const H5VL_loc_params_t *loc_params, H5VL_object_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
