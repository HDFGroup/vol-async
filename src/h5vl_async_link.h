/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Link callbacks */

#pragma once

#include <H5VLpublic.h>

herr_t H5VL_async_link_create (H5VL_link_create_type_t create_type,
							   void *obj,
							   const H5VL_loc_params_t *loc_params,
							   hid_t lcpl_id,
							   hid_t lapl_id,
							   hid_t dxpl_id,
							   void **req,
							   va_list arguments);
herr_t H5VL_async_link_copy (void *src_obj,
							 const H5VL_loc_params_t *loc_params1,
							 void *dst_obj,
							 const H5VL_loc_params_t *loc_params2,
							 hid_t lcpl_id,
							 hid_t lapl_id,
							 hid_t dxpl_id,
							 void **req);
herr_t H5VL_async_link_move (void *src_obj,
							 const H5VL_loc_params_t *loc_params1,
							 void *dst_obj,
							 const H5VL_loc_params_t *loc_params2,
							 hid_t lcpl_id,
							 hid_t lapl_id,
							 hid_t dxpl_id,
							 void **req);
herr_t H5VL_async_link_get (void *obj,
							const H5VL_loc_params_t *loc_params,
							H5VL_link_get_t get_type,
							hid_t dxpl_id,
							void **req,
							va_list arguments);
herr_t H5VL_async_link_specific (void *obj,
								 const H5VL_loc_params_t *loc_params,
								 H5VL_link_specific_t specific_type,
								 hid_t dxpl_id,
								 void **req,
								 va_list arguments);
herr_t H5VL_async_link_optional (
	void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
