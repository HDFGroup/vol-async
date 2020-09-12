/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Attribute callbacks */

#pragma once

#include "h5vl_async.h"

void *H5VL_async_attr_create (void *obj,
							  const H5VL_loc_params_t *loc_params,
							  const char *name,
							  hid_t type_id,
							  hid_t space_id,
							  hid_t acpl_id,
							  hid_t aapl_id,
							  hid_t dxpl_id,
							  void **req);
void *H5VL_async_attr_open (void *obj,
							const H5VL_loc_params_t *loc_params,
							const char *name,
							hid_t aapl_id,
							hid_t dxpl_id,
							void **req);
herr_t H5VL_async_attr_read (void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req);
herr_t H5VL_async_attr_write (
	void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req);
herr_t H5VL_async_attr_get (
	void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_attr_specific (void *obj,
								 const H5VL_loc_params_t *loc_params,
								 H5VL_attr_specific_t specific_type,
								 hid_t dxpl_id,
								 void **req,
								 va_list arguments);
herr_t H5VL_async_attr_optional (
	void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_async_attr_close (void *attr, hid_t dxpl_id, void **req);
