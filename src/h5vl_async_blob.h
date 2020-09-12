/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Blob callbacks */

#pragma once

#include <H5VLpublic.h>

herr_t H5VL_async_blob_put (void *obj, const void *buf, size_t size, void *blob_id, void *ctx);
herr_t H5VL_async_blob_get (void *obj, const void *blob_id, void *buf, size_t size, void *ctx);
herr_t H5VL_async_blob_specific (void *obj,
								 void *blob_id,
								 H5VL_blob_specific_t specific_type,
								 va_list arguments);
herr_t H5VL_async_blob_optional (void *obj,
								 void *blob_id,
								 H5VL_blob_optional_t opt_type,
								 va_list arguments);
