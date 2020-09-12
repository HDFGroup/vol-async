/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Token callbacks */

#pragma once

#include <H5VLpublic.h>

herr_t H5VL_async_token_cmp (void *obj,
							 const H5O_token_t *token1,
							 const H5O_token_t *token2,
							 int *cmp_value);
herr_t H5VL_async_token_to_str (void *obj,
								H5I_type_t obj_type,
								const H5O_token_t *token,
								char **token_str);
herr_t H5VL_async_token_from_str (void *obj,
								  H5I_type_t obj_type,
								  const char *token_str,
								  H5O_token_t *token);