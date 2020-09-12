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

#include <assert.h>
#include <stdlib.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_token.h"

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_token_cmp
 *
 * Purpose:     Compare two of the connector's object tokens, setting
 *              *cmp_value, following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t H5VL_async_token_cmp (void *obj,
							 const H5O_token_t *token1,
							 const H5O_token_t *token2,
							 int *cmp_value) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL TOKEN Compare\n");
#endif

	/* Sanity checks */
	assert (obj);
	assert (token1);
	assert (token2);
	assert (cmp_value);

	ret_value = H5VLtoken_cmp (o->under_object, o->under_vol_id, token1, token2, cmp_value);

	return ret_value;
} /* end H5VL_async_token_cmp() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_token_to_str
 *
 * Purpose:     Serialize the connector's object token into a string.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t H5VL_async_token_to_str (void *obj,
								H5I_type_t obj_type,
								const H5O_token_t *token,
								char **token_str) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL TOKEN To string\n");
#endif

	/* Sanity checks */
	assert (obj);
	assert (token);
	assert (token_str);

	ret_value = H5VLtoken_to_str (o->under_object, obj_type, o->under_vol_id, token, token_str);

	return ret_value;
} /* end H5VL_async_token_to_str() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_token_from_str
 *
 * Purpose:     Deserialize the connector's object token from a string.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t H5VL_async_token_from_str (void *obj,
								  H5I_type_t obj_type,
								  const char *token_str,
								  H5O_token_t *token) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL TOKEN From string\n");
#endif

	/* Sanity checks */
	assert (obj);
	assert (token);
	assert (token_str);

	ret_value = H5VLtoken_from_str (o->under_object, obj_type, o->under_vol_id, token_str, token);

	return ret_value;
} /* end H5VL_async_token_from_str() */