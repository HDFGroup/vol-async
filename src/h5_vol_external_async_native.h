/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:	The public header file for the async VOL connector.
 */

#ifndef _H5VLasync_H
#define _H5VLasync_H

/* Public headers needed by this file */
#include "H5VLpublic.h"        /* Virtual Object Layer                 */

/* Identifier for the async VOL connector */
#define H5VL_ASYNC	(H5VL_async_register())

/* Characteristics of the async VOL connector */
#define H5VL_ASYNC_NAME        "async"
#define H5VL_ASYNC_VALUE       707           /* VOL connector ID */
#define H5VL_ASYNC_VERSION     0
#define ASYNC_VOL_DEFAULT_NTHREAD   1
#define ALLOC_INITIAL_SIZE 2
#define ASYNC_VOL_PROP_NAME "use_async_vol"
#define ASYNC_VOL_CP_SIZE_LIMIT_NAME "async_vol_cp_size_limit"
#define ASYNC_VOL_ATTR_CP_SIZE_LIMIT 32768
#define ASYNC_ATTEMPT_CHECK_INTERVAL 8
#define ASYNC_MAGIC 10242048

struct H5VL_async_t;
struct H5RQ_token_int_t;


/* Pass-through VOL connector info */
typedef struct H5VL_async_info_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_vol_info;       /* VOL info for under VOL */
} H5VL_async_info_t;

typedef void * H5RQ_token_t;

#ifdef __cplusplus
extern "C" {
#endif

H5_DLL hid_t H5VL_async_register(void);

herr_t H5Pset_vol_async(hid_t fapl_id);
herr_t H5Pget_dxpl_async(hid_t dxpl, hbool_t *is_async);
herr_t H5Pset_dxpl_async(hid_t dxpl, hbool_t is_async);
herr_t H5Pget_dxpl_async_cp_limit(hid_t dxpl, hsize_t *size);
herr_t H5Pset_dxpl_async_cp_limit(hid_t dxpl, hsize_t size);
herr_t H5Dwait(hid_t dset);
herr_t H5Fwait(hid_t file);
void   H5VLasync_finalize();

/* Experimental token APIs */
herr_t H5RQ_token_check(H5RQ_token_t token, int *status);
herr_t H5RQ_token_wait(H5RQ_token_t token);
herr_t H5RQ_token_free(H5RQ_token_t token);
/* H5RQ_token_t *H5RQ_new_token(void); */

#ifdef __cplusplus
}
#endif

#endif /* _H5VLasync_H */

