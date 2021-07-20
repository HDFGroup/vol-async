/*
 * Purpose:	The public header file for the async VOL connector.
 */

#ifndef _h5_async_vol_H
#define _h5_async_vol_H

/* Public headers needed by this file */
#include "H5VLpublic.h" /* Virtual Object Layer                 */

/* Characteristics of the async VOL connector */
#define H5VL_ASYNC_NAME  "async"
#define H5VL_ASYNC_VALUE 512 /* VOL connector ID */

/* Pass-through VOL connector info (for H5Pset_vol_info) */
typedef struct H5VL_async_info_t {
    hid_t under_vol_id;   /* VOL ID for under VOL */
    void *under_vol_info; /* VOL info for under VOL */
} H5VL_async_info_t;

#endif /* _h5_async_vol_H */
