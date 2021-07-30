/*
 * Purpose:	The private header file for the async VOL connector.
 */

#ifndef _h5_async_vol_private_H
#define _h5_async_vol_private_H

/* Public headers needed by this file */
#include "h5_async_vol.h" /* Public header for connector */

/* Private characteristics of the pass-through VOL connector */
#define H5VL_ASYNC_VERSION 0

/* Names for dynamically registered operations */
#define H5VL_ASYNC_DYN_FILE_WAIT         "gov.lbl.async.file.wait"
#define H5VL_ASYNC_DYN_DATASET_WAIT      "gov.lbl.async.dataset.wait"
#define H5VL_ASYNC_DYN_FILE_START        "gov.lbl.async.file.start"
#define H5VL_ASYNC_DYN_DATASET_START     "gov.lbl.async.dataset.start"
#define H5VL_ASYNC_DYN_FILE_PAUSE        "gov.lbl.async.file.pause"
#define H5VL_ASYNC_DYN_DATASET_PAUSE     "gov.lbl.async.dataset.pause"
#define H5VL_ASYNC_DYN_FILE_DELAY        "gov.lbl.async.file.delay"
#define H5VL_ASYNC_DYN_DATASET_DELAY     "gov.lbl.async.dataset.delay"
#define H5VL_ASYNC_DYN_REQUEST_START     "gov.lbl.async.request.start"
#define H5VL_ASYNC_DYN_REQUEST_DEP       "gov.lbl.async.request.dep"
#define H5VL_ASYNC_DISABLE_IMPLICIT_NAME "gov.lbl.async.disable.implicit"
#define H5VL_ASYNC_DELAY_NAME            "gov.lbl.async.delay"
#define H5VL_ASYNC_PAUSE_NAME            "gov.lbl.async.pause"

/* Parameters for each of the dynamically registered operations */

/* H5VL_ASYNC_DYN_DATASET_DELAY */
/* H5VL_ASYNC_DYN_FILE_DELAY */
typedef struct H5VL_async_delay_args_t {
    uint64_t delay_time;
} H5VL_async_delay_args_t;

/* H5VL_ASYNC_DYN_REQUEST_DEP */
typedef struct H5VL_async_req_dep_args_t {
    void *parent_req;
} H5VL_async_req_dep_args_t;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif /* _h5_async_vol_private_H */
