/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Async VOL declarations */

#pragma once

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <H5VLpublic.h>
#include <H5VLconnector.h>
#include <hdf5dev.h>
#include <taskworks.h>

#include "h5vl_asynci_vector.h"

#define H5VL_ASYNC_UNUSED		__attribute__ ((unused))
#define H5VL_ASYNC_FALL_THROUGH __attribute__ ((fallthrough));

/* Characteristics of the async VOL connector */
#define H5VL_ASYNC_NAME	   "async"
#define H5VL_ASYNC_VALUE   707 /* VOL connector ID */
#define H5VL_ASYNC_VERSION 0

#define ASYNC_VOL_PROP_NAME "h5vl_async"

#define H5VL_async_inc_ref(A) \
	{                         \
		A->ref++;             \
		A->cnt++;             \
	}

#define H5VL_ASYNC_DYN_FILE_WAIT        "gov.lbl.async.file.wait"
#define H5VL_ASYNC_DYN_DATASET_WAIT     "gov.lbl.async.dataset.wait"
#define DEBUG_PRINT printf("%s:%s:%d\n", __FILE__, __func__, __LINE__); fflush(stdout); fflush(stderr);
/************/
/* Typedefs */
/************/

/* The async VOL object status */
typedef enum H5VL_async_stat_t {
	H5VL_async_stat_init,	// The object is still initializing
	H5VL_async_stat_ready,	// Object is initialized and ready
	H5VL_async_stat_err,	// Error with the object
	H5VL_async_stat_close	// Object marked for closing
} H5VL_async_stat_t;

typedef enum H5VL_async_req_stat_t {
    REQ_IN_PROGRESS,
    REQ_FAIL,
    REQ_CANCELLED,
    REQ_SUCCEED
}H5VL_async_req_stat_t;

/* The async VOL object */
typedef struct H5VL_async_t {
    int isReq;
	hid_t under_vol_id; /* ID for underlying VOL connector */
	void *under_object; /* Info object for underlying VOL connector */
	struct H5VL_async_t* shared_file_obj;
	volatile H5VL_async_stat_t stat;
	TW_Task_handle_t prev_task;
	//TW_Task_handle_t req_task; moved to H5VL_async_req_t
	//H5VL_async_req_stat_t req_stat; moved to H5VL_async_req_t
	// TW_Task_handle_t open_task;
	// TW_Task_handle_t close_task;
	// TW_Task_handle_t *tasks;
	// int ntask, ntask_alloc;
	// H5VL_asynci_vector_handle_t tasks;
	H5VL_asynci_mutex_handle_t lock;
	// int ref;
	// int cnt;
	// struct H5VL_async_t *parent;
	hid_t error_stack;  //error stack id.
} H5VL_async_t;

extern hid_t H5VL_ASYNC_g;
extern TW_Engine_handle_t H5VL_async_engine;
extern int H5VL_async_file_wait_op_g;
extern int H5VL_async_dataset_wait_op_g;
/********************* */
/* Function prototypes */
/********************* */

/* Internal functions */
H5VL_async_t *H5VL_async_new_obj (void *under_obj, hid_t under_vol_id);
herr_t H5VL_async_free_obj (H5VL_async_t *obj);
// herr_t H5VL_async_dec_ref (H5VL_async_t *obj);

/* "Management" callbacks */
herr_t H5VL_async_init (hid_t vipl_id);
herr_t H5VL_async_term (void);

/* Container/connector introspection callbacks */
herr_t H5VL_async_introspect_get_conn_cls (void *obj,
										   H5VL_get_conn_lvl_t lvl,
										   const H5VL_class_t **conn_cls);
herr_t H5VL_async_introspect_opt_query (void *obj,
										H5VL_subclass_t cls,
										int opt_type,
										uint64_t *flags);
static herr_t H5VL_async_introspect_get_cap_flags(const void *info, unsigned *cap_flags);
/* Generic optional callback */
herr_t H5VL_async_optional (void *obj, int op_type, hid_t dxpl_id, void **req, va_list arguments);
void dup_loc_param(H5VL_loc_params_t *dest, H5VL_loc_params_t const *loc_params);
uint64_t get_time_usec_uint64();
