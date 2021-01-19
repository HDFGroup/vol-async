/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright (C) 2020, Lawrence Berkeley National Laboratory.                *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of AsyncVOL. The full AsyncVOL copyright notice,      *
 * including terms governing use, modification, and redistribution, is       *
 * contained in the file COPYING at the root of the source code distribution *
 * tree.                                                                     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Async VOL implementation */

#include <hdf5.h>
#include <stdlib.h>
#include <taskworks.h>

/* Async VOL headers */
#include "h5vl_async.h"
#include "h5vl_async_attr.h"
#include "h5vl_async_blob.h"
#include "h5vl_async_dset.h"
#include "h5vl_async_dtype.h"
#include "h5vl_async_file.h"
#include "h5vl_async_group.h"
#include "h5vl_async_info.h"
#include "h5vl_async_link.h"
#include "h5vl_async_obj.h"
#include "h5vl_async_req.h"
#include "h5vl_async_token.h"
#include "h5vl_async_wrap.h"
#include "h5vl_asynci.h"

/*******************/
/* Local variables */
/*******************/

/* async VOL connector class struct */
static const H5VL_class_t H5VL_async_g = {
	H5VL_VERSION,	
	(H5VL_class_value_t)H5VL_ASYNC_VALUE, /* value        */
	H5VL_ASYNC_NAME,					  /* name         */
	H5VL_ASYNC_VERSION,                   /* version      */
	0,									  /* capability flags */
	H5VL_async_init,					  /* initialize   */
	H5VL_async_term,					  /* terminate    */
	{
		/* info_cls */
		sizeof (H5VL_async_info_t), /* size    */
		H5VL_async_info_copy,		/* copy    */
		H5VL_async_info_cmp,		/* compare */
		H5VL_async_info_free,		/* free    */
		H5VL_async_info_to_str,		/* to_str  */
		H5VL_async_str_to_info		/* from_str */
	},
	{
		/* wrap_cls */
		H5VL_async_get_object,	  /* get_object   */
		H5VL_async_get_wrap_ctx,  /* get_wrap_ctx */
		H5VL_async_wrap_object,	  /* wrap_object  */
		H5VL_async_unwrap_object, /* unwrap_object */
		H5VL_async_free_wrap_ctx  /* free_wrap_ctx */
	},
	{
		/* attribute_cls */
		H5VL_async_attr_create,	  /* create */
		H5VL_async_attr_open,	  /* open */
		H5VL_async_attr_read,	  /* read */
		H5VL_async_attr_write,	  /* write */
		H5VL_async_attr_get,	  /* get */
		H5VL_async_attr_specific, /* specific */
		H5VL_async_attr_optional, /* optional */
		H5VL_async_attr_close	  /* close */
	},
	{
		/* dataset_cls */
		H5VL_async_dataset_create,	 /* create */
		H5VL_async_dataset_open,	 /* open */
		H5VL_async_dataset_read,	 /* read */
		H5VL_async_dataset_write,	 /* write */
		H5VL_async_dataset_get,		 /* get */
		H5VL_async_dataset_specific, /* specific */
		H5VL_async_dataset_optional, /* optional */
		H5VL_async_dataset_close	 /* close */
	},
	{
		/* datatype_cls */
		H5VL_async_datatype_commit,	  /* commit */
		H5VL_async_datatype_open,	  /* open */
		H5VL_async_datatype_get,	  /* get_size */
		H5VL_async_datatype_specific, /* specific */
		H5VL_async_datatype_optional, /* optional */
		H5VL_async_datatype_close	  /* close */
	},
	{
		/* file_cls */
		H5VL_async_file_create,	  /* create */
		H5VL_async_file_open,	  /* open */
		H5VL_async_file_get,	  /* get */
		H5VL_async_file_specific, /* specific */
		H5VL_async_file_optional, /* optional */
		H5VL_async_file_close	  /* close */
	},
	{
		/* group_cls */
		H5VL_async_group_create,   /* create */
		H5VL_async_group_open,	   /* open */
		H5VL_async_group_get,	   /* get */
		H5VL_async_group_specific, /* specific */
		H5VL_async_group_optional, /* optional */
		H5VL_async_group_close	   /* close */
	},
	{
		/* link_cls */
		H5VL_async_link_create,	  /* create */
		H5VL_async_link_copy,	  /* copy */
		H5VL_async_link_move,	  /* move */
		H5VL_async_link_get,	  /* get */
		H5VL_async_link_specific, /* specific */
		H5VL_async_link_optional  /* optional */
	},
	{
		/* object_cls */
		H5VL_async_object_open,		/* open */
		H5VL_async_object_copy,		/* copy */
		H5VL_async_object_get,		/* get */
		H5VL_async_object_specific, /* specific */
		H5VL_async_object_optional	/* optional */
	},
	{
		/* introspect_cls */
		H5VL_async_introspect_get_conn_cls, /* get_conn_cls */
		H5VL_async_introspect_opt_query,	/* opt_query */
	},
	{
		/* request_cls */
		H5VL_async_request_wait,	 /* wait */
		H5VL_async_request_notify,	 /* notify */
		H5VL_async_request_cancel,	 /* cancel */
		H5VL_async_request_specific, /* specific */
		H5VL_async_request_optional, /* optional */
		H5VL_async_request_free		 /* free */
	},
	{
		/* blob_cls */
		H5VL_async_blob_put,	  /* put */
		H5VL_async_blob_get,	  /* get */
		H5VL_async_blob_specific, /* specific */
		H5VL_async_blob_optional  /* optional */
	},
	{
		/* token_cls */
		H5VL_async_token_cmp,	  /* cmp */
		H5VL_async_token_to_str,  /* to_str */
		H5VL_async_token_from_str /* from_str */
	},
	H5VL_async_optional /* optional */
};

/* The connector identification number, initialized at runtime */
hid_t H5VL_ASYNC_g = H5I_INVALID_HID;

/* Engine to run task */
TW_Engine_handle_t H5VL_async_engine;

H5PL_type_t H5PLget_plugin_type(void) {
    return H5PL_TYPE_VOL;
}
const void *H5PLget_plugin_info(void) {
    return &H5VL_async_g;
}

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_new_obj
 *
 * Purpose:     Create a new async object for an underlying object
 *
 * Return:      Success:    Pointer to the new async object
 *              Failure:    NULL
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
#define H5VL_ASYNC_OBJ_NTASK_INIT 32
H5VL_async_t *H5VL_async_new_obj () {
	herr_t err = 0;
	H5VL_async_t *new_obj;

	new_obj		  = (H5VL_async_t *)calloc (1, sizeof (H5VL_async_t));
	new_obj->stat = H5VL_async_stat_init;
	// new_obj->tasks = NULL;
	// new_obj->ref = 0;
	// new_obj->tasks = H5VL_asynci_vector_create ();
	// CHECK_PTR (new_obj->tasks)
	new_obj->lock = H5VL_asynci_mutex_create ();
	CHECK_PTR (new_obj->lock)
	// new_obj->prev_task	= TW_HANDLE_NULL;
	// new_obj->close_task = TW_HANDLE_NULL;
	new_obj->prev_task = TW_HANDLE_NULL;

	// new_obj->ntask		 = 0;
	// new_obj->ntask_alloc = H5VL_ASYNC_OBJ_NTASK_INIT;
	// new_obj->tasks == (TW_Task_handle_t *)malloc (sizeof (TW_Task_handle_t));

err_out:;
	if (err) { free (new_obj); }
	return new_obj;
} /* end H5VL_async_new_obj() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_free_obj
 *
 * Purpose:     Release a async object
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_free_obj (H5VL_async_t *obj) {
	hid_t err_id;

	err_id = H5Eget_current_stack ();

	H5Idec_ref (obj->under_vol_id);

	H5Eset_current_stack (err_id);

	// H5VL_asynci_vector_free (obj->tasks);
	H5VL_asynci_mutex_free (obj->lock);

	free (obj);

	return 0;
} /* end H5VL_async_free_obj() */

/*
herr_t H5VL_async_dec_ref (H5VL_async_t *obj) {
	herr_t err	 = 0;
	terr_t twerr = TW_SUCCESS;

	obj->ref--;
	if ((obj->stat == H5VL_async_stat_close) && (obj->ref == 0) &&
		(obj->close_task != TW_HANDLE_NULL)) {
		twerr			= TW_Task_commit (obj->close_task, H5VL_async_engine);
		obj->close_task = TW_HANDLE_NULL;
		CHK_TWERR
	}

err_out:;
	return err;
}
*/

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_register
 *
 * Purpose:     Register the pass-through VOL connector and retrieve an ID
 *              for it.
 *
 * Return:      Success:    The ID for the pass-through VOL connector
 *              Failure:    -1
 *
 * Programmer:  Quincey Koziol
 *              Wednesday, November 28, 2018
 *
 *-------------------------------------------------------------------------
 */
hid_t H5VL_async_register (void) {
	/* Singleton register the pass-through VOL connector ID */
	if (H5VL_ASYNC_g < 0) H5VL_ASYNC_g = H5VLregister_connector (&H5VL_async_g, H5P_DEFAULT);
	DEBUG_PRINT
	printf("H5VL_ASYNC_g = %d\n", H5VL_ASYNC_g);
	return H5VL_ASYNC_g;
} /* end H5VL_async_register() */

static int H5VL_async_file_wait_op_g = -1;
static int H5VL_async_dataset_wait_op_g = -1;

int _optional_ops_reg(){
    /* Register operation values for new API routines to use for operations */
    assert(-1 == H5VL_async_file_wait_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_WAIT, &H5VL_async_file_wait_op_g) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with H5VLregister_opt_operation\n");
        return(-1);
    }
    assert(-1 != H5VL_async_file_wait_op_g);
    assert(-1 == H5VL_async_dataset_wait_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_WAIT, &H5VL_async_dataset_wait_op_g) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with H5VLregister_opt_operation\n");
        return(-1);
    }
    assert(-1 != H5VL_async_dataset_wait_op_g);

//    /* Singleton register error class */
//    if (H5I_INVALID_HID == async_error_class_g) {
//        if((async_error_class_g = H5Eregister_class("Async VOL", "Async VOL", "0.1")) < 0) {
//            fprintf(stderr, "  [ASYNC VOL ERROR] with H5Eregister_class\n");
//            return -1;
//        }
//    }
    return 0;
}

int _optional_ops_unreg(){
    /* Reset operation values for new "API" routines */
    if(-1 != H5VL_async_file_wait_op_g) {
        if(H5VLunregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_WAIT) < 0)
            return(-1);
        H5VL_async_file_wait_op_g = (-1);
    } /* end if */
    if(-1 != H5VL_async_dataset_wait_op_g) {
        if(H5VLunregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_WAIT) < 0)
            return(-1);
        H5VL_async_dataset_wait_op_g = (-1);
    } /* end if */

    /* Unregister error class */
//    if(H5I_INVALID_HID != async_error_class_g) {
//        if (H5Eunregister_class(async_error_class_g) < 0)
//            fprintf(stderr,"  [ASYNC VOL ERROR] ASYNC VOL unregister error class failed\n");
//        async_error_class_g = H5I_INVALID_HID;
//    }
    return 0;
}
/*-------------------------------------------------------------------------
 * Function:    H5VL_async_init
 *
 * Purpose:     Initialize this VOL connector, performing any necessary
 *              operations for the connector that will apply to all containers
 *              accessed with the connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_init (hid_t H5VL_ASYNC_UNUSED vipl_id) {
	herr_t err = 0;
	terr_t twerr;
#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL INIT\n");
#endif

	twerr = TW_Init (TW_Backend_native, TW_Event_backend_none, NULL, NULL);
	CHK_TWERR

	twerr = TW_Engine_create (0, &H5VL_async_engine);
	CHK_TWERR
	if(_optional_ops_reg() < 0)
	    return -1;
err_out:;
	return err;
} /* end H5VL_async_init() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_term
 *
 * Purpose:     Terminate this VOL connector, performing any necessary
 *              operations for the connector that release connector-wide
 *              resources (usually created / initialized with the 'init'
 *              callback).
 *
 * Return:      Success:    0
 *              Failure:    (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
herr_t H5VL_async_term (void) {
	herr_t err = 0;
	terr_t twerr;
#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL TERM\n");
#endif

	twerr = TW_Engine_free (H5VL_async_engine);
	CHK_TWERR

	twerr = TW_Finalize ();
	CHK_TWERR

    if(_optional_ops_unreg() < 0)
        return -1;

	/* Reset VOL ID */
	H5VL_ASYNC_g = H5I_INVALID_HID;

err_out:;
	return err;
} /* end H5VL_async_term() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_introspect_get_conn_clss
 *
 * Purpose:     Query the connector class.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_introspect_get_conn_cls (void *obj,
										   H5VL_get_conn_lvl_t lvl,
										   const H5VL_class_t **conn_cls) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL INTROSPECT GetConnCls\n");
#endif

	/* Check for querying this connector's class */
	if (H5VL_GET_CONN_LVL_CURR == lvl) {
		*conn_cls = &H5VL_async_g;
		ret_value = 0;
	} /* end if */
	else
		ret_value = H5VLintrospect_get_conn_cls (o->under_object, o->under_vol_id, lvl, conn_cls);

	return ret_value;
} /* end H5VL_async_introspect_get_conn_cls() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_introspect_opt_query
 *
 * Purpose:     Query if an optional operation is supported by this connector
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_introspect_opt_query (void *obj,
										H5VL_subclass_t cls,
										int opt_type,
										hbool_t *supported) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL INTROSPECT OptQuery\n");
#endif

	ret_value =
		H5VLintrospect_opt_query (o->under_object, o->under_vol_id, cls, opt_type, supported);

	return ret_value;
} /* end H5VL_async_introspect_opt_query() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_optional
 *
 * Purpose:     Handles the generic 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t H5VL_async_optional (void *obj, int op_type, hid_t dxpl_id, void **req, va_list arguments) {
	H5VL_async_t *o = (H5VL_async_t *)obj;
	herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
	printf ("------- ASYNC VOL generic Optional\n");
#endif

	ret_value = H5VLoptional (o->under_object, o->under_vol_id, op_type, dxpl_id, req, arguments);

	return ret_value;
} /* end H5VL_async_optional() */
