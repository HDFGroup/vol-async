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
 * Purpose:     This is a "async" VOL connector, which forwards each
 *              VOL callback to an underlying connector.
 *
 *              Note that the HDF5 error stack must be preserved on code paths
 *              that could be invoked when the underlying VOL connector's
 *              callback can fail.
 *
 */


/* Header files needed */
#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Public HDF5 file */
#include "hdf5.h"

/* This connector's header */
#include "h5_vol_external_async_native.h"

/* Argobots header */
#include "abt.h"

/* Universal linked lists header */
#include "utlist.h"

/* #define ENABLE_LOG                  1 */
/* #define ENABLE_DBG_MSG              1 */
/* #define ENABLE_TIMING              1 */


/**********/
/* Macros */
/**********/

/* Whether to display log messge when callback is invoked */
/* (Uncomment to enable) */
/* #define ENABLE_ASYNC_LOGGING */

/* Hack for missing va_copy() in old Visual Studio editions
 * (from H5win2_defs.h - used on VS2012 and earlier)
 */
#if defined(_WIN32) && defined(_MSC_VER) && (_MSC_VER < 1800)
#define va_copy(D,S)      ((D) = (S))
#endif

/************/
/* Typedefs */
/************/


/* The async VOL wrapper context */
typedef struct H5VL_async_wrap_ctx_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_wrap_ctx;       /* Object wrapping context for under VOL */
} H5VL_async_wrap_ctx_t;


typedef enum {QTYPE_NONE, REGULAR, DEPENDENT, COLLECTIVE} task_list_qtype;
typedef enum {OP_NONE, READ, WRITE} obj_op_type;

const char* qtype_names_g[4] = {"QTYPE_NONE", "REGULAR", "DEPENDENT", "COLLECTIVE"};

typedef struct async_task_t {
    ABT_mutex           task_mutex;
    void                *h5_state;
    void                (*func)(void *);
    void                *args;
    obj_op_type         op;
    hid_t               under_vol_id;
    struct H5VL_async_t *async_obj;
    ABT_eventual        eventual;
    int                 in_abt_pool;
    int                 is_done;
    ABT_task            abt_task;
    ABT_thread          abt_thread;

    int                 n_dep;
    int                 n_dep_alloc;
    struct async_task_t **dep_tasks;

    struct H5VL_async_t *parent_obj;            /* pointer back to the parent async object */

    struct H5RQ_token_int_t *token;

    struct async_task_t *prev;
    struct async_task_t *next;
    struct async_task_t *file_list_prev;
    struct async_task_t *file_list_next;
} async_task_t;

typedef struct H5VL_async_t {
    int                 magic;
    void                *under_object;
    hid_t               under_vol_id;
    int                 is_obj_valid;
    async_task_t        *create_task;           /* task that creates the object */
    async_task_t        *file_task_list_head;
    ABT_mutex           file_task_list_mutex;
    struct H5VL_async_t *file_async_obj;
    int                 task_cnt;
    int                 attempt_check_cnt;
    ABT_mutex           obj_mutex;
    ABT_pool            *pool_ptr;
    hbool_t             is_col_meta;
    hbool_t             is_raw_io_delay;
    // To make dset write not requiring dset create to complete (still requries dset open to complete)
    hbool_t             has_dset_size;
    hsize_t             dtype_size;
    hsize_t             dset_size;
} H5VL_async_t;

typedef struct async_task_list_t {
    task_list_qtype     type;
    async_task_t        *task_list;

    struct async_task_list_t *prev;
    struct async_task_list_t *next;
} async_task_list_t;

typedef struct async_qhead_t {
    ABT_mutex           head_mutex;
    async_task_list_t   *queue;
} async_qhead_t;

typedef struct async_instance_t {
    async_qhead_t       qhead;
    ABT_pool            pool;
    int                 num_xstreams;
    ABT_xstream         *xstreams;
    ABT_xstream         *scheds;
    int                 nfopen;
    bool                env_async;           /* Async VOL is enabled by env var */
    bool                ex_delay;            /* Delay background thread execution */
    bool                ex_fclose;           /* Delay background thread execution until file close */
    bool                ex_gclose;           /* Delay background thread execution until group close */
    bool                ex_dclose;           /* Delay background thread execution until dset close */
    bool                start_abt_push;      /* Start pushing tasks to Argobots pool */
} async_instance_t;

typedef struct H5RQ_token_int_t {
    struct async_task_t *task;
} H5RQ_token_int_t;

typedef struct async_attr_create_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    type_id;
    hid_t                    space_id;
    hid_t                    acpl_id;
    hid_t                    aapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_attr_create_args_t;

typedef struct async_attr_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    aapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_attr_open_args_t;

typedef struct async_attr_read_args_t {
    void                     *attr;
    hid_t                    mem_type_id;
    void                     *buf;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_attr_read_args_t;

typedef struct async_attr_write_args_t {
    void                     *attr;
    hid_t                    mem_type_id;
    void                     *buf;
    hid_t                    dxpl_id;
    void                     **req;
    hbool_t                   buf_free;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_attr_write_args_t;

typedef struct async_attr_get_args_t {
    void                     *obj;
    H5VL_attr_get_t          get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_attr_get_args_t;

typedef struct async_attr_specific_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_attr_specific_t     specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_attr_specific_args_t;

typedef struct async_attr_optional_args_t {
    void                     *obj;
    H5VL_attr_optional_t     opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_attr_optional_args_t;

typedef struct async_attr_close_args_t {
    void                     *attr;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_attr_close_args_t;

typedef struct async_dataset_create_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    lcpl_id;
    hid_t                    type_id;
    hid_t                    space_id;
    hid_t                    dcpl_id;
    hid_t                    dapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_dataset_create_args_t;

typedef struct async_dataset_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    dapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_dataset_open_args_t;

typedef struct async_dataset_read_args_t {
    void                     *dset;
    hid_t                    mem_type_id;
    hid_t                    mem_space_id;
    hid_t                    file_space_id;
    hid_t                    plist_id;
    void                     *buf;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_dataset_read_args_t;

typedef struct async_dataset_write_args_t {
    void                     *dset;
    hid_t                    mem_type_id;
    hid_t                    mem_space_id;
    hid_t                    file_space_id;
    hid_t                    plist_id;
    void                     *buf;
    void                     **req;
    hbool_t                   buf_free;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_dataset_write_args_t;

typedef struct async_dataset_get_args_t {
    void                     *dset;
    H5VL_dataset_get_t       get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_dataset_get_args_t;

typedef struct async_dataset_specific_args_t {
    void                     *obj;
    H5VL_dataset_specific_t  specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_dataset_specific_args_t;

typedef struct async_dataset_optional_args_t {
    void                     *obj;
    H5VL_dataset_optional_t  opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_dataset_optional_args_t;

typedef struct async_dataset_close_args_t {
    void                     *dset;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_dataset_close_args_t;

typedef struct async_datatype_commit_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    type_id;
    hid_t                    lcpl_id;
    hid_t                    tcpl_id;
    hid_t                    tapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_datatype_commit_args_t;

typedef struct async_datatype_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    tapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_datatype_open_args_t;

typedef struct async_datatype_get_args_t {
    void                     *dt;
    H5VL_datatype_get_t      get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_datatype_get_args_t;

typedef struct async_datatype_specific_args_t {
    void                     *obj;
    H5VL_datatype_specific_t specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_datatype_specific_args_t;

typedef struct async_datatype_optional_args_t {
    void                     *obj;
    H5VL_datatype_optional_t opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_datatype_optional_args_t;

typedef struct async_datatype_close_args_t {
    void                     *dt;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_datatype_close_args_t;

typedef struct async_file_create_args_t {
    char                     *name;
    unsigned                 flags;
    hid_t                    fcpl_id;
    hid_t                    fapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_file_create_args_t;

typedef struct async_file_open_args_t {
    char                     *name;
    unsigned                 flags;
    hid_t                    fapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_file_open_args_t;

typedef struct async_file_get_args_t {
    void                     *file;
    H5VL_file_get_t          get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_file_get_args_t;

typedef struct async_file_specific_args_t {
    void                     *file;
    H5VL_file_specific_t     specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_file_specific_args_t;

typedef struct async_file_optional_args_t {
    void                     *file;
    H5VL_file_optional_t     opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_file_optional_args_t;

typedef struct async_file_close_args_t {
    void                     *file;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_file_close_args_t;

typedef struct async_group_create_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    lcpl_id;
    hid_t                    gcpl_id;
    hid_t                    gapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_group_create_args_t;

typedef struct async_group_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    gapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_group_open_args_t;

typedef struct async_group_get_args_t {
    void                     *obj;
    H5VL_group_get_t         get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_group_get_args_t;

typedef struct async_group_specific_args_t {
    void                     *obj;
    H5VL_group_specific_t    specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_group_specific_args_t;

typedef struct async_group_optional_args_t {
    void                     *obj;
    H5VL_group_optional_t    opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_group_optional_args_t;

typedef struct async_group_close_args_t {
    void                     *grp;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_group_close_args_t;

typedef struct async_link_create_args_t {
    H5VL_link_create_type_t  create_type;
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    hid_t                    lcpl_id;
    hid_t                    lapl_id;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_link_create_args_t;

typedef struct async_link_copy_args_t {
    void                     *src_obj;
    H5VL_loc_params_t        *loc_params1;
    void                     *dst_obj;
    H5VL_loc_params_t        *loc_params2;
    hid_t                    lcpl_id;
    hid_t                    lapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_link_copy_args_t;

typedef struct async_link_move_args_t {
    void                     *src_obj;
    H5VL_loc_params_t        *loc_params1;
    void                     *dst_obj;
    H5VL_loc_params_t        *loc_params2;
    hid_t                    lcpl_id;
    hid_t                    lapl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_link_move_args_t;

typedef struct async_link_get_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_link_get_t          get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_link_get_args_t;

typedef struct async_link_specific_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_link_specific_t     specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_link_specific_args_t;

typedef struct async_link_optional_args_t {
    void                     *obj;
    H5VL_link_optional_t     opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_link_optional_args_t;

typedef struct async_object_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5I_type_t               *opened_type;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_object_open_args_t;

typedef struct async_object_copy_args_t {
    void                     *src_obj;
    H5VL_loc_params_t        *src_loc_params;
    char                     *src_name;
    void                     *dst_obj;
    H5VL_loc_params_t        *dst_loc_params;
    char                     *dst_name;
    hid_t                    ocpypl_id;
    hid_t                    lcpl_id;
    hid_t                    dxpl_id;
    void                     **req;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_object_copy_args_t;

typedef struct async_object_get_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_object_get_t        get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_object_get_args_t;

typedef struct async_object_specific_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_object_specific_t   specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_object_specific_args_t;

typedef struct async_object_optional_args_t {
    void                     *obj;
    H5VL_object_optional_t   opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
#ifdef ENABLE_TIMING
    struct timeval create_time;
    struct timeval start_time;
#endif
} async_object_optional_args_t;


/*******************/
/* Global Variables*/
/*******************/
ABT_mutex           async_instance_mutex_g;
async_instance_t   *async_instance_g  = NULL;
hid_t               async_connector_id_g = -1;

/********************* */
/* Function prototypes */
/********************* */

/* Helper routines */
static herr_t H5VL_async_file_specific_reissue(void *obj, hid_t connector_id,
    H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, ...);
static herr_t H5VL_async_request_specific_reissue(void *obj, hid_t connector_id,
    H5VL_request_specific_t specific_type, ...);
static herr_t H5VL_async_link_create_reissue(H5VL_link_create_type_t create_type,
    void *obj, const H5VL_loc_params_t *loc_params, hid_t connector_id,
    hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, ...);
static H5VL_async_t *H5VL_async_new_obj(void *under_obj,
    hid_t under_vol_id);
static herr_t H5VL_async_free_obj(H5VL_async_t *obj);

/* "Management" callbacks */
static herr_t H5VL_async_init(hid_t vipl_id);
static herr_t H5VL_async_term(void);

/* VOL info callbacks */
static void *H5VL_async_info_copy(const void *info);
static herr_t H5VL_async_info_cmp(int *cmp_value, const void *info1, const void *info2);
static herr_t H5VL_async_info_free(void *info);
static herr_t H5VL_async_info_to_str(const void *info, char **str);
static herr_t H5VL_async_str_to_info(const char *str, void **info);

/* VOL object wrap / retrieval callbacks */
static void *H5VL_async_get_object(const void *obj);
static herr_t H5VL_async_get_wrap_ctx(const void *obj, void **wrap_ctx);
static void *H5VL_async_wrap_object(void *obj, H5I_type_t obj_type,
    void *wrap_ctx);
static void *H5VL_async_unwrap_object(void *obj);
static herr_t H5VL_async_free_wrap_ctx(void *obj);

/* Attribute callbacks */
static void *H5VL_async_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req);
static void *H5VL_async_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_async_attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_async_attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_attr_close(void *attr, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void *H5VL_async_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void *H5VL_async_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                                    hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_async_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
static herr_t H5VL_async_dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_dataset_specific(void *obj, H5VL_dataset_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_dataset_optional(void *obj, H5VL_dataset_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* Datatype callbacks */
static void *H5VL_async_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req);
static void *H5VL_async_datatype_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t tapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_datatype_get(void *dt, H5VL_datatype_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_datatype_specific(void *obj, H5VL_datatype_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_datatype_optional(void *obj, H5VL_datatype_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_datatype_close(void *dt, hid_t dxpl_id, void **req);

/* File callbacks */
static void *H5VL_async_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
static void *H5VL_async_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_file_specific(void *file, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
static void *H5VL_async_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static void *H5VL_async_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_group_specific(void *obj, H5VL_group_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_group_optional(void *obj, H5VL_group_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_group_close(void *grp, hid_t dxpl_id, void **req);

/* Link callbacks */
static herr_t H5VL_async_link_create(H5VL_link_create_type_t create_type, void *obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_link_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_link_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_link_optional(void *obj, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);

/* Object callbacks */
static void *H5VL_async_object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req);
static herr_t H5VL_async_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_object_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_async_object_optional(void *obj, H5VL_object_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);

/* Container/connector introspection callbacks */
static herr_t H5VL_async_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls);
static herr_t H5VL_async_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported);

/* Async request callbacks */
static herr_t H5VL_async_request_wait(void *req, uint64_t timeout, H5ES_status_t *status);
static herr_t H5VL_async_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx);
static herr_t H5VL_async_request_cancel(void *req);
static herr_t H5VL_async_request_specific(void *req, H5VL_request_specific_t specific_type, va_list arguments);
static herr_t H5VL_async_request_optional(void *req, H5VL_request_optional_t opt_type, va_list arguments);
static herr_t H5VL_async_request_free(void *req);

/* Blob callbacks */
static herr_t H5VL_async_blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx);
static herr_t H5VL_async_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx);
static herr_t H5VL_async_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_t specific_type, va_list arguments);
static herr_t H5VL_async_blob_optional(void *obj, void *blob_id, H5VL_blob_optional_t opt_type, va_list arguments);

/* Token callbacks */
static herr_t H5VL_async_token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value);
static herr_t H5VL_async_token_to_str(void *obj, H5I_type_t obj_type, const H5O_token_t *token, char **token_str);
static herr_t H5VL_async_token_from_str(void *obj, H5I_type_t obj_type, const char *token_str, H5O_token_t *token);

/* Generic optional callback */
static herr_t H5VL_async_optional(void *obj, int op_type, hid_t dxpl_id, void **req, va_list arguments);


/*******************/
/* Local variables */
/*******************/

/* async VOL connector class struct */
static const H5VL_class_t H5VL_async_g = {
    H5VL_ASYNC_VERSION,                          /* version      */
    (H5VL_class_value_t)H5VL_ASYNC_VALUE,        /* value        */
    H5VL_ASYNC_NAME,                             /* name         */
    0,                                              /* capability flags */
    H5VL_async_init,                         /* initialize   */
    H5VL_async_term,                         /* terminate    */
    {                                           /* info_cls */
        sizeof(H5VL_async_info_t),           /* size    */
        H5VL_async_info_copy,                /* copy    */
        H5VL_async_info_cmp,                 /* compare */
        H5VL_async_info_free,                /* free    */
        H5VL_async_info_to_str,              /* to_str  */
        H5VL_async_str_to_info               /* from_str */
    },
    {                                           /* wrap_cls */
        H5VL_async_get_object,               /* get_object   */
        H5VL_async_get_wrap_ctx,             /* get_wrap_ctx */
        H5VL_async_wrap_object,              /* wrap_object  */
        H5VL_async_unwrap_object,            /* unwrap_object */
        H5VL_async_free_wrap_ctx             /* free_wrap_ctx */
    },
    {                                           /* attribute_cls */
        H5VL_async_attr_create,              /* create */
        H5VL_async_attr_open,                /* open */
        H5VL_async_attr_read,                /* read */
        H5VL_async_attr_write,               /* write */
        H5VL_async_attr_get,                 /* get */
        H5VL_async_attr_specific,            /* specific */
        H5VL_async_attr_optional,            /* optional */
        H5VL_async_attr_close                /* close */
    },
    {                                           /* dataset_cls */
        H5VL_async_dataset_create,           /* create */
        H5VL_async_dataset_open,             /* open */
        H5VL_async_dataset_read,             /* read */
        H5VL_async_dataset_write,            /* write */
        H5VL_async_dataset_get,              /* get */
        H5VL_async_dataset_specific,         /* specific */
        H5VL_async_dataset_optional,         /* optional */
        H5VL_async_dataset_close             /* close */
    },
    {                                           /* datatype_cls */
        H5VL_async_datatype_commit,          /* commit */
        H5VL_async_datatype_open,            /* open */
        H5VL_async_datatype_get,             /* get_size */
        H5VL_async_datatype_specific,        /* specific */
        H5VL_async_datatype_optional,        /* optional */
        H5VL_async_datatype_close            /* close */
    },
    {                                           /* file_cls */
        H5VL_async_file_create,              /* create */
        H5VL_async_file_open,                /* open */
        H5VL_async_file_get,                 /* get */
        H5VL_async_file_specific,            /* specific */
        H5VL_async_file_optional,            /* optional */
        H5VL_async_file_close                /* close */
    },
    {                                           /* group_cls */
        H5VL_async_group_create,             /* create */
        H5VL_async_group_open,               /* open */
        H5VL_async_group_get,                /* get */
        H5VL_async_group_specific,           /* specific */
        H5VL_async_group_optional,           /* optional */
        H5VL_async_group_close               /* close */
    },
    {                                           /* link_cls */
        H5VL_async_link_create,              /* create */
        H5VL_async_link_copy,                /* copy */
        H5VL_async_link_move,                /* move */
        H5VL_async_link_get,                 /* get */
        H5VL_async_link_specific,            /* specific */
        H5VL_async_link_optional             /* optional */
    },
    {                                           /* object_cls */
        H5VL_async_object_open,              /* open */
        H5VL_async_object_copy,              /* copy */
        H5VL_async_object_get,               /* get */
        H5VL_async_object_specific,          /* specific */
        H5VL_async_object_optional           /* optional */
    },
    {                                           /* introspect_cls */
        H5VL_async_introspect_get_conn_cls,  /* get_conn_cls */
        H5VL_async_introspect_opt_query,     /* opt_query */
    },
    {                                           /* request_cls */
        H5VL_async_request_wait,             /* wait */
        H5VL_async_request_notify,           /* notify */
        H5VL_async_request_cancel,           /* cancel */
        H5VL_async_request_specific,         /* specific */
        H5VL_async_request_optional,         /* optional */
        H5VL_async_request_free              /* free */
    },
    {                                           /* blob_cls */
        H5VL_async_blob_put,                 /* put */
        H5VL_async_blob_get,                 /* get */
        H5VL_async_blob_specific,            /* specific */
        H5VL_async_blob_optional             /* optional */
    },
    {                                           /* token_cls */
        H5VL_async_token_cmp,                /* cmp */
        H5VL_async_token_to_str,             /* to_str */
        H5VL_async_token_from_str              /* from_str */
    },
    H5VL_async_optional                  /* optional */
};

/* The connector identification number, initialized at runtime */
static hid_t H5VL_ASYNC_g = H5I_INVALID_HID;

H5PL_type_t H5PLget_plugin_type(void) {return H5PL_TYPE_VOL;}
const void *H5PLget_plugin_info(void) {return &H5VL_async_g;}

static herr_t
async_init(hid_t vipl_id)
{
    herr_t ret_val = 1;
    int abt_ret;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] Success with async vol registration\n");
#endif

    /* Only init argobots once */
    if (ABT_SUCCESS != ABT_initialized()) {
        abt_ret = ABT_init(0, NULL);
        if (ABT_SUCCESS != abt_ret) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with Argobots init\n");
            ret_val = -1;
            goto done;
        }
#ifdef ENABLE_LOG
        else
            fprintf(stderr, "  [ASYNC VOL DBG] Success with Argobots init\n");
#endif

        /* Create a mutex for argobots I/O instance */
        abt_ret = ABT_mutex_create(&async_instance_mutex_g);
        if (ABT_SUCCESS != abt_ret) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with mutex create\n");
            ret_val = -1;
            goto done;
        }
    }

done:
    return ret_val;
}

herr_t
async_instance_finalize(void)
{
    int abt_ret, ret_val = 1;
    int i;

    if (NULL == async_instance_g)
        return 0;

    abt_ret = ABT_mutex_lock(async_instance_mutex_g);
    if (abt_ret != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_mutex_lock\n");
        ret_val = -1;
    }

    abt_ret = ABT_mutex_free(&async_instance_g->qhead.head_mutex);
    if (ABT_SUCCESS != abt_ret) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with head_mutex free\n", __func__);
        ret_val = -1;
    }

    if (async_instance_g->num_xstreams) {
        for (i = 0; i < async_instance_g->num_xstreams; i++) {
            ABT_xstream_join(async_instance_g->xstreams[i]);
            ABT_xstream_free(&async_instance_g->xstreams[i]);
        }
        free(async_instance_g->xstreams);
        /* Pool gets implicitly freed */
    }

    free(async_instance_g);
    async_instance_g = NULL;

    abt_ret = ABT_mutex_unlock(async_instance_mutex_g);
    if (abt_ret != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_mutex_unlock\n");
        ret_val = -1;
    }

#ifdef ENABLE_LOG
    fprintf(stderr, "  [ASYNC VOL DBG] Success with async_instance_finalize\n");
#endif

    return ret_val;
} // End async_instance_finalize

static herr_t
async_term(void)
{
    herr_t ret_val = 1;
    int    abt_ret;

    ret_val = async_instance_finalize();
    if (ret_val < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_instance_finalize\n");
        return -1;
    }

    /* Free the mutex */
    if (async_instance_mutex_g) {
        abt_ret = ABT_mutex_free(&async_instance_mutex_g);
        if (ABT_SUCCESS != abt_ret) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with mutex free\n");
            ret_val = -1;
            goto done;
        }
        async_instance_mutex_g = NULL;
    }

    /* abt_ret = ABT_finalize(); */
    /* if (ABT_SUCCESS != abt_ret) { */
    /*     fprintf(stderr, "  [ASYNC VOL ERROR] with finalize argobots\n"); */
    /*     ret_val = -1; */
    /*     goto done; */
    /* } */
    /* #ifdef ENABLE_DBG_MSG */
    /* else */
    /*     fprintf(stderr, "  [ASYNC VOL DBG] Success with Argobots finalize\n"); */
    /* #endif */

done:
    return ret_val;
}

/* Init Argobots for async IO */
herr_t
async_instance_init(int backing_thread_count)
{
    herr_t hg_ret = 0;
    async_instance_t *aid;
    ABT_pool pool;
    ABT_xstream self_xstream;
    ABT_xstream *progress_xstreams = NULL;
    ABT_sched *progress_scheds = NULL;
    int abt_ret, i;
    const char *env_var;

    if (backing_thread_count < 0) return -1;

    if (NULL != async_instance_g)
        return 1;

    async_init(H5P_DEFAULT);

#ifdef ENABLE_DBG_MSG
    fprintf(stderr, "  [ASYNC VOL DBG] Init Argobots with %d threads\n", backing_thread_count);
#endif

    /* Use mutex to guarentee there is only one Argobots IO instance (singleton) */
    abt_ret = ABT_mutex_lock(async_instance_mutex_g);
    if (abt_ret != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_mutex_lock\n");
        return -1;
    }

    aid = (async_instance_t*)calloc(1, sizeof(*aid));
    if (aid == NULL) { hg_ret = -1; goto done; }

    abt_ret = ABT_mutex_create(&aid->qhead.head_mutex);
    if (ABT_SUCCESS != abt_ret) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with head_mutex create\n", __func__);
        free(aid);
        return -1;
    }

    if (backing_thread_count == 0) {
        aid->num_xstreams = 0;
        abt_ret = ABT_xstream_self(&self_xstream);
        if (abt_ret != ABT_SUCCESS) { free(aid); hg_ret = -1; goto done; }
        abt_ret = ABT_xstream_get_main_pools(self_xstream, 1, &pool);
        if (abt_ret != ABT_SUCCESS) { free(aid); hg_ret = -1; goto done; }
    }
    else {
        progress_xstreams = (ABT_xstream *)calloc(backing_thread_count, sizeof(ABT_xstream));
        if (progress_xstreams == NULL) {
            free(aid);
            hg_ret = -1;
            goto done;
        }

        progress_scheds = (ABT_sched*)calloc(backing_thread_count, sizeof(ABT_sched));
        if (progress_scheds == NULL) {
            free(progress_xstreams);
            free(aid);
            hg_ret = -1;
            goto done;
        }

        /* All xstreams share one pool */
        abt_ret = ABT_pool_create_basic(ABT_POOL_FIFO_WAIT, ABT_POOL_ACCESS_MPMC, ABT_TRUE, &pool);
        if(abt_ret != ABT_SUCCESS) {
            free(progress_xstreams);
            free(progress_scheds);
            free(aid);
            hg_ret = -1;
            goto done;
        }

        for(i = 0; i < backing_thread_count; i++) {
            /* abt_ret = ABT_sched_create_basic(ABT_SCHED_BASIC, 1, &pool, */
            abt_ret = ABT_sched_create_basic(ABT_SCHED_BASIC_WAIT, 1, &pool,
               ABT_SCHED_CONFIG_NULL, &progress_scheds[i]);
            if (abt_ret != ABT_SUCCESS) {
                free(progress_xstreams);
                free(progress_scheds);
                free(aid);
                hg_ret = -1;
                goto done;
            }
            abt_ret = ABT_xstream_create(progress_scheds[i], &progress_xstreams[i]);
            if (abt_ret != ABT_SUCCESS) {
                free(progress_xstreams);
                free(progress_scheds);
                free(aid);
                hg_ret = -1;
                goto done;
            }
        } // end for
    } // end else

    aid->pool         = pool;
    aid->xstreams     = progress_xstreams;
    aid->num_xstreams = backing_thread_count;
    aid->nfopen       = 0;
    aid->env_async    = false;
    aid->ex_delay     = true;
    aid->ex_fclose    = true;
    aid->ex_gclose    = false;
    aid->ex_dclose    = false;
    aid->start_abt_push = false;

    env_var = getenv("HDF5_VOL_CONNECTOR");
    if (env_var && *env_var && strstr(env_var, "async") != NULL) {
        aid->ex_delay  = true;
        aid->env_async = true;
    }

    // Default start at fclose
    env_var = getenv("HDF5_ASYNC_EXE_FCLOSE");
    if (env_var && *env_var && atoi(env_var) > 0 )  {
        aid->ex_delay  = true;
        aid->ex_fclose = true;
    }

    env_var = getenv("HDF5_ASYNC_EXE_GCLOSE");
    if (env_var && *env_var && atoi(env_var) > 0 )  {
        aid->ex_delay  = true;
        aid->ex_gclose = true;
    }

    env_var = getenv("HDF5_ASYNC_EXE_DCLOSE");
    if (env_var && *env_var && atoi(env_var) > 0 )  {
        aid->ex_delay  = true;
        aid->ex_dclose = true;
    }

    async_instance_g = aid;

done:
    abt_ret = ABT_mutex_unlock(async_instance_mutex_g);
    if (abt_ret != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_mutex_unlock\n");
        if (progress_xstreams) free(progress_xstreams);
        if (progress_scheds)   free(progress_scheds);
        if (aid)               free(aid);
        return -1;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr, "  [ASYNC VOL DBG] Success with async_instance_init\n");
#endif
    return hg_ret;
} // End async_instance_init

hid_t
H5VL_async_register(void)
{
    int n_thread = ASYNC_VOL_DEFAULT_NTHREAD;
    /* Initialize the Argobots I/O instance */
    if (NULL == async_instance_g) {
        if (async_instance_init(n_thread) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] with async_instance_init\n");
            return -1;
        }
    }

    /* Singleton register the pass-through VOL connector ID */
    if(H5I_VOL != H5Iget_type(H5VL_ASYNC_g)) {
        H5VL_ASYNC_g = H5VLregister_connector(&H5VL_async_g, H5P_DEFAULT);
        if (H5VL_ASYNC_g <= 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5VLregister_connector\n");
            return -1;
        }
    }

    return H5VL_ASYNC_g;
}

static herr_t
H5VL_async_init(hid_t vipl_id)
{
    vipl_id = vipl_id;

    /* Initialize the Argobots I/O instance */
    int n_thread = ASYNC_VOL_DEFAULT_NTHREAD;
    if (NULL == async_instance_g) {
        if (async_instance_init(n_thread) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] with async_instance_init\n");
            return -1;
        }
    }

    return 0;
}

herr_t
H5Pset_vol_async(hid_t fapl_id)
{
    H5VL_async_info_t async_vol_info;
    hid_t under_vol_id;
    void *under_vol_info;
    herr_t status;
    /* int n_thread = ASYNC_VOL_DEFAULT_NTHREAD; */

    /* /1* Initialize the Argobots I/O instance *1/ */
    /* if (NULL == async_instance_g) { */
    /*     if (async_instance_init(n_thread) < 0) { */
    /*         fprintf(stderr,"  [ASYNC VOL ERROR] with async_instance_init\n"); */
    /*         return -1; */
    /*     } */
    /* } */

    if (H5VLis_connector_registered_by_name(H5VL_ASYNC_NAME) == 0) {
        if (H5VL_async_register() < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] H5Pset_vol_async: H5VL_async_register\n");
            goto done;
        }
    }

    status = H5Pget_vol_id(fapl_id, &under_vol_id);
    assert(status >= 0);
    assert(under_vol_id > 0);

    status = H5Pget_vol_info(fapl_id, &under_vol_info);
    assert(status >= 0);

    async_vol_info.under_vol_id   = under_vol_id;
    async_vol_info.under_vol_info = under_vol_info;

    if (H5Pset_vol(fapl_id, H5VL_ASYNC_g, &async_vol_info) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] with H5Pset_vol\n");
        goto done;
    }

done:
    return 1;
}


static herr_t
H5VL_async_term(void)
{
    herr_t ret_val = 0;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] ASYNC VOL terminate\n");
#endif
    H5VLasync_finalize();

    async_term();
    H5VL_ASYNC_g = H5I_INVALID_HID;

    return ret_val;
}

static void
free_async_task(async_task_t *task)
{
    if (ABT_mutex_free(&task->task_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__);
        return;
    }

    if (task->token) {
        task->token->task = NULL;
    }

    if (task->args) free(task->args);

    if (ABT_eventual_free(&task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_eventual_free\n", __func__);
        return;
    }
    /* if (task->children) free(task->children); */
    if (task->dep_tasks) free(task->dep_tasks);

    return;
}

/* // Debug only */
/* static void */
/* check_tasks_object_valid(H5VL_async_t *async_obj) */
/* { */
/*     async_task_t *task_iter, *tmp; */
/*     assert(async_obj); */

/*     if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__); */
/*         return; */
/*     } */

/*     DL_FOREACH_SAFE2(async_obj->file_task_list_head, task_iter, tmp, file_list_next) { */
/*         if (task_iter->async_obj->magic != ASYNC_MAGIC) { */
/*             printf("Error with magic number\n"); */
/*         } */
/*     } */

/*     if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__); */
/*         return; */
/*     } */
/* } */


/* static void */
/* remove_tasks_of_closed_object(H5VL_async_t *async_obj) */
/* { */
/*     async_task_t *task_iter, *tmp; */
/*     assert(async_obj); */

/*     if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__); */
/*         return; */
/*     } */

/*     DL_FOREACH_SAFE2(async_obj->file_async_obj->file_task_list_head, task_iter, tmp, file_list_next) { */
/*         if (task_iter->async_obj == async_obj && task_iter->is_done == 1) { */
/*             DL_DELETE2(async_obj->file_async_obj->file_task_list_head, task_iter, file_list_prev, file_list_next); */
/*             free_async_task(task_iter); */
/*             free(task_iter); */
/*         } */
/*     } */

/*     if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__); */
/*         return; */
/*     } */
/* } */


/* static void */
/* free_file_async_obj(H5VL_async_t *file) */
/* { */
/*     async_task_t *task_iter, *tmp; */

/*     assert(file); */

/*     if (ABT_mutex_lock(file->file_task_list_mutex) != ABT_SUCCESS) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__); */
/*         return; */
/*     } */

/*     DL_FOREACH_SAFE2(file->file_async_obj->file_task_list_head, task_iter, tmp, file_list_next) { */
/*         DL_DELETE2(file->file_async_obj->file_task_list_head, task_iter, file_list_prev, file_list_next); */
/*         free_async_task(task_iter); */
/*         free(task_iter); */
/*     } */

/*     if (ABT_mutex_unlock(file->file_task_list_mutex) != ABT_SUCCESS) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__); */
/*         return; */
/*     } */

/*     if (ABT_mutex_free(&file->obj_mutex) != ABT_SUCCESS) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__); */
/*         return; */
/*     } */
/*     if (ABT_mutex_free(&file->file_task_list_mutex) != ABT_SUCCESS) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__); */
/*         return; */
/*     } */
/*     free(file); */
/* } */

static herr_t
add_to_dep_task(async_task_t *task, async_task_t *dep_task)
{
    assert(task);
    assert(dep_task);

    if (task->n_dep_alloc == 0 || task->dep_tasks == NULL) {
        // Initial alloc
        task->dep_tasks = (async_task_t**)calloc(ALLOC_INITIAL_SIZE, sizeof(async_task_t*));
        if (NULL == task->dep_tasks) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s calloc failed\n", __func__);
            return -1;
        }
        task->n_dep_alloc  = ALLOC_INITIAL_SIZE;
        task->n_dep        = 0;
    }
    else if (task->n_dep == task->n_dep_alloc) {
        // Need to expand alloc
        task->dep_tasks = (async_task_t**)realloc(task->dep_tasks, task->n_dep_alloc * 2 * sizeof(async_task_t*));
        if (task->dep_tasks== NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s realloc failed\n", __func__);
            return -1;
        }
        task->n_dep_alloc *= 2;
    }

    task->dep_tasks[task->n_dep] = dep_task;
    task->n_dep++;

    return 1;
}

static herr_t
push_task_to_abt_pool(async_qhead_t *qhead, ABT_pool pool)
{
    int               i, is_dep_done;
    /* ABT_task_state    task_state; */
    ABT_thread_state  thread_state;
    /* ABT_thread        my_thread; */
    async_task_t      *task_elt, *task_tmp, *task_list;
    /* async_task_list_t *tmp; */

    assert(qhead);

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] entering %s \n", __func__);
#endif

    if (ABT_mutex_lock(qhead->head_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return -1;
    }

    if (NULL == qhead->queue)
        goto done;

    task_list = qhead->queue->task_list;
    DL_FOREACH_SAFE(task_list, task_elt, task_tmp) {
        is_dep_done = 1;
        if (qhead->queue->type  == DEPENDENT) {
            // Check if depenent tasks are finished
            for (i = 0; i < task_elt->n_dep; i++) {
                if (NULL != task_elt->dep_tasks[i]->abt_thread) {
                    /* ABT_thread_self(&my_thread); */
                    /* if (task_elt->dep_tasks[i]->abt_thread == my_thread) { */
                    /*     continue; */
                    /* } */
                    if (ABT_thread_get_state(task_elt->dep_tasks[i]->abt_thread, &thread_state) != ABT_SUCCESS) {
                        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_thread_get_state\n", __func__);
                        return -1;
                    }
                    /* if (ABT_task_get_state(task_elt->dep_tasks[i]->abt_task, &task_state) != ABT_SUCCESS) { */
                    /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_task_get_state\n", __func__); */
                    /*     return -1; */
                    /* } */
                    if (thread_state != ABT_THREAD_STATE_TERMINATED && thread_state != ABT_THREAD_STATE_RUNNING) {
                        is_dep_done = 0;
                        break;
                    }
                }
            }
        }

        if (is_dep_done == 0)
            continue;

#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] push task [%p] to Argobots pool\n", task_elt->func);
#endif

        if (ABT_thread_create(pool, task_elt->func, task_elt, ABT_THREAD_ATTR_NULL, &task_elt->abt_thread) != ABT_SUCCESS) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_thread_create failed for %p\n", __func__, task_elt->func);
            break;
        }
        /* if (ABT_task_create(pool, task_elt->func, task_elt, &task_elt->abt_task) != ABT_SUCCESS) { */
        /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_task_create failed for %p\n", __func__, task_elt->func); */
        /*     break; */
        /* } */
        task_elt->in_abt_pool = 1;

        DL_DELETE(qhead->queue->task_list, task_elt);
        task_elt->prev = NULL;
        task_elt->next = NULL;
        break;
    }

    // Remove head if all its tasks have been pushed to Argobots pool
    if (qhead->queue->task_list == NULL) {
        /* tmp = qhead->queue; */
        DL_DELETE(qhead->queue, qhead->queue);
        /* qhead->queue->prev = qhead->queue->next->prev; */
        /* qhead->queue = qhead->queue->next; */
        /* free(tmp); */
    }

done:
    if (ABT_mutex_unlock(qhead->head_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return -1;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

    return 1;
} // End push_task_to_abt_pool

int get_n_running_task_in_queue(async_task_t *task)
{
    int remaining_task = 0;
    ABT_thread_state thread_state;
    async_task_t *task_elt;

    /* if (ABT_mutex_lock(task->async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) { */
    /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__); */
    /*     return -1; */
    /* } */

    DL_FOREACH2(task->async_obj->file_task_list_head, task_elt, file_list_next) {
        if (task_elt->abt_thread != NULL) {
            ABT_thread_get_state(task_elt->abt_thread, &thread_state);
            if (thread_state == ABT_THREAD_STATE_RUNNING){
                remaining_task++;
            }
        }
    }

    /* if (ABT_mutex_unlock(task->async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) { */
    /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__); */
    /*     return -1; */
    /* } */

    return remaining_task;
}


int get_n_running_task_in_queue_obj(H5VL_async_t *async_obj)
{
    int remaining_task = 0;
    ABT_thread_state thread_state;
    async_task_t *task_elt;

    DL_FOREACH2(async_obj->file_task_list_head, task_elt, file_list_next) {
        if (task_elt->abt_thread != NULL) {
            ABT_thread_get_state(task_elt->abt_thread, &thread_state);
            if (thread_state == ABT_THREAD_STATE_RUNNING){
                remaining_task++;
            }
        }
    }

    return remaining_task;
}

/*
 * Any read/write operation must be executed after a prior write operation of same object.
 * Any write operation must be executed after a prior read operation of same object.
 * Any collective operation must be executed in same order with regards to other collective operations.
 * There can only be 1 collective operation in execution at any time (amongst all the threads on a process).
 */
static herr_t
add_task_to_queue(async_qhead_t *qhead, async_task_t *task, task_list_qtype task_type)
{
    int is_end, is_end2;
    /* int is_dep = 0; */
    async_task_list_t *tail_list, *task_list_elt;
    async_task_t      *task_elt, *tail_task;

    assert(qhead);
    assert(task);

    /* 1. Check for collective operation
     *       If collective, create a new CTL, or append it to an existing tail CTL.
     * 2. Check for object dependency.
     *       E.g. a group open depends on its file open/create.
     *       (Rule 1) Any read/write operation depends on a prior write operation of same object.
     *       (Rule 2) Any write operation depends on a prior read operation of same object.
     *       If satisfies any of the above, create a new DTL and insert to it.
     * 3. If the async task is not the above 2, create or insert it to tail RTL.
     *       If current RTL is also head, add to Argobots pool.
     * 4. Any time an async task has completed, push all tasks in the head into Argobots pool.
     */

    tail_list = qhead->queue == NULL ? NULL : qhead->queue->prev;

    // Need to depend on the object's createion (create/open) task to finish
    if (task_type != COLLECTIVE) {
        if (task->parent_obj && task->parent_obj->is_obj_valid != 1) {
            /* is_dep = 1; */
            task_type = DEPENDENT;
            if (add_to_dep_task(task, task->parent_obj->create_task) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s add_to_dep_task failed\n", __func__);
                return -1;
            }
        }

        if (task != task->async_obj->create_task && task->async_obj->is_obj_valid != 1) {
            /* is_dep = 1; */
            task_type = DEPENDENT;
            if (add_to_dep_task(task, task->async_obj->create_task) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s add_to_dep_task failed\n", __func__);
                return -1;
            }
        }

        /* Any read/write operation must be executed after a prior write operation of same object. */
        /* Any write operation must be executed after a prior read operation of same object. */
        is_end = 0;
        DL_FOREACH2(tail_list, task_list_elt, prev) {
            tail_task = task_list_elt->task_list == NULL ? NULL : task_list_elt->task_list->prev;
            is_end2 = 0;
            DL_FOREACH2(tail_task, task_elt, prev) {
                if (task_elt->async_obj && task_elt->async_obj == task->async_obj &&
                        !(task->op == READ && task_elt->op == READ)) {
                    task_type = DEPENDENT;
                    if (add_to_dep_task(task, task_elt) < 0) {
                        fprintf(stderr,"  [ASYNC VOL ERROR] %s add_to_dep_task failed\n", __func__);
                        return -1;
                    }
                    /* is_dep = 1; */
                    /* break; */
                }
                if (is_end2 == 1 && tail_task == task_elt)
                    break;
                is_end2 = 1;
            }
            if (is_end == 1 && tail_list == task_list_elt)
                break;
            is_end = 1;
            /* if (is_dep == 1) { break; } */
        }

        if (ABT_mutex_lock(task->async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
            return -1;
        }
        DL_FOREACH2(task->async_obj->file_task_list_head, task_elt, file_list_next) {
            if (task_elt->in_abt_pool == 1 && task_elt->async_obj && task_elt->async_obj == task->async_obj &&
                    !(task->op == READ && task_elt->op == READ)) {
                task_type = DEPENDENT;
                if (add_to_dep_task(task, task_elt) < 0) {
                    fprintf(stderr,"  [ASYNC VOL ERROR] %s add_to_dep_task failed\n", __func__);
                    return -1;
                }
            }
        }

        if (ABT_mutex_unlock(task->async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
            return -1;
        }
    }

    /* // If regular task, add to Argobots pool for execution directly */
    /* if (task_type == REGULAR) { */
    /*     if (ABT_thread_create(*(task->async_obj->pool_ptr), task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) { */
    /*         fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func); */
    /*         return -1; */
    /*     } */
    /*     return 1; */
    /* } */

    if (ABT_mutex_lock(qhead->head_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return -1;
    }

    // Check if the tail is of the same type, append to it if so
    if (qhead->queue && qhead->queue->prev->type == task_type && task_type != COLLECTIVE) {
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] append [%p] to %s task list\n",
                task->func, qtype_names_g[task_type]);
#endif
        DL_APPEND(qhead->queue->prev->task_list, task);
    }
    else {
        // Create a new task list in queue and add the current task to it
        async_task_list_t *new_list = (async_task_list_t*)calloc(1, sizeof(async_task_list_t));
        new_list->type = task_type;
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] create and append [%p] to new %s task list\n",
                task->func, qtype_names_g[task_type]);
#endif
        DL_APPEND(new_list->task_list, task);
        DL_APPEND(qhead->queue, new_list);
    }


    if (ABT_mutex_unlock(qhead->head_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return -1;
    }

    /* if (get_n_running_task_in_queue(task) == 0) */
    /*     push_task_to_abt_pool(qhead, *(task->async_obj->pool_ptr)); */

    return 1;
} // add_task_to_queue

void dup_loc_param(H5VL_loc_params_t *dest, H5VL_loc_params_t const *loc_params)
{
    size_t ref_size;

    assert(dest);
    assert(loc_params);

    memcpy(dest, loc_params, sizeof(*loc_params));

    if (loc_params->type == H5VL_OBJECT_BY_NAME) {
        dest->loc_data.loc_by_name.name    =  strdup(loc_params->loc_data.loc_by_name.name);
        dest->loc_data.loc_by_name.lapl_id = H5Pcopy(loc_params->loc_data.loc_by_name.lapl_id);
    }
    else if (loc_params->type == H5VL_OBJECT_BY_IDX) {
        dest->loc_data.loc_by_idx.name    =  strdup(loc_params->loc_data.loc_by_idx.name);
        dest->loc_data.loc_by_idx.lapl_id = H5Pcopy(loc_params->loc_data.loc_by_idx.lapl_id);
    }
    else if (loc_params->type == H5VL_OBJECT_BY_TOKEN) {
        ref_size = 16; // taken from H5VLnative_object.c
        dest->loc_data.loc_by_token.token = malloc(ref_size);
        memcpy((void*)(dest->loc_data.loc_by_token.token), loc_params->loc_data.loc_by_token.token, ref_size);
    }

}

void free_loc_param(H5VL_loc_params_t *loc_params)
{
    assert(loc_params);

    if (loc_params->type == H5VL_OBJECT_BY_NAME) {
        free    ((void*)loc_params->loc_data.loc_by_name.name);
        H5Pclose(loc_params->loc_data.loc_by_name.lapl_id);
    }
    else if (loc_params->type == H5VL_OBJECT_BY_IDX) {
        free    ((void*)loc_params->loc_data.loc_by_idx.name);
        H5Pclose(loc_params->loc_data.loc_by_idx.lapl_id);
    }
    /* else if (loc_params->type == H5VL_OBJECT_BY_ADDR) { */
    /* } */
    else if (loc_params->type == H5VL_OBJECT_BY_TOKEN) {
        free    ((void*)loc_params->loc_data.loc_by_token.token);
    }
}


/* herr_t */
/* H5Pget_async_token(hid_t dxpl, H5ES_token *token) */
/* { */


/* } */

/* herr_t */
/* H5Gget_async_token(hid_t gid, H5ES_token *token) */
/* { */


/* } */

/* herr_t */
/* H5Dtest(hid_t dset, int *completed) */
/* { */
/*     herr_t ret_value; */
/*     H5VL_async_t *async_obj; */
/*     async_task_t *task_iter; */
/*     ABT_task_state state; */

/*     if (NULL == (async_obj = (H5VL_async_t*)H5VL_async_get_object((void*)dset))) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s H5I_object_verify failed\n", __func__); */
/*         goto done; */
/*     } */

/*     if (ABT_mutex_lock(async_obj->obj_mutex) != ABT_SUCCESS) { */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
/*         goto done; */
/*     } */

/*     // Check for all tasks on this dset of a file */
/*     DL_FOREACH(async_obj->file_task_list_head, task_iter) { */
/*         if (task_iter->async_obj == async_obj) { */
/*             ABT_task_get_state (task_iter->abt_task, &state); */
/*             if (ABT_TASK_STATE_RUNNING == state || ABT_TASK_STATE_READY == state) { */
/*                 *completed = 0; */
/*                 goto done; */
/*             } */
/*             else */
/*                 *completed = 1; */
/*         } */
/*     } */

/* done: */
/*     if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) */
/*         fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */

/*     return ret_value; */
/* } */

herr_t H5VL_async_dataset_wait(H5VL_async_t *async_obj)
{
    /* herr_t ret_value; */
    async_task_t *task_iter;
    /* ABT_task_state state; */
    /* ABT_thread_state thread_state; */
    hbool_t acquired = false;

    async_instance_g->start_abt_push = true;

    if (get_n_running_task_in_queue_obj(async_obj) == 0 )
	push_task_to_abt_pool(&async_instance_g->qhead, *async_obj->pool_ptr);

    if (H5TSmutex_release() < 0)
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5TSmutex_release\n", __func__);

    // Check for all tasks on this dset of a file

    if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return -1;
    }
    DL_FOREACH2(async_obj->file_task_list_head, task_iter, file_list_next) {
        /* if (ABT_mutex_lock(async_obj->obj_mutex) != ABT_SUCCESS) { */
        /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
        /*     return -1; */
        /* } */

        if (task_iter->async_obj == async_obj) {
            if (task_iter->is_done != 1) {
                ABT_eventual_wait(task_iter->eventual, NULL);
            }
        }
        /* if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) */
        /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */

    }

    if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return -1;
    }
    while (false == acquired) {
        if (H5TSmutex_acquire(&acquired) < 0)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
    }

    return 0;
}

herr_t H5VL_async_file_wait(H5VL_async_t *async_obj)
{
    /* herr_t ret_value; */
    async_task_t *task_iter;
    /* ABT_task_state state; */
    /* ABT_thread_state thread_state; */
    hbool_t acquired = false;

    async_instance_g->start_abt_push = true;

    if (get_n_running_task_in_queue_obj(async_obj) == 0 )
	push_task_to_abt_pool(&async_instance_g->qhead, *async_obj->pool_ptr);

    if (H5TSmutex_release() < 0)
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5TSmutex_release\n", __func__);

    if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return -1;
    }
    // Check for all tasks on this dset of a file
    DL_FOREACH2(async_obj->file_task_list_head, task_iter, file_list_next) {
        /* if (ABT_mutex_lock(async_obj->obj_mutex) != ABT_SUCCESS) { */
        /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
        /*     return -1; */
        /* } */

        if (task_iter->is_done != 1) {
            ABT_eventual_wait(task_iter->eventual, NULL);
        }
        /* if (NULL != task_iter->abt_thread) { */
        /* if (NULL != task_iter->abt_task) { */
            /* ABT_thread_get_state(task_iter->abt_thread, &thread_state); */
            /* if (ABT_THREAD_STATE_TERMINATED != thread_state) */
                /* ABT_eventual_wait(task_iter->eventual, NULL); */
            /* ABT_task_get_state (task_iter->abt_task, &state); */
            /* if (ABT_TASK_STATE_TERMINATED != state) */
            /*     ABT_eventual_wait(task_iter->eventual, NULL); */
        /* } */
        /* if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) */
        /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
    }

    if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return -1;
    }

    while (false == acquired) {
        if (H5TSmutex_acquire(&acquired) < 0)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
    }

    return 0;
}

/* static void */
/* execute_parent_task_recursive(async_task_t *task) */
/* { */
/*     if (task == NULL ) */
/*         return; */

/*     if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS) */
/*          fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_mutex_unlock failed\n", __func__); */

/*     if (task->parent_obj != NULL) */
/*         execute_parent_task_recursive(task->parent_obj->create_task); */

/* #ifdef ENABLE_DBG_MSG */
/*     fprintf(stderr,"  [ASYNC VOL DBG] %s: cancel argobots task and execute now \n", __func__); */
/*     fflush(stderr); */
/* #endif */


/*     // Execute the task in current thread */
/*     task->func(task); */

/*     // Cancel the task already in the pool */
/*     /1* ABT_pool_remove(*task->async_obj->pool_ptr, task); *1/ */
/*     /1* ABT_thread_cancel(task); *1/ */
/*     ABT_task_cancel(task); */

/* #ifdef ENABLE_DBG_MSG */
/*     fprintf(stderr,"  [ASYNC VOL DBG] %s: finished executing task \n", __func__); */
/* #endif */
/* } */

void
H5VL_async_start()
{
    if (NULL != async_instance_g->qhead.queue)
       push_task_to_abt_pool(&async_instance_g->qhead, async_instance_g->pool);
}

herr_t
H5Pget_dxpl_async(hid_t dxpl, hbool_t *is_async)
{
    herr_t ret;
    assert(is_async);

    if ((ret = H5Pexist(dxpl, ASYNC_VOL_PROP_NAME)) <= 0) {
        if (ret < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Pexist failed\n", __func__);
            return ret;
        }
        else {
            *is_async = false;
        }
    }
    else{
        if ((ret = H5Pget(dxpl, ASYNC_VOL_PROP_NAME, (void*)is_async)) < 0)
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Pget failed\n", __func__);
    }

    return ret;
}

herr_t
H5Pset_dxpl_async(hid_t dxpl, hbool_t is_async)
{
    herr_t ret;
    size_t len = sizeof(hbool_t);

    if ((ret = H5Pinsert(dxpl, ASYNC_VOL_PROP_NAME, len, (void*)&is_async, NULL, NULL, NULL, NULL, NULL, NULL)) < 0)
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Pinsert failed\n", __func__);

    return ret;
}

herr_t
H5Pget_dxpl_async_cp_limit(hid_t dxpl, hsize_t *size)
{
    herr_t ret;
    assert(size);

    if ((ret = H5Pexist(dxpl, ASYNC_VOL_CP_SIZE_LIMIT_NAME)) <= 0) {
        if (ret < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Pexist failed\n", __func__);
            return ret;
        }
        else {
            *size = 0;
        }
    }
    else{
        if ((ret = H5Pget(dxpl, ASYNC_VOL_CP_SIZE_LIMIT_NAME, (void*)size)) < 0)
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Pget failed\n", __func__);
    }

    return ret;
}

herr_t
H5Pset_dxpl_async_cp_limit(hid_t dxpl, hsize_t size)
{
    herr_t ret;
    size_t len = sizeof(hsize_t);

    if ((ret = H5Pinsert(dxpl, ASYNC_VOL_CP_SIZE_LIMIT_NAME, len, (void*)&size, NULL, NULL, NULL, NULL, NULL, NULL)) < 0)
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Pinsert failed\n", __func__);

    return ret;

}

static herr_t
dataset_get_wrapper(void *dset, hid_t driver_id, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, ...)
{
    herr_t ret;
    va_list args;
    va_start(args, req);
    ret = H5VLdataset_get(dset, driver_id, get_type, dxpl_id, req, args);
    va_end(args);
    return ret;
}

herr_t
H5VLasync_get_data_size(void *dset, hid_t space, hid_t connector_id, hsize_t *size)
{
    herr_t ret;
    hid_t dset_type, dset_space;

    if ((ret = dataset_get_wrapper(dset, connector_id, H5VL_DATASET_GET_TYPE, H5P_DATASET_XFER_DEFAULT, NULL, (void*)&dset_type)) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s dataset_get_wrapper failed\n", __func__);
        goto done;
    }

    *size = H5Tget_size(dset_type);
    if ((ret = H5Tclose(dset_type)) < 0)
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Tclose failed\n", __func__);

    if (H5S_ALL == space) {
        if ((ret = dataset_get_wrapper(dset, connector_id, H5VL_DATASET_GET_SPACE,
                                       H5P_DATASET_XFER_DEFAULT, NULL, (void*)&dset_space)) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s dataset_get_wrapper failed\n", __func__);
            goto done;
        }
        (*size) *= H5Sget_simple_extent_npoints(dset_space);

        if ((ret = H5Sclose(dset_space)) < 0)
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Sclose failed\n", __func__);
    }
    else
        (*size) *= H5Sget_select_npoints(space);


done:
    return ret;
}

herr_t
H5VLasync_get_data_nelem(void *dset, hid_t space, hid_t connector_id, hsize_t *size)
{
    herr_t ret;
    hid_t dset_space;

    if (H5S_ALL == space) {
        if ((ret = dataset_get_wrapper(dset, connector_id, H5VL_DATASET_GET_SPACE,
                                       H5P_DATASET_XFER_DEFAULT, NULL, (void*)&dset_space)) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s dataset_get_wrapper failed\n", __func__);
            goto done;
        }
        (*size) = H5Sget_simple_extent_npoints(dset_space);

        if ((ret = H5Sclose(dset_space)) < 0)
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Sclose failed\n", __func__);
    }
    else
        (*size) = H5Sget_select_npoints(space);

done:
    return ret;
}


double get_elapsed_time(struct timeval *tstart, struct timeval *tend)
{
    return (double)(((tend->tv_sec-tstart->tv_sec)*1000000LL + tend->tv_usec-tstart->tv_usec) / 1000000.0);
}

void H5VLasync_waitall()
{
    int sleeptime = 100000;
    size_t size = 1;

    while(async_instance_g && (async_instance_g->nfopen > 0 || size > 0)) {

        usleep(sleeptime);

        ABT_pool_get_size(async_instance_g->pool, &size);
        /* printf("H5VLasync_finalize: pool size is %lu\n", size); */

        if (size == 0) {
            if (async_instance_g->nfopen == 0)
                break;
        }
    }

    return;
}


void H5VLasync_finalize()
{
    H5VLasync_waitall();
}


/* static void print_cpuset(int rank, int cpuset_size, int *cpuset) */
/* { */
/*     char *cpuset_str = NULL; */
/*     int i; */
/*     size_t pos = 0, len; */

/*     cpuset_str = (char *)calloc(cpuset_size * 5, sizeof(char)); */
/*     assert(cpuset_str); */

/*     for (i = 0; i < cpuset_size; i++) { */
/*         if (i) { */
/*             sprintf(&cpuset_str[pos], ",%d", cpuset[i]); */
/*         } else { */
/*             sprintf(&cpuset_str[pos], "%d", cpuset[i]); */
/*         } */
/*         len = strlen(&cpuset_str[pos]); */
/*         pos += len; */
/*     } */
/*     fprintf(stderr, "[E%d] CPU set (%d): {%s}\n", rank, cpuset_size, cpuset_str); */

/*     free(cpuset_str); */
/*     fflush(stderr); */
/* } */

/* static void async_set_affinity() */
/* { */
/*     int ret, j; */
/*     cpu_set_t cpuset; */

/*     CPU_ZERO(&cpuset); */
/*     for (j = 0; j < 2; j++) */
/*         CPU_SET(j, &cpuset); */
/*     ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset); */
/*     if (ret != 0) { */
/*         fprintf(stderr, "  [ASYNC VOL ERROR] with pthread_setaffinity_np: %d\n", ret); */
/*         return; */
/*     } */
/* } */


/* static void async_get_affinity() */
/* { */
/*     int ret, j; */
/*     cpu_set_t cpuset; */
/*     int cpu; */

/*     cpu = sched_getcpu(); */
/*     fprintf(stderr, "Argobots thread is running on CPU %d\n", cpu); */

/*     /1* ret = pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset); *1/ */
/*     /1* if (ret != 0) { *1/ */
/*     /1*     fprintf(stderr, "  [ASYNC VOL ERROR] with pthread_getaffinity_np: %d\n", ret); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* fprintf(stderr, "Set returned by pthread_getaffinity_np() contained:\n"); *1/ */
/*     /1* for (j = 0; j < CPU_SETSIZE; j++) *1/ */
/*     /1*     if (CPU_ISSET(j, &cpuset)) *1/ */
/*     /1*         fprintf(stderr, "    CPU %d\n", j); *1/ */
/* } */

/* static void test_affinity() */
/* { */
/*     ABT_xstream xstream; */
/*     int i, rank, ret; */
/*     int cpuid = -1, new_cpuid; */
/*     int cpuset_size, num_cpus = -1; */
/*     int *cpuset = NULL; */
/*     int num_xstreams = 1; */

/*     ret = ABT_xstream_self(&xstream); */
/*     if (ret != ABT_SUCCESS) { */
/*         fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_xstream_self\n"); */
/*         return; */
/*     } */

/*     ret = ABT_xstream_get_rank(xstream, &rank); */
/*     if (ret != ABT_SUCCESS) { */
/*         fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_xstream_get_rank\n"); */
/*         return; */
/*     } */

/*     /1* ret = ABT_xstream_get_cpubind(xstream, &cpuid); *1/ */
/*     /1* if (ret != ABT_SUCCESS) { *1/ */
/*     /1*     fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_xstream_get_cpubind\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* fprintf(stderr, "[E%d] CPU bind: %d\n", rank, cpuid); *1/ */

/*     /1* new_cpuid = (cpuid + 1) % num_xstreams; *1/ */
/*     /1* fprintf(stderr, "[E%d] change binding: %d -> %d\n", rank, cpuid, new_cpuid); *1/ */

/*     /1* ret = ABT_xstream_set_cpubind(xstream, new_cpuid); *1/ */
/*     /1* if (ret != ABT_SUCCESS) { *1/ */
/*     /1*     fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_xstream_set_cpubind\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* ret = ABT_xstream_get_cpubind(xstream, &cpuid); *1/ */
/*     /1* if (ret != ABT_SUCCESS) { *1/ */
/*     /1*     fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_xstream_get_cpubind\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */

/*     /1* /2* assert(cpuid == new_cpuid); *2/ *1/ */
/*     /1* fprintf(stderr, "[E%d] CPU bind: %d\n", rank, cpuid); *1/ */

/*     ret = ABT_xstream_get_affinity(xstream, 0, NULL, &num_cpus); */
/*     if (ret != ABT_SUCCESS) { */
/*         fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_xstream_get_affinity\n"); */
/*         return; */
/*     } */

/*     fprintf(stderr, "[E%d] num_cpus=%d\n", rank, num_cpus); */
/*     if (num_cpus > 0) { */
/*         cpuset_size = num_cpus; */
/*         cpuset = (int *)malloc(cpuset_size * sizeof(int)); */
/*         /1* assert(cpuset); *1/ */

/*         num_cpus = 0; */
/*         ret = ABT_xstream_get_affinity(xstream, cpuset_size, cpuset, &num_cpus); */
/*         if (ret != ABT_SUCCESS) { */
/*             fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_xstream_get_affinity\n"); */
/*             return; */
/*         } */
/*         /1* assert(num_cpus == cpuset_size); *1/ */
/*         print_cpuset(rank, cpuset_size, cpuset); */

/*         free(cpuset); */
/*     } */

/*     /1* cpuset = (int *)malloc(num_xstreams * sizeof(int)); *1/ */
/*     /1* /2* assert(cpuset); *2/ *1/ */
/*     /1* for (i = 0; i < num_xstreams; i++) { *1/ */
/*     /1*     cpuset[i] = i; *1/ */
/*     /1* } *1/ */
/*     /1* ret = ABT_xstream_set_affinity(xstream, num_xstreams, cpuset); *1/ */
/*     /1* if (ret != ABT_SUCCESS) { *1/ */
/*     /1*     fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_xstream_set_affinity\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* ret = ABT_xstream_get_affinity(xstream, num_xstreams, cpuset, &num_cpus); *1/ */
/*     /1* if (ret != ABT_SUCCESS) { *1/ */
/*     /1*     fprintf(stderr, "  [ASYNC VOL ERROR] with ABT_xstream_get_affinity\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* /2* assert(num_cpus == num_xstreams); *2/ *1/ */
/*     /1* print_cpuset(rank, num_xstreams, cpuset); *1/ */
/*     /1* free(cpuset); *1/ */
/* } */

H5RQ_token_int_t *
H5RQ__new_token(void)
{
    H5RQ_token_int_t *ret = (H5RQ_token_int_t*)calloc(1, sizeof(H5RQ_token_int_t));

    return ret;
}

herr_t
H5RQ_token_check(H5RQ_token_t token, int *status)
{
    H5RQ_token_int_t *in_token;

    if (NULL == token || NULL == status) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s input NULL!\n", __func__);
        return -1;
    }

    in_token = (H5RQ_token_int_t*)(token);

    if (in_token->task)
        *status = in_token->task->is_done;
    else
        *status = 1;

    return 1;
}

herr_t
H5RQ_token_wait(H5RQ_token_t token)
{
    /* hbool_t acquired = false; */
    H5RQ_token_int_t *in_token;

    if (NULL == token) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s input NULL!\n", __func__);
        return -1;
    }

    in_token = (H5RQ_token_int_t*)(token);

    if (NULL == in_token->task)
        return 1;

    if (in_token->task->is_done != 1) {

        if(NULL == in_token->task->async_obj) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with task async object\n", __func__);
            return -1;
        }

        ABT_eventual_wait(in_token->task->eventual, NULL);

    }

    return 1;
}

herr_t
H5RQ_token_free(H5RQ_token_t token)
{
    H5RQ_token_int_t *in_token;

    if (NULL == token) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s input NULL!\n", __func__);
        return -1;
    }

    in_token = (H5RQ_token_int_t*)(token);

    if (in_token->task)
        in_token->task->token = NULL;

    free(in_token);

    return 1;
}

static void
async_attr_create_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_create_args_t *args = (async_attr_create_args_t*)(task->args);

#ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
#endif

#ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
#endif
#ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
#endif
#ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
#endif

    while (acquired == false) {
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
#endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
#endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
#endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
#ifdef ENABLE_DBG_MSG
#ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
#endif
#endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
#ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
#ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
#endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
#endif

    if (1 == task->async_obj->is_obj_valid) {
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

#ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
#endif

    if ((obj = H5VLattr_create(args->obj, args->loc_params, task->under_vol_id, args->name, args->type_id, args->space_id, args->acpl_id, args->aapl_id, args->dxpl_id, args->req)) == NULL ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLattr_create failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
#endif

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;




#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->type_id > 0)    H5Tclose(args->type_id);
    if(args->space_id > 0)    H5Sclose(args->space_id);
    if(args->acpl_id > 0)    H5Pclose(args->acpl_id);
    if(args->aapl_id > 0)    H5Pclose(args->aapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
#ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
#endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

#ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
#endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
#ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
#endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
#ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
#endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
#endif
    return;
} // End async_attr_create_fn

static H5VL_async_t*
async_attr_create(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_attr_create_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_create_args_t*)calloc(1, sizeof(async_attr_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
#ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
#endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->name             = strdup(name);
    if(type_id > 0)
        args->type_id = H5Tcopy(type_id);
    if(space_id > 0)
        args->space_id = H5Scopy(space_id);
    if(acpl_id > 0)
        args->acpl_id = H5Pcopy(acpl_id);
    if(aapl_id > 0)
        args->aapl_id = H5Pcopy(aapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_attr_create_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
#ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
#endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

#ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_attr_create



static void
async_attr_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_open_args_t *args = (async_attr_open_args_t*)(task->args);

#ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
#endif

#ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
#endif
#ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
#endif
#ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
#endif

    while (acquired == false) {
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
#endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
#endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
#endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
#ifdef ENABLE_DBG_MSG
#ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
#endif
#endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
#ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
#ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
#endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
#endif

    if (1 == task->async_obj->is_obj_valid) {
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

#ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
#endif

    if ((obj = H5VLattr_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->aapl_id, args->dxpl_id, args->req)) == NULL ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLattr_open failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
#endif

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->aapl_id > 0)    H5Pclose(args->aapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
#ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
#endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

#ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
#endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
#ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
#endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
#ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
#endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
#endif
    return;
} // End async_attr_open_fn

static H5VL_async_t*
async_attr_open(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_attr_open_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_open_args_t*)calloc(1, sizeof(async_attr_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
#ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
#endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->name             = strdup(name);
    if(aapl_id > 0)
        args->aapl_id = H5Pcopy(aapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_attr_open_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
#ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
#endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

#ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_attr_open



static void
async_attr_read_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_read_args_t *args = (async_attr_read_args_t*)(task->args);

#ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
#endif

#ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
#endif
#ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->attr) {
        if (NULL != task->parent_obj->under_object) {
            args->attr = task->parent_obj->under_object;
        }
        else {
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
#endif
#ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
#endif

    while (acquired == false) {
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
#endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
#endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
#endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
#ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
#ifdef ENABLE_DBG_MSG
#ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
#endif
#endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
#ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
#ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
#endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
#endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

#ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
#endif

    if ( H5VLattr_read(args->attr, task->under_vol_id, args->mem_type_id, args->buf, args->dxpl_id, args->req) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLattr_read failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
#endif

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif


done:
    fflush(stdout);
    if(args->mem_type_id > 0)    H5Tclose(args->mem_type_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
#ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
   #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

#ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
#endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
#ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
#endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
#ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
#endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
#endif
    return;
} // End async_attr_read_fn

static herr_t
async_attr_read(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req)
{
    // For implicit mode (env var), make all read to be blocking
    if(aid->env_async) is_blocking = 1;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_attr_read_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_read_args_t*)calloc(1, sizeof(async_attr_read_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
#ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
#endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->attr             = parent_obj->under_object;
    if(mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    args->buf              = buf;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_attr_read_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
#ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
#endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

#ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
#endif
#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_attr_read



static void
async_attr_write_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_write_args_t *args = (async_attr_write_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->attr) {
        if (NULL != task->parent_obj->under_object) {
            args->attr = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLattr_write(args->attr, task->under_vol_id, args->mem_type_id, args->buf, args->dxpl_id, args->req) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLattr_write failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->mem_type_id > 0)    H5Tclose(args->mem_type_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    free(args->buf);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_attr_write_fn

static herr_t
async_attr_write(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_attr_write_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_write_args_t*)calloc(1, sizeof(async_attr_write_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->attr             = parent_obj->under_object;
    if(mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }

    if (NULL == (args->buf = malloc(ASYNC_VOL_ATTR_CP_SIZE_LIMIT))) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s malloc failed!\n", __func__);
        goto done;
    }
    memcpy(args->buf, buf, ASYNC_VOL_ATTR_CP_SIZE_LIMIT);

    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_attr_write_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_attr_write



static void
async_attr_get_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_get_args_t *args = (async_attr_get_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLattr_get(args->obj, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLattr_get failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_attr_get_fn

static herr_t
async_attr_get(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_attr_get_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_get_args_t*)calloc(1, sizeof(async_attr_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_attr_get_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_attr_get



static void
async_attr_specific_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_specific_args_t *args = (async_attr_specific_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLattr_specific(args->obj, args->loc_params, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLattr_specific failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_attr_specific_fn

static herr_t
async_attr_specific(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_attr_specific_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_specific_args_t*)calloc(1, sizeof(async_attr_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_attr_specific_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_attr_specific



static void
async_attr_optional_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_optional_args_t *args = (async_attr_optional_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLattr_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLattr_optional failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_attr_optional_fn

static herr_t
async_attr_optional(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_attr_optional_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_optional_args_t*)calloc(1, sizeof(async_attr_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_attr_optional_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_attr_optional



static void
async_attr_close_fn(void *foo)
{
    herr_t ret_value;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_close_args_t *args = (async_attr_close_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->attr) {
        if (NULL != task->parent_obj->under_object) {
            args->attr = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( (ret_value = H5VLattr_close(args->attr, task->under_vol_id, args->dxpl_id, args->req)) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLattr_close failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_attr_close_fn

static herr_t
async_attr_close(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_attr_close_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_close_args_t*)calloc(1, sizeof(async_attr_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->attr             = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_attr_close_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    else {
            if (get_n_running_task_in_queue(async_task) == 0)
                push_task_to_abt_pool(&aid->qhead, aid->pool);

    }

    aid->start_abt_push = true;
    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_attr_close



static void
async_dataset_create_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_create_args_t *args = (async_dataset_create_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    if (1 == task->async_obj->is_obj_valid) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ((obj = H5VLdataset_create(args->obj, args->loc_params, task->under_vol_id, args->name, args->lcpl_id, args->type_id, args->space_id, args->dcpl_id, args->dapl_id, args->dxpl_id, args->req)) == NULL ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdataset_create failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->type_id > 0)    H5Tclose(args->type_id);
    if(args->space_id > 0)    H5Sclose(args->space_id);
    if(args->dcpl_id > 0)    H5Pclose(args->dcpl_id);
    if(args->dapl_id > 0)    H5Pclose(args->dapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_dataset_create_fn

static H5VL_async_t*
async_dataset_create(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_dataset_create_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_create_args_t*)calloc(1, sizeof(async_dataset_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->name             = strdup(name);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(type_id > 0)
        args->type_id = H5Tcopy(type_id);
    if(space_id > 0)
        args->space_id = H5Scopy(space_id);
    if(dcpl_id > 0)
        args->dcpl_id = H5Pcopy(dcpl_id);
    if(dapl_id > 0)
        args->dapl_id = H5Pcopy(dapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_dataset_create_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;
    async_obj->dtype_size = H5Tget_size(type_id);
    async_obj->dset_size = async_obj->dtype_size * H5Sget_simple_extent_npoints(space_id);
    if (async_obj->dset_size > 0)
        async_obj->has_dset_size = true;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_dataset_create



static void
async_dataset_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_open_args_t *args = (async_dataset_open_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    if (1 == task->async_obj->is_obj_valid) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ((obj = H5VLdataset_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->dapl_id, args->dxpl_id, args->req)) == NULL ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdataset_open failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->dapl_id > 0)    H5Pclose(args->dapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_dataset_open_fn

static H5VL_async_t*
async_dataset_open(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_dataset_open_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_open_args_t*)calloc(1, sizeof(async_dataset_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->name             = strdup(name);
    if(dapl_id > 0)
        args->dapl_id = H5Pcopy(dapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_dataset_open_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_dataset_open



static void
async_dataset_read_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_read_args_t *args = (async_dataset_read_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dset) {
        if (NULL != task->parent_obj->under_object) {
            args->dset = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLdataset_read(args->dset, task->under_vol_id, args->mem_type_id, args->mem_space_id, args->file_space_id, args->plist_id, args->buf, args->req) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdataset_read failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->mem_type_id > 0)    H5Tclose(args->mem_type_id);
    if(args->mem_space_id > 0)    H5Sclose(args->mem_space_id);
    if(args->file_space_id > 0)    H5Sclose(args->file_space_id);
    if(args->plist_id > 0)    H5Pclose(args->plist_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_dataset_read_fn

static herr_t
async_dataset_read(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    // For implicit mode (env var), make all read to be blocking
    if(aid->env_async) is_blocking = 1;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_dataset_read_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_read_args_t*)calloc(1, sizeof(async_dataset_read_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->dset             = parent_obj->under_object;
    if(mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    if(mem_space_id > 0)
        args->mem_space_id = H5Scopy(mem_space_id);
    if(file_space_id > 0)
        args->file_space_id = H5Scopy(file_space_id);
    if(plist_id > 0)
        args->plist_id = H5Pcopy(plist_id);
    args->buf              = buf;
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_dataset_read_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        H5FD_mpio_xfer_t xfer_mode;
        H5Pget_dxpl_mpio(plist_id, &xfer_mode);
        if (xfer_mode == H5FD_MPIO_COLLECTIVE)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_dataset_read



static void
async_dataset_write_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_write_args_t *args = (async_dataset_write_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dset) {
        if (NULL != task->parent_obj->under_object) {
            args->dset = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLdataset_write(args->dset, task->under_vol_id, args->mem_type_id, args->mem_space_id, args->file_space_id, args->plist_id, args->buf, args->req) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdataset_write failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->mem_type_id > 0)    H5Tclose(args->mem_type_id);
    if(args->mem_space_id > 0)    H5Sclose(args->mem_space_id);
    if(args->file_space_id > 0)    H5Sclose(args->file_space_id);
    if(args->plist_id > 0)    H5Pclose(args->plist_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    if (args->buf_free == true) free(args->buf);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_dataset_write_fn

static herr_t
async_dataset_write(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    hbool_t enable_async = false;
    hsize_t buf_size, cp_size_limit = 0;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_dataset_write_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_write_args_t*)calloc(1, sizeof(async_dataset_write_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    if (H5Pget_dxpl_async(plist_id, &enable_async) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5Pget_dxpl_async\n", __func__);
        goto error;
    }
    if (H5Pget_dxpl_async_cp_limit(plist_id, &cp_size_limit) < 0) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5Pget_dxpl_async\n", __func__);
        goto error;
    }
    if (cp_size_limit > 0) { enable_async = true; }
    if (aid->env_async) enable_async = true;
    if (enable_async == true && cp_size_limit == 0) cp_size_limit = ULONG_MAX;

    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->dset             = parent_obj->under_object;
    if(mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    if(mem_space_id > 0)
        args->mem_space_id = H5Scopy(mem_space_id);
    if(file_space_id > 0)
        args->file_space_id = H5Scopy(file_space_id);
    if(plist_id > 0)
        args->plist_id = H5Pcopy(plist_id);
    args->buf              = (void*)buf;
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
            cp_size_limit = 0;
        }
    }

    if (cp_size_limit > 0) {
        if (parent_obj->has_dset_size && args->file_space_id == H5S_ALL && parent_obj->dset_size > 0) {
            buf_size = parent_obj->dset_size;
        }
        else {
            if (parent_obj->dtype_size > 0) {
                if (H5VLasync_get_data_nelem(args->dset, args->file_space_id, parent_obj->under_vol_id, &buf_size) < 0) {
                    fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLasync_get_data_nelem failed\n", __func__);
                    goto done;
                }
                buf_size *= parent_obj->dtype_size;
            }
            else
                if (H5VLasync_get_data_size(args->dset, args->file_space_id, parent_obj->under_vol_id, &buf_size) < 0) {
                    fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLasync_get_data_size failed\n", __func__);
                    goto done;
                }
        }
        if (buf_size <= cp_size_limit) {
            if (NULL == (args->buf = malloc(buf_size))) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s malloc failed!\n", __func__);
                goto done;
            }
            memcpy(args->buf, buf, buf_size);
            args->buf_free = true;
        }
        else {
            enable_async = false;
            fprintf(stdout,"  [ASYNC VOL] %s buf size [%llu] is larger than cp_size_limit [%llu], using synchronous write\n", __func__, buf_size, cp_size_limit);
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_dataset_write_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        H5FD_mpio_xfer_t xfer_mode;
        H5Pget_dxpl_mpio(plist_id, &xfer_mode);
        if (xfer_mode == H5FD_MPIO_COLLECTIVE)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    if (is_blocking == 1 ||enable_async == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_dataset_write



static void
async_dataset_get_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_get_args_t *args = (async_dataset_get_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dset) {
        if (NULL != task->parent_obj->under_object) {
            args->dset = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLdataset_get(args->dset, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdataset_get failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_dataset_get_fn

static herr_t
async_dataset_get(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_dataset_get_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_get_args_t*)calloc(1, sizeof(async_dataset_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->dset             = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_dataset_get_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_dataset_get



static void
async_dataset_specific_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_specific_args_t *args = (async_dataset_specific_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLdataset_specific(args->obj, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdataset_specific failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_dataset_specific_fn

static herr_t
async_dataset_specific(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_dataset_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_dataset_specific_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_specific_args_t*)calloc(1, sizeof(async_dataset_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_dataset_specific_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_dataset_specific



static void
async_dataset_optional_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_optional_args_t *args = (async_dataset_optional_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLdataset_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdataset_optional failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_dataset_optional_fn

static herr_t
async_dataset_optional(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_dataset_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_dataset_optional_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_optional_args_t*)calloc(1, sizeof(async_dataset_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_dataset_optional_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_dataset_optional



static void
async_dataset_close_fn(void *foo)
{
    herr_t ret_value;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_close_args_t *args = (async_dataset_close_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dset) {
        if (NULL != task->parent_obj->under_object) {
            args->dset = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( (ret_value = H5VLdataset_close(args->dset, task->under_vol_id, args->dxpl_id, args->req)) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdataset_close failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_dataset_close_fn

static herr_t
async_dataset_close(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_dataset_close_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_close_args_t*)calloc(1, sizeof(async_dataset_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->dset             = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_dataset_close_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    else {
        if (aid->ex_dclose) {
            if (get_n_running_task_in_queue(async_task) == 0)
                push_task_to_abt_pool(&aid->qhead, aid->pool);

        }
    }

    aid->start_abt_push = true;
    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_dataset_close



static void
async_datatype_commit_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_commit_args_t *args = (async_datatype_commit_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLdatatype_commit(args->obj, args->loc_params, task->under_vol_id, args->name, args->type_id, args->lcpl_id, args->tcpl_id, args->tapl_id, args->dxpl_id, args->req) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdatatype_commit failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->type_id > 0)    H5Tclose(args->type_id);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->tcpl_id > 0)    H5Pclose(args->tcpl_id);
    if(args->tapl_id > 0)    H5Pclose(args->tapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_datatype_commit_fn

static H5VL_async_t*
async_datatype_commit(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_datatype_commit_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_commit_args_t*)calloc(1, sizeof(async_datatype_commit_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->name             = strdup(name);
    if(type_id > 0)
        args->type_id = H5Tcopy(type_id);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(tcpl_id > 0)
        args->tcpl_id = H5Pcopy(tcpl_id);
    if(tapl_id > 0)
        args->tapl_id = H5Pcopy(tapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_datatype_commit_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_datatype_commit



static void
async_datatype_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_open_args_t *args = (async_datatype_open_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    if (1 == task->async_obj->is_obj_valid) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ((obj = H5VLdatatype_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->tapl_id, args->dxpl_id, args->req)) == NULL ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdatatype_open failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->tapl_id > 0)    H5Pclose(args->tapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_datatype_open_fn

static H5VL_async_t*
async_datatype_open(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_datatype_open_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_open_args_t*)calloc(1, sizeof(async_datatype_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->name             = strdup(name);
    if(tapl_id > 0)
        args->tapl_id = H5Pcopy(tapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_datatype_open_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_datatype_open



static void
async_datatype_get_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_get_args_t *args = (async_datatype_get_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dt) {
        if (NULL != task->parent_obj->under_object) {
            args->dt = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLdatatype_get(args->dt, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdatatype_get failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_datatype_get_fn

static herr_t
async_datatype_get(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_datatype_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_datatype_get_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_get_args_t*)calloc(1, sizeof(async_datatype_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->dt               = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_datatype_get_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_datatype_get



static void
async_datatype_specific_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_specific_args_t *args = (async_datatype_specific_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLdatatype_specific(args->obj, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdatatype_specific failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_datatype_specific_fn

static herr_t
async_datatype_specific(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_datatype_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_datatype_specific_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_specific_args_t*)calloc(1, sizeof(async_datatype_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_datatype_specific_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_datatype_specific



static void
async_datatype_optional_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_optional_args_t *args = (async_datatype_optional_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLdatatype_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdatatype_optional failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_datatype_optional_fn

static herr_t
async_datatype_optional(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_datatype_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_datatype_optional_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_optional_args_t*)calloc(1, sizeof(async_datatype_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_datatype_optional_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_datatype_optional



static void
async_datatype_close_fn(void *foo)
{
    herr_t ret_value;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_close_args_t *args = (async_datatype_close_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dt) {
        if (NULL != task->parent_obj->under_object) {
            args->dt = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( (ret_value = H5VLdatatype_close(args->dt, task->under_vol_id, args->dxpl_id, args->req)) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLdatatype_close failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_datatype_close_fn

static herr_t
async_datatype_close(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_datatype_close_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_close_args_t*)calloc(1, sizeof(async_datatype_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->dt               = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_datatype_close_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    else {
            if (get_n_running_task_in_queue(async_task) == 0)
                push_task_to_abt_pool(&aid->qhead, aid->pool);

    }

    aid->start_abt_push = true;
    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_datatype_close



static void
async_file_create_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    H5VL_async_info_t *info = NULL;
    hid_t under_fapl_id = -1;
    async_task_t *task = (async_task_t*)foo;
    async_file_create_args_t *args = (async_file_create_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    async_instance_g->start_abt_push = false;

    if (1 == task->async_obj->is_obj_valid) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(args->fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(args->fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    if (info) {
        H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);
    }
    else {
        hid_t under_vol_id;
        H5Pget_vol_id(args->fapl_id, &under_vol_id);
        H5Pset_vol(under_fapl_id, under_vol_id, NULL);
    }
    if ((obj = H5VLfile_create(args->name, args->flags, args->fcpl_id, under_fapl_id, args->dxpl_id, args->req)) == NULL ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfile_create failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif

    // Increase file open ref count
    if (ABT_mutex_lock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC ABT ERROR] with ABT_mutex_lock\n");
        goto done;
    };
    async_instance_g->nfopen++;
    if (ABT_mutex_unlock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC ABT ERROR] with ABT_mutex_ulock\n");
        goto done;
    };

done:
    fflush(stdout);
    if(under_fapl_id > 0)    H5Pclose(under_fapl_id);
    if(NULL != info)         H5VL_async_info_free(info);
    free(args->name);
    args->name = NULL;
    if(args->fcpl_id > 0)    H5Pclose(args->fcpl_id);
    if(args->fapl_id > 0)    H5Pclose(args->fapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_file_create_fn

static H5VL_async_t*
async_file_create(int is_blocking, async_instance_t* aid, const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    hid_t under_vol_id;
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_file_create_args_t *args = NULL;
    int lock_self;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);

    H5Pget_vol_id(fapl_id, &under_vol_id);

    if ((args = (async_file_create_args_t*)calloc(1, sizeof(async_file_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_async_obj      = async_obj;
    if (ABT_mutex_create(&(async_obj->file_task_list_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->name             = strdup(name);
    args->flags            = flags;
    if(fcpl_id > 0)
        args->fcpl_id = H5Pcopy(fcpl_id);
    if(fapl_id > 0)
        args->fapl_id = H5Pcopy(fapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_file_create_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = under_vol_id;
    async_task->async_obj  = async_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    /* Lock async_obj */
    while (1) {
        if (async_obj->obj_mutex && ABT_mutex_trylock(async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else
            fprintf(stderr,"  [ASYNC VOL DBG] %s error with try_lock\n", __func__);
        usleep(1000);
    }
    lock_self = 1;

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(async_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    async_obj->task_cnt++;
    async_obj->pool_ptr = &aid->pool;
    H5Pget_coll_metadata_write(fapl_id, &async_obj->is_col_meta);
    add_task_to_queue(&aid->qhead, async_task, REGULAR);
    if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_self = 0;

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (get_n_running_task_in_queue(async_task) == 0)
        push_task_to_abt_pool(&aid->qhead, aid->pool);
    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_self == 1) {
        if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL DBG] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_file_create



static void
async_file_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    H5VL_async_info_t *info = NULL;
    hid_t under_fapl_id = -1;
    async_task_t *task = (async_task_t*)foo;
    async_file_open_args_t *args = (async_file_open_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    async_instance_g->start_abt_push = false;

    if (1 == task->async_obj->is_obj_valid) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(args->fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(args->fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    if (info) {
        H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);
    }
    else {
        hid_t under_vol_id;
        H5Pget_vol_id(args->fapl_id, &under_vol_id);
        H5Pset_vol(under_fapl_id, under_vol_id, NULL);
    }
    if ((obj = H5VLfile_open(args->name, args->flags, under_fapl_id, args->dxpl_id, args->req)) == NULL ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfile_open failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif

    // Increase file open ref count
    if (ABT_mutex_lock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC ABT ERROR] with ABT_mutex_lock\n");
        goto done;
    };
    async_instance_g->nfopen++;
    if (ABT_mutex_unlock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC ABT ERROR] with ABT_mutex_ulock\n");
        goto done;
    };

done:
    fflush(stdout);
    if(under_fapl_id > 0)    H5Pclose(under_fapl_id);
    if(NULL != info)         H5VL_async_info_free(info);
    free(args->name);
    args->name = NULL;
    if(args->fapl_id > 0)    H5Pclose(args->fapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_file_open_fn

static H5VL_async_t*
async_file_open(int is_blocking, async_instance_t* aid, const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    hid_t under_vol_id;
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_file_open_args_t *args = NULL;
    int lock_self;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);

    H5Pget_vol_id(fapl_id, &under_vol_id);

    if ((args = (async_file_open_args_t*)calloc(1, sizeof(async_file_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_async_obj      = async_obj;
    if (ABT_mutex_create(&(async_obj->file_task_list_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->name             = strdup(name);
    args->flags            = flags;
    if(fapl_id > 0)
        args->fapl_id = H5Pcopy(fapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_file_open_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = under_vol_id;
    async_task->async_obj  = async_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    /* Lock async_obj */
    while (1) {
        if (async_obj->obj_mutex && ABT_mutex_trylock(async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else
            fprintf(stderr,"  [ASYNC VOL DBG] %s error with try_lock\n", __func__);
        usleep(1000);
    }
    lock_self = 1;

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(async_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    async_obj->task_cnt++;
    async_obj->pool_ptr = &aid->pool;
    H5Pget_coll_metadata_write(fapl_id, &async_obj->is_col_meta);
    add_task_to_queue(&aid->qhead, async_task, REGULAR);
    if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_self = 0;

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (get_n_running_task_in_queue(async_task) == 0)
        push_task_to_abt_pool(&aid->qhead, aid->pool);
    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_self == 1) {
        if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL DBG] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_file_open



static void
async_file_get_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_file_get_args_t *args = (async_file_get_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->file) {
        if (NULL != task->parent_obj->under_object) {
            args->file = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLfile_get(args->file, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfile_get failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_file_get_fn

static herr_t
async_file_get(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_file_get_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_file_get_args_t*)calloc(1, sizeof(async_file_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->file             = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_file_get_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_file_get



static void
async_file_specific_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_file_specific_args_t *args = (async_file_specific_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->file) {
        if (NULL != task->parent_obj->under_object) {
            args->file = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLfile_specific(args->file, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfile_specific failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_file_specific_fn

static herr_t
async_file_specific(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_file_specific_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_file_specific_args_t*)calloc(1, sizeof(async_file_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->file             = parent_obj->under_object;
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_file_specific_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_file_specific



static void
async_file_optional_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_file_optional_args_t *args = (async_file_optional_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->file) {
        if (NULL != task->parent_obj->under_object) {
            args->file = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLfile_optional(args->file, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfile_optional failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_file_optional_fn

static herr_t
async_file_optional(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_file_optional_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_file_optional_args_t*)calloc(1, sizeof(async_file_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->file             = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_file_optional_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (get_n_running_task_in_queue(async_task) == 0)
        push_task_to_abt_pool(&aid->qhead, aid->pool);
    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_file_optional



static void
async_file_close_fn(void *foo)
{
    herr_t ret_value;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_file_close_args_t *args = (async_file_close_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->file) {
        if (NULL != task->parent_obj->under_object) {
            args->file = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( (ret_value = H5VLfile_close(args->file, task->under_vol_id, args->dxpl_id, args->req)) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfile_close failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif

    // Decrease file open ref count
    if (ABT_mutex_lock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC ABT ERROR] with ABT_mutex_lock\n");
        goto done;
    };
    if (async_instance_g->nfopen > 0)  async_instance_g->nfopen--;
    if (ABT_mutex_unlock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC ABT ERROR] with ABT_mutex_ulock\n");
        goto done;
    };

done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_file_close_fn

static herr_t
async_file_close(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_file_close_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_file_close_args_t*)calloc(1, sizeof(async_file_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->file             = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_file_close_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    else {
            if (get_n_running_task_in_queue(async_task) == 0)
                push_task_to_abt_pool(&aid->qhead, aid->pool);

    }

    aid->start_abt_push = true;
    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_file_close



static void
async_group_create_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_create_args_t *args = (async_group_create_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    if (1 == task->async_obj->is_obj_valid) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ((obj = H5VLgroup_create(args->obj, args->loc_params, task->under_vol_id, args->name, args->lcpl_id, args->gcpl_id, args->gapl_id, args->dxpl_id, args->req)) == NULL ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLgroup_create failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->gcpl_id > 0)    H5Pclose(args->gcpl_id);
    if(args->gapl_id > 0)    H5Pclose(args->gapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_group_create_fn

static H5VL_async_t*
async_group_create(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_group_create_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_create_args_t*)calloc(1, sizeof(async_group_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->name             = strdup(name);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(gcpl_id > 0)
        args->gcpl_id = H5Pcopy(gcpl_id);
    if(gapl_id > 0)
        args->gapl_id = H5Pcopy(gapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_group_create_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_group_create



static void
async_group_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_open_args_t *args = (async_group_open_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    if (1 == task->async_obj->is_obj_valid) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ((obj = H5VLgroup_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->gapl_id, args->dxpl_id, args->req)) == NULL ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLgroup_open failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->gapl_id > 0)    H5Pclose(args->gapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_group_open_fn

static H5VL_async_t*
async_group_open(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_group_open_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_open_args_t*)calloc(1, sizeof(async_group_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->name             = strdup(name);
    if(gapl_id > 0)
        args->gapl_id = H5Pcopy(gapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_group_open_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_group_open



static void
async_group_get_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_get_args_t *args = (async_group_get_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLgroup_get(args->obj, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLgroup_get failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_group_get_fn

static herr_t
async_group_get(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_group_get_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_get_args_t*)calloc(1, sizeof(async_group_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_group_get_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_group_get



static void
async_group_specific_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_specific_args_t *args = (async_group_specific_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLgroup_specific(args->obj, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLgroup_specific failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_group_specific_fn

static herr_t
async_group_specific(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_group_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_group_specific_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_specific_args_t*)calloc(1, sizeof(async_group_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_group_specific_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_group_specific



static void
async_group_optional_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_optional_args_t *args = (async_group_optional_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLgroup_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLgroup_optional failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_group_optional_fn

static herr_t
async_group_optional(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_group_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_group_optional_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_optional_args_t*)calloc(1, sizeof(async_group_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_group_optional_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_group_optional



static void
async_group_close_fn(void *foo)
{
    herr_t ret_value;
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_close_args_t *args = (async_group_close_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->grp) {
        if (NULL != task->parent_obj->under_object) {
            args->grp = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( (ret_value = H5VLgroup_close(args->grp, task->under_vol_id, args->dxpl_id, args->req)) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLgroup_close failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_group_close_fn

static herr_t
async_group_close(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_group_close_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_close_args_t*)calloc(1, sizeof(async_group_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->grp              = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_group_close_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    else {
        if (aid->ex_gclose) {
            if (get_n_running_task_in_queue(async_task) == 0)
                push_task_to_abt_pool(&aid->qhead, aid->pool);

        }
    }

    aid->start_abt_push = true;
    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_group_close



static void
async_link_create_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_create_args_t *args = (async_link_create_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    if (1 == task->async_obj->is_obj_valid) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLlink_create(args->create_type, args->obj, args->loc_params, task->under_vol_id, args->lcpl_id, args->lapl_id, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLlink_create failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->lapl_id > 0)    H5Pclose(args->lapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_link_create_fn

static H5VL_async_t*
async_link_create(int is_blocking, async_instance_t* aid, H5VL_link_create_type_t create_type, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_link_create_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_create_args_t*)calloc(1, sizeof(async_link_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->create_type      = create_type;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(lapl_id > 0)
        args->lapl_id = H5Pcopy(lapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_link_create_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_link_create



static void
async_link_copy_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_copy_args_t *args = (async_link_copy_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->src_obj) {
        if (NULL != task->parent_obj->under_object) {
            args->src_obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLlink_copy(args->src_obj, args->loc_params1, args->dst_obj, args->loc_params2, task->under_vol_id, args->lcpl_id, args->lapl_id, args->dxpl_id, args->req) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLlink_copy failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params1);
    free_loc_param((H5VL_loc_params_t*)args->loc_params2);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->lapl_id > 0)    H5Pclose(args->lapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_link_copy_fn

static herr_t
async_link_copy(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params1, H5VL_async_t *parent_obj2, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_link_copy_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_copy_args_t*)calloc(1, sizeof(async_link_copy_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->src_obj          = parent_obj->under_object;
    args->loc_params1 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params1));
    dup_loc_param(args->loc_params1, loc_params1);
    args->dst_obj          = parent_obj2->under_object;
    args->loc_params2 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params2));
    dup_loc_param(args->loc_params2, loc_params2);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(lapl_id > 0)
        args->lapl_id = H5Pcopy(lapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_link_copy_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_link_copy



static void
async_link_move_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_move_args_t *args = (async_link_move_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->src_obj) {
        if (NULL != task->parent_obj->under_object) {
            args->src_obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLlink_move(args->src_obj, args->loc_params1, args->dst_obj, args->loc_params2, task->under_vol_id, args->lcpl_id, args->lapl_id, args->dxpl_id, args->req) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLlink_move failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params1);
    free_loc_param((H5VL_loc_params_t*)args->loc_params2);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->lapl_id > 0)    H5Pclose(args->lapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_link_move_fn

static herr_t
async_link_move(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params1, H5VL_async_t *parent_obj2, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_link_move_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_move_args_t*)calloc(1, sizeof(async_link_move_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->src_obj          = parent_obj->under_object;
    args->loc_params1 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params1));
    dup_loc_param(args->loc_params1, loc_params1);
    args->dst_obj          = parent_obj2->under_object;
    args->loc_params2 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params2));
    dup_loc_param(args->loc_params2, loc_params2);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(lapl_id > 0)
        args->lapl_id = H5Pcopy(lapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_link_move_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_link_move



static void
async_link_get_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_get_args_t *args = (async_link_get_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLlink_get(args->obj, args->loc_params, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLlink_get failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_link_get_fn

static herr_t
async_link_get(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_link_get_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_get_args_t*)calloc(1, sizeof(async_link_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_link_get_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_link_get



static void
async_link_specific_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_specific_args_t *args = (async_link_specific_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLlink_specific(args->obj, args->loc_params, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLlink_specific failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_link_specific_fn

static herr_t
async_link_specific(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_link_specific_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_specific_args_t*)calloc(1, sizeof(async_link_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_link_specific_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_link_specific



static void
async_link_optional_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_optional_args_t *args = (async_link_optional_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLlink_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLlink_optional failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_link_optional_fn

static herr_t
async_link_optional(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_link_optional_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_optional_args_t*)calloc(1, sizeof(async_link_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_link_optional_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_link_optional



static void
async_object_open_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_open_args_t *args = (async_object_open_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    if (1 == task->async_obj->is_obj_valid) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        return;
    }
    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLobject_open(args->obj, args->loc_params, task->under_vol_id, args->opened_type, args->dxpl_id, args->req) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLobject_open failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_object_open_fn

static H5VL_async_t*
async_object_open(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_object_open_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_object_open_args_t*)calloc(1, sizeof(async_object_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->magic = ASYNC_MAGIC;
    if (ABT_mutex_create(&(async_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->opened_type      = opened_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_object_open_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return NULL;
} // End async_object_open



static void
async_object_copy_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_copy_args_t *args = (async_object_copy_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->src_obj) {
        if (NULL != task->parent_obj->under_object) {
            args->src_obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLobject_copy(args->src_obj, args->src_loc_params, args->src_name, args->dst_obj, args->dst_loc_params, args->dst_name, args->ocpypl_id, task->under_vol_id, args->lcpl_id, args->dxpl_id, args->req) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLobject_copy failed\n", __func__);
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->src_loc_params);
    free(args->src_name);
    args->src_name = NULL;
    free_loc_param((H5VL_loc_params_t*)args->dst_loc_params);
    free(args->dst_name);
    args->dst_name = NULL;
    if(args->ocpypl_id > 0)    H5Pclose(args->ocpypl_id);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_object_copy_fn

static herr_t
async_object_copy(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, H5VL_async_t *parent_obj2, const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_object_copy_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_object_copy_args_t*)calloc(1, sizeof(async_object_copy_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->src_obj          = parent_obj->under_object;
    args->src_loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*src_loc_params));
    dup_loc_param(args->src_loc_params, src_loc_params);
    args->src_name         = strdup(src_name);
    args->dst_obj          = parent_obj2->under_object;
    args->dst_loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*dst_loc_params));
    dup_loc_param(args->dst_loc_params, dst_loc_params);
    args->dst_name         = strdup(dst_name);
    if(ocpypl_id > 0)
        args->ocpypl_id = H5Pcopy(ocpypl_id);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_object_copy_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_object_copy



static void
async_object_get_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_get_args_t *args = (async_object_get_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLobject_get(args->obj, args->loc_params, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLobject_get failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_object_get_fn

static herr_t
async_object_get(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_object_get_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_object_get_args_t*)calloc(1, sizeof(async_object_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_object_get_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_object_get



static void
async_object_specific_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_specific_args_t *args = (async_object_specific_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLobject_specific(args->obj, args->loc_params, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLobject_specific failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_object_specific_fn

static herr_t
async_object_specific(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_object_specific_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_object_specific_args_t*)calloc(1, sizeof(async_object_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_object_specific_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_object_specific



static void
async_object_optional_fn(void *foo)
{
    hbool_t acquired = false;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_optional_args_t *args = (async_object_optional_args_t*)(task->args);

    #ifdef ENABLE_TIMING
    struct timeval now_time;
    struct timeval timer1;
    struct timeval timer2;
    struct timeval timer3;
    struct timeval timer4;
    struct timeval timer5;
    struct timeval timer6;
    struct timeval timer7;
    struct timeval timer8;
    struct timeval timer9;
    gettimeofday(&args->start_time, NULL);
    #endif

    #ifdef ENABLE_TIMING
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s start, time=%ld.%06ld\n", __func__, args->start_time.tv_sec, args->start_time.tv_usec);
    #endif
    #ifdef ENABLE_LOG
    fprintf(stdout,"  [ASYNC ABT LOG] entering %s\n", __func__);
    fflush(stdout);
    #endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
            #endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            return;
        }
    }

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
    fflush(stderr);
    #endif
    #ifdef ENABLE_TIMING
    gettimeofday(&timer1, NULL);
    double time1 = get_elapsed_time(&args->start_time, &timer1);
    #endif

    while (acquired == false) {
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        #endif
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock count = %d, time=%ld.%06ld\n", __func__, attempt_count, now_time.tv_sec, now_time.tv_usec);
        #endif
        if (H5TSmutex_acquire(&acquired) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            goto done;
        }
        if (false == acquired) {
            #ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
            #endif
            if(sleep_time > 0) usleep(sleep_time);
            continue;
        }
        #ifdef ENABLE_TIMING
        gettimeofday(&now_time, NULL);
        fprintf(stderr,"  [ASYNC ABT DBG] %s lock SUCCESSFULLY acquired, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
        #endif
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            #ifdef ENABLE_DBG_MSG
            #ifdef ENABLE_TIMING
            gettimeofday(&now_time, NULL);
            fprintf(stderr,"  [ASYNC ABT DBG] %s after wait lock count = %d, time=%ld.%06ld\n", __func__, new_attempt_count, now_time.tv_sec, now_time.tv_usec);
            #endif
            #endif
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release() < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&now_time, NULL);
                fprintf(stderr,"  [ASYNC ABT DBG] %s lock YIELD to main thread, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
                #endif
                acquired = false;
            }
            else {
                break;
            }
            attempt_count = new_attempt_count;
            task->async_obj->file_async_obj->attempt_check_cnt++;
            task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
        }
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&timer2, NULL);
    double time2 = get_elapsed_time(&timer1, &timer2);
    #endif

    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
    fflush(stderr);
    #endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;


    // Restore previous library state
    assert(task->h5_state);
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    #ifdef ENABLE_TIMING
    gettimeofday(&timer3, NULL);
    double time3 = get_elapsed_time(&timer2, &timer3);
    #endif

    if ( H5VLobject_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments) < 0 ) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLobject_optional failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    #ifdef ENABLE_TIMING
    gettimeofday(&timer4, NULL);
    double time4 = get_elapsed_time(&timer3, &timer4);
    #endif




    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
    #endif


done:
    fflush(stdout);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer5, NULL);
    double time5 = get_elapsed_time(&timer4, &timer5);
    #endif

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&timer6, NULL);
    double time6 = get_elapsed_time(&timer5, &timer6);
    #endif

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer7, NULL);
    double time7 = get_elapsed_time(&timer6, &timer7);
    #endif

    if(is_lib_state_restored && H5VLreset_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLreset_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;
    #ifdef ENABLE_TIMING
    gettimeofday(&timer8, NULL);
    double time8 = get_elapsed_time(&timer7, &timer8);
    #endif

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
       push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    #ifdef ENABLE_TIMING
    gettimeofday(&timer9, NULL);
    double exec_time   = get_elapsed_time(&args->start_time, &timer9);
    double total_time  = get_elapsed_time(&args->create_time, &timer9);
    double wait_time   = total_time - exec_time;
    printf("  [ASYNC ABT TIMING] %-24s \ttotal time      : %f\n", __func__, total_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  wait time     : %f\n", __func__, wait_time);
    printf("  [ASYNC ABT TIMING] %-24s \t  execute time  : %f\n", __func__, exec_time);
    printf("  [ASYNC ABT TIMING] %-24s \t    time2       : %f\n", __func__, time2);
    printf("  [ASYNC ABT TIMING] %-24s \t    time3       : %f\n", __func__, time3);
    printf("  [ASYNC ABT TIMING] %-24s \t    time4(n.vol): %f\n", __func__, time4);
    fflush(stdout);
    #endif
    return;
} // End async_object_optional_fn

static herr_t
async_object_optional(int is_blocking, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_object_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    H5RQ_token_int_t *token = NULL;
    async_object_optional_args_t *args = NULL;
    int lock_parent;
    hbool_t acquired = false;

    #ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
    fflush(stderr);
    #endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_object_optional_args_t*)calloc(1, sizeof(async_object_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    #ifdef ENABLE_TIMING
    gettimeofday(&args->create_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] entering %s, time=%ld.%06ld\n", __func__, args->create_time.tv_sec, args->create_time.tv_usec);
    fflush(stderr);
    #endif
    /* create a new task and insert into its file task list */
    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }

    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        token = H5RQ__new_token();
        if (token == NULL) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s token is NULL!\n", __func__);
        }
        else {
            token->task = async_task;
            async_task->token = token;
            *req = (void*)token;
        }
    }


    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func       = async_object_optional_fn;
    async_task->args       = args;
    async_task->op         = READ;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = parent_obj;
    async_task->parent_obj = parent_obj;
    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        goto error;
    }


    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = 1;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (parent_obj->is_obj_valid != 1) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = 0;
    #ifdef ENABLE_TIMING
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    printf("  [ASYNC VOL TIMING] %-24s \t  create time   : %f\n",
		 __func__, get_elapsed_time(&args->create_time, &now_time));
    #endif
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking == 1) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release() < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
        fflush(stderr);
        #endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
        #ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
        fflush(stderr);
        #endif
        while (acquired == false) {
            if (H5TSmutex_acquire(&acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&now_time, NULL);
    fprintf(stderr,"  [ASYNC VOL TIMING] leaving %s, time=%ld.%06ld\n", __func__, now_time.tv_sec, now_time.tv_usec);
    #endif
    #ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
    #endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent == 1) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != args) free(args);
    return -1;
} // End async_object_optional





/*-------------------------------------------------------------------------
 * Function:    H5VL__async_new_obj
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
static H5VL_async_t *
H5VL_async_new_obj(void *under_obj, hid_t under_vol_id)
{
    H5VL_async_t *new_obj;

    new_obj = (H5VL_async_t *)calloc(1, sizeof(H5VL_async_t));
    new_obj->under_object = under_obj;
    new_obj->under_vol_id = under_vol_id;
    H5Iinc_ref(new_obj->under_vol_id);

    return new_obj;
} /* end H5VL__async_new_obj() */


/*-------------------------------------------------------------------------
 * Function:    H5VL__async_free_obj
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
static herr_t
H5VL_async_free_obj(H5VL_async_t *obj)
{
    hid_t err_id;

    err_id = H5Eget_current_stack();

    H5Idec_ref(obj->under_vol_id);

    H5Eset_current_stack(err_id);

    free(obj);

    return 0;
} /* end H5VL__async_free_obj() */



/*---------------------------------------------------------------------------
 * Function:    H5VL_async_info_copy
 *
 * Purpose:     Duplicate the connector's info object.
 *
 * Returns:     Success:    New connector info object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_async_info_copy(const void *_info)
{
    const H5VL_async_info_t *info = (const H5VL_async_info_t *)_info;
    H5VL_async_info_t *new_info;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INFO Copy\n");
#endif

    /* Allocate new VOL info struct for the async connector */
    new_info = (H5VL_async_info_t *)calloc(1, sizeof(H5VL_async_info_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_info->under_vol_id = info->under_vol_id;
    H5Iinc_ref(new_info->under_vol_id);
    if(info->under_vol_info)
        H5VLcopy_connector_info(new_info->under_vol_id, &(new_info->under_vol_info), info->under_vol_info);

    return new_info;
} /* end H5VL_async_info_copy() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_async_info_cmp
 *
 * Purpose:     Compare two of the connector's info objects, setting *cmp_value,
 *              following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_async_info_cmp(int *cmp_value, const void *_info1, const void *_info2)
{
    const H5VL_async_info_t *info1 = (const H5VL_async_info_t *)_info1;
    const H5VL_async_info_t *info2 = (const H5VL_async_info_t *)_info2;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INFO Compare\n");
#endif

    /* Sanity checks */
    assert(info1);
    assert(info2);

    /* Initialize comparison value */
    *cmp_value = 0;

    /* Compare under VOL connector classes */
    H5VLcmp_connector_cls(cmp_value, info1->under_vol_id, info2->under_vol_id);
    if(*cmp_value != 0)
        return 0;

    /* Compare under VOL connector info objects */
    H5VLcmp_connector_info(cmp_value, info1->under_vol_id, info1->under_vol_info, info2->under_vol_info);
    if(*cmp_value != 0)
        return 0;

    return 0;
} /* end H5VL_async_info_cmp() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_async_info_free
 *
 * Purpose:     Release an info object for the connector.
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_async_info_free(void *_info)
{
    H5VL_async_info_t *info = (H5VL_async_info_t *)_info;
    hid_t err_id;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INFO Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and info */
    if(info->under_vol_info)
        H5VLfree_connector_info(info->under_vol_id, info->under_vol_info);
    H5Idec_ref(info->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free async info object itself */
    free(info);

    return 0;
} /* end H5VL_async_info_free() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_async_info_to_str
 *
 * Purpose:     Serialize an info object for this connector into a string
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_async_info_to_str(const void *_info, char **str)
{
    const H5VL_async_info_t *info = (const H5VL_async_info_t *)_info;
    H5VL_class_value_t under_value = (H5VL_class_value_t)-1;
    char *under_vol_string = NULL;
    size_t under_vol_str_len = 0;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INFO To String\n");
#endif

    /* Get value and string for underlying VOL connector */
    H5VLget_value(info->under_vol_id, &under_value);
    H5VLconnector_info_to_str(info->under_vol_info, info->under_vol_id, &under_vol_string);

    /* Determine length of underlying VOL info string */
    if(under_vol_string)
        under_vol_str_len = strlen(under_vol_string);

    /* Allocate space for our info */
    *str = (char *)H5allocate_memory(32 + under_vol_str_len, (hbool_t)0);
    assert(*str);

    /* Encode our info
     * Normally we'd use snprintf() here for a little extra safety, but that
     * call had problems on Windows until recently. So, to be as platform-independent
     * as we can, we're using sprintf() instead.
     */
    sprintf(*str, "under_vol=%u;under_info={%s}", (unsigned)under_value, (under_vol_string ? under_vol_string : ""));

    return 0;
} /* end H5VL_async_info_to_str() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_async_str_to_info
 *
 * Purpose:     Deserialize a string into an info object for this connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_async_str_to_info(const char *str, void **_info)
{
    H5VL_async_info_t *info;
    unsigned under_vol_value;
    const char *under_vol_info_start, *under_vol_info_end;
    hid_t under_vol_id;
    void *under_vol_info = NULL;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INFO String To Info\n");
#endif

    /* Retrieve the underlying VOL connector value and info */
    sscanf(str, "under_vol=%u;", &under_vol_value);
    under_vol_id = H5VLregister_connector_by_value((H5VL_class_value_t)under_vol_value, H5P_DEFAULT);
    under_vol_info_start = strchr(str, '{');
    under_vol_info_end = strrchr(str, '}');
    assert(under_vol_info_end > under_vol_info_start);
    if(under_vol_info_end != (under_vol_info_start + 1)) {
        char *under_vol_info_str;

        under_vol_info_str = (char *)malloc((size_t)(under_vol_info_end - under_vol_info_start));
        memcpy(under_vol_info_str, under_vol_info_start + 1, (size_t)((under_vol_info_end - under_vol_info_start) - 1));
        *(under_vol_info_str + (under_vol_info_end - under_vol_info_start)) = '\0';

        H5VLconnector_str_to_info(under_vol_info_str, under_vol_id, &under_vol_info);

        free(under_vol_info_str);
    } /* end else */

    /* Allocate new async VOL connector info and set its fields */
    info = (H5VL_async_info_t *)calloc(1, sizeof(H5VL_async_info_t));
    info->under_vol_id = under_vol_id;
    info->under_vol_info = under_vol_info;

    /* Set return value */
    *_info = info;

    return 0;
} /* end H5VL_async_str_to_info() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_async_get_object
 *
 * Purpose:     Retrieve the 'data' for a VOL object.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_async_get_object(const void *obj)
{
    const H5VL_async_t *o = (const H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL Get object\n");
#endif

    return H5VLget_object(o->under_object, o->under_vol_id);
} /* end H5VL_async_get_object() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_async_get_wrap_ctx
 *
 * Purpose:     Retrieve a "wrapper context" for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_async_get_wrap_ctx(const void *obj, void **wrap_ctx)
{
    const H5VL_async_t *o = (const H5VL_async_t *)obj;
    H5VL_async_wrap_ctx_t *new_wrap_ctx;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL WRAP CTX Get\n");
#endif

    /* Allocate new VOL object wrapping context for the async connector */
    new_wrap_ctx = (H5VL_async_wrap_ctx_t *)calloc(1, sizeof(H5VL_async_wrap_ctx_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_wrap_ctx->under_vol_id = o->under_vol_id;
    H5Iinc_ref(new_wrap_ctx->under_vol_id);
    /* ==== added for aysnc vol ==== */
    if (o->under_object) {
        H5VLget_wrap_ctx(o->under_object, o->under_vol_id, &new_wrap_ctx->under_wrap_ctx);
    }

    /* Set wrap context to return */
    *wrap_ctx = new_wrap_ctx;

    return 0;
} /* end H5VL_async_get_wrap_ctx() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_async_wrap_object
 *
 * Purpose:     Use a "wrapper context" to wrap a data object
 *
 * Return:      Success:    Pointer to wrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_async_wrap_object(void *obj, H5I_type_t obj_type, void *_wrap_ctx)
{
    H5VL_async_wrap_ctx_t *wrap_ctx = (H5VL_async_wrap_ctx_t *)_wrap_ctx;
    H5VL_async_t *new_obj;
    void *under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL WRAP Object\n");
#endif

    /* Wrap the object with the underlying VOL */
    under = H5VLwrap_object(obj, obj_type, wrap_ctx->under_vol_id, wrap_ctx->under_wrap_ctx);
    if(under)
        new_obj = H5VL_async_new_obj(under, wrap_ctx->under_vol_id);
    else
        new_obj = NULL;

    return new_obj;
} /* end H5VL_async_wrap_object() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_async_unwrap_object
 *
 * Purpose:     Unwrap a wrapped object, discarding the wrapper, but returning
 *		underlying object.
 *
 * Return:      Success:    Pointer to unwrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_async_unwrap_object(void *obj)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    void *under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL UNWRAP Object\n");
#endif

    /* Unrap the object with the underlying VOL */
    under = H5VLunwrap_object(o->under_object, o->under_vol_id);

    if(under)
        H5VL_async_free_obj(o);

    return under;
} /* end H5VL_async_unwrap_object() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_async_free_wrap_ctx
 *
 * Purpose:     Release a "wrapper context" for an object
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_async_free_wrap_ctx(void *_wrap_ctx)
{
    H5VL_async_wrap_ctx_t *wrap_ctx = (H5VL_async_wrap_ctx_t *)_wrap_ctx;
    hid_t err_id;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL WRAP CTX Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and wrap context */
    if(wrap_ctx->under_wrap_ctx)
        H5VLfree_wrap_ctx(wrap_ctx->under_wrap_ctx, wrap_ctx->under_vol_id);
    H5Idec_ref(wrap_ctx->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free async wrap context object itself */
    free(wrap_ctx);

    return 0;
} /* end H5VL_async_free_wrap_ctx() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_create
 *
 * Purpose:     Creates an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_attr_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id,
    hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *attr;
    H5VL_async_t *o = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Create\n");
#endif

    attr = async_attr_create(0, async_instance_g, o, loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);


    return (void*)attr;
} /* end H5VL_async_attr_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_open
 *
 * Purpose:     Opens an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_attr_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *attr;
    H5VL_async_t *o = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Open\n");
#endif

    attr = async_attr_open(0, async_instance_g, o, loc_params, name, aapl_id, dxpl_id, req);


    return (void *)attr;
} /* end H5VL_async_attr_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_read
 *
 * Purpose:     Reads data from attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_attr_read(void *attr, hid_t mem_type_id, void *buf,
    hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)attr;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Read\n");
#endif

    if ((ret_value = async_attr_read(0, async_instance_g, o, mem_type_id, buf, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_read\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_attr_read() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_write
 *
 * Purpose:     Writes data to attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_attr_write(void *attr, hid_t mem_type_id, const void *buf,
    hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)attr;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Write\n");
#endif

    if ((ret_value = async_attr_write(0, async_instance_g, o, mem_type_id, buf, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_write\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_attr_write() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_get
 *
 * Purpose:     Gets information about an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Get\n");
#endif

    if ((ret_value = async_attr_get(1, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_get\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_attr_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_specific
 *
 * Purpose:     Specific operation on attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_attr_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Specific\n");
#endif

    if ((ret_value = async_attr_specific(1, async_instance_g, o, loc_params, specific_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_specific\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_attr_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_optional
 *
 * Purpose:     Perform a connector-specific operation on an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_attr_optional(void *obj, H5VL_attr_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Optional\n");
#endif

    if ((ret_value = async_attr_optional(1, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_optional\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_attr_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_attr_close
 *
 * Purpose:     Closes an attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1, attr not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_attr_close(void *attr, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)attr;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Close\n");
#endif

    if ((ret_value = async_attr_close(0, async_instance_g, o, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_close\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_attr_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_create
 *
 * Purpose:     Creates a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
    hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *dset;
    H5VL_async_t *o = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Create\n");
#endif

    dset = async_dataset_create(0, async_instance_g, o, loc_params, name, lcpl_id, type_id, space_id, dcpl_id,  dapl_id, dxpl_id, req);


    return (void *)dset;
} /* end H5VL_async_dataset_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_open
 *
 * Purpose:     Opens a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_dataset_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *dset;
    H5VL_async_t *o = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Open\n");
#endif

    dset = async_dataset_open(1, async_instance_g, o, loc_params, name, dapl_id, dxpl_id, req);


    return (void *)dset;
} /* end H5VL_async_dataset_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_read
 *
 * Purpose:     Reads data elements from a dataset into a buffer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Read\n");
#endif

    if ((ret_value = async_dataset_read(0, async_instance_g, o, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_read\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_dataset_read() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_write
 *
 * Purpose:     Writes data elements from a buffer into a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Write\n");
#endif

    if ((ret_value = async_dataset_write(0, async_instance_g, o, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_write\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_dataset_write() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_get
 *
 * Purpose:     Gets information about a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_dataset_get(void *dset, H5VL_dataset_get_t get_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Get\n");
#endif

    if ((ret_value = async_dataset_get(1, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_get\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_dataset_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_specific
 *
 * Purpose:     Specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_dataset_specific(void *obj, H5VL_dataset_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL H5Dspecific\n");
#endif

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    // For H5Dwait
    if (H5VL_DATASET_WAIT == specific_type)
        return (H5VL_async_dataset_wait(o));

    if ((ret_value = async_dataset_specific(1, async_instance_g, o, specific_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_specific\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, under_vol_id); */

    return ret_value;
} /* end H5VL_async_dataset_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_optional
 *
 * Purpose:     Perform a connector-specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_dataset_optional(void *obj, H5VL_dataset_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Optional\n");
#endif

    if ((ret_value = async_dataset_optional(1, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_optional\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_dataset_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_dataset_close
 *
 * Purpose:     Closes a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1, dataset not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Close\n");
#endif

    if ((ret_value = async_dataset_close(0, async_instance_g, o, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_close\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_dataset_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_commit
 *
 * Purpose:     Commits a datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id,
    hid_t dxpl_id, void **req)
{
    H5VL_async_t *dt;
    H5VL_async_t *o = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Commit\n");
#endif

    dt = async_datatype_commit(0, async_instance_g, o, loc_params, name, type_id, lcpl_id, tcpl_id, tapl_id, dxpl_id, req);


    return (void *)dt;
} /* end H5VL_async_datatype_commit() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_open
 *
 * Purpose:     Opens a named datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_datatype_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *dt;
    H5VL_async_t *o = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Open\n");
#endif

    dt = async_datatype_open(0, async_instance_g, o, loc_params, name, tapl_id, dxpl_id, req);


    return (void *)dt;
} /* end H5VL_async_datatype_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_get
 *
 * Purpose:     Get information about a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_datatype_get(void *dt, H5VL_datatype_get_t get_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)dt;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Get\n");
#endif

    if ((ret_value = async_datatype_get(1, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_datatype_get\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_datatype_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_specific
 *
 * Purpose:     Specific operations for datatypes
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_datatype_specific(void *obj, H5VL_datatype_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Specific\n");
#endif

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    if ((ret_value = async_datatype_specific(1, async_instance_g, o, specific_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_datatype_specific\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, under_vol_id); */

    return ret_value;
} /* end H5VL_async_datatype_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_optional
 *
 * Purpose:     Perform a connector-specific operation on a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_datatype_optional(void *obj, H5VL_datatype_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Optional\n");
#endif

    if ((ret_value = async_datatype_optional(1, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_datatype_optional\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_datatype_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_datatype_close
 *
 * Purpose:     Closes a datatype.
 *
 * Return:      Success:    0
 *              Failure:    -1, datatype not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)dt;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Close\n");
#endif

    assert(o->under_object);

    if ((ret_value = async_datatype_close(0, async_instance_g, o, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_datatype_close\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_datatype_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_file_create(const char *name, unsigned flags, hid_t fcpl_id,
    hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_info_t *info;
    H5VL_async_t *file;
    hid_t under_fapl_id;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Create\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    file = async_file_create(0, async_instance_g, name, flags, fcpl_id, under_fapl_id, dxpl_id, req);


    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    H5VL_async_info_free(info);

    return (void *)file;
} /* end H5VL_async_file_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_file_open(const char *name, unsigned flags, hid_t fapl_id,
    hid_t dxpl_id, void **req)
{
    H5VL_async_info_t *info;
    H5VL_async_t *file;
    hid_t under_fapl_id;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Open\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    file = async_file_open(0, async_instance_g, name, flags, under_fapl_id, dxpl_id, req);


    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    H5VL_async_info_free(info);

    return (void *)file;
} /* end H5VL_async_file_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_get
 *
 * Purpose:     Get info about a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)file;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Get\n");
#endif

    if ((ret_value = async_file_get(1, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_file_get\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_file_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_specific_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              file specific callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_file_specific_reissue(void *obj, hid_t connector_id,
    H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, ...)
{
    va_list arguments;
    herr_t ret_value;

    va_start(arguments, req);
    ret_value = H5VLfile_specific(obj, connector_id, specific_type, dxpl_id, req, arguments);
    va_end(arguments);

    return ret_value;
} /* end H5VL_async_file_specific_reissue() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_specific
 *
 * Purpose:     Specific operation on file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_file_specific(void *file, H5VL_file_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)file;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Specific\n");
#endif

    /* Unpack arguments to get at the child file pointer when mounting a file */
    if(specific_type == H5VL_FILE_MOUNT) {
        H5I_type_t loc_type;
        const char *name;
        H5VL_async_t *child_file;
        hid_t plist_id;

        /* Retrieve parameters for 'mount' operation, so we can unwrap the child file */
        loc_type = (H5I_type_t)va_arg(arguments, int); /* enum work-around */
        name = va_arg(arguments, const char *);
        child_file = (H5VL_async_t *)va_arg(arguments, void *);
        plist_id = va_arg(arguments, hid_t);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = o->under_vol_id;

        /* Re-issue 'file specific' call, using the unwrapped pieces */
        ret_value = H5VL_async_file_specific_reissue(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, (int)loc_type, name, child_file->under_object, plist_id);
    } /* end if */
    else if(H5VL_FILE_WAIT) {
        /* ==== Added for async H5Fwait */
        return (H5VL_async_file_wait(o));
    }
    else if(specific_type == H5VL_FILE_IS_ACCESSIBLE || specific_type == H5VL_FILE_DELETE) {
        H5VL_async_info_t *info;
        hid_t fapl_id, under_fapl_id;
        const char *name;
        htri_t *ret;

        /* Get the arguments for the 'is accessible' check */
        fapl_id = va_arg(arguments, hid_t);
        name    = va_arg(arguments, const char *);
        ret     = va_arg(arguments, htri_t *);

        /* Get copy of our VOL info from FAPL */
        H5Pget_vol_info(fapl_id, (void **)&info);

        /* Copy the FAPL */
        under_fapl_id = H5Pcopy(fapl_id);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = info->under_vol_id;

        /* Re-issue 'file specific' call */
        ret_value = H5VL_async_file_specific_reissue(NULL, info->under_vol_id, specific_type, dxpl_id, req, under_fapl_id, name, ret);

        /* Close underlying FAPL */
        H5Pclose(under_fapl_id);

        /* Release copy of our VOL info */
        H5VL_async_info_free(info);
    } /* end else-if */
    else {
        va_list my_arguments;

        /* Make a copy of the argument list for later, if reopening */
        if(specific_type == H5VL_FILE_REOPEN)
            va_copy(my_arguments, arguments);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = o->under_vol_id;

        ret_value = H5VLfile_specific(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, arguments);

        /* Wrap file struct pointer, if we reopened one */
        if(specific_type == H5VL_FILE_REOPEN) {
            if(ret_value >= 0) {
                void      **ret = va_arg(my_arguments, void **);

/*                 if(ret && *ret) */
/*                     *ret = H5VL_async_new_obj(*ret, o->under_vol_id); */
            } /* end if */

            /* Finish use of copied vararg list */
            va_end(my_arguments);
        } /* end if */
    } /* end else */

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, under_vol_id); */

    return ret_value;
} /* end H5VL_async_file_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_optional
 *
 * Purpose:     Perform a connector-specific operation on a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_file_optional(void *file, H5VL_file_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)file;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL File Optional\n");
#endif

    if ((ret_value = async_file_optional(1, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_file_optional\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_file_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_file_close
 *
 * Purpose:     Closes a file.
 *
 * Return:      Success:    0
 *              Failure:    -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_file_close(void *file, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)file;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Close\n");
#endif

    if ((ret_value = async_file_close(0, async_instance_g, o, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_file_close\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_file_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_create
 *
 * Purpose:     Creates a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_group_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id,
    hid_t dxpl_id, void **req)
{
    H5VL_async_t *group;
    H5VL_async_t *o = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Create\n");
#endif

    group = async_group_create(0, async_instance_g, o, loc_params, name, lcpl_id, gcpl_id,  gapl_id, dxpl_id, req);


    return (void *)group;
} /* end H5VL_async_group_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_open
 *
 * Purpose:     Opens a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_group_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *group;
    H5VL_async_t *o = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Open\n");
#endif

    group = async_group_open(0, async_instance_g, o, loc_params, name, gapl_id, dxpl_id, req);


    return (void *)group;
} /* end H5VL_async_group_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_get
 *
 * Purpose:     Get info about a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Get\n");
#endif

    if ((ret_value = async_group_get(1, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_group_get\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_group_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_specific
 *
 * Purpose:     Specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_group_specific(void *obj, H5VL_group_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Specific\n");
#endif

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    if ((ret_value = async_group_specific(1, async_instance_g, o, specific_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_group_specific\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, under_vol_id); */

    return ret_value;
} /* end H5VL_async_group_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_optional
 *
 * Purpose:     Perform a connector-specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_group_optional(void *obj, H5VL_group_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Optional\n");
#endif

    if ((ret_value = async_group_optional(1, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_group_optional\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_group_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_group_close
 *
 * Purpose:     Closes a group.
 *
 * Return:      Success:    0
 *              Failure:    -1, group not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_group_close(void *grp, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)grp;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL H5Gclose\n");
#endif

    if ((ret_value = async_group_close(0, async_instance_g, o, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_group_close\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_group_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_create_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              link create callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_link_create_reissue(H5VL_link_create_type_t create_type,
    void *obj, const H5VL_loc_params_t *loc_params, hid_t connector_id,
    hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, ...)
{
    va_list arguments;
    herr_t ret_value;

    va_start(arguments, req);
    ret_value = H5VLlink_create(create_type, obj, loc_params, connector_id, lcpl_id, lapl_id, dxpl_id, req, arguments);
    va_end(arguments);

    return ret_value;
} /* end H5VL_async_link_create_reissue() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_create
 *
 * Purpose:     Creates a hard / soft / UD / external link.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_link_create(H5VL_link_create_type_t create_type, void *obj,
    const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Create\n");
#endif

    /* Try to retrieve the "under" VOL id */
    if(o)
        under_vol_id = o->under_vol_id;

    /* Fix up the link target object for hard link creation */
    if(H5VL_LINK_CREATE_HARD == create_type) {
        void         *cur_obj;
        H5VL_loc_params_t *cur_params;

        /* Retrieve the object & loc params for the link target */
        cur_obj = va_arg(arguments, void *);
        cur_params = va_arg(arguments, H5VL_loc_params_t *);

        /* If it's a non-NULL pointer, find the 'under object' and re-set the property */
        if(cur_obj) {
            /* Check if we still need the "under" VOL ID */
            if(under_vol_id < 0)
                under_vol_id = ((H5VL_async_t *)cur_obj)->under_vol_id;

            /* Set the object for the link target */
            cur_obj = ((H5VL_async_t *)cur_obj)->under_object;
        } /* end if */

        /* Re-issue 'link create' call, using the unwrapped pieces */
        ret_value = H5VL_async_link_create_reissue(create_type, (o ? o->under_object : NULL), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req, cur_obj, cur_params);
    } /* end if */
    else
        ret_value = H5VLlink_create(create_type, (o ? o->under_object : NULL), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req, arguments);

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, under_vol_id); */

    return ret_value;
} /* end H5VL_async_link_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_copy
 *
 * Purpose:     Renames an object within an HDF5 container and copies it to a new
 *              group.  The original name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o_src = (H5VL_async_t *)src_obj;
    H5VL_async_t *o_dst = (H5VL_async_t *)dst_obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Copy\n");
#endif

    /* Retrieve the "under" VOL id */
    if(o_src)
        under_vol_id = o_src->under_vol_id;
    else if(o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    ret_value = H5VLlink_copy((o_src ? o_src->under_object : NULL), loc_params1, (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, under_vol_id); */

    return ret_value;
} /* end H5VL_async_link_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_move
 *
 * Purpose:     Moves a link within an HDF5 file to a new group.  The original
 *              name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o_src = (H5VL_async_t *)src_obj;
    H5VL_async_t *o_dst = (H5VL_async_t *)dst_obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Move\n");
#endif

    /* Retrieve the "under" VOL id */
    if(o_src)
        under_vol_id = o_src->under_vol_id;
    else if(o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    ret_value = H5VLlink_move((o_src ? o_src->under_object : NULL), loc_params1, (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, under_vol_id); */

    return ret_value;
} /* end H5VL_async_link_move() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_get
 *
 * Purpose:     Get info about a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_link_get(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Get\n");
#endif

    ret_value = H5VLlink_get(o->under_object, loc_params, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_link_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_specific
 *
 * Purpose:     Specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_link_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Specific\n");
#endif

    ret_value = H5VLlink_specific(o->under_object, loc_params, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_link_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_link_optional
 *
 * Purpose:     Perform a connector-specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_link_optional(void *obj, H5VL_link_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Optional\n");
#endif

    ret_value = H5VLlink_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_link_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_open
 *
 * Purpose:     Opens an object inside a container.
 *
 * Return:      Success:    Pointer to object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_async_object_open(void *obj, const H5VL_loc_params_t *loc_params,
    H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    H5VL_async_t *new_obj;
    H5VL_async_t *o = (H5VL_async_t *)obj;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Open\n");
#endif

    new_obj = async_object_open(0, async_instance_g, o, loc_params, opened_type, dxpl_id, req);


    return (void *)new_obj;
} /* end H5VL_async_object_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_copy
 *
 * Purpose:     Copies an object inside a container.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params,
    const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id,
    void **req)
{
    H5VL_async_t *o_src = (H5VL_async_t *)src_obj;
    H5VL_async_t *o_dst = (H5VL_async_t *)dst_obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Copy\n");
#endif

    if ((ret_value = async_object_copy(0, async_instance_g, o_src, src_loc_params, src_name, o_dst, dst_loc_params, dst_name, ocpypl_id, lcpl_id, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_object_copy\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o_src->under_vol_id); */

    return ret_value;
} /* end H5VL_async_object_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_get
 *
 * Purpose:     Get info about an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Get\n");
#endif

    if ((ret_value = async_object_get(1, async_instance_g, o, loc_params, get_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_object_get\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_object_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_specific
 *
 * Purpose:     Specific operation on an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_object_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Specific\n");
#endif

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    if ((ret_value = async_object_specific(1, async_instance_g, o, loc_params, specific_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_object_specific\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, under_vol_id); */

    return ret_value;
} /* end H5VL_async_object_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_object_optional
 *
 * Purpose:     Perform a connector-specific operation for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_object_optional(void *obj, H5VL_object_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Optional\n");
#endif

    if ((ret_value = async_object_optional(1, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_object_optional\n");
    }

    /* Check for async request */
/*     if(req && *req) */
/*         *req = H5VL_async_new_obj(*req, o->under_vol_id); */

    return ret_value;
} /* end H5VL_async_object_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_introspect_get_conn_clss
 *
 * Purpose:     Query the connector class.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_async_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
    const H5VL_class_t **conn_cls)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INTROSPECT GetConnCls\n");
#endif

    /* Check for querying this connector's class */
    if(H5VL_GET_CONN_LVL_CURR == lvl) {
        *conn_cls = &H5VL_async_g;
        ret_value = 0;
    } /* end if */
    else
        ret_value = H5VLintrospect_get_conn_cls(o->under_object, o->under_vol_id,
            lvl, conn_cls);

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
herr_t
H5VL_async_introspect_opt_query(void *obj, H5VL_subclass_t cls,
    int opt_type, hbool_t *supported)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INTROSPECT OptQuery\n");
#endif

    ret_value = H5VLintrospect_opt_query(o->under_object, o->under_vol_id, cls,
        opt_type, supported);

    return ret_value;
} /* end H5VL_async_introspect_opt_query() */
/* ====REPLACE_END==== */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_wait
 *
 * Purpose:     Wait (with a timeout) for an async operation to complete
 *
 * Note:        Releases the request if the operation has completed and the
 *              connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_request_wait(void *obj, uint64_t timeout,
    H5ES_status_t *status)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Wait\n");
#endif

    ret_value = H5VLrequest_wait(o->under_object, o->under_vol_id, timeout, status);

    if(ret_value >= 0 && *status != H5ES_STATUS_IN_PROGRESS)
        H5VL_async_free_obj(o);

    return ret_value;
} /* end H5VL_async_request_wait() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_notify
 *
 * Purpose:     Registers a user callback to be invoked when an asynchronous
 *              operation completes
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Notify\n");
#endif

    ret_value = H5VLrequest_notify(o->under_object, o->under_vol_id, cb, ctx);

    if(ret_value >= 0)
        H5VL_async_free_obj(o);

    return ret_value;
} /* end H5VL_async_request_notify() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_cancel
 *
 * Purpose:     Cancels an asynchronous operation
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_request_cancel(void *obj)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Cancel\n");
#endif

    ret_value = H5VLrequest_cancel(o->under_object, o->under_vol_id);

    if(ret_value >= 0)
        H5VL_async_free_obj(o);

    return ret_value;
} /* end H5VL_async_request_cancel() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_specific_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              request specific callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_request_specific_reissue(void *obj, hid_t connector_id,
    H5VL_request_specific_t specific_type, ...)
{
    va_list arguments;
    herr_t ret_value;

    va_start(arguments, specific_type);
    ret_value = H5VLrequest_specific(obj, connector_id, specific_type, arguments);
    va_end(arguments);

    return ret_value;
} /* end H5VL_async_request_specific_reissue() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_specific
 *
 * Purpose:     Specific operation on a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_request_specific(void *obj, H5VL_request_specific_t specific_type,
    va_list arguments)
{
    herr_t ret_value = -1;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Specific\n");
#endif

    if(H5VL_REQUEST_WAITANY == specific_type ||
            H5VL_REQUEST_WAITSOME == specific_type ||
            H5VL_REQUEST_WAITALL == specific_type) {
        va_list tmp_arguments;
        size_t req_count;

        /* Sanity check */
        assert(obj == NULL);

        /* Get enough info to call the underlying connector */
        va_copy(tmp_arguments, arguments);
        req_count = va_arg(tmp_arguments, size_t);

        /* Can only use a request to invoke the underlying VOL connector when there's >0 requests */
        if(req_count > 0) {
            void **req_array;
            void **under_req_array;
            uint64_t timeout;
            H5VL_async_t *o;
            size_t u;               /* Local index variable */

            /* Get the request array */
            req_array = va_arg(tmp_arguments, void **);

            /* Get a request to use for determining the underlying VOL connector */
            o = (H5VL_async_t *)req_array[0];

            /* Create array of underlying VOL requests */
            under_req_array = (void **)malloc(req_count * sizeof(void **));
            for(u = 0; u < req_count; u++)
                under_req_array[u] = ((H5VL_async_t *)req_array[u])->under_object;

            /* Remove the timeout value from the vararg list (it's used in all the calls below) */
            timeout = va_arg(tmp_arguments, uint64_t);

            /* Release requests that have completed */
            if(H5VL_REQUEST_WAITANY == specific_type) {
                size_t *idx;          /* Pointer to the index of completed request */
                H5ES_status_t *status;  /* Pointer to the request's status */

                /* Retrieve the remaining arguments */
                idx = va_arg(tmp_arguments, size_t *);
                assert(*idx <= req_count);
                status = va_arg(tmp_arguments, H5ES_status_t *);

                /* Reissue the WAITANY 'request specific' call */
                ret_value = H5VL_async_request_specific_reissue(o->under_object, o->under_vol_id, specific_type, req_count, under_req_array, timeout,
                                                                       idx,
                                                                       status);

                /* Release the completed request, if it completed */
                if(ret_value >= 0 && *status != H5ES_STATUS_IN_PROGRESS) {
                    H5VL_async_t *tmp_o;

                    tmp_o = (H5VL_async_t *)req_array[*idx];
                    H5VL_async_free_obj(tmp_o);
                } /* end if */
            } /* end if */
            else if(H5VL_REQUEST_WAITSOME == specific_type) {
                size_t *outcount;               /* # of completed requests */
                unsigned *array_of_indices;     /* Array of indices for completed requests */
                H5ES_status_t *array_of_statuses; /* Array of statuses for completed requests */

                /* Retrieve the remaining arguments */
                outcount = va_arg(tmp_arguments, size_t *);
                assert(*outcount <= req_count);
                array_of_indices = va_arg(tmp_arguments, unsigned *);
                array_of_statuses = va_arg(tmp_arguments, H5ES_status_t *);

                /* Reissue the WAITSOME 'request specific' call */
                ret_value = H5VL_async_request_specific_reissue(o->under_object, o->under_vol_id, specific_type, req_count, under_req_array, timeout, outcount, array_of_indices, array_of_statuses);

                /* If any requests completed, release them */
                if(ret_value >= 0 && *outcount > 0) {
                    unsigned *idx_array;    /* Array of indices of completed requests */

                    /* Retrieve the array of completed request indices */
                    idx_array = va_arg(tmp_arguments, unsigned *);

                    /* Release the completed requests */
                    for(u = 0; u < *outcount; u++) {
                        H5VL_async_t *tmp_o;

                        tmp_o = (H5VL_async_t *)req_array[idx_array[u]];
                        H5VL_async_free_obj(tmp_o);
                    } /* end for */
                } /* end if */
            } /* end else-if */
            else {      /* H5VL_REQUEST_WAITALL == specific_type */
                H5ES_status_t *array_of_statuses; /* Array of statuses for completed requests */

                /* Retrieve the remaining arguments */
                array_of_statuses = va_arg(tmp_arguments, H5ES_status_t *);

                /* Reissue the WAITALL 'request specific' call */
                ret_value = H5VL_async_request_specific_reissue(o->under_object, o->under_vol_id, specific_type, req_count, under_req_array, timeout, array_of_statuses);

                /* Release the completed requests */
                if(ret_value >= 0) {
                    for(u = 0; u < req_count; u++) {
                        if(array_of_statuses[u] != H5ES_STATUS_IN_PROGRESS) {
                            H5VL_async_t *tmp_o;

                            tmp_o = (H5VL_async_t *)req_array[u];
                            H5VL_async_free_obj(tmp_o);
                        } /* end if */
                    } /* end for */
                } /* end if */
            } /* end else */

            /* Release array of requests for underlying connector */
            free(under_req_array);
        } /* end if */

        /* Finish use of copied vararg list */
        va_end(tmp_arguments);
    } /* end if */
    else
        assert(0 && "Unknown 'specific' operation");

    return ret_value;
} /* end H5VL_async_request_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_optional
 *
 * Purpose:     Perform a connector-specific operation for a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_request_optional(void *obj, H5VL_request_optional_t opt_type,
    va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Optional\n");
#endif

    ret_value = H5VLrequest_optional(o->under_object, o->under_vol_id, opt_type, arguments);

    return ret_value;
} /* end H5VL_async_request_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_request_free
 *
 * Purpose:     Releases a request, allowing the operation to complete without
 *              application tracking
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_async_request_free(void *obj)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Free\n");
#endif

    ret_value = H5VLrequest_free(o->under_object, o->under_vol_id);

    if(ret_value >= 0)
        H5VL_async_free_obj(o);

    return ret_value;
} /* end H5VL_async_request_free() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_blob_put
 *
 * Purpose:     Handles the blob 'put' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_async_blob_put(void *obj, const void *buf, size_t size,
    void *blob_id, void *ctx)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL BLOB Put\n");
#endif

    ret_value = H5VLblob_put(o->under_object, o->under_vol_id, buf, size,
        blob_id, ctx);

    return ret_value;
} /* end H5VL_async_blob_put() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_blob_get
 *
 * Purpose:     Handles the blob 'get' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_async_blob_get(void *obj, const void *blob_id, void *buf,
    size_t size, void *ctx)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL BLOB Get\n");
#endif

    ret_value = H5VLblob_get(o->under_object, o->under_vol_id, blob_id, buf,
        size, ctx);

    return ret_value;
} /* end H5VL_async_blob_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_blob_specific
 *
 * Purpose:     Handles the blob 'specific' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_async_blob_specific(void *obj, void *blob_id,
    H5VL_blob_specific_t specific_type, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL BLOB Specific\n");
#endif

    ret_value = H5VLblob_specific(o->under_object, o->under_vol_id, blob_id,
        specific_type, arguments);

    return ret_value;
} /* end H5VL_async_blob_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_blob_optional
 *
 * Purpose:     Handles the blob 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_async_blob_optional(void *obj, void *blob_id,
    H5VL_blob_optional_t opt_type, va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL BLOB Optional\n");
#endif

    ret_value = H5VLblob_optional(o->under_object, o->under_vol_id, blob_id,
        opt_type, arguments);

    return ret_value;
} /* end H5VL_async_blob_optional() */


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
static herr_t
H5VL_async_token_cmp(void *obj, const H5O_token_t *token1,
    const H5O_token_t *token2, int *cmp_value)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL TOKEN Compare\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token1);
    assert(token2);
    assert(cmp_value);

    ret_value = H5VLtoken_cmp(o->under_object, o->under_vol_id, token1, token2, cmp_value);

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
static herr_t
H5VL_async_token_to_str(void *obj, H5I_type_t obj_type,
    const H5O_token_t *token, char **token_str)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL TOKEN To string\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token);
    assert(token_str);

    ret_value = H5VLtoken_to_str(o->under_object, obj_type, o->under_vol_id, token, token_str);

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
static herr_t
H5VL_async_token_from_str(void *obj, H5I_type_t obj_type,
    const char *token_str, H5O_token_t *token)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL TOKEN From string\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token);
    assert(token_str);

    ret_value = H5VLtoken_from_str(o->under_object, obj_type, o->under_vol_id, token_str, token);

    return ret_value;
} /* end H5VL_async_token_from_str() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_async_optional
 *
 * Purpose:     Handles the generic 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_async_optional(void *obj, int op_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL generic Optional\n");
#endif

    ret_value = H5VLoptional(o->under_object, o->under_vol_id, op_type,
        dxpl_id, req, arguments);

    return ret_value;
} /* end H5VL_async_optional() */

