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
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

/* Public HDF5 file */
#include "hdf5dev.h"
#include "H5VLconnector.h"

/* This connector's header */
#include "h5_vol_external_async_native.h"

/* Header for async "extension" routines */
#include "h5_async_lib.h"

/* Argobots header */
#include "abt.h"

/* Universal linked lists header */
#include "utlist.h"

#include "node_local_util.h"
/**********/
/* Macros */
/**********/

/* Whether to display log messge when callback is invoked */
/* (Uncomment to enable) */
/* #define ENABLE_LOG                  1 */
/* #define ENABLE_DBG_MSG              1 */
#define ENABLE_TIMING               1
/* #define PRINT_ERROR_STACK           1 */
/* #define ENABLE_ASYNC_LOGGING */


/* Default # of background threads */
#define ASYNC_VOL_DEFAULT_NTHREAD   1

/* Default # of dependencies to allocate */
#define ALLOC_INITIAL_SIZE 2

/* Default interval between checking for HDF5 global lock */
#define ASYNC_ATTEMPT_CHECK_INTERVAL 1

/* Magic #'s for memory structures */
#define ASYNC_MAGIC 10242048
#define TASK_MAGIC 20481024

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
    struct H5VL_async_t *file_async_obj;
} H5VL_async_wrap_ctx_t;

typedef enum {QTYPE_NONE, REGULAR, DEPENDENT, COLLECTIVE, BLOCKING, ISOLATED} task_list_qtype;
typedef enum {OP_NONE, READ, WRITE} obj_op_type;
const char* qtype_names_g[10] = {"QTYPE_NONE", "REGULAR", "DEPENDENT", "COLLECTIVE", "BLOCKING", "ISOLATED"};

struct H5VL_async_t;

typedef struct async_task_t {
    hid_t               under_vol_id;
    int                 magic;
    ABT_mutex           task_mutex;
    void                *h5_state;
    void                (*func)(void *);
    void                *args;
    obj_op_type         op;
    struct H5VL_async_t *async_obj;
    ABT_eventual        eventual;
    int                 in_abt_pool;
    int                 is_done;
    /* ABT_task            abt_task; */
    ABT_thread          abt_thread;
    hid_t               err_stack;

    int                 n_dep;
    int                 n_dep_alloc;
    struct async_task_t **dep_tasks;

    struct H5VL_async_t *parent_obj;            /* pointer back to the parent async object */

    clock_t             create_time;
    clock_t             start_time;
    clock_t             end_time;

    struct async_task_t *prev;
    struct async_task_t *next;
    struct async_task_t *file_list_prev;
    struct async_task_t *file_list_next;
} async_task_t;

typedef struct H5VL_async_t {
    hid_t               under_vol_id;
    void                *under_object;
    int                 magic;
    int                 is_obj_valid;
    async_task_t        *create_task;           /* task that creates the object */
    async_task_t        *close_task;
    async_task_t        *my_task;               /* for request */
    async_task_t        *file_task_list_head;
    ABT_mutex           file_task_list_mutex;
    struct H5VL_async_t *file_async_obj;
    int                 task_cnt;
    int                 attempt_check_cnt;
    ABT_mutex           obj_mutex;
    ABT_pool            *pool_ptr;
    hbool_t             is_col_meta;
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
    ABT_sched           *progress_scheds;
    int                 nfopen;
    bool                ex_delay;            /* Delay background thread execution */
    bool                ex_fclose;           /* Delay background thread execution until file close */
    bool                ex_gclose;           /* Delay background thread execution until group close */
    bool                ex_dclose;           /* Delay background thread execution until dset close */
    bool                start_abt_push;      /* Start pushing tasks to Argobots pool */
} async_instance_t;

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
} async_attr_create_args_t;

typedef struct async_attr_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    aapl_id;
    hid_t                    dxpl_id;
    void                     **req;
} async_attr_open_args_t;

typedef struct async_attr_read_args_t {
    void                     *attr;
    hid_t                    mem_type_id;
    void                     *buf;
    hid_t                    dxpl_id;
    void                     **req;
} async_attr_read_args_t;

typedef struct async_attr_write_args_t {
    void                     *attr;
    hid_t                    mem_type_id;
    void                     *buf;
    hid_t                    dxpl_id;
    void                     **req;
} async_attr_write_args_t;

typedef struct async_attr_get_args_t {
    void                     *obj;
    H5VL_attr_get_t          get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_attr_get_args_t;

typedef struct async_attr_specific_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_attr_specific_t     specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_attr_specific_args_t;

typedef struct async_attr_optional_args_t {
    void                     *obj;
    H5VL_attr_optional_t     opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_attr_optional_args_t;

typedef struct async_attr_close_args_t {
    void                     *attr;
    hid_t                    dxpl_id;
    void                     **req;
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
} async_dataset_create_args_t;

typedef struct async_dataset_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    dapl_id;
    hid_t                    dxpl_id;
    void                     **req;
} async_dataset_open_args_t;

typedef struct async_dataset_read_args_t {
    void                     *dset;
    hid_t                    mem_type_id;
    hid_t                    mem_space_id;
    hid_t                    file_space_id;
    hid_t                    plist_id;
    void                     *buf;
    void                     **req;
} async_dataset_read_args_t;

typedef struct async_dataset_write_args_t {
    void                     *dset;
    hid_t                    mem_type_id;
    hid_t                    mem_space_id;
    hid_t                    file_space_id;
    hid_t                    plist_id;
    void                     *buf;
    void                     **req;
} async_dataset_write_args_t;

typedef struct async_dataset_get_args_t {
    void                     *dset;
    H5VL_dataset_get_t       get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_dataset_get_args_t;

typedef struct async_dataset_specific_args_t {
    void                     *obj;
    H5VL_dataset_specific_t  specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_dataset_specific_args_t;

typedef struct async_dataset_optional_args_t {
    void                     *obj;
    H5VL_dataset_optional_t  opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_dataset_optional_args_t;

typedef struct async_dataset_close_args_t {
    void                     *dset;
    hid_t                    dxpl_id;
    void                     **req;
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
} async_datatype_commit_args_t;

typedef struct async_datatype_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    tapl_id;
    hid_t                    dxpl_id;
    void                     **req;
} async_datatype_open_args_t;

typedef struct async_datatype_get_args_t {
    void                     *dt;
    H5VL_datatype_get_t      get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_datatype_get_args_t;

typedef struct async_datatype_specific_args_t {
    void                     *obj;
    H5VL_datatype_specific_t specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_datatype_specific_args_t;

typedef struct async_datatype_optional_args_t {
    void                     *obj;
    H5VL_datatype_optional_t opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_datatype_optional_args_t;

typedef struct async_datatype_close_args_t {
    void                     *dt;
    hid_t                    dxpl_id;
    void                     **req;
} async_datatype_close_args_t;

typedef struct async_file_create_args_t {
    char                     *name;
    unsigned                 flags;
    hid_t                    fcpl_id;
    hid_t                    fapl_id;
    hid_t                    dxpl_id;
    void                     **req;
} async_file_create_args_t;

typedef struct async_file_open_args_t {
    char                     *name;
    unsigned                 flags;
    hid_t                    fapl_id;
    hid_t                    dxpl_id;
    void                     **req;
} async_file_open_args_t;

typedef struct async_file_get_args_t {
    void                     *file;
    H5VL_file_get_t          get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_file_get_args_t;

typedef struct async_file_specific_args_t {
    void                     *file;
    H5VL_file_specific_t     specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_file_specific_args_t;

typedef struct async_file_optional_args_t {
    void                     *file;
    H5VL_file_optional_t     opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_file_optional_args_t;

typedef struct async_file_close_args_t {
    void                     *file;
    hid_t                    dxpl_id;
    void                     **req;
    bool                     is_reopen;
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
} async_group_create_args_t;

typedef struct async_group_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    char                     *name;
    hid_t                    gapl_id;
    hid_t                    dxpl_id;
    void                     **req;
} async_group_open_args_t;

typedef struct async_group_get_args_t {
    void                     *obj;
    H5VL_group_get_t         get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_group_get_args_t;

typedef struct async_group_specific_args_t {
    void                     *obj;
    H5VL_group_specific_t    specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_group_specific_args_t;

typedef struct async_group_optional_args_t {
    void                     *obj;
    H5VL_group_optional_t    opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_group_optional_args_t;

typedef struct async_group_close_args_t {
    void                     *grp;
    hid_t                    dxpl_id;
    void                     **req;
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
} async_link_move_args_t;

typedef struct async_link_get_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_link_get_t          get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_link_get_args_t;

typedef struct async_link_specific_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_link_specific_t     specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_link_specific_args_t;

typedef struct async_link_optional_args_t {
    void                     *obj;
    H5VL_link_optional_t     opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_link_optional_args_t;

typedef struct async_object_open_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5I_type_t               *opened_type;
    hid_t                    dxpl_id;
    void                     **req;
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
} async_object_copy_args_t;

typedef struct async_object_get_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_object_get_t        get_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_object_get_args_t;

typedef struct async_object_specific_args_t {
    void                     *obj;
    H5VL_loc_params_t        *loc_params;
    H5VL_object_specific_t   specific_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_object_specific_args_t;

typedef struct async_object_optional_args_t {
    void                     *obj;
    H5VL_object_optional_t   opt_type;
    hid_t                    dxpl_id;
    void                     **req;
    va_list                  arguments;
} async_object_optional_args_t;

/*******************/
/* Global Variables*/
/*******************/
ABT_mutex           async_instance_mutex_g;
async_instance_t   *async_instance_g  = NULL;
hid_t               async_connector_id_g = -1;
hid_t               async_error_class_g = H5I_INVALID_HID;

/********************* */
/* Function prototypes */
/********************* */

/* Helper routines */
static H5VL_async_t *H5VL_async_new_obj(void *under_obj, hid_t under_vol_id);
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
static herr_t H5VL_async_introspect_get_cap_flags(const void *info, unsigned *cap_flags);
static herr_t H5VL_async_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, uint64_t *flags);

/* Async request callbacks */
static herr_t H5VL_async_request_wait(void *req, uint64_t timeout, H5VL_request_status_t *status);
static herr_t H5VL_async_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx);
static herr_t H5VL_async_request_cancel(void *req, H5VL_request_status_t *status);
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
    H5VL_VERSION,                            /* VOL class struct version */
    (H5VL_class_value_t)H5VL_ASYNC_VALUE,    /* value        */
    H5VL_ASYNC_NAME,                         /* name         */
    H5VL_ASYNC_VERSION,                      /* connector version */
    H5VL_CAP_FLAG_ASYNC,                     /* capability flags */
    H5VL_async_init,                         /* initialize   */
    H5VL_async_term,                         /* terminate    */
    {   /* info_cls */
        sizeof(H5VL_async_info_t),           /* size    */
        H5VL_async_info_copy,                /* copy    */
        H5VL_async_info_cmp,                 /* compare */
        H5VL_async_info_free,                /* free    */
        H5VL_async_info_to_str,              /* to_str  */
        H5VL_async_str_to_info               /* from_str */
    },
    {   /* wrap_cls */
        H5VL_async_get_object,               /* get_object   */
        H5VL_async_get_wrap_ctx,             /* get_wrap_ctx */
        H5VL_async_wrap_object,              /* wrap_object  */
        H5VL_async_unwrap_object,            /* unwrap_object */
        H5VL_async_free_wrap_ctx             /* free_wrap_ctx */
    },
    {   /* attribute_cls */
        H5VL_async_attr_create,              /* create */
        H5VL_async_attr_open,                /* open */
        H5VL_async_attr_read,                /* read */
        H5VL_async_attr_write,               /* write */
        H5VL_async_attr_get,                 /* get */
        H5VL_async_attr_specific,            /* specific */
        H5VL_async_attr_optional,            /* optional */
        H5VL_async_attr_close                /* close */
    },
    {   /* dataset_cls */
        H5VL_async_dataset_create,           /* create */
        H5VL_async_dataset_open,             /* open */
        H5VL_async_dataset_read,             /* read */
        H5VL_async_dataset_write,            /* write */
        H5VL_async_dataset_get,              /* get */
        H5VL_async_dataset_specific,         /* specific */
        H5VL_async_dataset_optional,         /* optional */
        H5VL_async_dataset_close             /* close */
    },
    {   /* datatype_cls */
        H5VL_async_datatype_commit,          /* commit */
        H5VL_async_datatype_open,            /* open */
        H5VL_async_datatype_get,             /* get_size */
        H5VL_async_datatype_specific,        /* specific */
        H5VL_async_datatype_optional,        /* optional */
        H5VL_async_datatype_close            /* close */
    },
    {   /* file_cls */
        H5VL_async_file_create,              /* create */
        H5VL_async_file_open,                /* open */
        H5VL_async_file_get,                 /* get */
        H5VL_async_file_specific,            /* specific */
        H5VL_async_file_optional,            /* optional */
        H5VL_async_file_close                /* close */
    },
    {   /* group_cls */
        H5VL_async_group_create,             /* create */
        H5VL_async_group_open,               /* open */
        H5VL_async_group_get,                /* get */
        H5VL_async_group_specific,           /* specific */
        H5VL_async_group_optional,           /* optional */
        H5VL_async_group_close               /* close */
    },
    {   /* link_cls */
        H5VL_async_link_create,              /* create */
        H5VL_async_link_copy,                /* copy */
        H5VL_async_link_move,                /* move */
        H5VL_async_link_get,                 /* get */
        H5VL_async_link_specific,            /* specific */
        H5VL_async_link_optional             /* optional */
    },
    {   /* object_cls */
        H5VL_async_object_open,              /* open */
        H5VL_async_object_copy,              /* copy */
        H5VL_async_object_get,               /* get */
        H5VL_async_object_specific,          /* specific */
        H5VL_async_object_optional           /* optional */
    },
    {   /* introspect_cls */
        H5VL_async_introspect_get_conn_cls,  /* get_conn_cls */
        H5VL_async_introspect_get_cap_flags, /* get_cap_flags */
        H5VL_async_introspect_opt_query,     /* opt_query */
    },
    {   /* request_cls */
        H5VL_async_request_wait,             /* wait */
        H5VL_async_request_notify,           /* notify */
        H5VL_async_request_cancel,           /* cancel */
        H5VL_async_request_specific,         /* specific */
        H5VL_async_request_optional,         /* optional */
        H5VL_async_request_free              /* free */
    },
    {   /* blob_cls */
        H5VL_async_blob_put,                 /* put */
        H5VL_async_blob_get,                 /* get */
        H5VL_async_blob_specific,            /* specific */
        H5VL_async_blob_optional             /* optional */
    },
    {   /* token_cls */
        H5VL_async_token_cmp,                /* cmp */
        H5VL_async_token_to_str,             /* to_str */
        H5VL_async_token_from_str            /* from_str */
    },
    H5VL_async_optional                      /* optional */
};

/* Operation values for new API routines */
/* These are initialized in the VOL connector's 'init' callback at runtime.
 *      It's good practice to reset them back to -1 in the 'term' callback.
 */
static int H5VL_async_file_wait_op_g = -1;
static int H5VL_async_dataset_wait_op_g = -1;
static int H5VL_async_file_start_op_g = -1;

H5PL_type_t H5PLget_plugin_type(void) {
    return H5PL_TYPE_VOL;
}
const void *H5PLget_plugin_info(void) {
    return &H5VL_async_g;
}

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

static herr_t
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

    free(async_instance_g->progress_scheds);
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

    abt_ret = ABT_finalize();
    if (ABT_SUCCESS != abt_ret) {
        fprintf(stderr, "  [ASYNC VOL ERROR] with finalize argobots\n");
        ret_val = -1;
        goto done;
    }
    #ifdef ENABLE_DBG_MSG
    else
        fprintf(stderr, "  [ASYNC VOL DBG] Success with Argobots finalize\n");
    #endif

done:
    return ret_val;
}

/* Init Argobots for async IO */
static herr_t
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
    if (aid == NULL) {
        hg_ret = -1;
        goto done;
    }

    abt_ret = ABT_mutex_create(&aid->qhead.head_mutex);
    if (ABT_SUCCESS != abt_ret) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with head_mutex create\n", __func__);
        free(aid);
        return -1;
    }

    if (backing_thread_count == 0) {
        aid->num_xstreams = 0;
        abt_ret = ABT_xstream_self(&self_xstream);
        if (abt_ret != ABT_SUCCESS) {
            free(aid);
            hg_ret = -1;
            goto done;
        }
        abt_ret = ABT_xstream_get_main_pools(self_xstream, 1, &pool);
        if (abt_ret != ABT_SUCCESS) {
            free(aid);
            hg_ret = -1;
            goto done;
        }
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
    aid->progress_scheds = progress_scheds;
    aid->nfopen       = 0;
    aid->ex_delay     = false;
    aid->ex_fclose    = false;
    aid->ex_gclose    = false;
    aid->ex_dclose    = false;
    aid->start_abt_push = false;

    // Check for delaying operations to file / group / dataset close operations
    env_var = getenv("HDF5_ASYNC_EXE_FCLOSE");
    if (env_var && *env_var && atoi(env_var) > 0 )
        aid->ex_fclose = true;
    env_var = getenv("HDF5_ASYNC_EXE_GCLOSE");
    if (env_var && *env_var && atoi(env_var) > 0 )
        aid->ex_gclose = true;
    env_var = getenv("HDF5_ASYNC_EXE_DCLOSE");
    if (env_var && *env_var && atoi(env_var) > 0 )
        aid->ex_dclose = true;

    /* Set "delay execution" convenience flag, if any of the others are set */
    if(aid->ex_fclose || aid->ex_gclose || aid->ex_dclose)
        aid->ex_delay  = true;

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

static herr_t
H5VL_async_init(hid_t __attribute__((unused)) vipl_id)
{
    /* Initialize the Argobots I/O instance */
    if (NULL == async_instance_g) {
        int n_thread = ASYNC_VOL_DEFAULT_NTHREAD;

        if (async_instance_init(n_thread) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] with async_instance_init\n");
            return -1;
        }
    }

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
    assert(-1 == H5VL_async_file_start_op_g);
    if(H5VLregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_START, &H5VL_async_file_start_op_g) < 0) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with H5VLregister_opt_operation\n");
        return(-1);
    }
    assert(-1 != H5VL_async_file_start_op_g);

    /* Singleton register error class */
    if (H5I_INVALID_HID == async_error_class_g) {
        if((async_error_class_g = H5Eregister_class("Async VOL", "Async VOL", "0.1")) < 0) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with H5Eregister_class\n");
            return -1;
        }
    }

    return 0;
}

static void
async_waitall(void)
{
    int sleeptime = 100000;
    size_t size = 1;

    while(async_instance_g && (async_instance_g->nfopen > 0 || size > 0)) {

        usleep(sleeptime);

        ABT_pool_get_size(async_instance_g->pool, &size);
        /* printf("H5VLasync_wailall: pool size is %lu\n", size); */

        if (size == 0) {
            if (async_instance_g->nfopen == 0)
                break;
        }
    }

    return;
}

static herr_t
H5VL_async_term(void)
{
    herr_t ret_val = 0;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] ASYNC VOL terminate\n");
#endif

    /* Wait for all operations to complete */
    async_waitall();

    /* Shut down Argobots */
    async_term();

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
    if(-1 != H5VL_async_file_start_op_g) {
        if(H5VLunregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_START) < 0)
            return(-1);
        H5VL_async_file_start_op_g = (-1);
    } /* end if */

    /* Unregister error class */
    if(H5I_INVALID_HID != async_error_class_g) {
        if (H5Eunregister_class(async_error_class_g) < 0)
            fprintf(stderr,"  [ASYNC VOL ERROR] ASYNC VOL unregister error class failed\n");
        async_error_class_g = H5I_INVALID_HID;
    }

    return ret_val;
}

static async_task_t *
create_async_task(void)
{
    async_task_t *async_task;

    if ((async_task = (async_task_t*)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s calloc failed\n", __func__);
        return NULL;
    }

    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s ABT_mutex_create failed\n", __func__);
        return NULL;
    }

    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        return NULL;
    }

    async_task->magic = TASK_MAGIC;

    return async_task;
}

static void
free_async_task(async_task_t *task)
{
    assert(task->magic == TASK_MAGIC);

    ABT_mutex_lock(task->task_mutex);

    if (task->args) free(task->args);

    if (ABT_eventual_free(&task->eventual) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_eventual_free\n", __func__);
        return;
    }
    /* if (task->children) free(task->children); */
    if (task->dep_tasks) free(task->dep_tasks);
    
    if (task->err_stack != 0) H5Eclose_stack(task->err_stack);

    ABT_mutex_unlock(task->task_mutex);

    if (ABT_mutex_free(&task->task_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__);
        return;
    }

    memset(task, 0, sizeof(async_task_t));
    free(task);

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

static void async_file_close_fn(void *foo);

static void
free_file_async_resources(H5VL_async_t *file)
{
    async_task_t *task_iter, *tmp;

    assert(file);
    assert(file->magic == ASYNC_MAGIC);

    // When closing a reopened file
    if (NULL == file->file_async_obj) {
        return;
    }

    if (file->file_task_list_mutex && ABT_mutex_lock(file->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return;
    }

    DL_FOREACH_SAFE2(file->file_async_obj->file_task_list_head, task_iter, tmp, file_list_next) {
        DL_DELETE2(file->file_async_obj->file_task_list_head, task_iter, file_list_prev, file_list_next);
        // Defer the file close task free operation to later request free so H5ESwait works even after file is closed
        if (task_iter->func != async_file_close_fn && task_iter->magic == TASK_MAGIC) {
            free_async_task(task_iter);
        }
    }

    if (file->file_task_list_mutex && ABT_mutex_unlock(file->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return;
    }

    /* if (file->obj_mutex && ABT_mutex_free(&file->obj_mutex) != ABT_SUCCESS) { */
    /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__); */
    /*     return; */
    /* } */

    if (file->file_task_list_mutex && ABT_mutex_free(&file->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__);
        return;
    }

    // File object is freed later at request free time for event set to working after file close
    /* free(file); */
}

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
    int               i, is_dep_done = 1;
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

                /* // If dependent parent failed, do not push to Argobots pool */
                /* if (task_elt->dep_tasks[i]->err_stack != 0) { */
                /*     task_elt->err_stack = H5Ecreate_stack(); */
                /*     H5Eappend_stack(task_elt->err_stack, task_elt->dep_tasks[i]->err_stack, false); */
                /*     H5Epush(task_elt->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, */
                /*         H5E_VOL, H5E_CANTCREATE, "Parent task failed"); */

/* #ifdef PRINT_ERROR_STACK */
                /*     H5Eprint2(task_elt->err_stack, stderr); */
/* #endif */
                /*     DL_DELETE(qhead->queue->task_list, task_elt); */
                /*     task_elt->prev = NULL; */
                /*     task_elt->next = NULL; */
                /*     is_dep_done = 0; */
                /*     break; */
                /* } */

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
                    if (thread_state != ABT_THREAD_STATE_TERMINATED && thread_state != ABT_THREAD_STATE_RUNNING && thread_state != ABT_THREAD_STATE_READY) {
                        is_dep_done = 0;
#ifdef ENABLE_DBG_MSG
                        fprintf(stderr,"  [ASYNC VOL DBG] dependent task [%p] not finished\n", task_elt->dep_tasks[i]->func);
#endif
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

    /* // If the dependent parents of tasks in the dependent task list failed, resulting in no executing, */
    /* // continue to push the next task list */
    /* if (qhead->queue != NULL && is_dep_done == 0) { */
    /*     push_task_to_abt_pool(qhead, pool); */
    /* } */

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

    return 1;
} // End push_task_to_abt_pool

static int
get_n_running_task_in_queue(async_task_t *task)
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
            if (thread_state != ABT_THREAD_STATE_TERMINATED) {
            /* if (thread_state == ABT_THREAD_STATE_RUNNING) { */
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
        if (task_elt->magic == TASK_MAGIC && task_elt->abt_thread != NULL) {
            ABT_thread_get_state(task_elt->abt_thread, &thread_state);
            if (thread_state == ABT_THREAD_STATE_RUNNING || thread_state == ABT_THREAD_STATE_READY)
                remaining_task++;
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
    if (task_type == DEPENDENT) {
        if (task->parent_obj && task->parent_obj->is_obj_valid != 1) {
            /* is_dep = 1; */
            task_type = DEPENDENT;
            if (add_to_dep_task(task, task->parent_obj->create_task) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s add_to_dep_task failed\n", __func__);
                return -1;
            }
        }

        if (task != task->async_obj->create_task && task->async_obj->is_obj_valid != 1 && task->parent_obj->create_task != task->async_obj->create_task) {
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

static void
dup_loc_param(H5VL_loc_params_t *dest, H5VL_loc_params_t const *loc_params)
{
    size_t ref_size;

    assert(dest);
    assert(loc_params);

    memcpy(dest, loc_params, sizeof(*loc_params));

    if (loc_params->type == H5VL_OBJECT_BY_NAME) {
        if (NULL != loc_params->loc_data.loc_by_name.name)
            dest->loc_data.loc_by_name.name = strdup(loc_params->loc_data.loc_by_name.name);
        dest->loc_data.loc_by_name.lapl_id = H5Pcopy(loc_params->loc_data.loc_by_name.lapl_id);
    }
    else if (loc_params->type == H5VL_OBJECT_BY_IDX) {
        if (NULL != loc_params->loc_data.loc_by_idx.name)
            dest->loc_data.loc_by_idx.name = strdup(loc_params->loc_data.loc_by_idx.name);
        dest->loc_data.loc_by_idx.lapl_id = H5Pcopy(loc_params->loc_data.loc_by_idx.lapl_id);
    }
    else if (loc_params->type == H5VL_OBJECT_BY_TOKEN) {
        ref_size = 16; // taken from H5VLnative_object.c
        dest->loc_data.loc_by_token.token = malloc(ref_size);
        memcpy((void*)(dest->loc_data.loc_by_token.token), loc_params->loc_data.loc_by_token.token, ref_size);
    }

}

static void free_loc_param(H5VL_loc_params_t *loc_params)
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

herr_t H5VL_async_object_wait(H5VL_async_t *async_obj)
{
    /* herr_t ret_value; */
    async_task_t *task_iter;
    /* ABT_task_state state; */
    /* ABT_thread_state thread_state; */
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    hbool_t tmp = async_instance_g->start_abt_push;

    async_instance_g->start_abt_push = true;

    if (get_n_running_task_in_queue_obj(async_obj) == 0 )
        push_task_to_abt_pool(&async_instance_g->qhead, *async_obj->pool_ptr);

    if (H5TSmutex_release(&mutex_count) < 0)
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
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr, "  [ASYNC VOL DBG] %s setting start_abt_push false!\n", __func__);
#endif
    async_instance_g->start_abt_push = tmp;

    return 0;
}


herr_t H5VL_async_dataset_wait(H5VL_async_t *async_obj)
{
    /* herr_t ret_value; */
    async_task_t *task_iter;
    /* ABT_task_state state; */
    /* ABT_thread_state thread_state; */
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    hbool_t tmp = async_instance_g->start_abt_push;

    async_instance_g->start_abt_push = true;

    if (get_n_running_task_in_queue_obj(async_obj) == 0 )
        push_task_to_abt_pool(&async_instance_g->qhead, *async_obj->pool_ptr);

    if (H5TSmutex_release(&mutex_count) < 0)
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
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr, "  [ASYNC VOL DBG] %s setting start_abt_push false!\n", __func__);
#endif
    async_instance_g->start_abt_push = tmp;

    return 0;
}

herr_t H5VL_async_file_wait(H5VL_async_t *async_obj)
{
    /* herr_t ret_value; */
    async_task_t *task_iter;
    /* ABT_task_state state; */
    /* ABT_thread_state thread_state; */
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    hbool_t tmp = async_instance_g->start_abt_push;

    async_instance_g->start_abt_push = true;

    if (get_n_running_task_in_queue_obj(async_obj) == 0 )
        push_task_to_abt_pool(&async_instance_g->qhead, *async_obj->pool_ptr);

    if (H5TSmutex_release(&mutex_count) < 0)
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
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
    }

    async_instance_g->start_abt_push = tmp;
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

herr_t
H5VL_async_start()
{
    assert(async_instance_g);
    async_instance_g->start_abt_push = true;
    if (async_instance_g && NULL != async_instance_g->qhead.queue)
        push_task_to_abt_pool(&async_instance_g->qhead, async_instance_g->pool);
    return 0;
}

/* double get_elapsed_time(clock_t *tstart, clock_t *tend) */
/* { */
/*     return (double)(((tend-tstart) / CLOCKS_PER_SEC *1000000000LL ; */
/* } */

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

static int 
check_parent_task(H5VL_async_t *parent_obj)
{
    int ret_val = 0;
    if (parent_obj->create_task && parent_obj->create_task->err_stack != 0) 
        return -1;

    return ret_val;
}
    
static void
async_attr_create_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_create_args_t *args = (async_attr_create_args_t*)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLattr_create(args->obj, args->loc_params, task->under_vol_id, args->name, args->type_id, args->space_id, args->acpl_id, args->aapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->type_id > 0)    H5Tclose(args->type_id);
    if(args->space_id > 0)   H5Sclose(args->space_id);
    if(args->acpl_id > 0)    H5Pclose(args->acpl_id);
    if(args->aapl_id > 0)    H5Pclose(args->aapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_create_fn

static H5VL_async_t*
async_attr_create(async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_attr_create_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_create_args_t*)calloc(1, sizeof(async_attr_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
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
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        /* fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__); */
        H5VL_async_free_obj(async_obj);
        free_async_task(async_task);
        async_obj = NULL;
        goto done;
    }

    async_task->func       = async_attr_create_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0) {
            async_obj = NULL;
            goto error;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_attr_create

static void
async_attr_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_open_args_t *args = (async_attr_open_args_t*)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLattr_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->aapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->aapl_id > 0)    H5Pclose(args->aapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_open_fn

static H5VL_async_t*
async_attr_open(async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_attr_open_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_open_args_t*)calloc(1, sizeof(async_attr_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if(aapl_id > 0)
        args->aapl_id = H5Pcopy(aapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_attr_open

static void
async_attr_read_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_read_args_t *args = (async_attr_read_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLattr_read(args->attr, task->under_vol_id, args->mem_type_id, args->buf, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->mem_type_id > 0)    H5Tclose(args->mem_type_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_read_fn

static herr_t
async_attr_read(async_instance_t* aid, H5VL_async_t *parent_obj, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req)
{
    // For implicit mode (env var), make all read to be blocking
    assert(async_instance_g);
    async_task_t *async_task = NULL;
    async_attr_read_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_read_args_t*)calloc(1, sizeof(async_attr_read_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

    args->attr             = parent_obj->under_object;
    if(mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    args->buf              = buf;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_attr_read

static void
async_attr_write_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_write_args_t *args = (async_attr_write_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLattr_write(args->attr, task->under_vol_id, args->mem_type_id, args->buf, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->mem_type_id > 0)    H5Tclose(args->mem_type_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
    /* free(args->buf); */
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_write_fn

static herr_t
async_attr_write(async_instance_t* aid, H5VL_async_t *parent_obj, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    async_attr_write_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    /* hsize_t attr_size; */

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_write_args_t*)calloc(1, sizeof(async_attr_write_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

    args->attr             = parent_obj->under_object;
    if(mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
    }

    /* attr_size = H5Tget_size(mem_type_id); */
    /* attr_size = H5Aget_storage_size((hid_t)args->attr); */
    /* if (attr_size <= 0) { */
    /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s H5Aget_storage_size failed!\n", __func__); */
    /*     goto done; */
    /* } */

    /* if (NULL == (args->buf = malloc(attr_size))) { */
    /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s malloc failed!\n", __func__); */
    /*     goto done; */
    /* } */
    /* memcpy(args->buf, buf, attr_size); */
    args->buf = (void*)buf;

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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_attr_write

static void
async_attr_get_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_get_args_t *args = (async_attr_get_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLattr_get(args->obj, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_get_fn

static herr_t
async_attr_get(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_attr_get_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_get_args_t*)calloc(1, sizeof(async_attr_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_attr_get

static void
async_attr_specific_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_specific_args_t *args = (async_attr_specific_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
#endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    if (args->specific_type != H5VL_ATTR_ITER) {
        while (1) {
            if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
                is_lock = 1;
                break;
            }
            else {
                fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
                break;
            }
            usleep(1000);
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLattr_specific(args->obj, args->loc_params, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_specific_fn

static herr_t
async_attr_specific(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_attr_specific_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if (qtype == BLOCKING)
        is_blocking = true;

    if ((args = (async_attr_specific_args_t*)calloc(1, sizeof(async_attr_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;

    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_attr_specific

static void
async_attr_optional_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_optional_args_t *args = (async_attr_optional_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLattr_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_optional_fn

static herr_t
async_attr_optional(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_attr_optional_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_attr_optional_args_t*)calloc(1, sizeof(async_attr_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_attr_optional

static void
async_attr_close_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_attr_close_args_t *args = (async_attr_close_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLattr_close(args->attr, task->under_vol_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_close_fn

static herr_t
async_attr_close(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    async_attr_close_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if (qtype == BLOCKING)
        is_blocking = true;

    if ((args = (async_attr_close_args_t*)calloc(1, sizeof(async_attr_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->attr             = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == BLOCKING || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
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
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_attr_close

static void
async_dataset_create_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_create_args_t *args = (async_dataset_create_args_t*)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLdataset_create(args->obj, args->loc_params, task->under_vol_id, args->name, args->lcpl_id, args->type_id, args->space_id, args->dcpl_id, args->dapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->type_id > 0)    H5Tclose(args->type_id);
    if(args->space_id > 0)    H5Sclose(args->space_id);
    if(args->dcpl_id > 0)    H5Pclose(args->dcpl_id);
    if(args->dapl_id > 0)    H5Pclose(args->dapl_id);
    if(args->dxpl_id > 0)   H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_create_fn

static H5VL_async_t*
async_dataset_create(async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_dataset_create_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_create_args_t*)calloc(1, sizeof(async_dataset_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    if (sizeof(*loc_params) > 0) {
        args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
        dup_loc_param(args->loc_params, loc_params);
    }
    else
        args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(H5VL_loc_params_t));
    if (NULL != name)
        args->name = strdup(name);
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
    else
        goto error;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        /* fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__); */
        H5VL_async_free_obj(async_obj);
        free_async_task(async_task);
        async_obj = NULL;
        goto done;
    }

    async_task->func       = async_dataset_create_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_dataset_create

static void
async_dataset_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_open_args_t *args = (async_dataset_open_args_t*)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLdataset_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->dapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->dapl_id > 0)    H5Pclose(args->dapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_open_fn

static H5VL_async_t*
async_dataset_open(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_dataset_open_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_open_args_t*)calloc(1, sizeof(async_dataset_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if(dapl_id > 0)
        args->dapl_id = H5Pcopy(dapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (parent_obj->file_async_obj && parent_obj->file_async_obj->file_task_list_mutex) {
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
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_dataset_open

static void
async_dataset_read_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_read_args_t *args = (async_dataset_read_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdataset_read(args->dset, task->under_vol_id, args->mem_type_id, args->mem_space_id, args->file_space_id, args->plist_id, args->buf, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->mem_type_id > 0)    H5Tclose(args->mem_type_id);
    if(args->mem_space_id > 0)    H5Sclose(args->mem_space_id);
    if(args->file_space_id > 0)    H5Sclose(args->file_space_id);
    if(args->plist_id > 0)    H5Pclose(args->plist_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_read_fn

static herr_t
async_dataset_read(async_instance_t* aid, H5VL_async_t *parent_obj, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    // For implicit mode (env var), make all read to be blocking
    assert(async_instance_g);
    async_task_t *async_task = NULL;
    async_dataset_read_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_read_args_t*)calloc(1, sizeof(async_dataset_read_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
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
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
#ifndef ASYNC_VOL_NO_MPI
        H5FD_mpio_xfer_t xfer_mode;
        H5Pget_dxpl_mpio(plist_id, &xfer_mode);
        if (xfer_mode == H5FD_MPIO_COLLECTIVE)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
#endif
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_dataset_read

static void
async_dataset_write_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_write_args_t *args = (async_dataset_write_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false &&
                task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdataset_write(args->dset, task->under_vol_id, args->mem_type_id, args->mem_space_id,
                           args->file_space_id, args->plist_id, args->buf, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
#ifdef ENABLE_LOG
        fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s failed\n", __func__);
#endif
#ifdef PRINT_ERROR_STACK
        H5Eprint2(task->err_stack, stderr);
#endif
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->mem_type_id > 0)    H5Tclose(args->mem_type_id);
    if(args->mem_space_id > 0)    H5Sclose(args->mem_space_id);
    if(args->file_space_id > 0)    H5Sclose(args->file_space_id);
    if(args->plist_id > 0)    H5Pclose(args->plist_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_write_fn

static herr_t
async_dataset_write(async_instance_t* aid, H5VL_async_t *parent_obj,
                    hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                    hid_t plist_id, const void *buf, void **req)
{
    async_task_t *async_task = NULL;
    async_dataset_write_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_write_args_t*)calloc(1, sizeof(async_dataset_write_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
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
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
#ifndef ASYNC_VOL_NO_MPI
        H5FD_mpio_xfer_t xfer_mode;
        H5Pget_dxpl_mpio(plist_id, &xfer_mode);
        if (xfer_mode == H5FD_MPIO_COLLECTIVE)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
#endif
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;

error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_dataset_write

static void
async_dataset_get_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_get_args_t *args = (async_dataset_get_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdataset_get(args->dset, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_get_fn

static herr_t
async_dataset_get(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_dataset_get_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if (qtype == BLOCKING)
        is_blocking = true;

    if ((args = (async_dataset_get_args_t*)calloc(1, sizeof(async_dataset_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dset             = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 0;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_dataset_get

static void
async_dataset_specific_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_specific_args_t *args = (async_dataset_specific_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdataset_specific(args->obj, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_specific_fn

static herr_t
async_dataset_specific(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_dataset_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_dataset_specific_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if (qtype == BLOCKING)
        is_blocking = true;

    if ((args = (async_dataset_specific_args_t*)calloc(1, sizeof(async_dataset_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_dataset_specific

static void
async_dataset_optional_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_optional_args_t *args = (async_dataset_optional_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdataset_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_optional_fn

static herr_t
async_dataset_optional(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_dataset_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_dataset_optional_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_dataset_optional_args_t*)calloc(1, sizeof(async_dataset_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_dataset_optional

static void
async_dataset_close_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_dataset_close_args_t *args = (async_dataset_close_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdataset_close(args->dset, task->under_vol_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_close_fn

static herr_t
async_dataset_close(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    async_dataset_close_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if (qtype == BLOCKING)
        is_blocking = true;

    if ((args = (async_dataset_close_args_t*)calloc(1, sizeof(async_dataset_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dset             = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (parent_obj->file_async_obj && parent_obj->file_async_obj->file_task_list_mutex) {
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
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (lock_parent && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
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
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_dataset_close

static void
async_datatype_commit_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_commit_args_t *args = (async_datatype_commit_args_t*)(task->args);
    void *under_obj;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        under_obj = H5VLdatatype_commit(args->obj, args->loc_params, task->under_vol_id, args->name, args->type_id, args->lcpl_id, args->tcpl_id, args->tapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL ==  under_obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }
    task->async_obj->under_object = under_obj;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->type_id > 0)    H5Tclose(args->type_id);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->tcpl_id > 0)    H5Pclose(args->tcpl_id);
    if(args->tapl_id > 0)    H5Pclose(args->tapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_commit_fn

static H5VL_async_t*
async_datatype_commit(async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_datatype_commit_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_commit_args_t*)calloc(1, sizeof(async_datatype_commit_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if(type_id > 0)
        args->type_id = H5Tcopy(type_id);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(tcpl_id > 0)
        args->tcpl_id = H5Pcopy(tcpl_id);
    if(tapl_id > 0)
        args->tapl_id = H5Pcopy(tapl_id);
    else
        goto error;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_datatype_commit

static void
async_datatype_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_open_args_t *args = (async_datatype_open_args_t*)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLdatatype_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->tapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( NULL == obj ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->tapl_id > 0)    H5Pclose(args->tapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_open_fn

static H5VL_async_t*
async_datatype_open(async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_datatype_open_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_open_args_t*)calloc(1, sizeof(async_datatype_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if(tapl_id > 0)
        args->tapl_id = H5Pcopy(tapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_datatype_open

static void
async_datatype_get_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_get_args_t *args = (async_datatype_get_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdatatype_get(args->dt, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_get_fn

static herr_t
async_datatype_get(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_datatype_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_datatype_get_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_get_args_t*)calloc(1, sizeof(async_datatype_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dt               = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_datatype_get

static void
async_datatype_specific_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_specific_args_t *args = (async_datatype_specific_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdatatype_specific(args->obj, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_specific_fn

static herr_t
async_datatype_specific(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_datatype_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_datatype_specific_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_specific_args_t*)calloc(1, sizeof(async_datatype_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_datatype_specific

static void
async_datatype_optional_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_optional_args_t *args = (async_datatype_optional_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdatatype_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_optional_fn

static herr_t
async_datatype_optional(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_datatype_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_datatype_optional_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_optional_args_t*)calloc(1, sizeof(async_datatype_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_datatype_optional

static void
async_datatype_close_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_datatype_close_args_t *args = (async_datatype_close_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLdatatype_close(args->dt, task->under_vol_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_close_fn

static herr_t
async_datatype_close(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    async_datatype_close_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_datatype_close_args_t*)calloc(1, sizeof(async_datatype_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dt               = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
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
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_datatype_close

static void
async_file_create_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    H5VL_async_info_t *info = NULL;
    async_task_t *task = (async_task_t*)foo;
    async_file_create_args_t *args = (async_file_create_args_t*)(task->args);
    /* herr_t status; */
    hid_t under_vol_id;
    /* uint64_t supported;          /1* Whether 'post open' operation is supported by VOL connector *1/ */

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
#endif
    /* async_instance_g->start_abt_push = false; */

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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Get the underlying VOL ID */
    H5Pget_vol_id(args->fapl_id, &under_vol_id);
    assert(under_vol_id);

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLfile_create(args->name, args->flags, args->fcpl_id, args->fapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    /* /1* Check for 'post open' callback *1/ */
    /* supported = 0; */
    /* if(H5VLintrospect_opt_query(obj, under_vol_id, H5VL_SUBCLS_FILE, H5VL_NATIVE_FILE_POST_OPEN, &supported) < 0) { */
    /*     fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLintrospect_opt_query failed\n", __func__); */
    /*     goto done; */
    /* } */
    /* if(supported & H5VL_OPT_QUERY_SUPPORTED) { */
    /*     /1* Make the 'post open' callback *1/ */
    /*     /1* Try executing operation, without default error stack handling *1/ */
    /*     H5E_BEGIN_TRY { */
    /*         status = H5VLfile_optional_vararg(obj, under_vol_id, H5VL_NATIVE_FILE_POST_OPEN, args->dxpl_id, NULL); */
    /*     } H5E_END_TRY */
    /*     if ( status < 0 ) { */
    /*         if ((task->err_stack = H5Eget_current_stack()) < 0) */
    /*             fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__); */
    /*         goto done; */
    /*     } */
    /* } /1* end if *1/ */

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
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(NULL != info)         H5VL_async_info_free(info);
    free(args->name);
    args->name = NULL;
    if(args->fcpl_id > 0)    H5Pclose(args->fcpl_id);
    if(args->fapl_id > 0)    H5Pclose(args->fapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_create_fn

static H5VL_async_t*
async_file_create(async_instance_t* aid, const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    hid_t under_vol_id;
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_file_create_args_t *args = NULL;
    bool lock_self = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);

    H5Pget_vol_id(fapl_id, &under_vol_id);

    if ((args = (async_file_create_args_t*)calloc(1, sizeof(async_file_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_async_obj      = async_obj;
    if (ABT_mutex_create(&(async_obj->file_task_list_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (NULL != name)
        args->name = strdup(name);
    args->flags            = flags;
    if(fcpl_id > 0)
        args->fcpl_id = H5Pcopy(fcpl_id);
    if(fapl_id > 0)
        args->fapl_id = H5Pcopy(fapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);

    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        new_req->file_async_obj = async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        /* fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__); */
        H5VL_async_free_obj(async_obj);
        free_async_task(async_task);
        async_obj = NULL;
        goto done;
    }

    async_task->func       = async_file_create_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = under_vol_id;
    async_task->async_obj  = async_obj;

    /* Lock async_obj */
    while (1) {
        if (async_obj->obj_mutex && ABT_mutex_trylock(async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else
            fprintf(stderr,"  [ASYNC VOL DBG] %s error with try_lock\n", __func__);
        usleep(1000);
    }
    lock_self = true;

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
#ifndef ASYNC_VOL_NO_MPI
    H5Pget_coll_metadata_write(fapl_id, &async_obj->is_col_meta);
#endif
    add_task_to_queue(&aid->qhead, async_task, REGULAR);
    if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_self = false;

    if (get_n_running_task_in_queue(async_task) == 0)
        push_task_to_abt_pool(&aid->qhead, aid->pool);
    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_self) {
        if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL DBG] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_file_create

static void
async_file_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    H5VL_async_info_t *info = NULL;
    async_task_t *task = (async_task_t*)foo;
    async_file_open_args_t *args = (async_file_open_args_t*)(task->args);
    /* herr_t status; */
    hid_t under_vol_id;
    /* uint64_t supported;          /1* Whether 'post open' operation is supported by VOL connector *1/ */

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
#endif
    /* async_instance_g->start_abt_push = false; */

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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Get the underlying VOL ID */
    H5Pget_vol_id(args->fapl_id, &under_vol_id);
    assert(under_vol_id);

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLfile_open(args->name, args->flags, args->fapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    /* /1* Check for 'post open' callback *1/ */
    /* supported = 0; */
    /* /1* Try executing operation, without default error stack handling *1/ */
    /* H5E_BEGIN_TRY { */
    /*     status = H5VLintrospect_opt_query(obj, under_vol_id, H5VL_SUBCLS_FILE, H5VL_NATIVE_FILE_POST_OPEN, &supported); */
    /* } H5E_END_TRY */
    /* if ( status < 0 ) { */
    /*     if ((task->err_stack = H5Eget_current_stack()) < 0) */
    /*         fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__); */
    /*     goto done; */
    /* } */
    /* if(supported & H5VL_OPT_QUERY_SUPPORTED) { */
    /*     /1* Make the 'post open' callback *1/ */
    /*     /1* Try executing operation, without default error stack handling *1/ */
    /*     H5E_BEGIN_TRY { */
    /*         status = H5VLfile_optional_vararg(obj, under_vol_id, H5VL_NATIVE_FILE_POST_OPEN, args->dxpl_id, NULL); */
    /*     } H5E_END_TRY */
    /*     if ( status < 0 ) { */
    /*         if ((task->err_stack = H5Eget_current_stack()) < 0) */
    /*             fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__); */
    /*         goto done; */
    /*     } */
    /* } /1* end if *1/ */

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
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(NULL != info)         H5VL_async_info_free(info);
    free(args->name);
    args->name = NULL;
    if(args->fapl_id > 0)    H5Pclose(args->fapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_open_fn

static H5VL_async_t*
async_file_open(task_list_qtype qtype, async_instance_t* aid, const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    hid_t under_vol_id;
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_file_open_args_t *args = NULL;
    bool lock_self = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);

    H5Pget_vol_id(fapl_id, &under_vol_id);

    if ((args = (async_file_open_args_t*)calloc(1, sizeof(async_file_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_async_obj      = async_obj;
    if (ABT_mutex_create(&(async_obj->file_task_list_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (NULL != name)
        args->name = strdup(name);
    args->flags            = flags;
    if(fapl_id > 0)
        args->fapl_id = H5Pcopy(fapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        new_req->file_async_obj = async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock async_obj */
    while (1) {
        if (async_obj->obj_mutex && ABT_mutex_trylock(async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else
            fprintf(stderr,"  [ASYNC VOL DBG] %s error with try_lock\n", __func__);
        usleep(1000);
    }
    lock_self = true;

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
#ifndef ASYNC_VOL_NO_MPI
    H5Pget_coll_metadata_write(fapl_id, &async_obj->is_col_meta);
#endif
    if (qtype == ISOLATED) 
        add_task_to_queue(&aid->qhead, async_task, ISOLATED);
    else
        add_task_to_queue(&aid->qhead, async_task, REGULAR);
    if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_self = false;

    if (get_n_running_task_in_queue(async_task) == 0)
        push_task_to_abt_pool(&aid->qhead, aid->pool);
    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;

error:
    if (lock_self) {
        if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL DBG] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_file_open

static void
async_file_get_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_file_get_args_t *args = (async_file_get_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLfile_get(args->file, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_get_fn

static herr_t
async_file_get(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_file_get_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_file_get_args_t*)calloc(1, sizeof(async_file_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->file             = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_file_get

static void
async_file_specific_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_file_specific_args_t *args = (async_file_specific_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLfile_specific(args->file, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_specific_fn

static herr_t
async_file_specific(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_file_specific_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_file_specific_args_t*)calloc(1, sizeof(async_file_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->file             = parent_obj->under_object;
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (parent_obj->file_async_obj && ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (parent_obj->file_async_obj && ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_file_specific

static void
async_file_optional_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_file_optional_args_t *args = (async_file_optional_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLfile_optional(args->file, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_optional_fn

static herr_t
async_file_optional(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_file_optional_t opt_type,
                    hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_file_optional_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_file_optional_args_t*)calloc(1, sizeof(async_file_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->file             = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (parent_obj->file_async_obj && ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (parent_obj->file_async_obj && ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (get_n_running_task_in_queue(async_task) == 0)
        push_task_to_abt_pool(&aid->qhead, aid->pool);
    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (parent_obj && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_file_optional

static void
async_file_close_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_file_close_args_t *args = (async_file_close_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
#endif

    /* Aquire async obj mutex and set the obj */
    /* assert(task->async_obj->obj_mutex); */
    /* assert(task->async_obj->magic == ASYNC_MAGIC); */
    while (task->async_obj && task->async_obj->obj_mutex) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            is_lock = 1;
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLfile_close(args->file, task->under_vol_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

    // Decrease file open ref count
    if (ABT_mutex_lock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC ABT ERROR] with ABT_mutex_lock\n");
        goto done;
    };
    if (async_instance_g->nfopen > 0 && args->is_reopen == false)  async_instance_g->nfopen--;
    if (ABT_mutex_unlock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC ABT ERROR] with ABT_mutex_ulock\n");
        goto done;
    };

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);

    // Free all the resources allocated for this file, e.g. tasks
    if (task->task_mutex) {
        ABT_mutex_lock(task->task_mutex);
        free_file_async_resources(task->async_obj);
        ABT_mutex_unlock(task->task_mutex);
    }

    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_close_fn

static herr_t
async_file_close(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    async_file_close_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    // When there is already a close task created
    if (parent_obj->close_task) {
        async_task = parent_obj->close_task;
        is_blocking = 1;
        goto wait;
    }

    if (qtype == BLOCKING)
        is_blocking = true;

    if ((args = (async_file_close_args_t*)calloc(1, sizeof(async_file_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
    }

    // Closing a reopened file
    if (parent_obj->file_async_obj == NULL) {
        is_blocking = true;
        args->is_reopen = true;
    }

    args->file             = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        else if (parent_obj->obj_mutex == NULL) {
            break;
        }
        usleep(1000);
    }

    if (parent_obj->file_async_obj && ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (parent_obj->file_async_obj && ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    parent_obj->close_task = async_task;

    /* Check if its parent has valid object */
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            // For closing a reopened file
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
        /*     fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__); */
        /*     goto error; */
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (parent_obj->obj_mutex && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;

    if (get_n_running_task_in_queue(async_task) == 0)
        push_task_to_abt_pool(&aid->qhead, aid->pool);


wait:
    aid->start_abt_push = true;
    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (parent_obj->obj_mutex && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_file_close

static void
async_group_create_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_create_args_t *args = (async_group_create_args_t*)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLgroup_create(args->obj, args->loc_params, task->under_vol_id, args->name, args->lcpl_id, args->gcpl_id, args->gapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->gcpl_id > 0)    H5Pclose(args->gcpl_id);
    if(args->gapl_id > 0)    H5Pclose(args->gapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);
    free(args);
    task->args = NULL;

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_create_fn

static H5VL_async_t*
async_group_create(async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_group_create_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_create_args_t*)calloc(1, sizeof(async_group_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(gcpl_id > 0)
        args->gcpl_id = H5Pcopy(gcpl_id);
    if(gapl_id > 0)
        args->gapl_id = H5Pcopy(gapl_id);
    else
        goto error;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if ( H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        /* fprintf(stderr,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__); */
        H5VLfree_lib_state(async_task->h5_state);
        H5VL_async_free_obj(async_obj);
        free_async_task(async_task);
        async_task = NULL;
        async_obj = NULL;
        goto done;
    }

    async_task->func       = async_group_create_fn;
    async_task->args       = args;
    async_task->op         = WRITE;
    async_task->under_vol_id  = parent_obj->under_vol_id;
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    if (NULL != async_task->h5_state && H5VLfree_lib_state(async_task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    async_task->h5_state = NULL;
    return NULL;
} // End async_group_create

static void
async_group_open_fn(void *foo)
{
    void *obj;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_open_args_t *args = (async_group_open_args_t*)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLgroup_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->gapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    free(args->name);
    args->name = NULL;
    if(args->gapl_id > 0)    H5Pclose(args->gapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_open_fn

static H5VL_async_t*
async_group_open(async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_group_open_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_open_args_t*)calloc(1, sizeof(async_group_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if(gapl_id > 0)
        args->gapl_id = H5Pcopy(gapl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (lock_parent && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_group_open

static void
async_group_get_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_get_args_t *args = (async_group_get_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLgroup_get(args->obj, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_get_fn

static herr_t
async_group_get(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_group_get_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_get_args_t*)calloc(1, sizeof(async_group_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (qtype == BLOCKING)
        is_blocking = true;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_group_get

static void
async_group_specific_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_specific_args_t *args = (async_group_specific_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLgroup_specific(args->obj, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_specific_fn

static herr_t
async_group_specific(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_group_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_group_specific_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_specific_args_t*)calloc(1, sizeof(async_group_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_group_specific

static void
async_group_optional_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_optional_args_t *args = (async_group_optional_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLgroup_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_optional_fn

static herr_t
async_group_optional(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_group_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_group_optional_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_optional_args_t*)calloc(1, sizeof(async_group_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_group_optional

static void
async_group_close_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_group_close_args_t *args = (async_group_close_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
#endif

    // There may be cases, e.g. with link iteration, that enters group close without a valid async_obj mutex
    if (task->async_obj->obj_mutex) {
        /* Aquire async obj mutex and set the obj */
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
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLgroup_close(args->grp, task->under_vol_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (task->async_obj->obj_mutex) {
        if (is_lock == 1) {
            if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
        }
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_close_fn

static herr_t
async_group_close(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    async_group_close_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_group_close_args_t*)calloc(1, sizeof(async_group_close_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->grp              = parent_obj->under_object;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    // There may be cases, e.g. with link iteration, that enters group close without a valid async_obj mutex
    if (parent_obj->obj_mutex) {
        /* Lock parent_obj */
        while (1) {
            if (ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
                lock_parent = true;
                break;
            }
            usleep(1000);
        }

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
        if (NULL == parent_obj->under_object) {
            if (NULL != parent_obj->create_task) {
                add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
            }
            else {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
                goto error;
            }
        }
        else {
            if (NULL == req || qtype == ISOLATED)
                add_task_to_queue(&aid->qhead, async_task, ISOLATED);
            else if (async_task->async_obj->is_col_meta == true)
                add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
            else
                add_task_to_queue(&aid->qhead, async_task, REGULAR);
        }

        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
            goto error;
        }
        lock_parent = false;
    }
    else {
        // link iteration may enter here with parent obj mutex to be NULL
        add_task_to_queue(&aid->qhead, async_task, REGULAR);
        is_blocking = true;
    }
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
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    if (NULL != async_task->h5_state && H5VLfree_lib_state(async_task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    async_task->h5_state = NULL;
    return -1;
} // End async_group_close

static void
async_link_create_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_create_args_t *args = (async_link_create_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLlink_create(args->create_type, args->obj, args->loc_params, task->under_vol_id, args->lcpl_id, args->lapl_id, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->lapl_id > 0)    H5Pclose(args->lapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_create_fn

herr_t
async_link_create(task_list_qtype qtype, async_instance_t* aid, H5VL_link_create_type_t create_type, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_link_create_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_create_args_t*)calloc(1, sizeof(async_link_create_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->create_type      = create_type;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    else
        goto error;
    if(lapl_id > 0)
        args->lapl_id = H5Pcopy(lapl_id);
    else
        goto error;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 0;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_link_create

static void
async_link_copy_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_copy_args_t *args = (async_link_copy_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
    /* if (NULL == args->src_obj) { */
    /*     if (NULL != task->parent_obj->under_object) { */
    /*         args->src_obj = task->parent_obj->under_object; */
    /*     } */
    /*     else { */
    /*         if (check_parent_task(task->parent_obj) != 0) { */
    /*             task->err_stack = H5Ecreate_stack(); */
    /*             H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false); */
    /*             H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, */
    /*                 H5E_VOL, H5E_CANTCREATE, "Parent task failed"); */

/* #ifdef PRINT_ERROR_STACK */
    /*             H5Eprint2(task->err_stack, stderr); */
/* #endif */

    /*             goto done; */
    /*         } */
/* #ifdef ENABLE_DBG_MSG */
    /*         fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__); */
/* #endif */
    /*         if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) { */
    /*             fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func); */
    /*         } */

    /*         goto done; */
    /*     } */
    /* } */

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLlink_copy(args->src_obj, args->loc_params1, args->dst_obj, args->loc_params2, task->under_vol_id, args->lcpl_id, args->lapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params1);
    free_loc_param((H5VL_loc_params_t*)args->loc_params2);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->lapl_id > 0)    H5Pclose(args->lapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_copy_fn

static herr_t
async_link_copy(async_instance_t* aid, H5VL_async_t *parent_obj1, const H5VL_loc_params_t *loc_params1, H5VL_async_t *parent_obj2, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    async_link_copy_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    H5VL_async_t *parent_obj = parent_obj1 ? parent_obj1 : parent_obj2;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_copy_args_t*)calloc(1, sizeof(async_link_copy_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params1->type == H5VL_OBJECT_BY_NAME && loc_params1->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params1->type == H5VL_OBJECT_BY_IDX && loc_params1->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    if (loc_params2->type == H5VL_OBJECT_BY_NAME && loc_params2->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params2->type == H5VL_OBJECT_BY_IDX && loc_params2->loc_data.loc_by_idx.lapl_id < 0)
        goto error;

    if (parent_obj1)
        args->src_obj          = parent_obj1->under_object;
    args->loc_params1 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params1));
    dup_loc_param(args->loc_params1, loc_params1);
    if (parent_obj2)
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
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_link_copy

static void
async_link_move_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_move_args_t *args = (async_link_move_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
    /* if (NULL == args->src_obj) { */
    /*     if (NULL != task->parent_obj->under_object) { */
    /*         args->src_obj = task->parent_obj->under_object; */
    /*     } */
    /*     else { */
    /*         if (check_parent_task(task->parent_obj) != 0) { */
    /*             task->err_stack = H5Ecreate_stack(); */
    /*             H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false); */
    /*             H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, */
    /*                 H5E_VOL, H5E_CANTCREATE, "Parent task failed"); */

/* #ifdef PRINT_ERROR_STACK */
    /*             H5Eprint2(task->err_stack, stderr); */
/* #endif */

    /*             goto done; */
    /*         } */
/* #ifdef ENABLE_DBG_MSG */
    /*         fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__); */
/* #endif */
    /*         if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) { */
    /*             fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func); */
    /*         } */

    /*         goto done; */
    /*     } */
    /* } */

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLlink_move(args->src_obj, args->loc_params1, args->dst_obj, args->loc_params2, task->under_vol_id, args->lcpl_id, args->lapl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params1);
    free_loc_param((H5VL_loc_params_t*)args->loc_params2);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->lapl_id > 0)    H5Pclose(args->lapl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_move_fn

static herr_t
async_link_move(async_instance_t* aid, H5VL_async_t *parent_obj1, const H5VL_loc_params_t *loc_params1, H5VL_async_t *parent_obj2, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    async_link_move_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    H5VL_async_t *parent_obj = parent_obj1 ? parent_obj1 : parent_obj2;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_move_args_t*)calloc(1, sizeof(async_link_move_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params1->type == H5VL_OBJECT_BY_NAME && loc_params1->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params1->type == H5VL_OBJECT_BY_IDX && loc_params1->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    if (loc_params2->type == H5VL_OBJECT_BY_NAME && loc_params2->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params2->type == H5VL_OBJECT_BY_IDX && loc_params2->loc_data.loc_by_idx.lapl_id < 0)
        goto error;

    if (parent_obj1)
        args->src_obj          = parent_obj1->under_object;
    args->loc_params1 = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params1));
    dup_loc_param(args->loc_params1, loc_params1);
    if (parent_obj2)
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
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_link_move

static void
async_link_get_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_get_args_t *args = (async_link_get_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLlink_get(args->obj, args->loc_params, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_get_fn

static herr_t
async_link_get(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_link_get_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_get_args_t*)calloc(1, sizeof(async_link_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_link_get

static void
async_link_specific_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_specific_args_t *args = (async_link_specific_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
#endif

    assert(task->async_obj->magic == ASYNC_MAGIC);
    /* No need to lock the object with iteration */
    if (args->specific_type != H5VL_LINK_ITER) {
        /* Aquire async obj mutex and set the obj */
        assert(task->async_obj->obj_mutex);
        while (1) {
            if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
                is_lock = 1;
                break;
            }
            else {
                fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
                break;
            }
            usleep(1000);
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLlink_specific(args->obj, args->loc_params, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_specific_fn

static herr_t
async_link_specific(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_link_specific_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_specific_args_t*)calloc(1, sizeof(async_link_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_link_specific

static void
async_link_optional_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_link_optional_args_t *args = (async_link_optional_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLlink_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_optional_fn

static herr_t
async_link_optional(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_link_optional_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_link_optional_args_t*)calloc(1, sizeof(async_link_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_link_optional

static void
async_object_open_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_open_args_t *args = (async_object_open_args_t*)(task->args);
    void *obj;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        obj = H5VLobject_open(args->obj, args->loc_params, task->under_vol_id, args->opened_type, args->dxpl_id, args->req);
    } H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    task->async_obj->create_task = NULL;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue )
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_open_fn

static H5VL_async_t*
async_object_open(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    H5VL_async_t *async_obj = NULL;
    async_task_t *async_task = NULL;
    async_object_open_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if (qtype == BLOCKING)
        is_blocking = true;

    if ((args = (async_object_open_args_t*)calloc(1, sizeof(async_object_open_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta = parent_obj->is_col_meta;
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->opened_type      = opened_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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
    async_task->async_obj  = async_obj;
    async_task->parent_obj = parent_obj;

    async_obj->create_task = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return NULL;
} // End async_object_open

static void
async_object_copy_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_copy_args_t *args = (async_object_copy_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
    /* if (NULL == args->src_obj) { */
    /*     if (NULL != task->parent_obj->under_object) { */
    /*         args->src_obj = task->parent_obj->under_object; */
    /*     } */
    /*     else { */
    /*         if (check_parent_task(task->parent_obj) != 0) { */
    /*             task->err_stack = H5Ecreate_stack(); */
    /*             H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false); */
    /*             H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, */
    /*                 H5E_VOL, H5E_CANTCREATE, "Parent task failed"); */

/* #ifdef PRINT_ERROR_STACK */
    /*             H5Eprint2(task->err_stack, stderr); */
/* #endif */

    /*             goto done; */
    /*         } */
/* #ifdef ENABLE_DBG_MSG */
    /*         fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__); */
/* #endif */
    /*         if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) { */
    /*             fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func); */
    /*         } */

    /*         goto done; */
    /*     } */
    /* } */

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLobject_copy(args->src_obj, args->src_loc_params, args->src_name, args->dst_obj, args->dst_loc_params, args->dst_name, task->under_vol_id, args->ocpypl_id, args->lcpl_id, args->dxpl_id, args->req);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->src_loc_params);
    free(args->src_name);
    args->src_name = NULL;
    free_loc_param((H5VL_loc_params_t*)args->dst_loc_params);
    free(args->dst_name);
    args->dst_name = NULL;
    if(args->ocpypl_id > 0)    H5Pclose(args->ocpypl_id);
    if(args->lcpl_id > 0)    H5Pclose(args->lcpl_id);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_copy_fn

static herr_t
async_object_copy(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj1, const H5VL_loc_params_t *src_loc_params, const char *src_name, H5VL_async_t *parent_obj2, const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    async_task_t *async_task = NULL;
    async_object_copy_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    H5VL_async_t *parent_obj = parent_obj1 ? parent_obj1 : parent_obj2;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_object_copy_args_t*)calloc(1, sizeof(async_object_copy_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (parent_obj1)
        args->src_obj          = parent_obj1->under_object;
    args->src_loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*src_loc_params));
    dup_loc_param(args->src_loc_params, src_loc_params);
    if (NULL != src_name)
        args->src_name = strdup(src_name);
    if (parent_obj2)
        args->dst_obj          = parent_obj2->under_object;
    args->dst_loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*dst_loc_params));
    dup_loc_param(args->dst_loc_params, dst_loc_params);
    if (NULL != dst_name)
        args->dst_name = strdup(dst_name);
    if(ocpypl_id > 0)
        args->ocpypl_id = H5Pcopy(ocpypl_id);
    if(lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_object_copy

static void
async_object_get_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_get_args_t *args = (async_object_get_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
#endif

    /* Aquire async obj mutex and set the obj */
    /* assert(task->async_obj->obj_mutex); */
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (NULL == task->async_obj) {
            break;
        }
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            is_lock = 1;
            break;
        }
        else {
            fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLobject_get(args->obj, args->loc_params, task->under_vol_id, args->get_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_get_fn

static herr_t
async_object_get(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_object_get_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_object_get_args_t*)calloc(1, sizeof(async_object_get_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->get_type         = get_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex) {
            if (ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
                lock_parent = true;
                break;
            }
        }
        else {
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(stderr,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock file_task_list_mutex\n", __func__);
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
    /* if (NULL == parent_obj->obj) { */
    /*     if (NULL != parent_obj->create_task) { */
    /*         add_task_to_queue(&aid->qhead, async_task, DEPENDENT); */
    /*     } */
    /*     else { */
    /*         fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__); */
    /*         goto error; */
    /*     } */
    /* } */
    /* else { */
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    /* } */

    if (lock_parent && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock parent_obj\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_object_get

static void
async_object_specific_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_specific_args_t *args = (async_object_specific_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
#endif

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    if (args->specific_type != H5VL_OBJECT_VISIT) {
        while (1) {
            if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
                is_lock = 1;
                break;
            }
            else {
                fprintf(stderr,"  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
                break;
            }
            usleep(1000);
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLobject_specific(args->obj, args->loc_params, task->under_vol_id, args->specific_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t*)args->loc_params);
    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_specific_fn

static herr_t
async_object_specific(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_object_specific_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if (qtype == BLOCKING)
        is_blocking = true;

    if ((args = (async_object_specific_args_t*)calloc(1, sizeof(async_object_specific_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0)
        goto error;
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0)
        goto error;
    args->obj              = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t*)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->specific_type    = specific_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = true;

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    return -1;
} // End async_object_specific

static void
async_object_optional_fn(void *foo)
{
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    int is_lock = 0, sleep_time = 500;
    unsigned int attempt_count, new_attempt_count;
    hbool_t is_lib_state_restored = false;
    ABT_pool *pool_ptr;
    async_task_t *task = (async_task_t*)foo;
    async_object_optional_args_t *args = (async_object_optional_args_t*)(task->args);
    herr_t status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
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
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g,
                    H5E_VOL, H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            fprintf(stderr,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL, &task->abt_thread) != ABT_SUCCESS) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__, task->func);
            }

            goto done;
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif

    while (acquired == false) {
        if (async_instance_g->ex_delay == false && H5TSmutex_get_attempt_count(&attempt_count) < 0) {
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
            goto done;
        }
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
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
        if(async_instance_g->ex_delay == false && task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
            if(sleep_time > 0) usleep(sleep_time);
            if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                goto done;
            }
            if (new_attempt_count > attempt_count) {
                if (H5TSmutex_release(&mutex_count) < 0) {
                    fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
                }
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

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s: global lock acquired\n", __func__);
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
    if (H5VLstart_lib_state() < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;


    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY {
        status = H5VLobject_optional(args->obj, task->under_vol_id, args->opt_type, args->dxpl_id, args->req, args->arguments);
    } H5E_END_TRY
    if ( status < 0 ) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }


#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    fflush(stdout);
    if(is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if(args->dxpl_id > 0)    H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr,"  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }


    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done = 1;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(stderr,"  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_optional_fn

static herr_t
async_object_optional(task_list_qtype qtype, async_instance_t* aid, H5VL_async_t *parent_obj, H5VL_object_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    async_task_t *async_task = NULL;
    async_object_optional_args_t *args = NULL;
    bool lock_parent = false;
    bool is_blocking = false;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;

#ifdef ENABLE_LOG
    fprintf(stderr,"  [ASYNC VOL LOG] entering %s\n", __func__);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    if ((args = (async_object_optional_args_t*)calloc(1, sizeof(async_object_optional_args_t))) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj              = parent_obj->under_object;
    args->opt_type         = opt_type;
    if(dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req              = req;
    va_copy(args->arguments, arguments);

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req = (void*)new_req;
    }
    else {
        is_blocking = true;
        async_instance_g->start_abt_push = true;
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

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = true;

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
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false) {
        if (get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s waiting to finish all previous tasks\n", __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        fprintf(stderr,"  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(stderr,"  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

#ifdef ENABLE_DBG_MSG
    fprintf(stderr,"  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    fflush(stdout);
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
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
    new_obj->magic = ASYNC_MAGIC;
    new_obj->under_object = under_obj;
    new_obj->under_vol_id = under_vol_id;
    H5Iinc_ref(new_obj->under_vol_id);

    if (ABT_mutex_create(&(new_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        return NULL;
    }

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

    if (obj->obj_mutex && ABT_mutex_free(&(obj->obj_mutex)) != ABT_SUCCESS) 
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__);

    memset(obj, 0, sizeof(H5VL_async_t));
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
    const H5VL_async_t *o_async = (const H5VL_async_t *)obj;
    hid_t under_vol_id = 0;
    void *under_object = NULL;
    H5VL_async_wrap_ctx_t *new_wrap_ctx;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL WRAP CTX Get\n");
#endif

    assert (o_async->magic == ASYNC_MAGIC);

    /* Allocate new VOL object wrapping context for the async connector */
    new_wrap_ctx = (H5VL_async_wrap_ctx_t *)calloc(1, sizeof(H5VL_async_wrap_ctx_t));

    if (o_async->under_vol_id > 0) {
        under_vol_id = o_async->under_vol_id;
    }
    else if (o_async->file_async_obj && o_async->file_async_obj->under_vol_id > 0) {
        under_vol_id = o_async->file_async_obj->under_vol_id;
    }
    else {
        fprintf(stderr,"  [ASYNC VOL ERROR] with H5VL_async_get_wrap_ctx\n");
        return -1;
    }

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_wrap_ctx->under_vol_id = under_vol_id;
    H5Iinc_ref(new_wrap_ctx->under_vol_id);
    new_wrap_ctx->file_async_obj = o_async->file_async_obj;

    under_object = o_async->under_object;
    if (under_object) {
        H5VLget_wrap_ctx(under_object, under_vol_id, &new_wrap_ctx->under_wrap_ctx);
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
    if(under) {
        new_obj = H5VL_async_new_obj(under, wrap_ctx->under_vol_id);
        new_obj->file_async_obj = wrap_ctx->file_async_obj;
    }
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

    attr = async_attr_create(async_instance_g, o, loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);

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

    attr = async_attr_open(async_instance_g, o, loc_params, name, aapl_id, dxpl_id, req);

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

    if ((ret_value = async_attr_read(async_instance_g, o, mem_type_id, buf, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_read\n");
    }

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

    if ((ret_value = async_attr_write(async_instance_g, o, mem_type_id, buf, dxpl_id, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_write\n");
    }

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Get\n");
#endif

    if ((ret_value = async_attr_get(qtype, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_get\n");

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Specific\n");
#endif
    if (H5VL_ATTR_EXISTS == specific_type)
        qtype = BLOCKING;

    if ((ret_value = async_attr_specific(qtype, async_instance_g, o, loc_params, specific_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_specific\n");

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Optional\n");
#endif

    if ((ret_value = async_attr_optional(qtype, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_optional\n");

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
    hbool_t is_term;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Close\n");
#endif

    if ((ret_value = H5is_library_terminating(&is_term)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with H5is_library_terminating\n");

    /* If the library is shutting down, execute the close synchronously */
    if (is_term) 
        qtype = BLOCKING;

    if ((ret_value = async_attr_close(qtype, async_instance_g, o, dxpl_id, req)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_attr_close\n");

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

    dset = async_dataset_create(async_instance_g, o, loc_params, name, lcpl_id, type_id, space_id, dcpl_id,  dapl_id, dxpl_id, req);

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Open\n");
#endif

    dset = async_dataset_open(qtype, async_instance_g, o, loc_params, name, dapl_id, dxpl_id, req);

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

    if ((ret_value = async_dataset_read(async_instance_g, o, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_read\n");
    }

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

    if ((ret_value = async_dataset_write(async_instance_g, o, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req)) < 0 ) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_write\n");
    }

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Get\n");
#endif
    if (H5VL_DATASET_GET_SPACE == get_type)
        qtype = BLOCKING;

    if ((ret_value = async_dataset_get(qtype, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_get\n");

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
    herr_t ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL H5Dspecific\n");
#endif
    if (H5VL_DATASET_SET_EXTENT == specific_type)
        qtype = BLOCKING;

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    /* under_vol_id = o->under_vol_id; */

    if ((ret_value = async_dataset_specific(qtype, async_instance_g, o, specific_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_specific\n");

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Optional\n");
#endif

    // For H5Dwait
    if (H5VL_async_dataset_wait_op_g == opt_type)
        return (H5VL_async_dataset_wait(o));

    if ((ret_value = async_dataset_optional(qtype, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_optional\n");

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
    hbool_t is_term;
    task_list_qtype qtype = REGULAR;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Close\n");
#endif

    if ((ret_value = H5is_library_terminating(&is_term)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with H5is_library_terminating\n");

    /* If the library is shutting down, execute the close synchronously */
    if (is_term)
        qtype = BLOCKING;

    if ((ret_value = async_dataset_close(qtype, async_instance_g, o, dxpl_id, req)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_dataset_close\n");

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

    dt = async_datatype_commit(async_instance_g, o, loc_params, name, type_id, lcpl_id, tcpl_id, tapl_id, dxpl_id, req);

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

    dt = async_datatype_open(async_instance_g, o, loc_params, name, tapl_id, dxpl_id, req);

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Get\n");
#endif

    if ((ret_value = async_datatype_get(qtype, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_datatype_get\n");

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
    herr_t ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Specific\n");
#endif

    if ((ret_value = async_datatype_specific(qtype, async_instance_g, o, specific_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_datatype_specific\n");

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Optional\n");
#endif

    if ((ret_value = async_datatype_optional(qtype, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_datatype_optional\n");

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
    hbool_t is_term;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Close\n");
#endif

    /* assert(o->under_object); */

    if ((ret_value = H5is_library_terminating(&is_term)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with H5is_library_terminating\n");

    /* If the library is shutting down, execute the close synchronously */
    if (is_term)
        qtype = BLOCKING;

    if ((ret_value = async_datatype_close(qtype, async_instance_g, o, dxpl_id, req)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_datatype_close\n");

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

/* Check for threadsafe initialization of MPI, if built with MPI compiler
 * and the FAPL indicates using the MPI-IO VFD to access the file.
 */
#ifdef MPI_VERSION
{
    unsigned cap_flags = 0;

    /* Query the capability flags for the underlying VOL connector */
    if (H5VLintrospect_get_cap_flags(info->under_vol_info, info->under_vol_id, &cap_flags) < 0)
        return NULL;

    /* Querying for the VFD is only meaninful when using the native VOL connector */
    if ((cap_flags & H5VL_CAP_FLAG_NATIVE_FILES) > 0) {
        hid_t vfd_id;       /* VFD for file */

        /* Retrieve the ID for the VFD */
        if ((vfd_id = H5Pget_driver(fapl_id)) < 0)
            return NULL;

        /* Check for MPI-IO VFD */
        if (H5FD_MPIO == vfd_id) {
            int mpi_thread_lvl = -1;

            /* Check for MPI thread level */
            if (MPI_SUCCESS != MPI_Query_thread(&mpi_thread_lvl))
                return NULL;

            /* We require MPI_THREAD_MULTIPLE to operate correctly */
            if (MPI_THREAD_MULTIPLE != mpi_thread_lvl) {
                fprintf(stderr, "[ASYNC VOL ERROR] MPI is not initialized with MPI_THREAD_MULTIPLE!\n");
                return NULL;
            }
        } /* end if */
    } /* end if */
}
#endif /* MPI_VERSION */

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    file = async_file_create(async_instance_g, name, flags, fcpl_id, under_fapl_id, dxpl_id, req);

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
    task_list_qtype qtype = REGULAR;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Open\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

/* Check for threadsafe initialization of MPI, if built with MPI compiler
 * and the FAPL indicates using the MPI-IO VFD to access the file.
 */
#ifdef MPI_VERSION
{
    unsigned cap_flags = 0;

    /* Query the capability flags for the underlying VOL connector */
    if (H5VLintrospect_get_cap_flags(info->under_vol_info, info->under_vol_id, &cap_flags) < 0)
        return NULL;

    /* Querying for the VFD is only meaninful when using the native VOL connector */
    if ((cap_flags & H5VL_CAP_FLAG_NATIVE_FILES) > 0) {
        hid_t vfd_id;       /* VFD for file */

        /* Retrieve the ID for the VFD */
        if ((vfd_id = H5Pget_driver(fapl_id)) < 0)
            return NULL;

        /* Check for MPI-IO VFD */
        if (H5FD_MPIO == vfd_id) {
            int mpi_thread_lvl = -1;

            /* Check for MPI thread level */
            if (MPI_SUCCESS != MPI_Query_thread(&mpi_thread_lvl))
                return NULL;

            /* We require MPI_THREAD_MULTIPLE to operate correctly */
            if (MPI_THREAD_MULTIPLE != mpi_thread_lvl)
                return NULL;
        } /* end if */
    } /* end if */
}
#endif /* MPI_VERSION */

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    file = async_file_open(qtype, async_instance_g, name, flags, under_fapl_id, dxpl_id, req);

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Get\n");
#endif

    if ((ret_value = async_file_get(qtype, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_file_get\n");

    return ret_value;
} /* end H5VL_async_file_get() */


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
    hid_t under_vol_id;
    herr_t ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Specific\n");
#endif

    /* Return error if file object not open / created */
    if(o && !o->is_obj_valid) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_file_specific, invalid object\n");
        return(-1);
    }

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
        ret_value = H5VLfile_specific_vararg(o->under_object, under_vol_id, specific_type, dxpl_id, req, (int)loc_type, name, child_file->under_object, plist_id);
    } /* end if */
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

        if (NULL == info)
            return -1;

        /* Copy the FAPL */
        under_fapl_id = H5Pcopy(fapl_id);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = info->under_vol_id;

        /* Re-issue 'file specific' call */
        ret_value = H5VLfile_specific_vararg(NULL, under_vol_id, specific_type, dxpl_id, req, under_fapl_id, name, ret);

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

        if ((ret_value = async_file_specific(qtype, async_instance_g, o, specific_type, dxpl_id, req, arguments)) < 0 )
            fprintf(stderr,"  [ASYNC VOL ERROR] with async_file_specific\n");

        /* Wrap file struct pointer, if we reopened one */
        if(specific_type == H5VL_FILE_REOPEN) {
            if(ret_value >= 0) {
                void      **ret = va_arg(my_arguments, void **);

                if(ret && *ret)
                    *ret = H5VL_async_new_obj(*ret, o->under_vol_id);
            } /* end if */

            /* Finish use of copied vararg list */
            va_end(my_arguments);
        } /* end if */
    } /* end else */

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
    task_list_qtype qtype = REGULAR;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL File Optional\n");
#endif

    // For H5Fwait
    if(opt_type == H5VL_async_file_wait_op_g)
        return (H5VL_async_file_wait(o));
    else if(opt_type == H5VL_async_file_start_op_g)
        return (H5VL_async_start());

    if ((ret_value = async_file_optional(qtype, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_file_optional\n");

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
    hbool_t is_term;
    task_list_qtype qtype = REGULAR;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Close\n");
#endif
    if ((ret_value = H5is_library_terminating(&is_term)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with H5is_library_terminating\n");

    /* If the library is shutting down, execute the close synchronously */
    if (is_term)
        qtype = BLOCKING;

    if ((ret_value = async_file_close(qtype, async_instance_g, o, dxpl_id, req)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_file_close\n");

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

    group = async_group_create(async_instance_g, o, loc_params, name, lcpl_id, gcpl_id,  gapl_id, dxpl_id, req);

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

    group = async_group_open(async_instance_g, o, loc_params, name, gapl_id, dxpl_id, req);

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Get\n");
#endif
    if (H5VL_GROUP_GET_INFO == get_type)
        qtype = BLOCKING;

    if ((ret_value = async_group_get(qtype, async_instance_g, o, get_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_group_get\n");

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
    herr_t ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Specific\n");
#endif

    if ((ret_value = async_group_specific(qtype, async_instance_g, o, specific_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_group_specific\n");

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Optional\n");
#endif

    if ((ret_value = async_group_optional(qtype, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_group_optional\n");

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
    hbool_t is_term;
    task_list_qtype qtype = REGULAR;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL H5Gclose\n");
#endif
    if ((ret_value = H5is_library_terminating(&is_term)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with H5is_library_terminating\n");

    /* If the library is shutting down, execute the close synchronously */
    if (is_term)
        qtype = BLOCKING;

    if ((ret_value = async_group_close(qtype, async_instance_g, o, dxpl_id, req)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_group_close\n");

    return ret_value;
} /* end H5VL_async_group_close() */

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Create\n");
#endif

    /* Return error if object not open / created */
    if(req == NULL && loc_params && loc_params->obj_type != H5I_BADID && o && !o->is_obj_valid){
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_file_specific, invalid object\n");
        return(-1);
    }

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

        if (o && NULL == o->under_object)
            H5VL_async_object_wait(o);

        /* Re-issue 'link create' call, using the unwrapped pieces */
        ret_value = H5VLlink_create_vararg(create_type, (o ? o->under_object : NULL), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req, cur_obj, cur_params);
    } /* end if */
    else
        ret_value = async_link_create(qtype, async_instance_g, create_type, o, loc_params, lcpl_id, lapl_id, dxpl_id, req, arguments);

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

    /* Return error if objects not open / created */
    if(o_src && !o_src->is_obj_valid) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with %s, invalid o_src\n", __func__);
        return(-1);
    }
    if(o_dst && !o_dst->is_obj_valid){
        fprintf(stderr,"  [ASYNC VOL ERROR] with %s, invalid o_dst\n", __func__);
        return(-1);
    }

    /* Retrieve the "under" VOL id */
    if(o_src)
        under_vol_id = o_src->under_vol_id;
    else if(o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    ret_value = async_link_copy(async_instance_g, o_src, loc_params1, o_dst, loc_params2, lcpl_id, lapl_id, dxpl_id, req);

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

    /* Return error if objects not open / created */
    if(o_src && !o_src->is_obj_valid) {
        fprintf(stderr,"  [ASYNC VOL ERROR] with %s, invalid o_src\n", __func__);
        return(-1);
    }
    if(o_dst && !o_dst->is_obj_valid){
        fprintf(stderr,"  [ASYNC VOL ERROR] with %s, invalid o_dst\n", __func__);
        return(-1);
    }

    /* Retrieve the "under" VOL id */
    if(o_src)
        under_vol_id = o_src->under_vol_id;
    else if(o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    ret_value = async_link_move(async_instance_g, o_src, loc_params1, o_dst, loc_params2, lcpl_id, lapl_id, dxpl_id, req);

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Get\n");
#endif

    ret_value = async_link_get(qtype, async_instance_g, o, loc_params, get_type, dxpl_id, req, arguments);

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Specific\n");
#endif

    ret_value = async_link_specific(qtype, async_instance_g, o, loc_params, specific_type, dxpl_id, req, arguments);

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Optional\n");
#endif

    ret_value = async_link_optional(qtype, async_instance_g, o, opt_type, dxpl_id, req, arguments);

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
    task_list_qtype qtype = BLOCKING;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Open\n");
#endif

    new_obj = async_object_open(qtype, async_instance_g, o, loc_params, opened_type, dxpl_id, req);

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Copy\n");
#endif

    if ((ret_value = async_object_copy(qtype, async_instance_g, o_src, src_loc_params, src_name, o_dst, dst_loc_params, dst_name, ocpypl_id, lcpl_id, dxpl_id, req)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_object_copy\n");

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Get\n");
#endif

    if ((ret_value = async_object_get(qtype, async_instance_g, o, loc_params, get_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_object_get\n");

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
    herr_t ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Specific\n");
#endif
    if (H5VL_OBJECT_REFRESH == specific_type)
        qtype = BLOCKING;

    if ((ret_value = async_object_specific(qtype, async_instance_g, o, loc_params, specific_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_object_specific\n");

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
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Optional\n");
#endif

    if ((ret_value = async_object_optional(qtype, async_instance_g, o, opt_type, dxpl_id, req, arguments)) < 0 )
        fprintf(stderr,"  [ASYNC VOL ERROR] with async_object_optional\n");

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
 * Function:    H5VL_async_introspect_get_cap_flags
 *
 * Purpose:     Query the capability flags for this connector and any
 *              underlying connector(s).
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_async_introspect_get_cap_flags(const void *_info, unsigned *cap_flags)
{
    const H5VL_async_info_t *info = (const H5VL_async_info_t *)_info;
    herr_t                          ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INTROSPECT GetCapFlags\n");
#endif

    /* Invoke the query on the underlying VOL connector */
    ret_value = H5VLintrospect_get_cap_flags(info->under_vol_info, info->under_vol_id, cap_flags);

    /* Bitwise OR our capability flags in */
    if (ret_value >= 0)
        *cap_flags |= H5VL_async_g.cap_flags;

    return ret_value;
} /* end H5VL_async_introspect_get_cap_flags() */


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
                                int opt_type, uint64_t *flags)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INTROSPECT OptQuery\n");
#endif

    /* Check for 'post open' query and return immediately here, we will
     * query for the underlying VOL connector's support in the actual file
     * create or query operation.
     */
    if(H5VL_NATIVE_FILE_POST_OPEN == opt_type) {
        if(flags)
            *flags = 1;
        ret_value = 0;
    } /* end if */
    else
        ret_value = H5VLintrospect_opt_query(o->under_object, o->under_vol_id, cls,
                                             opt_type, flags);

    return ret_value;
} /* end H5VL_async_introspect_opt_query() */


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
H5VL_async_request_wait(void *obj, uint64_t timeout, H5VL_request_status_t *status)
{
    herr_t ret_value = 0;
    clock_t start_time, now_time;
    double elapsed, trigger;
    H5VL_async_t *request;
    async_task_t *task;
    ABT_thread_state    state;
    hbool_t acquired = false;
    unsigned int mutex_count = 1;
    hbool_t tmp = async_instance_g->start_abt_push;

    assert(obj);
    assert(status);

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Wait\n");
#endif

    request = (H5VL_async_t*)obj;
    task = request->my_task;
    if (task == NULL) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s task from request is invalid\n", __func__);
        *status = H5VL_REQUEST_STATUS_FAIL;
        return -1;
    }
    else if (task->magic != TASK_MAGIC) {
        // Task already completed and freed
        *status = H5VL_REQUEST_STATUS_SUCCEED;
        return ret_value;
    }

    async_instance_g->start_abt_push = true;

    if (task->async_obj && get_n_running_task_in_queue_obj(task->async_obj) == 0 && task->async_obj->pool_ptr && async_instance_g->qhead.queue)
        push_task_to_abt_pool(&async_instance_g->qhead, *task->async_obj->pool_ptr);

    if (H5TSmutex_release(&mutex_count) < 0)
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5TSmutex_release\n", __func__);

    trigger = (double)timeout;
    start_time = clock();

    do {
        /* if (NULL == task->abt_thread) { */
            if (task->is_done == 1 || task->magic != TASK_MAGIC) {
                if(task->err_stack)
                    *status = H5VL_REQUEST_STATUS_FAIL;
                else
                    *status = H5VL_REQUEST_STATUS_SUCCEED;
                goto done;
            }
        /* } */

        if (timeout == H5ES_WAIT_FOREVER && task->eventual) {
            ABT_eventual_wait(task->eventual, NULL);
            *status = H5VL_REQUEST_STATUS_SUCCEED;
            if(task->err_stack != 0) {
                *status = H5VL_REQUEST_STATUS_FAIL;
                goto done;
            }
            goto done;
        }
        else if (task->abt_thread) {
            ABT_thread_get_state (task->abt_thread, &state);
            if (ABT_THREAD_STATE_TERMINATED != state) {
                *status = H5VL_REQUEST_STATUS_IN_PROGRESS;
            }
            if(task->err_stack != 0) {
                *status = H5VL_REQUEST_STATUS_FAIL;
                goto done;
            }
        }

        usleep(100000);
        now_time = clock();
        elapsed = (double)(now_time - start_time) / CLOCKS_PER_SEC;

        *status = H5VL_REQUEST_STATUS_IN_PROGRESS;
    } while (elapsed < trigger) ;

#ifdef ENABLE_DBG_MSG
    if (elapsed > timeout)
        fprintf(stderr, "  [ASYNC VOL] timedout during wait (elapsed=%es, timeout=%.1fs)\n", elapsed, trigger);
#endif

done:
    while (false == acquired) {
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0)
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
    }
 
    async_instance_g->start_abt_push = tmp;

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
H5VL_async_request_cancel(void *obj, H5VL_request_status_t *status)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Cancel\n");
#endif

    ret_value = H5VLrequest_cancel(o->under_object, o->under_vol_id, status);

    if(ret_value >= 0)
        H5VL_async_free_obj(o);

    return ret_value;
} /* end H5VL_async_request_cancel() */


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
    H5VL_async_t *async_obj;
    async_task_t *task;
    hid_t *err_stack_id_ptr;
    uint64_t *op_exec_ts, *op_exec_time;
    herr_t ret_value = -1;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Specific\n");
#endif

    if(H5VL_REQUEST_GET_ERR_STACK == specific_type) {
        async_obj = (H5VL_async_t*)obj;
        task = async_obj->my_task;
        if (task == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object\n", __func__);
            return -1;
        }

        /* Retrieve pointer to error stack ID */
        err_stack_id_ptr = va_arg(arguments, hid_t *);
        assert(err_stack_id_ptr);

        /* Increment refcount on task's error stack, if it has one */
        if(H5I_INVALID_HID != task->err_stack)
            H5Iinc_ref(task->err_stack);

        /* Return the task's error stack (including H5I_INVALID_HID) */
        *err_stack_id_ptr = task->err_stack;

        ret_value = 0;
    } /* end if */
    else if (H5VL_REQUEST_GET_EXEC_TIME == specific_type) {
        async_obj = (H5VL_async_t*)obj;
        task = async_obj->my_task;
        if (task == NULL) {
            fprintf(stderr, "  [ASYNC VOL ERROR] %s with request object\n", __func__);
            return -1;
        }

        op_exec_ts = va_arg(arguments, uint64_t *);
        op_exec_time = va_arg(arguments, uint64_t *);

        *op_exec_ts = (uint64_t)task->create_time;
        *op_exec_time = (uint64_t)(task->end_time - task->start_time)/CLOCKS_PER_SEC*1000000000LL;

    }
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

    assert(obj);

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Free\n");
#endif

    // Free the file close task that is not previously freed
    if (o->my_task->func == async_file_close_fn) {
        free_async_task(o->my_task);
        H5VL_async_free_obj(o->file_async_obj);
    }

    H5VL_async_free_obj(o);

    return 0;
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

