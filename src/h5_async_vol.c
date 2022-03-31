/*******************************************************************************
Asynchronous I/O VOL Connector (AsyncVOL) Copyright (c) 2021, The
Regents of the University of California, through Lawrence Berkeley
National Laboratory (subject to receipt of any required approvals from
the U.S. Dept. of Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at
IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative
works, and perform publicly and display publicly, and to permit others to do so.
********************************************************************************/

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

#ifdef ENABLE_WRITE_MEMCPY
/* For collecting system memory info */
#include <sys/sysinfo.h>
#endif

/* Public HDF5 file */
#include "hdf5.h"
#include "H5VLconnector.h"

/* This connector's private header */
#include "h5_async_vol_private.h"

/* Argobots header */
#include "abt.h"

/* Universal linked lists header */
#include "utlist.h"

/* #include "node_local_util.h" */

/**********/
/* Macros */
/**********/

/* Whether to display log messge when callback is invoked */
/* (Uncomment to enable) */
/* #define ENABLE_LOG     1 */
/* #define ENABLE_DBG_MSG 1 */
/* #define PRINT_ERROR_STACK           1 */
/* #define ENABLE_ASYNC_LOGGING */

#define ASYNC_DBG_MSG_RANK 0

/* Record timing information */
#define ENABLE_TIMING 1

/* Default # of background threads */
#define ASYNC_VOL_DEFAULT_NTHREAD 1

/* Default # of dependencies to allocate */
#define ALLOC_INITIAL_SIZE 2

/* Default interval between checking for HDF5 global lock */
#define ASYNC_ATTEMPT_CHECK_INTERVAL   4
#define ASYNC_APP_CHECK_SLEEP_TIME     600
#define ASYNC_APP_CHECK_SLEEP_TIME_MAX 4000
#define ASYNC_ATTR_MEMCPY_SIZE         4096

/* Magic #'s for memory structures */
#define ASYNC_MAGIC 10242048
#define TASK_MAGIC  20481024

/* Hack for missing va_copy() in old Visual Studio editions
 * (from H5win2_defs.h - used on VS2012 and earlier)
 */
#if defined(_WIN32) && defined(_MSC_VER) && (_MSC_VER < 1800)
#define va_copy(D, S) ((D) = (S))
#endif

FILE *fout_g;

/************/
/* Typedefs */
/************/

/* The async VOL wrapper context */
typedef struct H5VL_async_wrap_ctx_t {
    hid_t                under_vol_id;   /* VOL ID for under VOL */
    void *               under_wrap_ctx; /* Object wrapping context for under VOL */
    struct H5VL_async_t *file_async_obj;
} H5VL_async_wrap_ctx_t;

typedef enum { QTYPE_NONE, REGULAR, DEPENDENT, COLLECTIVE, BLOCKING, ISOLATED } task_list_qtype;
typedef enum { OP_NONE, READ, WRITE } obj_op_type;
const char *qtype_names_g[10] = {"QTYPE_NONE", "REGULAR", "DEPENDENT", "COLLECTIVE", "BLOCKING", "ISOLATED"};

struct H5VL_async_t;
struct async_task_t;
struct async_future_obj_t;

typedef void (*async_after_op_cb_t)(struct async_task_t *task, void *ctx);

typedef struct async_task_t {
    hid_t     under_vol_id;
    int       magic;
    ABT_mutex task_mutex;
    void *    h5_state;
    void (*func)(void *);
    void *                args;
    obj_op_type           op;
    struct H5VL_async_t * async_obj;
    ABT_eventual          eventual;
    int                   in_abt_pool;
    int                   is_done;
    ABT_thread            abt_thread;
    hid_t                 err_stack;
    int                   n_dep;
    int                   n_dep_alloc;
    struct async_task_t **dep_tasks;

    struct H5VL_async_t *parent_obj; /* pointer back to the parent async object */

    clock_t create_time;
    clock_t start_time;
    clock_t end_time;

    /* Future ID/object handling, for tasks which return IDs */
    struct async_future_obj_t *future_obj;

    struct async_task_t *prev;
    struct async_task_t *next;
    struct async_task_t *file_list_prev;
    struct async_task_t *file_list_next;
} async_task_t;

typedef struct H5VL_async_t {
    hid_t                under_vol_id;
    void *               under_object;
    int                  magic;
    int                  is_obj_valid;
    async_task_t *       create_task; /* task that creates the object */
    async_task_t *       close_task;
    async_task_t *       my_task; /* for request */
    async_task_t *       file_task_list_head;
    ABT_mutex            file_task_list_mutex;
    struct H5VL_async_t *file_async_obj;
    int                  task_cnt;
    int                  attempt_check_cnt;
    ABT_mutex            obj_mutex;
    ABT_pool *           pool_ptr;
    hbool_t              is_col_meta;
#ifdef ENABLE_WRITE_MEMCPY
    hsize_t data_size;
#endif
} H5VL_async_t;

typedef struct async_task_list_t {
    task_list_qtype type;
    async_task_t *  task_list;

    struct async_task_list_t *prev;
    struct async_task_list_t *next;
} async_task_list_t;

typedef struct async_qhead_t {
    ABT_mutex          head_mutex;
    async_task_list_t *queue;
} async_qhead_t;

typedef struct async_instance_t {
    async_qhead_t qhead;
    ABT_pool      pool;
    int           num_xstreams;
    ABT_xstream * xstreams;
    ABT_xstream * scheds;
    ABT_sched *   progress_scheds;
    int           nfopen;
    int           mpi_size;
    int           mpi_rank;
    bool          ex_delay;              /* Delay background thread execution */
    bool          ex_fclose;             /* Delay background thread execution until file close */
    bool          ex_gclose;             /* Delay background thread execution until group close */
    bool          ex_dclose;             /* Delay background thread execution until dset close */
    bool          start_abt_push;        /* Start pushing tasks to Argobots pool */
    bool          prev_push_state;       /* Previous state of start_abt_push before a change*/
    bool          pause;                 /* Pause background thread execution */
    bool          disable_implicit_file; /* Disable implicit async execution globally */
    bool          disable_implicit;      /* Disable implicit async execution for dxpl */
    bool          delay_time_env;        /* Flag that indicates the delay time is set by env variable */
    uint64_t      delay_time; /* Sleep time before background thread trying to acquire global mutex */
    int           sleep_time; /* Sleep time between checking the global mutex attemp count */
#ifdef ENABLE_WRITE_MEMCPY
    hsize_t max_mem;
    hsize_t used_mem;
    hsize_t max_attr_size;
#endif
} async_instance_t;

typedef struct async_future_obj_t {
    hid_t         id;
    async_task_t *task;
} async_future_obj_t;

typedef struct async_attr_create_args_t {
    void *             obj;
    H5VL_loc_params_t *loc_params;
    char *             name;
    hid_t              type_id;
    hid_t              space_id;
    hid_t              acpl_id;
    hid_t              aapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_attr_create_args_t;

typedef struct async_attr_open_args_t {
    void *             obj;
    H5VL_loc_params_t *loc_params;
    char *             name;
    hid_t              aapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_attr_open_args_t;

typedef struct async_attr_read_args_t {
    void * attr;
    hid_t  mem_type_id;
    void * buf;
    hid_t  dxpl_id;
    void **req;
} async_attr_read_args_t;

typedef struct async_attr_write_args_t {
    void * attr;
    hid_t  mem_type_id;
    void * buf;
    hid_t  dxpl_id;
    void **req;
#ifdef ENABLE_WRITE_MEMCPY
    bool    free_buf;
    hsize_t data_size;
#endif
} async_attr_write_args_t;

typedef struct async_attr_get_args_t {
    void *               obj;
    H5VL_attr_get_args_t args;
    hid_t                dxpl_id;
    void **              req;
} async_attr_get_args_t;

typedef struct async_attr_specific_args_t {
    void *                    obj;
    H5VL_loc_params_t *       loc_params;
    H5VL_attr_specific_args_t args;
    hid_t                     dxpl_id;
    void **                   req;
} async_attr_specific_args_t;

typedef struct async_attr_optional_args_t {
    void *                           obj;
    H5VL_optional_args_t             args;
    H5VL_native_attr_optional_args_t opt_args;
    hid_t                            dxpl_id;
    void **                          req;
} async_attr_optional_args_t;

typedef struct async_attr_close_args_t {
    void * attr;
    hid_t  dxpl_id;
    void **req;
} async_attr_close_args_t;

typedef struct async_dataset_create_args_t {
    void *             obj;
    H5VL_loc_params_t *loc_params;
    char *             name;
    hid_t              lcpl_id;
    hid_t              type_id;
    hid_t              space_id;
    hid_t              dcpl_id;
    hid_t              dapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_dataset_create_args_t;

typedef struct async_dataset_open_args_t {
    void *             obj;
    H5VL_loc_params_t *loc_params;
    char *             name;
    hid_t              dapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_dataset_open_args_t;

typedef struct async_dataset_read_args_t {
    void * dset;
    hid_t  mem_type_id;
    hid_t  mem_space_id;
    hid_t  file_space_id;
    hid_t  plist_id;
    void * buf;
    void **req;
} async_dataset_read_args_t;

typedef struct async_dataset_write_args_t {
    void * dset;
    hid_t  mem_type_id;
    hid_t  mem_space_id;
    hid_t  file_space_id;
    hid_t  plist_id;
    void * buf;
    void **req;
#ifdef ENABLE_WRITE_MEMCPY
    bool    free_buf;
    hsize_t data_size;
#endif
} async_dataset_write_args_t;

typedef struct async_dataset_get_args_t {
    void *                  dset;
    H5VL_dataset_get_args_t args;
    hid_t                   dxpl_id;
    void **                 req;
} async_dataset_get_args_t;

typedef struct async_dataset_specific_args_t {
    void *                       obj;
    H5VL_dataset_specific_args_t args;
    hid_t                        dxpl_id;
    void **                      req;
} async_dataset_specific_args_t;

typedef struct async_dataset_optional_args_t {
    void *                              obj;
    H5VL_optional_args_t                args;
    H5VL_native_dataset_optional_args_t opt_args;
    hid_t                               dxpl_id;
    void **                             req;
} async_dataset_optional_args_t;

typedef struct async_dataset_close_args_t {
    void * dset;
    hid_t  dxpl_id;
    void **req;
} async_dataset_close_args_t;

typedef struct async_datatype_commit_args_t {
    void *             obj;
    H5VL_loc_params_t *loc_params;
    char *             name;
    hid_t              type_id;
    hid_t              lcpl_id;
    hid_t              tcpl_id;
    hid_t              tapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_datatype_commit_args_t;

typedef struct async_datatype_open_args_t {
    void *             obj;
    H5VL_loc_params_t *loc_params;
    char *             name;
    hid_t              tapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_datatype_open_args_t;

typedef struct async_datatype_get_args_t {
    void *                   dt;
    H5VL_datatype_get_args_t args;
    hid_t                    dxpl_id;
    void **                  req;
} async_datatype_get_args_t;

typedef struct async_datatype_specific_args_t {
    void *                        obj;
    H5VL_datatype_specific_args_t args;
    hid_t                         dxpl_id;
    void **                       req;
} async_datatype_specific_args_t;

typedef struct async_datatype_optional_args_t {
    void *               obj;
    H5VL_optional_args_t args;
#ifdef NOT_YET
    H5VL_native_datatype_optional_args_t opt_args;
#endif /* NOT_YET */
    hid_t  dxpl_id;
    void **req;
} async_datatype_optional_args_t;

typedef struct async_datatype_close_args_t {
    void * dt;
    hid_t  dxpl_id;
    void **req;
} async_datatype_close_args_t;

typedef struct async_file_create_args_t {
    char *   name;
    unsigned flags;
    hid_t    fcpl_id;
    hid_t    fapl_id;
    hid_t    dxpl_id;
    void **  req;
} async_file_create_args_t;

typedef struct async_file_open_args_t {
    char *   name;
    unsigned flags;
    hid_t    fapl_id;
    hid_t    dxpl_id;
    void **  req;
} async_file_open_args_t;

typedef struct async_file_get_args_t {
    void *               file;
    H5VL_file_get_args_t args;
    hid_t                dxpl_id;
    void **              req;
} async_file_get_args_t;

typedef struct async_file_specific_args_t {
    void *                    file;
    H5VL_file_specific_args_t args;
    hid_t                     dxpl_id;
    void **                   req;
} async_file_specific_args_t;

typedef struct async_file_optional_args_t {
    void *                           file;
    H5VL_optional_args_t             args;
    H5VL_native_file_optional_args_t opt_args;
    hid_t                            dxpl_id;
    void **                          req;
} async_file_optional_args_t;

typedef struct async_file_close_args_t {
    void * file;
    hid_t  dxpl_id;
    void **req;
    bool   is_reopen;
} async_file_close_args_t;

typedef struct async_group_create_args_t {
    void *             obj;
    H5VL_loc_params_t *loc_params;
    char *             name;
    hid_t              lcpl_id;
    hid_t              gcpl_id;
    hid_t              gapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_group_create_args_t;

typedef struct async_group_open_args_t {
    void *             obj;
    H5VL_loc_params_t *loc_params;
    char *             name;
    hid_t              gapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_group_open_args_t;

typedef struct async_group_get_args_t {
    void *                obj;
    H5VL_group_get_args_t args;
    hid_t                 dxpl_id;
    void **               req;
} async_group_get_args_t;

typedef struct async_group_specific_args_t {
    void *                     obj;
    H5VL_group_specific_args_t args;
    hid_t                      dxpl_id;
    void **                    req;
} async_group_specific_args_t;

typedef struct async_group_optional_args_t {
    void *                            obj;
    H5VL_optional_args_t              args;
    H5VL_native_group_optional_args_t opt_args;
    hid_t                             dxpl_id;
    void **                           req;
} async_group_optional_args_t;

typedef struct async_group_close_args_t {
    void * grp;
    hid_t  dxpl_id;
    void **req;
} async_group_close_args_t;

typedef struct async_link_create_args_t {
    H5VL_link_create_args_t args;
    void *                  obj;
    H5VL_loc_params_t *     loc_params;
    hid_t                   lcpl_id;
    hid_t                   lapl_id;
    hid_t                   dxpl_id;
    void **                 req;
    va_list                 arguments;
} async_link_create_args_t;

typedef struct async_link_copy_args_t {
    void *             src_obj;
    H5VL_loc_params_t *loc_params1;
    void *             dst_obj;
    H5VL_loc_params_t *loc_params2;
    hid_t              lcpl_id;
    hid_t              lapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_link_copy_args_t;

typedef struct async_link_move_args_t {
    void *             src_obj;
    H5VL_loc_params_t *loc_params1;
    void *             dst_obj;
    H5VL_loc_params_t *loc_params2;
    hid_t              lcpl_id;
    hid_t              lapl_id;
    hid_t              dxpl_id;
    void **            req;
} async_link_move_args_t;

typedef struct async_link_get_args_t {
    void *               obj;
    H5VL_loc_params_t *  loc_params;
    H5VL_link_get_args_t args;
    hid_t                dxpl_id;
    void **              req;
} async_link_get_args_t;

typedef struct async_link_specific_args_t {
    void *                    obj;
    H5VL_loc_params_t *       loc_params;
    H5VL_link_specific_args_t args;
    hid_t                     dxpl_id;
    void **                   req;
} async_link_specific_args_t;

typedef struct async_link_optional_args_t {
    void *               obj;
    H5VL_loc_params_t *  loc_params;
    H5VL_optional_args_t args;
#ifdef NOT_YET
    H5VL_native_link_optional_args_t opt_args;
#endif /* NOT_YET */
    hid_t  dxpl_id;
    void **req;
} async_link_optional_args_t;

typedef struct async_object_open_args_t {
    void *             obj;
    H5VL_loc_params_t *loc_params;
    H5I_type_t *       opened_type;
    hid_t              dxpl_id;
    void **            req;
} async_object_open_args_t;

typedef struct async_object_copy_args_t {
    void *             src_obj;
    H5VL_loc_params_t *src_loc_params;
    char *             src_name;
    void *             dst_obj;
    H5VL_loc_params_t *dst_loc_params;
    char *             dst_name;
    hid_t              ocpypl_id;
    hid_t              lcpl_id;
    hid_t              dxpl_id;
    void **            req;
} async_object_copy_args_t;

typedef struct async_object_get_args_t {
    void *                 obj;
    H5VL_loc_params_t *    loc_params;
    H5VL_object_get_args_t args;
    hid_t                  dxpl_id;
    void **                req;
} async_object_get_args_t;

typedef struct async_object_specific_args_t {
    void *                      obj;
    H5VL_loc_params_t *         loc_params;
    H5VL_object_specific_args_t args;
    hid_t                       dxpl_id;
    void **                     req;
} async_object_specific_args_t;

typedef struct async_object_optional_args_t {
    void *                             obj;
    H5VL_loc_params_t *                loc_params;
    H5VL_optional_args_t               args;
    H5VL_native_object_optional_args_t opt_args;
    hid_t                              dxpl_id;
    void **                            req;
} async_object_optional_args_t;

/*******************/
/* Global Variables*/
/*******************/
ABT_mutex         async_instance_mutex_g;
async_instance_t *async_instance_g     = NULL;
hid_t             async_connector_id_g = -1;
hid_t             async_error_class_g  = H5I_INVALID_HID;

/********************* */
/* Function prototypes */
/********************* */

/* Helper routines */
static H5VL_async_t *H5VL_async_new_obj(void *under_obj, hid_t under_vol_id);
static herr_t        H5VL_async_free_obj(H5VL_async_t *obj);

/* "Management" callbacks */
static herr_t H5VL_async_init(hid_t vipl_id);
static herr_t H5VL_async_term(void);

/* VOL info callbacks */
static void * H5VL_async_info_copy(const void *info);
static herr_t H5VL_async_info_cmp(int *cmp_value, const void *info1, const void *info2);
static herr_t H5VL_async_info_free(void *info);
static herr_t H5VL_async_info_to_str(const void *info, char **str);
static herr_t H5VL_async_str_to_info(const char *str, void **info);

/* VOL object wrap / retrieval callbacks */
static void * H5VL_async_get_object(const void *obj);
static herr_t H5VL_async_get_wrap_ctx(const void *obj, void **wrap_ctx);
static void * H5VL_async_wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx);
static void * H5VL_async_unwrap_object(void *obj);
static herr_t H5VL_async_free_wrap_ctx(void *obj);

/* Attribute callbacks */
static void * H5VL_async_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                     hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id,
                                     hid_t dxpl_id, void **req);
static void * H5VL_async_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                   hid_t aapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_async_attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id,
                                    void **req);
static herr_t H5VL_async_attr_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_attr_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                       H5VL_attr_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_attr_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_attr_close(void *attr, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void * H5VL_async_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                        hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id,
                                        hid_t dapl_id, hid_t dxpl_id, void **req);
static void * H5VL_async_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                      hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                                      hid_t plist_id, void *buf, void **req);
static herr_t H5VL_async_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                                       hid_t plist_id, const void *buf, void **req);
static herr_t H5VL_async_dataset_get(void *dset, H5VL_dataset_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_dataset_specific(void *obj, H5VL_dataset_specific_args_t *args, hid_t dxpl_id,
                                          void **req);
static herr_t H5VL_async_dataset_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* Datatype callbacks */
static void * H5VL_async_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                         hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id,
                                         hid_t dxpl_id, void **req);
static void * H5VL_async_datatype_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                       hid_t tapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_datatype_get(void *dt, H5VL_datatype_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_datatype_specific(void *obj, H5VL_datatype_specific_args_t *args, hid_t dxpl_id,
                                           void **req);
static herr_t H5VL_async_datatype_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_datatype_close(void *dt, hid_t dxpl_id, void **req);

/* File callbacks */
static void *H5VL_async_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id,
                                    hid_t dxpl_id, void **req);
static void *H5VL_async_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_file_get(void *file, H5VL_file_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_file_specific(void *file, H5VL_file_specific_args_t *args, hid_t dxpl_id,
                                       void **req);
static herr_t H5VL_async_file_optional(void *file, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
static void * H5VL_async_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                      hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static void * H5VL_async_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                    hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_group_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_group_specific(void *obj, H5VL_group_specific_args_t *args, hid_t dxpl_id,
                                        void **req);
static herr_t H5VL_async_group_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_group_close(void *grp, hid_t dxpl_id, void **req);

/* Link callbacks */
static herr_t H5VL_async_link_create(H5VL_link_create_args_t *args, void *obj,
                                     const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
                                     hid_t dxpl_id, void **req);
static herr_t H5VL_async_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                                   const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                                   hid_t dxpl_id, void **req);
static herr_t H5VL_async_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                                   const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                                   hid_t dxpl_id, void **req);
static herr_t H5VL_async_link_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *args,
                                  hid_t dxpl_id, void **req);
static herr_t H5VL_async_link_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                       H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_link_optional(void *obj, const H5VL_loc_params_t *loc_params,
                                       H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/* Object callbacks */
static void * H5VL_async_object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type,
                                     hid_t dxpl_id, void **req);
static herr_t H5VL_async_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params,
                                     const char *src_name, void *dst_obj,
                                     const H5VL_loc_params_t *dst_loc_params, const char *dst_name,
                                     hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_async_object_get(void *obj, const H5VL_loc_params_t *loc_params,
                                    H5VL_object_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_object_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                         H5VL_object_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_async_object_optional(void *obj, const H5VL_loc_params_t *loc_params,
                                         H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/* Container/connector introspection callbacks */
static herr_t H5VL_async_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
                                                 const H5VL_class_t **conn_cls);
static herr_t H5VL_async_introspect_get_cap_flags(const void *info, unsigned *cap_flags);
static herr_t H5VL_async_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, uint64_t *flags);

/* Async request callbacks */
static herr_t H5VL_async_request_wait(void *req, uint64_t timeout, H5VL_request_status_t *status);
static herr_t H5VL_async_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx);
static herr_t H5VL_async_request_cancel(void *req, H5VL_request_status_t *status);
static herr_t H5VL_async_request_specific(void *req, H5VL_request_specific_args_t *args);
static herr_t H5VL_async_request_optional(void *req, H5VL_optional_args_t *args);
static herr_t H5VL_async_request_free(void *req);

/* Blob callbacks */
static herr_t H5VL_async_blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx);
static herr_t H5VL_async_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx);
static herr_t H5VL_async_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_args_t *args);
static herr_t H5VL_async_blob_optional(void *obj, void *blob_id, H5VL_optional_args_t *args);

/* Token callbacks */
static herr_t H5VL_async_token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2,
                                   int *cmp_value);
static herr_t H5VL_async_token_to_str(void *obj, H5I_type_t obj_type, const H5O_token_t *token,
                                      char **token_str);
static herr_t H5VL_async_token_from_str(void *obj, H5I_type_t obj_type, const char *token_str,
                                        H5O_token_t *token);

/* Generic optional callback */
static herr_t H5VL_async_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/*******************/
/* Local variables */
/*******************/

/* async VOL connector class struct */
static const H5VL_class_t H5VL_async_g = {
    H5VL_VERSION,                         /* VOL class struct version */
    (H5VL_class_value_t)H5VL_ASYNC_VALUE, /* value        */
    H5VL_ASYNC_NAME,                      /* name         */
    H5VL_ASYNC_VERSION,                   /* connector version */
    H5VL_CAP_FLAG_ASYNC,                  /* capability flags */
    H5VL_async_init,                      /* initialize   */
    H5VL_async_term,                      /* terminate    */
    {
        /* info_cls */
        sizeof(H5VL_async_info_t), /* size    */
        H5VL_async_info_copy,      /* copy    */
        H5VL_async_info_cmp,       /* compare */
        H5VL_async_info_free,      /* free    */
        H5VL_async_info_to_str,    /* to_str  */
        H5VL_async_str_to_info     /* from_str */
    },
    {
        /* wrap_cls */
        H5VL_async_get_object,    /* get_object   */
        H5VL_async_get_wrap_ctx,  /* get_wrap_ctx */
        H5VL_async_wrap_object,   /* wrap_object  */
        H5VL_async_unwrap_object, /* unwrap_object */
        H5VL_async_free_wrap_ctx  /* free_wrap_ctx */
    },
    {
        /* attribute_cls */
        H5VL_async_attr_create,   /* create */
        H5VL_async_attr_open,     /* open */
        H5VL_async_attr_read,     /* read */
        H5VL_async_attr_write,    /* write */
        H5VL_async_attr_get,      /* get */
        H5VL_async_attr_specific, /* specific */
        H5VL_async_attr_optional, /* optional */
        H5VL_async_attr_close     /* close */
    },
    {
        /* dataset_cls */
        H5VL_async_dataset_create,   /* create */
        H5VL_async_dataset_open,     /* open */
        H5VL_async_dataset_read,     /* read */
        H5VL_async_dataset_write,    /* write */
        H5VL_async_dataset_get,      /* get */
        H5VL_async_dataset_specific, /* specific */
        H5VL_async_dataset_optional, /* optional */
        H5VL_async_dataset_close     /* close */
    },
    {
        /* datatype_cls */
        H5VL_async_datatype_commit,   /* commit */
        H5VL_async_datatype_open,     /* open */
        H5VL_async_datatype_get,      /* get_size */
        H5VL_async_datatype_specific, /* specific */
        H5VL_async_datatype_optional, /* optional */
        H5VL_async_datatype_close     /* close */
    },
    {
        /* file_cls */
        H5VL_async_file_create,   /* create */
        H5VL_async_file_open,     /* open */
        H5VL_async_file_get,      /* get */
        H5VL_async_file_specific, /* specific */
        H5VL_async_file_optional, /* optional */
        H5VL_async_file_close     /* close */
    },
    {
        /* group_cls */
        H5VL_async_group_create,   /* create */
        H5VL_async_group_open,     /* open */
        H5VL_async_group_get,      /* get */
        H5VL_async_group_specific, /* specific */
        H5VL_async_group_optional, /* optional */
        H5VL_async_group_close     /* close */
    },
    {
        /* link_cls */
        H5VL_async_link_create,   /* create */
        H5VL_async_link_copy,     /* copy */
        H5VL_async_link_move,     /* move */
        H5VL_async_link_get,      /* get */
        H5VL_async_link_specific, /* specific */
        H5VL_async_link_optional  /* optional */
    },
    {
        /* object_cls */
        H5VL_async_object_open,     /* open */
        H5VL_async_object_copy,     /* copy */
        H5VL_async_object_get,      /* get */
        H5VL_async_object_specific, /* specific */
        H5VL_async_object_optional  /* optional */
    },
    {
        /* introspect_cls */
        H5VL_async_introspect_get_conn_cls,  /* get_conn_cls */
        H5VL_async_introspect_get_cap_flags, /* get_cap_flags */
        H5VL_async_introspect_opt_query,     /* opt_query */
    },
    {
        /* request_cls */
        H5VL_async_request_wait,     /* wait */
        H5VL_async_request_notify,   /* notify */
        H5VL_async_request_cancel,   /* cancel */
        H5VL_async_request_specific, /* specific */
        H5VL_async_request_optional, /* optional */
        H5VL_async_request_free      /* free */
    },
    {
        /* blob_cls */
        H5VL_async_blob_put,      /* put */
        H5VL_async_blob_get,      /* get */
        H5VL_async_blob_specific, /* specific */
        H5VL_async_blob_optional  /* optional */
    },
    {
        /* token_cls */
        H5VL_async_token_cmp,     /* cmp */
        H5VL_async_token_to_str,  /* to_str */
        H5VL_async_token_from_str /* from_str */
    },
    H5VL_async_optional /* optional */
};

/* Operation values for new API routines */
/* These are initialized in the VOL connector's 'init' callback at runtime.
 *      It's good practice to reset them back to -1 in the 'term' callback.
 */
static int H5VL_async_file_wait_op_g      = -1;
static int H5VL_async_dataset_wait_op_g   = -1;
static int H5VL_async_file_start_op_g     = -1;
static int H5VL_async_dataset_start_op_g  = -1;
static int H5VL_async_file_pause_op_g     = -1;
static int H5VL_async_dataset_pause_op_g  = -1;
static int H5VL_async_file_delay_op_g     = -1;
static int H5VL_async_dataset_delay_op_g  = -1;
static int H5VL_async_request_start_op_g  = -1;
static int H5VL_async_request_depend_op_g = -1;

H5PL_type_t
H5PLget_plugin_type(void)
{
    return H5PL_TYPE_VOL;
}
const void *
H5PLget_plugin_info(void)
{
    return &H5VL_async_g;
}

/** @defgroup ASYNC
 * This group is for async VOL functionalities.
 */

/**
 * \ingroup ASYNC
 *
 * \brief Init the async VOL with Argobots threads.
 *
 * \param[in] VOL initialization property list identifier
 *
 * \return \herr_t
 *
 * \details
 *          vipl_id is either H5P_DEFAULT or the identifier of a VOL initialization
 *          property list of class H5P_VOL_INITIALIZE created with H5Pcreate().
 *          When created, this property list contains no library properties.
 *          If a VOL connector author decides that initialization-specific data are
 *          needed, they can be added to the empty list and retrieved by the connector
 *          in the VOL connector's initialize callback. Use of the VOL initialization
 *          property list is uncommon, as most VOL-specific properties are added to
 *          the file access property list via the connector's API calls which set
 *          the VOL connector for the file open/create. For more information, see
 *          VOL documentation.
 *
 */
static herr_t
async_init(hid_t vipl_id)
{
    herr_t ret_val = 1;
    int    abt_ret;

#ifdef ENABLE_DBG_MSG
    fprintf(stderr, "  [ASYNC VOL DBG] Success with async vol registration\n");
#endif

    /* Only init argobots once */
    if (ABT_SUCCESS != ABT_initialized()) {
        abt_ret = ABT_init(0, NULL);
        if (ABT_SUCCESS != abt_ret) {
            fprintf(stderr, "  [ASYNC VOL ERROR] with Argobots init\n");
            ret_val = -1;
            goto done;
        }
#ifdef ENABLE_DBG_MSG
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

/**
 * \ingroup ASYNC
 *
 * \brief Release resources used by Argobots.
 *
 * \return \herr_t
 *
 * \details
 *          Free various mutexes and Argobot streams and scheduler.
 *
 */
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

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(stderr, "  [ASYNC VOL DBG] Success with async_instance_finalize\n");
#endif

    return ret_val;
} // End async_instance_finalize

/**
 * \ingroup ASYNC
 *
 * \brief Finalize Argobots.
 *
 * \return \herr_t
 *
 * \details
 *          Free async VOL global mutex and finalize Argobots
 *
 */
static herr_t
async_term(void)
{
    herr_t ret_val = 1;
    int    abt_ret;

    ret_val = async_instance_finalize();
    if (ret_val < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with async_instance_finalize\n");
        return -1;
    }

    /* Free the mutex */
    if (async_instance_mutex_g) {
        abt_ret = ABT_mutex_free(&async_instance_mutex_g);
        if (ABT_SUCCESS != abt_ret) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] with mutex free\n");
            ret_val = -1;
            goto done;
        }
        async_instance_mutex_g = NULL;
    }

    abt_ret = ABT_finalize();
    if (ABT_SUCCESS != abt_ret) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with finalize argobots\n");
        ret_val = -1;
        goto done;
    }
#ifdef ENABLE_DBG_MSG
    else if (async_instance_g &&
             (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] Success with Argobots finalize\n");
#endif

    if (ASYNC_DBG_MSG_RANK == -1) {
        fclose(fout_g);
    }
done:
    return ret_val;
}

/**
 * \ingroup ASYNC
 *
 * \brief Init various mutexes and Argobots resources.
 *
 * \param[in] backing_thread_count Number of background threads to use
 *
 * \return \herr_t
 *
 * \details
 *          Create Argobots xstream, pool, and scheduler. Retrieve environment variables
 *          set by userSet initial values for async internal structure values.
 *
 *          \par Currently only supports 1 background thread due to HDF5 global mutex.
 *
 */
static herr_t
async_instance_init(int backing_thread_count)
{
    herr_t            hg_ret = 0;
    async_instance_t *aid;
    ABT_pool          pool;
    ABT_xstream       self_xstream;
    ABT_xstream *     progress_xstreams = NULL;
    ABT_sched *       progress_scheds   = NULL;
    int               abt_ret, i;
    const char *      env_var;

    if (backing_thread_count < 0)
        return -1;

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

    aid = (async_instance_t *)calloc(1, sizeof(*aid));
    if (aid == NULL) {
        hg_ret = -1;
        goto done;
    }
    aid->sleep_time = ASYNC_APP_CHECK_SLEEP_TIME;

    abt_ret = ABT_mutex_create(&aid->qhead.head_mutex);
    if (ABT_SUCCESS != abt_ret) {
        fprintf(stderr, "  [ASYNC VOL ERROR] %s with head_mutex create\n", __func__);
        free(aid);
        return -1;
    }

    if (backing_thread_count == 0) {
        aid->num_xstreams = 0;
        abt_ret           = ABT_xstream_self(&self_xstream);
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

        progress_scheds = (ABT_sched *)calloc(backing_thread_count, sizeof(ABT_sched));
        if (progress_scheds == NULL) {
            free(progress_xstreams);
            free(aid);
            hg_ret = -1;
            goto done;
        }

        /* All xstreams share one pool */
        abt_ret = ABT_pool_create_basic(ABT_POOL_FIFO_WAIT, ABT_POOL_ACCESS_MPMC, ABT_TRUE, &pool);
        if (abt_ret != ABT_SUCCESS) {
            free(progress_xstreams);
            free(progress_scheds);
            free(aid);
            hg_ret = -1;
            goto done;
        }

        for (i = 0; i < backing_thread_count; i++) {
            /* abt_ret = ABT_sched_create_basic(ABT_SCHED_BASIC, 1, &pool, */
            abt_ret = ABT_sched_create_basic(ABT_SCHED_BASIC_WAIT, 1, &pool, ABT_SCHED_CONFIG_NULL,
                                             &progress_scheds[i]);
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
    }     // end else

    aid->pool                  = pool;
    aid->xstreams              = progress_xstreams;
    aid->num_xstreams          = backing_thread_count;
    aid->progress_scheds       = progress_scheds;
    aid->nfopen                = 0;
    aid->ex_delay              = false;
    aid->ex_fclose             = false;
    aid->ex_gclose             = false;
    aid->ex_dclose             = false;
    aid->pause                 = false;
    aid->start_abt_push        = false;
    aid->disable_implicit      = false;
    aid->disable_implicit_file = false;
    aid->delay_time_env        = false;

    // Check for delaying operations to file / group / dataset close operations
    env_var = getenv("HDF5_ASYNC_EXE_FCLOSE");
    if (env_var && *env_var && atoi(env_var) > 0)
        aid->ex_fclose = true;
    env_var = getenv("HDF5_ASYNC_EXE_GCLOSE");
    if (env_var && *env_var && atoi(env_var) > 0)
        aid->ex_gclose = true;
    env_var = getenv("HDF5_ASYNC_EXE_DCLOSE");
    if (env_var && *env_var && atoi(env_var) > 0)
        aid->ex_dclose = true;

    env_var = getenv("HDF5_ASYNC_DELAY_MICROSECOND");
    if (env_var && *env_var && atoi(env_var) > 0) {
        aid->delay_time     = atoi(env_var);
        aid->delay_time_env = true;
    }

    /* Set "delay execution" convenience flag, if any of the others are set */
    if (aid->ex_fclose || aid->ex_gclose || aid->ex_dclose)
        aid->ex_delay = true;

#ifdef ENABLE_WRITE_MEMCPY
    // Get max memory allowed for async memcpy
    env_var = getenv("HDF5_ASYNC_MAX_MEM_MB");
    if (env_var && *env_var && atoi(env_var) > 0) {
        aid->max_mem = atoi(env_var);
        aid->max_mem *= 1048576;
    }
    else
        aid->max_mem = (hsize_t)get_avphys_pages() * sysconf(_SC_PAGESIZE);
#endif

#ifdef MPI_VERSION
    {
        int flag;
        MPI_Initialized(&flag);
        if (flag) {
            MPI_Comm_size(MPI_COMM_WORLD, &aid->mpi_size);
            MPI_Comm_rank(MPI_COMM_WORLD, &aid->mpi_rank);
        }
    }
#endif

    async_instance_g = aid;

    fout_g = stderr;
    if (ASYNC_DBG_MSG_RANK == -1) {
        char fname[128];
        sprintf(fname, "async.log.%d", aid->mpi_rank);
        fout_g = fopen(fname, "w");
    }
done:
    abt_ret = ABT_mutex_unlock(async_instance_mutex_g);
    if (abt_ret != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_mutex_unlock\n");
        if (progress_xstreams)
            free(progress_xstreams);
        if (progress_scheds)
            free(progress_scheds);
        if (aid)
            free(aid);
        return -1;
    }

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] Success with async_instance_init\n");
#endif
    return hg_ret;
} // End async_instance_init

/**
 * \ingroup ASYNC
 *
 * \brief Set whether to disable implicit async mode.
 *
 * \param[in] is_disable flag to whether disable or not
 *
 * \return \herr_t
 *
 * \details
 *          By setting is_disable to true, async VOL will execute each of the HDF5 API using the
 *          underlying VOL connector (default is native) without going through the async VOL task
 *          management or creating Argobots threads.
 *
 */
herr_t
H5VL_async_set_disable_implicit(hbool_t is_disable)
{
    assert(async_instance_g);
    async_instance_g->disable_implicit_file = is_disable;

    return 0;
}

/**
 * \ingroup ASYNC
 *
 * \brief Set disable implicit async mode based on the fapl ID.
 *
 * \param[in] fapl File access property list.
 *
 * \return \herr_t
 *
 * \details
 *          This routine first check whether the disable implicit flag exists in the fapl,
 *          retrieves the value if it does, and set the value correspondingly.
 *
 */
herr_t
H5VL_async_fapl_set_disable_implicit(hid_t fapl)
{
    herr_t  status     = 0;
    hbool_t is_disable = false;

    assert(async_instance_g);

    if (fapl > 0) {
        status = H5Pexist(fapl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME);
        if (status < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
            return -1;
        }
        else if (status > 0) {
            status = H5Pget(fapl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME, &is_disable);
            if (status < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5Pget failed!\n", __func__);
                return -1;
            }

#ifdef ENABLE_DBG_MSG
            if (async_instance_g->disable_implicit_file != is_disable && async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC VOL DBG] set implicit mode to %d\n", is_disable);
#endif
            async_instance_g->disable_implicit_file = is_disable;
        }
        else {
            if (async_instance_g->disable_implicit_file) {
                async_instance_g->disable_implicit_file = false;
#ifdef ENABLE_DBG_MSG
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] set implicit mode to false (new fapl used)\n");
#endif
            }
        }
    }

    return status;
}

/**
 * \ingroup ASYNC
 *
 * \brief Set disable implicit async mode based on the dxpl ID.
 *
 * \param[in] dxpl Dataset access property list.
 *
 * \return \herr_t
 *
 * \details
 *          This routine first check whether the disable implicit flag exists in the dxpl,
 *          retrieves the value if it does, and set the value correspondingly.
 *
 */
herr_t
H5VL_async_dxpl_set_disable_implicit(hid_t dxpl)
{
    herr_t  status     = 0;
    hbool_t is_disable = false;

    assert(async_instance_g);

    if (dxpl > 0) {
        status = H5Pexist(dxpl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME);
        if (status < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
            return -1;
        }
        else if (status > 0) {
            status = H5Pget(dxpl, H5VL_ASYNC_DISABLE_IMPLICIT_NAME, &is_disable);
            if (status < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5Pget failed!\n", __func__);
                return -1;
            }

            if (async_instance_g->disable_implicit_file) {
#ifdef ENABLE_DBG_MSG
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] already set disable implicit file\n");
#endif
            }
            else {
#ifdef ENABLE_DBG_MSG
                if (async_instance_g->disable_implicit != is_disable && async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] set implicit mode to %d\n", is_disable);
#endif
                async_instance_g->disable_implicit = is_disable;
            }
        }
        else {
            if (async_instance_g->disable_implicit_file) {
#ifdef ENABLE_DBG_MSG
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] already set disable implicit file\n");
#endif
            }
            else {
#ifdef ENABLE_DBG_MSG
                if (async_instance_g->disable_implicit != is_disable && async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] set implicit mode to %d\n", is_disable);
#endif
                async_instance_g->disable_implicit = is_disable;
            }
        }
    }

    return status;
}

/**
 * \ingroup ASYNC
 *
 * \brief Set disable pause async mode based on the dxpl ID.
 *
 * \param[in] dxpl Dataset access property list.
 *
 * \return \herr_t
 *
 * \details
 *          This routine first check whether the pause and delay flag exist in the dxpl,
 *          retrieves the their value if they do, and set the values correspondingly.
 *
 *          \par The pause flag controls whether to pause all async operations, and won't
 *          execute any operations until the flag is unset. The delay flag has an additional
 *          value indicating how long to delay each of the background thread execution.
 *          They are mainly used to overcome the limit from the HDF5 global mutex and
 *          avoid the potential interleaved execution that makes everything effectively
 *          synchronous.
 *
 */
herr_t
H5VL_async_dxpl_set_pause(hid_t dxpl)
{
    herr_t   status   = 0;
    hbool_t  is_pause = false;
    uint64_t delay_us = 0;

    assert(async_instance_g);

    if (dxpl > 0) {
        status = H5Pexist(dxpl, H5VL_ASYNC_PAUSE_NAME);
        if (status < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
            return -1;
        }
        else if (status > 0) {
            status = H5Pget(dxpl, H5VL_ASYNC_PAUSE_NAME, &is_pause);
            if (status < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5Pget failed!\n", __func__);
                return -1;
            }

            if (async_instance_g->pause != is_pause) {
                async_instance_g->pause = is_pause;
#ifdef ENABLE_DBG_MSG
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] set pause async execution to %d\n", is_pause);
#endif
            }
        }
        else {
            if (async_instance_g->pause != is_pause) {
                async_instance_g->pause = false;
#ifdef ENABLE_DBG_MSG
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] set pause async execution to false (new dxpl used)\n");
#endif
            }
        }

        status = H5Pexist(dxpl, H5VL_ASYNC_DELAY_NAME);
        if (status < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5Pexist failed!\n", __func__);
            return -1;
        }
        else if (status > 0) {
            status = H5Pget(dxpl, H5VL_ASYNC_DELAY_NAME, &delay_us);
            if (status < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5Pget failed!\n", __func__);
                return -1;
            }

            if (async_instance_g->delay_time != delay_us) {
                async_instance_g->delay_time = delay_us;
#ifdef ENABLE_DBG_MSG
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] set delay execution to %ld\n", delay_us);
#endif
            }
        }
        else {
            if (async_instance_g->delay_time != 0 && async_instance_g->delay_time_env == false) {
                async_instance_g->delay_time = 0;
#ifdef ENABLE_DBG_MSG
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] set async delay time to 0 (new dxpl used)\n");
#endif
            }
        }
    } // End if dxpl

    return status;
}

/**
 * \ingroup ASYNC
 *
 * \brief Init the async VOL with dynamic registered operations.
 *
 * \param[in] VOL initialization property list identifier
 *
 * \return \herr_t
 *
 * \details
 *          vipl_id is either H5P_DEFAULT or the identifier of a VOL initialization
 *          property list of class H5P_VOL_INITIALIZE created with H5Pcreate().
 *          When created, this property list contains no library properties.
 *          If a VOL connector author decides that initialization-specific data are
 *          needed, they can be added to the empty list and retrieved by the connector
 *          in the VOL connector's initialize callback. Use of the VOL initialization
 *          property list is uncommon, as most VOL-specific properties are added to
 *          the file access property list via the connector's API calls which set
 *          the VOL connector for the file open/create. For more information, see
 *          VOL documentation.
 *
 */
static herr_t H5VL_async_init(hid_t __attribute__((unused)) vipl_id)
{
    /* Initialize the Argobots I/O instance */
    if (NULL == async_instance_g) {
        int n_thread = ASYNC_VOL_DEFAULT_NTHREAD;

        if (async_instance_init(n_thread) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_instance_init\n");
            return -1;
        }

        /* Register operation values for new API routines to use for operations */
        assert(-1 == H5VL_async_file_wait_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_WAIT,
                                       &H5VL_async_file_wait_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_file_wait_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_file_wait_op_g);

        assert(-1 == H5VL_async_dataset_wait_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_WAIT,
                                       &H5VL_async_dataset_wait_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_dataset_wait_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_dataset_wait_op_g);

        assert(-1 == H5VL_async_file_start_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_START,
                                       &H5VL_async_file_start_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_file_start_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_file_start_op_g);

        assert(-1 == H5VL_async_dataset_start_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_START,
                                       &H5VL_async_dataset_start_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_dataset_start_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_dataset_start_op_g);

        assert(-1 == H5VL_async_file_pause_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_PAUSE,
                                       &H5VL_async_file_pause_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_file_pause_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_file_pause_op_g);

        assert(-1 == H5VL_async_dataset_pause_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_PAUSE,
                                       &H5VL_async_dataset_pause_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_dataset_pause_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_dataset_pause_op_g);

        assert(-1 == H5VL_async_file_delay_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_DELAY,
                                       &H5VL_async_file_delay_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_file_delay_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_file_delay_op_g);

        assert(-1 == H5VL_async_dataset_delay_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_DELAY,
                                       &H5VL_async_dataset_delay_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_dataset_delay_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_dataset_delay_op_g);

        assert(-1 == H5VL_async_request_start_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_REQUEST, H5VL_ASYNC_DYN_REQUEST_START,
                                       &H5VL_async_request_start_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_request_start_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_request_start_op_g);

        assert(-1 == H5VL_async_request_depend_op_g);
        if (H5VLregister_opt_operation(H5VL_SUBCLS_REQUEST, H5VL_ASYNC_DYN_REQUEST_DEP,
                                       &H5VL_async_request_depend_op_g) < 0) {
            fprintf(fout_g,
                    "  [ASYNC VOL ERROR] with H5VLregister_opt_operation H5VL_async_request_depend_op_g\n");
            return (-1);
        }
        assert(-1 != H5VL_async_request_depend_op_g);
    }

    /* Singleton register error class */
    if (H5I_INVALID_HID == async_error_class_g) {
        if ((async_error_class_g = H5Eregister_class("Async VOL", "Async VOL", "0.1")) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] with H5Eregister_class\n");
            return -1;
        }
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] ASYNC VOL init\n");
#endif

    return 0;
}

/**
 * \ingroup ASYNC
 *
 * \brief   Wait for all operations.
 *
 * \details
 *          Block and wait for all async operations to finish, based on the number of opened files and
 *          Argobots pool content.
 *
 */
static void
async_waitall(void)
{
    int    sleeptime = 100000;
    size_t size      = 1;

    while (async_instance_g && (async_instance_g->nfopen > 0 || size > 0)) {

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

/**
 * \ingroup ASYNC
 *
 * \brief Terminate async VOL
 *
 * \return \herr_t
 *
 * \details
 *          Wait for all operations to finish, terminate Argobots threads and
 *          unregister all async dynamic registered operations.
 *
 */
static herr_t
H5VL_async_term(void)
{
    herr_t ret_val = 0;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] ASYNC VOL terminate\n");
#endif

    /* Wait for all operations to complete */
    async_waitall();

    /* Shut down Argobots */
    async_term();

    /* Reset operation values for new "API" routines */
    if (-1 != H5VL_async_file_wait_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_WAIT) < 0)
            return (-1);
        H5VL_async_file_wait_op_g = (-1);
    } /* end if */
    if (-1 != H5VL_async_dataset_wait_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_WAIT) < 0)
            return (-1);
        H5VL_async_dataset_wait_op_g = (-1);
    } /* end if */
    if (-1 != H5VL_async_file_start_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_START) < 0)
            return (-1);
        H5VL_async_file_start_op_g = (-1);
    } /* end if */
    if (-1 != H5VL_async_dataset_start_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_START) < 0)
            return (-1);
        H5VL_async_dataset_start_op_g = (-1);
    } /* end if */
    if (-1 != H5VL_async_file_pause_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_PAUSE) < 0)
            return (-1);
        H5VL_async_file_pause_op_g = (-1);
    } /* end if */
    if (-1 != H5VL_async_dataset_pause_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_PAUSE) < 0)
            return (-1);
        H5VL_async_dataset_pause_op_g = (-1);
    } /* end if */
    if (-1 != H5VL_async_file_delay_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_FILE, H5VL_ASYNC_DYN_FILE_DELAY) < 0)
            return (-1);
        H5VL_async_file_delay_op_g = (-1);
    } /* end if */
    if (-1 != H5VL_async_dataset_delay_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_DATASET, H5VL_ASYNC_DYN_DATASET_DELAY) < 0)
            return (-1);
        H5VL_async_dataset_delay_op_g = (-1);
    } /* end if */
    if (-1 != H5VL_async_request_start_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_REQUEST, H5VL_ASYNC_DYN_REQUEST_START) < 0)
            return (-1);
        H5VL_async_request_start_op_g = (-1);
    } /* end if */
    if (-1 != H5VL_async_request_depend_op_g) {
        if (H5VLunregister_opt_operation(H5VL_SUBCLS_REQUEST, H5VL_ASYNC_DYN_REQUEST_DEP) < 0)
            return (-1);
        H5VL_async_request_depend_op_g = (-1);
    } /* end if */

    /* Unregister error class */
    if (H5I_INVALID_HID != async_error_class_g) {
        if (H5Eunregister_class(async_error_class_g) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] ASYNC VOL unregister error class failed\n");
        async_error_class_g = H5I_INVALID_HID;
    }

    return ret_val;
}

/**
 * \ingroup ASYNC
 *
 * \brief Create an async task
 *
 * \return async_task_t
 *
 * \details
 *          Allocate an async task structure, create mutex and Argobots eventual (used for determining
 *          whether a task has completed).
 *
 */
static async_task_t *
create_async_task(void)
{
    async_task_t *async_task;

    if ((async_task = (async_task_t *)calloc(1, sizeof(async_task_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s calloc failed\n", __func__);
        return NULL;
    }

    if (ABT_mutex_create(&(async_task->task_mutex)) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s ABT_mutex_create failed\n", __func__);
        return NULL;
    }

    if (ABT_eventual_create(0, &async_task->eventual) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s ABT_eventual_create failed\n", __func__);
        return NULL;
    }

    async_task->magic = TASK_MAGIC;

    return async_task;
}

/**
 * \ingroup ASYNC
 *
 * \brief Free an async task
 *
 * \param[in] task Async VOL task
 *
 * \details
 *          Free the async task structure, mutex and Argobots eventual.
 *
 */
static void
free_async_task(async_task_t *task)
{
    assert(task->magic == TASK_MAGIC);

    ABT_mutex_lock(task->task_mutex);

    if (task->args)
        free(task->args);

    if (ABT_eventual_free(&task->eventual) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_free\n", __func__);
        return;
    }
    /* if (task->children) free(task->children); */
    if (task->dep_tasks)
        free(task->dep_tasks);

    if (task->err_stack != 0)
        H5Eclose_stack(task->err_stack);

    ABT_mutex_unlock(task->task_mutex);

    if (ABT_mutex_free(&task->task_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__);
        return;
    }

    if (task->abt_thread)
        ABT_thread_free(&task->abt_thread);

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
/*         fprintf(fout_g,"  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__); */
/*         return; */
/*     } */

/*     DL_FOREACH_SAFE2(async_obj->file_task_list_head, task_iter, tmp, file_list_next) { */
/*         if (task_iter->async_obj->magic != ASYNC_MAGIC) { */
/*             printf("Error with magic number\n"); */
/*         } */
/*     } */

/*     if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) { */
/*         fprintf(fout_g,"  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__); */
/*         return; */
/*     } */
/* } */

static void async_file_close_fn(void *foo);

/**
 * \ingroup ASYNC
 *
 * \brief Free all async VOL resources of a file
 *
 * \param[in] file Async VOL file object
 *
 * \details
 *          Free the completed async operations of the file, their async task structure,
 *          mutex and Argobots eventual.
 *
 *          \par Note that the file object itself it not freed here, due to H5ESwait requires it.
 *          It is freed at the request free time later
 *
 */
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return;
    }

    DL_FOREACH_SAFE2(file->file_async_obj->file_task_list_head, task_iter, tmp, file_list_next)
    {
        DL_DELETE2(file->file_async_obj->file_task_list_head, task_iter, file_list_prev, file_list_next);
        // Defer the file close task free operation to later request free so H5ESwait works even after file is
        // closed
        if (task_iter->func != async_file_close_fn && task_iter->magic == TASK_MAGIC) {
            free_async_task(task_iter);
        }
    }

    if (file->file_task_list_mutex && ABT_mutex_unlock(file->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return;
    }

    /* if (file->obj_mutex && ABT_mutex_free(&file->obj_mutex) != ABT_SUCCESS) { */
    /*     fprintf(fout_g,"  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__); */
    /*     return; */
    /* } */

    if (file->file_task_list_mutex && ABT_mutex_free(&file->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__);
        return;
    }

    // File object is freed later at request free time for event set to working after file close
    /* free(file); */
}

/**
 * \ingroup ASYNC
 *
 * \brief Add dependency to a task
 *
 * \param[in] task Task with dependency
 * \param[in] parent_task Dependent parent of the task
 *
 * \return \herr_t
 *
 * \details Add a dependent parent to a task, both must exist. The memory allocation is automatically
 *          adjusted when the number of parents exceeds previous allocation.
 *
 */
static herr_t
add_to_dep_task(async_task_t *task, async_task_t *parent_task)
{
    assert(task);
    /* assert(parent_task); */
    if (NULL == parent_task)
        return 1;

    if (task->n_dep_alloc == 0 || task->dep_tasks == NULL) {
        // Initial alloc
        task->dep_tasks = (async_task_t **)calloc(ALLOC_INITIAL_SIZE, sizeof(async_task_t *));
        if (NULL == task->dep_tasks) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s calloc failed\n", __func__);
            return -1;
        }
        task->n_dep_alloc = ALLOC_INITIAL_SIZE;
        task->n_dep       = 0;
    }
    else if (task->n_dep == task->n_dep_alloc) {
        // Need to expand alloc
        task->dep_tasks =
            (async_task_t **)realloc(task->dep_tasks, task->n_dep_alloc * 2 * sizeof(async_task_t *));
        if (task->dep_tasks == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s realloc failed\n", __func__);
            return -1;
        }
        task->n_dep_alloc *= 2;
    }

    task->dep_tasks[task->n_dep] = parent_task;
    task->n_dep++;

    return 1;
}

/**
 * \ingroup ASYNC
 *
 * \brief Get the number of tasks in Argobots pool
 *
 * \param[in] task Async task
 *
 * \return int
 *
 * \details Retrieve the number of tasks already pushed into the Argobots pool but iterating through
 *          all existing tasks and check their Argobots running state.
 *
 */
static size_t
get_n_running_task_in_queue(async_task_t *task, const char *call_func)
{
    size_t           pool_size = 0;
    ABT_thread_state thread_state;
    async_task_t *   task_elt;
    ABT_thread       self_thread;
    ABT_bool         is_equal;

    if (task == NULL)
        return 0;

    if (ABT_pool_get_total_size(*(task->async_obj->pool_ptr), &pool_size) != ABT_SUCCESS)
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_pool_get_total_size\n", __func__);

    if (pool_size == 0) {
        if (ABT_thread_self(&self_thread) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_thread_self\n", __func__);

        DL_FOREACH2(task->async_obj->file_task_list_head, task_elt, file_list_next)
        {
            ABT_thread_equal(task_elt->abt_thread, self_thread, &is_equal);
            if (task_elt && task_elt->abt_thread != NULL && is_equal == false) {
                ABT_thread_get_state(task_elt->abt_thread, &thread_state);
                if (thread_state != ABT_THREAD_STATE_TERMINATED)
                    pool_size++;
            }
        }
    }

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] %s, pool size %lu, called by [%s]\n", __func__, pool_size,
                call_func);
#endif

    return pool_size;
}

/**
 * \ingroup ASYNC
 *
 * \brief Get the number of tasks in Argobots pool.
 *
 * \param[in] async_obj Async object
 *
 * \return int
 *
 * \details Retrieve the number of tasks already pushed into the Argobots pool but iterating through
 *          all existing tasks and check their Argobots running state.
 *
 */
int
get_n_running_task_in_queue_obj(H5VL_async_t *async_obj, const char *call_func)
{
    size_t           pool_size = 0;
    ABT_thread_state thread_state;
    async_task_t *   task_elt;
    ABT_thread       self_thread;
    ABT_bool         is_equal;

    if (async_obj == NULL)
        return 0;

    if (ABT_pool_get_total_size(*(async_obj->pool_ptr), &pool_size) != ABT_SUCCESS)
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_pool_get_total_size\n", __func__);

    if (pool_size == 0) {
        if (ABT_thread_self(&self_thread) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_thread_self\n", __func__);

        DL_FOREACH2(async_obj->file_task_list_head, task_elt, file_list_next)
        {
            ABT_thread_equal(task_elt->abt_thread, self_thread, &is_equal);
            if (task_elt && task_elt->abt_thread != NULL && is_equal == false) {
                ABT_thread_get_state(task_elt->abt_thread, &thread_state);
                if (thread_state != ABT_THREAD_STATE_TERMINATED)
                    pool_size++;
            }
        }
    }

#ifdef ENABLE_DBG_MSG
    fprintf(fout_g, "  [ASYNC VOL DBG] %s, pool size %lu, called by [%s]\n", __func__, pool_size, call_func);
#endif

    return pool_size;
}

/**
 * \ingroup ASYNC
 *
 * \brief Wait for a task until completion
 *
 * \param[in] task Async task
 *
 * \return \herr_t
 *
 * \details Block and wait for an existing async task to be completed in the background
 *          thread. There may be other async tasks executed before the specified one, and
 *          it does not increase the priority of the current task in any way.
 *
 */
herr_t
H5VL_async_task_wait(async_task_t *async_task)
{
    hbool_t      acquired    = false;
    unsigned int mutex_count = 1;
    hbool_t      tmp         = async_instance_g->start_abt_push;

    async_instance_g->start_abt_push = true;

    if (H5TSmutex_release(&mutex_count) < 0)
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_release\n", __func__);

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] %s, released %u count\n", __func__, mutex_count);
#endif

    if (async_task->is_done != 1)
        ABT_eventual_wait(async_task->eventual, NULL);

    while (false == acquired && mutex_count > 0) {
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
        if (false == acquired)
            usleep(1000);
    }

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] %s, reacquired global lock, %u count\n", __func__, mutex_count);
#endif

    async_instance_g->start_abt_push = tmp;

    return 0;
}

/**
 * \ingroup ASYNC
 *
 * \brief Push a task to Argobots pool for background thread execution.
 *
 * \param[in] qhead Head pointer of the async queue
 * \param[in] pool  Argobots pool
 *
 * \return \herr_t
 *
 * \details Push a task from the async queue that can be executed without dependency requirements,
 *          first checks the type of the task to see whether it has dependency, if so, go over all
 *          its dependent parents and only execute it if all of them are completed.
 *
 */
static herr_t
push_task_to_abt_pool(async_qhead_t *qhead, ABT_pool pool, const char *call_func)
{
    int                i, is_dep_done = 1, ntask;
    ABT_thread_state   thread_state;
    async_task_t *     task_elt, *task_tmp;
    async_task_list_t *task_list_tmp, *task_list_elt;

    assert(qhead);

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] entering %s, called by [%s], mode=%d\n", __func__, call_func,
                async_instance_g->start_abt_push);
#endif

    if (NULL == qhead->queue) {
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s, qhead->queue is NULL\n", __func__);
#endif
        goto done;
    }

    if (ABT_mutex_lock(qhead->head_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return -1;
    }

    DL_FOREACH_SAFE(qhead->queue, task_list_elt, task_list_tmp)
    {
        DL_FOREACH_SAFE(task_list_elt->task_list, task_elt, task_tmp)
        {

#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC VOL DBG] checking task func [%p] dependency\n", task_elt->func);
#endif
            is_dep_done = 1;
            // Check if depenent tasks are finished
            for (i = 0; i < task_elt->n_dep; i++) {

                /* // If dependent parent failed, do not push to Argobots pool */
                /* if (task_elt->dep_tasks[i]->err_stack != 0) { */
                /*     task_elt->err_stack = H5Ecreate_stack(); */
                /*     H5Eappend_stack(task_elt->err_stack, task_elt->dep_tasks[i]->err_stack, false); */
                /*     H5Epush(task_elt->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, */
                /*         H5E_VOL, H5E_CANTCREATE, "Parent task failed"); */

                /* #ifdef PRINT_ERROR_STACK */
                /*     H5Eprint2(task_elt->err_stack, fout_g); */
                /* #endif */
                /*     DL_DELETE(qhead->queue->task_list, task_elt); */
                /*     task_elt->prev = NULL; */
                /*     task_elt->next = NULL; */
                /*     is_dep_done = 0; */
                /*     break; */
                /* } */

                if (task_elt->dep_tasks[i]->is_done == 1)
                    continue;
                else {
                    is_dep_done = 0;
#ifdef ENABLE_DBG_MSG
                    if (async_instance_g &&
                        (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                        fprintf(fout_g, "  [ASYNC VOL DBG] [%p] has dependent [%p] not finished\n",
                                task_elt->func, task_elt->dep_tasks[i]->func);
#endif
                }
                if (task_elt && task_elt->dep_tasks[i] && NULL != task_elt->dep_tasks[i]->abt_thread) {
                    if (ABT_thread_get_state(task_elt->dep_tasks[i]->abt_thread, &thread_state) !=
                        ABT_SUCCESS) {
                        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_thread_get_state\n", __func__);
                        return -1;
                    }

                    if (thread_state != ABT_THREAD_STATE_TERMINATED) {
#ifdef ENABLE_DBG_MSG
                        if (async_instance_g &&
                            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                            fprintf(fout_g, "  [ASYNC VOL DBG] wait for dep task [%p] to be executed, %d\n",
                                    task_elt->dep_tasks[i]->func, thread_state);
#endif
                        // Dependent task already in abt pool, release lock and wait for it to finish
                        H5VL_async_task_wait(task_elt->dep_tasks[i]);
#ifdef ENABLE_DBG_MSG
                        if (async_instance_g &&
                            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                            fprintf(fout_g, "  [ASYNC VOL DBG] done waiting for dependent task [%p]\n",
                                    task_elt->dep_tasks[i]->func);
#endif
                        is_dep_done = 1;
                        continue;
                    } // End if thread is not terminated
                }     // End if dependent task is not finished
            }         // End for dependent parents of current task

            if (is_dep_done == 0) {
#ifdef ENABLE_DBG_MSG
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] func [%p] has dependent not finished\n",
                            task_elt->func);
#endif
                continue;
            }

#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC VOL DBG] will create abt thread for func [%p], type %d\n",
                        task_elt->func, task_list_elt->type);
#endif

            if (task_elt && task_elt->is_done == 0) {
                ntask = get_n_running_task_in_queue(task_elt, __func__);
                if (ntask >= 1) {
#ifdef ENABLE_DBG_MSG
                    if (async_instance_g &&
                        (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                        fprintf(fout_g, "  [ASYNC VOL DBG] %d tasks in pool, skip current one\n", ntask);
#endif
                    goto done;
                }

#ifdef ENABLE_DBG_MSG
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] create abt thread for [%p]\n", task_elt->func);
#endif
                /* assert(task_elt->abt_thread == NULL); */
                if (ABT_thread_create(pool, task_elt->func, task_elt, ABT_THREAD_ATTR_NULL,
                                      &task_elt->abt_thread) != ABT_SUCCESS) {
                    fprintf(fout_g, "  [ASYNC VOL ERROR] %s ABT_thread_create failed for %p\n", __func__,
                            task_elt->func);
                    break;
                }
                task_elt->in_abt_pool = 1;
            }
#ifdef ENABLE_DBG_MSG
            else {
                if (async_instance_g &&
                    (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                    fprintf(fout_g, "  [ASYNC VOL DBG] func [%p] is already completed, skip create\n",
                            task_elt->func);
            }
#endif

            // Remove task from current task list
            DL_DELETE(task_list_elt->task_list, task_elt);
            task_elt->prev = NULL;
            task_elt->next = NULL;
            goto done;
        } // End  DL_FOREACH_SAFE(task_list_elt, task_elt,  task_tmp)
    }     // End DL_FOREACH_SAFE(qhead->queue, task_list_elt, task_list_tmp)

done:
    // Remove head if all its tasks have been pushed to Argobots pool
    if (qhead->queue && qhead->queue->task_list == NULL) {
        /* tmp = qhead->queue; */
        DL_DELETE(qhead->queue, qhead->queue);
        /* qhead->queue->prev = qhead->queue->next->prev; */
        /* qhead->queue = qhead->queue->next; */
        /* free(tmp); */
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s removed empty queue task list\n", __func__);
#endif
        goto done;
    }

    if (ABT_mutex_unlock(qhead->head_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return -1;
    }

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    return 1;
} // End push_task_to_abt_pool

/**
 * \ingroup ASYNC
 *
 * \brief Insert a task to the async queue
 *
 * \param[in] qhead Head pointer of the async queue
 * \param[in] task  Async task
 * \param[in] task_type Type of task
 *
 * \return \herr_t
 *
 * \details This is the main async queue and task management routine. It enforces the following rules:
 *          Any read/write operation must be executed after a prior write operation of same object.
 *          Any write operation must be executed after a prior read operation of same object.
 *          Any collective operation must be executed in same order with regards to other collective
 * operations. There can only be 1 collective operation in execution at any time (amongst all the threads on a
 * process). by the following procedure:
 *          1. Check for collective operation
 *                If collective, create a new CTL, or append it to an existing tail CTL.
 *          2. Check for object dependency.
 *                E.g. a group open depends on its file open/create.
 *                (Rule 1) Any read/write operation depends on a prior write operation of same object.
 *                (Rule 2) Any write operation depends on a prior read operation of same object.
 *                If satisfies any of the above, create a new DTL and insert to it.
 *          3. If the async task is not the above 2, create or insert it to tail RTL.
 *                If current RTL is also head, add to Argobots pool.
 *          4. Any time an async task has completed, push all tasks in the head into Argobots pool.
 *
 */
static herr_t
add_task_to_queue(async_qhead_t *qhead, async_task_t *task, task_list_qtype task_type)
{
    int is_end, is_end2;
    /* int is_dep = 0; */
    async_task_list_t *tail_list, *task_list_elt;
    async_task_t *     task_elt, *tail_task;

    assert(qhead);
    assert(task);

    tail_list = qhead->queue == NULL ? NULL : qhead->queue->prev;

    // Need to depend on the object's createion (create/open) task to finish
    if (task_type == DEPENDENT) {
        if (task->parent_obj && task->parent_obj->is_obj_valid != 1) {
            /* is_dep = 1; */
            task_type = DEPENDENT;
            if (add_to_dep_task(task, task->parent_obj->create_task) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s add_to_dep_task failed\n", __func__);
                return -1;
            }
        }

        if (task != task->async_obj->create_task && task->async_obj->is_obj_valid != 1 &&
            task->parent_obj->create_task != task->async_obj->create_task) {
            /* is_dep = 1; */
            task_type = DEPENDENT;
            if (add_to_dep_task(task, task->async_obj->create_task) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s add_to_dep_task failed\n", __func__);
                return -1;
            }
        }

        /* Any read/write operation must be executed after a prior write operation of same object. */
        /* Any write operation must be executed after a prior read operation of same object. */
        is_end = 0;
        DL_FOREACH2(tail_list, task_list_elt, prev)
        {
            tail_task = task_list_elt->task_list == NULL ? NULL : task_list_elt->task_list->prev;
            is_end2   = 0;
            DL_FOREACH2(tail_task, task_elt, prev)
            {
                if (task_elt->async_obj && task_elt->async_obj == task->async_obj &&
                    !(task->op == READ && task_elt->op == READ)) {
                    task_type = DEPENDENT;
                    if (add_to_dep_task(task, task_elt) < 0) {
                        fprintf(fout_g, "  [ASYNC VOL ERROR] %s add_to_dep_task failed\n", __func__);
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
        // A reopened file may not have valid file_async_obj
        if (task->async_obj->file_async_obj) {
            if (ABT_mutex_lock(task->async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
                return -1;
            }
            DL_FOREACH2(task->async_obj->file_task_list_head, task_elt, file_list_next)
            {
                if (task_elt->in_abt_pool == 1 && task_elt->async_obj &&
                    task_elt->async_obj == task->async_obj && !(task->op == READ && task_elt->op == READ)) {
                    task_type = DEPENDENT;
                    if (add_to_dep_task(task, task_elt) < 0) {
                        fprintf(fout_g, "  [ASYNC VOL ERROR] %s add_to_dep_task failed\n", __func__);
                        return -1;
                    }
                }
            }

            if (ABT_mutex_unlock(task->async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
                return -1;
            }
        } // End has valid file_async_obj
    }     // End if task type is DEPENDENT

    /* // If regular task, add to Argobots pool for execution directly */
    /* if (task_type == REGULAR) { */
    /*     if (ABT_thread_create(*(task->async_obj->pool_ptr), task->func, task, ABT_THREAD_ATTR_NULL,
     * &task->abt_thread) != ABT_SUCCESS) { */
    /*         fprintf(fout_g,"  [ASYNC VOL ERROR] %s ABT_thread_create failed for %p\n", __func__,
     * task->func); */
    /*         return -1; */
    /*     } */
    /*     return 1; */
    /* } */

    if (ABT_mutex_lock(qhead->head_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return -1;
    }

    // Check if the tail is of the same type, append to it if so
    if (qhead->queue && qhead->queue->prev->type == task_type && task_type != COLLECTIVE) {
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] append [%p] to %s task list\n", task->func,
                    qtype_names_g[task_type]);
#endif
        DL_APPEND(qhead->queue->prev->task_list, task);
    }
    else {
        // Create a new task list in queue and add the current task to it
        async_task_list_t *new_list = (async_task_list_t *)calloc(1, sizeof(async_task_list_t));
        new_list->type              = task_type;
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] create and append [%p] to new %s task list\n", task->func,
                    qtype_names_g[task_type]);
#endif
        DL_APPEND(new_list->task_list, task);
        DL_APPEND(qhead->queue, new_list);
    }

    if (ABT_mutex_unlock(qhead->head_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return -1;
    }

    /* if (get_n_running_task_in_queue(task) == 0) */
    /*     push_task_to_abt_pool(qhead, *(task->async_obj->pool_ptr)); */

    return 1;
} // add_task_to_queue

/**
 * \ingroup ASYNC
 *
 * \brief Set a dependency for objects with their request
 *
 * \param[in] request Request of a task
 * \param[in] parent_request Request of the task's parent
 *
 * \return \herr_t
 *
 * \details This functions provides a method to set user-defined dependency to a given
 *          async task through their requests. This is only meant to be used by another
 *          VOL connector and not for a general user.
 *
 */
herr_t
H5VL_async_set_request_dep(void *request, void *parent_request)
{
    herr_t        ret_val;
    H5VL_async_t *req, *parent_req;
    async_task_t *task, *parent_task;

    assert(request);
    assert(parent_request);

    req        = (H5VL_async_t *)request;
    parent_req = (H5VL_async_t *)parent_request;

    assert(req->my_task);
    assert(parent_req->my_task);

    task        = req->my_task;
    parent_task = parent_req->my_task;

    assert(task->magic == TASK_MAGIC);
    assert(parent_task->magic == TASK_MAGIC);

    /* ABT_mutex_lock(task->task_mutex); */

    ret_val = add_to_dep_task(task, parent_task);
    if (ret_val < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s calloc failed\n", __func__);
        return -1;
    }

    /* ABT_mutex_unlock(task->task_mutex); */

    return ret_val;
}

/**
 * \ingroup ASYNC
 *
 * \brief Wait for an object's task until completion
 *
 * \param[in] async_obj Async object pointer
 *
 * \return \herr_t
 *
 * \details Block and wait for an existing async object to be completed in the background
 *          thread. There may be other async tasks executed before the specified one, and
 *          it does not increase the priority of the current task in any way.
 *
 */
herr_t
H5VL_async_object_wait(H5VL_async_t *async_obj)
{
    async_task_t *task_iter;
    hbool_t       acquired    = false;
    unsigned int  mutex_count = 1;
    hbool_t       tmp         = async_instance_g->start_abt_push;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    async_instance_g->start_abt_push = true;

    if (get_n_running_task_in_queue_obj(async_obj, __func__) == 0)
        push_task_to_abt_pool(&async_instance_g->qhead, *async_obj->pool_ptr, __func__);

    if (H5TSmutex_release(&mutex_count) < 0)
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_release\n", __func__);

    // Check for all tasks on this dset of a file

    if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return -1;
    }
    DL_FOREACH2(async_obj->file_task_list_head, task_iter, file_list_next)
    {
        /* if (ABT_mutex_lock(async_obj->obj_mutex) != ABT_SUCCESS) { */
        /*     fprintf(fout_g,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
        /*     return -1; */
        /* } */

        if (task_iter->async_obj == async_obj) {
            if (task_iter->is_done != 1) {
                ABT_eventual_wait(task_iter->eventual, NULL);
            }
        }
        /* if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) */
        /*     fprintf(fout_g,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
    }

    if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return -1;
    }
    while (false == acquired && mutex_count > 0) {
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
    }

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s setting start_abt_push false!\n", __func__);
#endif
    async_instance_g->start_abt_push = tmp;

    return 0;
}

/**
 * \ingroup ASYNC
 *
 * \brief Wait for an object's task until completion
 *
 * \param[in] async_obj Async object pointer
 *
 * \return \herr_t
 *
 * \details Block and wait for an existing async object to be completed in the background
 *          thread. There may be other async tasks executed before the specified one, and
 *          it does not increase the priority of the current task in any way.
 *          This function is used specifically for H5Dwait.
 *
 */
herr_t
H5VL_async_dataset_wait(H5VL_async_t *async_obj)
{
    async_task_t *task_iter;
    hbool_t       acquired    = false;
    unsigned int  mutex_count = 1;
    hbool_t       tmp         = async_instance_g->start_abt_push;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    async_instance_g->start_abt_push = true;

    if (get_n_running_task_in_queue_obj(async_obj, __func__) == 0)
        push_task_to_abt_pool(&async_instance_g->qhead, *async_obj->pool_ptr, __func__);

    if (H5TSmutex_release(&mutex_count) < 0)
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_release\n", __func__);

    // Check for all tasks on this dset of a file

    if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return -1;
    }
    DL_FOREACH2(async_obj->file_task_list_head, task_iter, file_list_next)
    {
        /* if (ABT_mutex_lock(async_obj->obj_mutex) != ABT_SUCCESS) { */
        /*     fprintf(fout_g,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
        /*     return -1; */
        /* } */

        if (task_iter->async_obj == async_obj) {
            if (task_iter->is_done != 1) {
                ABT_eventual_wait(task_iter->eventual, NULL);
            }
        }
        /* if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) */
        /*     fprintf(fout_g,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
    }

    if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return -1;
    }
    while (false == acquired && mutex_count > 0) {
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
    }

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s setting start_abt_push false!\n", __func__);
#endif
    async_instance_g->start_abt_push = tmp;

    return 0;
}

/**
 * \ingroup ASYNC
 *
 * \brief Wait for an object's task until completion
 *
 * \param[in] async_obj Async object pointer
 *
 * \return \herr_t
 *
 * \details Block and wait for an existing async object to be completed in the background
 *          thread. There may be other async tasks executed before the specified one, and
 *          it does not increase the priority of the current task in any way.
 *          This function is used specifically for H5Fwait.
 *
 */
herr_t
H5VL_async_file_wait(H5VL_async_t *async_obj)
{
    async_task_t *task_iter;
    hbool_t       acquired    = false;
    unsigned int  mutex_count = 1;
    hbool_t       tmp         = async_instance_g->start_abt_push;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    async_instance_g->start_abt_push = true;

    if (get_n_running_task_in_queue_obj(async_obj, __func__) == 0)
        push_task_to_abt_pool(&async_instance_g->qhead, *async_obj->pool_ptr, __func__);

    if (H5TSmutex_release(&mutex_count) < 0)
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_release\n", __func__);

    if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        return -1;
    }
    // Check for all tasks on this dset of a file
    DL_FOREACH2(async_obj->file_task_list_head, task_iter, file_list_next)
    {
        /* if (ABT_mutex_lock(async_obj->obj_mutex) != ABT_SUCCESS) { */
        /*     fprintf(fout_g,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
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
        /*     fprintf(fout_g,"  [ASYNC VOL ERROR] %s ABT_mutex_lock failed\n", __func__); */
    }

    if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        return -1;
    }

    while (false == acquired && mutex_count > 0) {
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
    }

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s setting start_abt_push false!\n", __func__);
#endif

    async_instance_g->start_abt_push = tmp;
    return 0;
}

/**
 * \ingroup ASYNC
 *
 * \brief Force to start background task execution
 *
 * \return \herr_t
 *
 * \details Start the Argobots background thread immediately, must be used after a previous
 *          pause.
 *
 */
herr_t
H5VL_async_start()
{
    assert(async_instance_g);
    async_instance_g->start_abt_push = true;
    async_instance_g->pause          = false;
    if (async_instance_g && NULL != async_instance_g->qhead.queue)
        push_task_to_abt_pool(&async_instance_g->qhead, async_instance_g->pool, __func__);
    return 0;
}

/**
 * \ingroup ASYNC
 *
 * \brief Stop background task execution
 *
 * \return \herr_t
 *
 * \details Stop the Argobots background thread, existing tasks in the Argobots queue will not
 *          be affected and will run for completion. Effective for the next task to be inserted
 *          into the async queue.
 *
 */
herr_t
H5VL_async_pause()
{
    assert(async_instance_g);
    async_instance_g->start_abt_push = false;
    async_instance_g->pause          = true;
    return 0;
}

/**
 * \ingroup ASYNC
 *
 * \brief Set a delay time for each async task
 *
 * \param[in] time_us Time in us.
 *
 * \return \herr_t
 *
 * \details Set a delay time for each async task, effective for the next task to be executed.
 *
 */
int
H5VL_async_set_delay_time(uint64_t time_us)
{
    async_instance_g->delay_time = time_us;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s setting delay time to %lu us\n", __func__, time_us);
#endif
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
/*     fprintf(fout_g, "[E%d] CPU set (%d): {%s}\n", rank, cpuset_size, cpuset_str); */

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
/*         fprintf(fout_g, "  [ASYNC VOL ERROR] with pthread_setaffinity_np: %d\n", ret); */
/*         return; */
/*     } */
/* } */

/* static void async_get_affinity() */
/* { */
/*     int ret, j; */
/*     cpu_set_t cpuset; */
/*     int cpu; */

/*     cpu = sched_getcpu(); */
/*     fprintf(fout_g, "Argobots thread is running on CPU %d\n", cpu); */

/*     /1* ret = pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset); *1/ */
/*     /1* if (ret != 0) { *1/ */
/*     /1*     fprintf(fout_g, "  [ASYNC VOL ERROR] with pthread_getaffinity_np: %d\n", ret); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* fprintf(fout_g, "Set returned by pthread_getaffinity_np() contained:\n"); *1/ */
/*     /1* for (j = 0; j < CPU_SETSIZE; j++) *1/ */
/*     /1*     if (CPU_ISSET(j, &cpuset)) *1/ */
/*     /1*         fprintf(fout_g, "    CPU %d\n", j); *1/ */
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
/*         fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_xstream_self\n"); */
/*         return; */
/*     } */

/*     ret = ABT_xstream_get_rank(xstream, &rank); */
/*     if (ret != ABT_SUCCESS) { */
/*         fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_xstream_get_rank\n"); */
/*         return; */
/*     } */

/*     /1* ret = ABT_xstream_get_cpubind(xstream, &cpuid); *1/ */
/*     /1* if (ret != ABT_SUCCESS) { *1/ */
/*     /1*     fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_xstream_get_cpubind\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* fprintf(fout_g, "[E%d] CPU bind: %d\n", rank, cpuid); *1/ */

/*     /1* new_cpuid = (cpuid + 1) % num_xstreams; *1/ */
/*     /1* fprintf(fout_g, "[E%d] change binding: %d -> %d\n", rank, cpuid, new_cpuid); *1/ */

/*     /1* ret = ABT_xstream_set_cpubind(xstream, new_cpuid); *1/ */
/*     /1* if (ret != ABT_SUCCESS) { *1/ */
/*     /1*     fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_xstream_set_cpubind\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* ret = ABT_xstream_get_cpubind(xstream, &cpuid); *1/ */
/*     /1* if (ret != ABT_SUCCESS) { *1/ */
/*     /1*     fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_xstream_get_cpubind\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */

/*     /1* /2* assert(cpuid == new_cpuid); *2/ *1/ */
/*     /1* fprintf(fout_g, "[E%d] CPU bind: %d\n", rank, cpuid); *1/ */

/*     ret = ABT_xstream_get_affinity(xstream, 0, NULL, &num_cpus); */
/*     if (ret != ABT_SUCCESS) { */
/*         fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_xstream_get_affinity\n"); */
/*         return; */
/*     } */

/*     fprintf(fout_g, "[E%d] num_cpus=%d\n", rank, num_cpus); */
/*     if (num_cpus > 0) { */
/*         cpuset_size = num_cpus; */
/*         cpuset = (int *)malloc(cpuset_size * sizeof(int)); */
/*         /1* assert(cpuset); *1/ */

/*         num_cpus = 0; */
/*         ret = ABT_xstream_get_affinity(xstream, cpuset_size, cpuset, &num_cpus); */
/*         if (ret != ABT_SUCCESS) { */
/*             fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_xstream_get_affinity\n"); */
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
/*     /1*     fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_xstream_set_affinity\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* ret = ABT_xstream_get_affinity(xstream, num_xstreams, cpuset, &num_cpus); *1/ */
/*     /1* if (ret != ABT_SUCCESS) { *1/ */
/*     /1*     fprintf(fout_g, "  [ASYNC VOL ERROR] with ABT_xstream_get_affinity\n"); *1/ */
/*     /1*     return; *1/ */
/*     /1* } *1/ */
/*     /1* /2* assert(num_cpus == num_xstreams); *2/ *1/ */
/*     /1* print_cpuset(rank, num_xstreams, cpuset); *1/ */
/*     /1* free(cpuset); *1/ */
/* } */

/**
 * \ingroup ASYNC
 *
 * \brief Check whether the application thread is busy issuing HDF5 I/O calls.
 *
 * \param[in] time_us Time in us.
 *
 * \return \herr_t
 *
 * \details This function is a workaround of avoiding synchronous execution due to the HDF5 global
 *          mutex. If we start the background thread executing the task as they are created by
 *          the application, the backgrond thread will compete with the application thread for
 *          acquiring the HDF5 mutex and may effective make everything synchronous. Thus we
 *          implemented this "spying" approach by checking the HDF5 global mutex counter value,
 *          if the value does not increase within a predefined (short) amount of time, then we
 *          think that the application is likely to move to its non-I/O phase and thus we can start
 *          the background execution without worrying about the mutex competition.
 *
 */
static int
check_app_acquire_mutex(async_task_t *task, unsigned int *mutex_count, hbool_t *acquired)
{
    unsigned int attempt_count = 0, new_attempt_count = 0, wait_count = 0;

    if (async_instance_g->delay_time > 0) {
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC ABT DBG] %s delay for %lu us\n", __func__,
                    async_instance_g->delay_time);
#endif
        usleep(async_instance_g->delay_time);
    }

    while (async_instance_g->pause) {
        usleep(1000);
        wait_count++;
        if (wait_count == 10000) {
            fprintf(fout_g, "  [ASYNC ABT INFO] async operations are paused for 10 seconds, use "
                            "H5Fstart/H5Dstart to start execution\n");
            wait_count = 0;
        }
    }

    if (H5TSmutex_get_attempt_count(&attempt_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
        return -1;
    }

    if (!async_instance_g->start_abt_push && async_instance_g->ex_delay == false &&
        task->async_obj->file_async_obj) {
        while (1) {
            if (task->async_obj->file_async_obj->attempt_check_cnt % ASYNC_ATTEMPT_CHECK_INTERVAL == 0) {
                if (async_instance_g->sleep_time > 0)
                    usleep(async_instance_g->sleep_time);

                if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
                    fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
                    return -1;
                }

                if (new_attempt_count <= attempt_count) {
                    async_instance_g->sleep_time = ASYNC_APP_CHECK_SLEEP_TIME;

#ifdef ENABLE_DBG_MSG
                    if (async_instance_g &&
                        (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                        fprintf(fout_g, "  [ASYNC ABT DBG] %s counter %d/%d, reset wait time to %d \n",
                                __func__, attempt_count, new_attempt_count, async_instance_g->sleep_time);
#endif
                    break;
                }

                attempt_count = new_attempt_count;
                task->async_obj->file_async_obj->attempt_check_cnt++;
                task->async_obj->file_async_obj->attempt_check_cnt %= ASYNC_ATTEMPT_CHECK_INTERVAL;
            }
            else
                break;
        }
    }

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s going to acquire %u lock\n", __func__, *mutex_count);
#endif

    wait_count = 1;
    while (*acquired == false && *mutex_count > 0) {
        if (H5TSmutex_acquire(*mutex_count, acquired) < 0) {
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_acquire failed\n", __func__);
            return -1;
        }

        if (false == *acquired) {
#ifdef ENABLE_DBG_MSG
            if (wait_count % 1000 == 0 && async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT DBG] %s lock NOT acquired, wait\n", __func__);
#endif
        }
        else {
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT DBG] %s lock acquired, count %u\n", __func__, *mutex_count);
#endif
            break;
        }

        if (wait_count > 0) {
            usleep(1000);
            wait_count++;
            if (wait_count == 10000) {
                fprintf(fout_g,
                        "  [ASYNC ABT INFO] %s unable to acquire HDF5 mutex for 10 seconds, deadlock?\n",
                        __func__);
                wait_count = 1;
            }
        }
    }

    return (new_attempt_count > attempt_count ? new_attempt_count : attempt_count);
}

/**
 * \ingroup ASYNC
 *
 * \brief Check whether the application thread issued HDF5 I/O calls during background task execution.
 *
 * \param[in] attempt_count Previous HDF5 mutex counter value
 * \param[in] func_name     Name of the calling function
 *
 * \return \herr_t
 *
 * \details This function works with check_app_acquire_mutex(), it detects whether the HDF5 global
 *          mutex counter value increased right after a task is executed by the background thread.
 *          If so, then it is likely that our check period in check_app_acquire_mutex() is too short
 *          and we executed a task while the application has more HDF5 I/O calls. So we increase the
 *          check period by a factor of two. On the other hand, if we found the value did not increase
 *          then we can reduce the checking overhead by setting the check time to zero until next time
 *          we detect the value is increased.
 *
 */
static void
check_app_wait(int attempt_count, const char *func_name)
{
    unsigned int new_attempt_count, op_count = 1;

    // No need for status check when explicit file/group/dset delay is enabled
    if (async_instance_g->ex_delay)
        return;

    if (H5TSmutex_get_attempt_count(&new_attempt_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_get_attempt_count failed\n", __func__);
        return;
    }

    // If the application thread is waiting, double the current sleep time for next status check
    if (attempt_count > 0 && new_attempt_count > attempt_count + op_count) {
        if (async_instance_g->sleep_time < ASYNC_APP_CHECK_SLEEP_TIME_MAX) {
            if (async_instance_g->sleep_time == 0)
                async_instance_g->sleep_time = ASYNC_APP_CHECK_SLEEP_TIME;
            else
                async_instance_g->sleep_time *= 2;
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT DBG] %s counter %d/%d, increase wait time to %d \n", func_name,
                        attempt_count, new_attempt_count, async_instance_g->sleep_time);
#endif
        }
    }
    else if (new_attempt_count <= attempt_count + op_count &&
             async_instance_g->sleep_time > ASYNC_APP_CHECK_SLEEP_TIME) {
        async_instance_g->sleep_time = 0;
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC ABT DBG] %s counter %d/%d, reset wait time to %d \n", func_name,
                    attempt_count, new_attempt_count, async_instance_g->sleep_time);
#endif
    }
    return;
}

/**
 * \ingroup ASYNC
 *
 * \brief Check if a task's creation task is valid
 *
 * \param[in] parent_obj Async object
 *
 * \return \herr_t
 *
 * \details Check whether a task's parent task is successful or not.
 *
 */
static int
check_parent_task(H5VL_async_t *parent_obj)
{
    int ret_val = 0;
    if (parent_obj->create_task && parent_obj->create_task->err_stack != 0)
        return -1;

    return ret_val;
}
/**
 * \ingroup ASYNC
 *
 * \brief Execute all dependent parent tasks
 *
 * \param[in] task Async task
 *
 * \details Execute all the parent tasks of a given tasks recursively. If task already in
 *          the Argobots pool, wait for it, otherwise execute in current thread.
 *
 */
static void
execute_parent_task_recursive(async_task_t *task)
{
    int          i;
    hbool_t      acquired    = false;
    unsigned int mutex_count = 0;

    if (task == NULL || task->is_done == 1)
        return;

    for (i = 0; i < task->n_dep; i++)
        execute_parent_task_recursive(task->dep_tasks[i]);

    if (task->in_abt_pool == 1) {
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s: wait for argobots task\n", __func__);
#endif
        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
            return;
        }
        if (ABT_eventual_wait(task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            return;
        }
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                return;
            }
        }
    }
    else {
        task->func(task);
    }
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] %s: finished executing task \n", __func__);
#endif
}

/**
 * \ingroup ASYNC
 *
 * \brief Realize a future object
 *
 * \param[in] _future_object Future object
 * \param[in] actual_object_id Realized future object
 *
 * \return \herr_t
 *
 * \details Execute all the parent tasks of a given tasks recursively.
 *
 */
static herr_t
async_realize_future_cb(void *_future_object, hid_t *actual_object_id)
{
    hbool_t             acquired      = false;
    unsigned int        mutex_count   = 0;
    async_future_obj_t *future_object = (async_future_obj_t *)_future_object;

    // Drain the existing tasks in Argobots pool first
    while (get_n_running_task_in_queue(future_object->task, __func__) != 0) {
        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
            return -1;
        }
        usleep(1000);
    }
    while (mutex_count > 0 && acquired == false) {
        if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
            return -1;
        }
    }

    if (H5I_INVALID_HID == future_object->id) {
        /* Execute the task, recursively executing any parent tasks first */
        assert(future_object->task);
        execute_parent_task_recursive(future_object->task);

        if (future_object->task) {
            if (H5TSmutex_release(&mutex_count) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
                return -1;
            }
            if (ABT_eventual_wait(future_object->task->eventual, NULL) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
                return -1;
            }
            while (acquired == false && mutex_count > 0) {
                if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                    fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                    return -1;
                }
            }
        }

        assert(H5I_INVALID_HID != future_object->id);
    }
    /* Set the ID to return */
    *actual_object_id = future_object->id;

    return (0);
}

/**
 * \ingroup ASYNC
 *
 * \brief Free a future object.
 *
 * \param[in] _future_object Future object
 *
 * \return \herr_t
 *
 * \details Free a future object.
 *
 */
static herr_t
async_discard_future_cb(void *future_object)
{
    assert(future_object);

    free(future_object);

    return (0);
}

/**
 * \ingroup ASYNC
 *
 * \brief Duplicate an HDF5 location parameter object
 *
 * \param[in] dest Duplicated location parameters
 * \param[in] loc_params Exist location parameters
 *
 * \return \herr_t
 *
 * \details Duplicate an HDF5 location parameter object
 *
 */
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
        ref_size                          = 16; // taken from H5VLnative_object.c
        dest->loc_data.loc_by_token.token = malloc(ref_size);
        memcpy((void *)(dest->loc_data.loc_by_token.token), loc_params->loc_data.loc_by_token.token,
               ref_size);
    }
}

/**
 * \ingroup ASYNC
 *
 * \brief Free an HDF5 location parameter object
 *
 * \param[in] dest Duplicated location parameters
 *
 * \return \herr_t
 *
 * \details Free an HDF5 location parameter object
 *
 */
static void
free_loc_param(H5VL_loc_params_t *loc_params)
{
    assert(loc_params);

    if (loc_params->type == H5VL_OBJECT_BY_NAME) {
        free((void *)loc_params->loc_data.loc_by_name.name);
        H5Pclose(loc_params->loc_data.loc_by_name.lapl_id);
    }
    else if (loc_params->type == H5VL_OBJECT_BY_IDX) {
        free((void *)loc_params->loc_data.loc_by_idx.name);
        H5Pclose(loc_params->loc_data.loc_by_idx.lapl_id);
    }
    /* else if (loc_params->type == H5VL_OBJECT_BY_ADDR) { */
    /* } */
    else if (loc_params->type == H5VL_OBJECT_BY_TOKEN) {
        free((void *)loc_params->loc_data.loc_by_token.token);
    }
}

static int
dup_attr_get_args(H5VL_attr_get_args_t *dst_args, H5VL_attr_get_args_t *src_args, async_task_t *task)
{
    hid_t *    future_id_ptr  = NULL;      /* Pointer to ID for future ID */
    H5I_type_t future_id_type = H5I_BADID; /* Type ("class") of future ID */
    hbool_t    need_future_id = false;     /* Whether an operation needs a future ID */

    assert(dst_args);
    assert(src_args);
    assert(task);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things & set up future IDs for each operation */
    switch (src_args->op_type) {
        case H5VL_ATTR_GET_INFO:
            dup_loc_param(&dst_args->args.get_info.loc_params, &src_args->args.get_info.loc_params);
            if (src_args->args.get_info.attr_name)
                dst_args->args.get_info.attr_name = strdup(src_args->args.get_info.attr_name);
            break;

        case H5VL_ATTR_GET_NAME:
            dup_loc_param(&dst_args->args.get_name.loc_params, &src_args->args.get_name.loc_params);
            break;

        case H5VL_ATTR_GET_ACPL:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_GENPROP_LST;
            future_id_ptr  = &src_args->args.get_acpl.acpl_id; /* Note: src_args */
            break;

        case H5VL_ATTR_GET_TYPE:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_DATATYPE;
            future_id_ptr  = &src_args->args.get_type.type_id; /* Note: src_args */
            break;

        case H5VL_ATTR_GET_SPACE:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_DATASPACE;
            future_id_ptr  = &src_args->args.get_space.space_id; /* Note: src_args */
            break;

        case H5VL_ATTR_GET_STORAGE_SIZE:
            /* No items to deep copy & no future IDs */
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Set up future ID for operation */
    if (need_future_id) {
        async_future_obj_t *future_obj;

        /* Sanity check */
        assert(future_id_ptr);
        assert(H5I_BADID != future_id_type);

        /* Allocate & set up future object */
        if (NULL == (future_obj = calloc(1, sizeof(async_future_obj_t)))) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s allocating future object\n", __func__);
            return -1;
        }
        future_obj->id   = H5I_INVALID_HID;
        future_obj->task = task;

        /* Set future object for task */
        task->future_obj = future_obj;

        /* Register ID for future object, to return to caller */
        *future_id_ptr =
            H5Iregister_future(future_id_type, future_obj, async_realize_future_cb, async_discard_future_cb);
        if (*future_id_ptr < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s error creating future ID\n", __func__);
            return -1;
        }
    }

    return 0;
}

static void
free_attr_get_args(H5VL_attr_get_args_t *args, async_task_t *task)
{
    assert(args);
    assert(task);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_ATTR_GET_INFO:
            free_loc_param(&args->args.get_info.loc_params);
            if (args->args.get_info.attr_name)
                free((void *)args->args.get_info.attr_name);
            break;

        case H5VL_ATTR_GET_NAME:
            free_loc_param(&args->args.get_info.loc_params);
            break;

        case H5VL_ATTR_GET_ACPL:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_acpl.acpl_id;
            break;

        case H5VL_ATTR_GET_SPACE:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_space.space_id;
            break;

        case H5VL_ATTR_GET_TYPE:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_type.type_id;
            break;

        case H5VL_ATTR_GET_STORAGE_SIZE:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Detach future object from task (to be safe) */
    if (task->future_obj) {
        task->future_obj->task = NULL;
        task->future_obj       = NULL;
    }
}

static void
dup_attr_spec_args(H5VL_attr_specific_args_t *dst_args, const H5VL_attr_specific_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_ATTR_DELETE:
            if (src_args->args.del.name)
                dst_args->args.del.name = strdup(src_args->args.del.name);
            break;

        case H5VL_ATTR_EXISTS:
            if (src_args->args.exists.name)
                dst_args->args.exists.name = strdup(src_args->args.exists.name);
            break;

        case H5VL_ATTR_RENAME:
            if (src_args->args.rename.old_name)
                dst_args->args.rename.old_name = strdup(src_args->args.rename.old_name);
            if (src_args->args.rename.new_name)
                dst_args->args.rename.new_name = strdup(src_args->args.rename.new_name);
            break;

        case H5VL_ATTR_DELETE_BY_IDX:
        case H5VL_ATTR_ITER:
            /* No items to deep copy */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
free_attr_spec_args(H5VL_attr_specific_args_t *args)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_ATTR_DELETE:
            if (args->args.del.name)
                free((void *)args->args.del.name);
            break;

        case H5VL_ATTR_EXISTS:
            if (args->args.exists.name)
                free((void *)args->args.exists.name);
            break;

        case H5VL_ATTR_RENAME:
            if (args->args.rename.old_name)
                free((void *)args->args.rename.old_name);
            if (args->args.rename.new_name)
                free((void *)args->args.rename.new_name);
            break;

        case H5VL_ATTR_DELETE_BY_IDX:
        case H5VL_ATTR_ITER:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
dup_native_attr_optional_args(async_attr_optional_args_t *dst_args, const H5VL_optional_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Duplicate native operation info */
    if (src_args->op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        if (src_args->args) {
            memcpy(&dst_args->opt_args, src_args->args, sizeof(dst_args->opt_args));
            dst_args->args.args = &dst_args->opt_args;
        } /* end if */

        /* Deep copy appropriate things for each operation */
        switch (src_args->op_type) {
            case H5VL_NATIVE_ATTR_ITERATE_OLD:
                /* No items to deep copy */
                break;

            default:
                assert(0 && "unknown operation");
        }
    }
}

static void
free_native_attr_optional_args(async_attr_optional_args_t *args)
{
    assert(args);

    /* Free native operation info */
    if (args->args.op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        /* Free appropriate things for each operation */
        switch (args->args.op_type) {
            case H5VL_NATIVE_ATTR_ITERATE_OLD:
                /* No items to free */
                break;

            default:
                assert(0 && "unknown operation");
        }
    }
}

static int
dup_dataset_get_args(H5VL_dataset_get_args_t *dst_args, H5VL_dataset_get_args_t *src_args, async_task_t *task)
{
    hid_t *    future_id_ptr  = NULL;      /* Pointer to ID for future ID */
    H5I_type_t future_id_type = H5I_BADID; /* Type ("class") of future ID */
    hbool_t    need_future_id = false;     /* Whether an operation needs a future ID */

    assert(dst_args);
    assert(src_args);
    assert(task);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things & set up future IDs for each operation */
    switch (src_args->op_type) {
        case H5VL_DATASET_GET_DAPL:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_GENPROP_LST;
            future_id_ptr  = &src_args->args.get_dapl.dapl_id; /* Note: src_args */
            break;

        case H5VL_DATASET_GET_DCPL:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_GENPROP_LST;
            future_id_ptr  = &src_args->args.get_dcpl.dcpl_id; /* Note: src_args */
            break;

        case H5VL_DATASET_GET_SPACE:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_DATASPACE;
            future_id_ptr  = &src_args->args.get_space.space_id; /* Note: src_args */
            break;

        case H5VL_DATASET_GET_TYPE:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_DATATYPE;
            future_id_ptr  = &src_args->args.get_type.type_id; /* Note: src_args */
            break;

        case H5VL_DATASET_GET_SPACE_STATUS:
        case H5VL_DATASET_GET_STORAGE_SIZE:
            /* No items to deep copy & no future IDs */
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Set up future ID for operation */
    if (need_future_id) {
        async_future_obj_t *future_obj;

        /* Sanity check */
        assert(future_id_ptr);
        assert(H5I_BADID != future_id_type);

        /* Allocate & set up future object */
        if (NULL == (future_obj = calloc(1, sizeof(async_future_obj_t)))) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s allocating future object\n", __func__);
            return -1;
        }
        future_obj->id   = H5I_INVALID_HID;
        future_obj->task = task;

        /* Set future object for task */
        task->future_obj = future_obj;

        /* Register ID for future object, to return to caller */
        *future_id_ptr =
            H5Iregister_future(future_id_type, future_obj, async_realize_future_cb, async_discard_future_cb);
        if (*future_id_ptr < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s error creating future ID\n", __func__);
            return -1;
        }
    }

    return 0;
}

static void
free_dataset_get_args(H5VL_dataset_get_args_t *args, async_task_t *task)
{
    assert(args);
    assert(task);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_DATASET_GET_DAPL:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_dapl.dapl_id;
            break;

        case H5VL_DATASET_GET_DCPL:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_dcpl.dcpl_id;
            break;

        case H5VL_DATASET_GET_SPACE:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_space.space_id;
            break;

        case H5VL_DATASET_GET_TYPE:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_type.type_id;
            break;

        case H5VL_DATASET_GET_SPACE_STATUS:
        case H5VL_DATASET_GET_STORAGE_SIZE:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Detach future object from task (to be safe) */
    if (task->future_obj) {
        task->future_obj->task = NULL;
        task->future_obj       = NULL;
    }
}

static void
dup_dataset_spec_args(H5VL_dataset_specific_args_t *dst_args, const H5VL_dataset_specific_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_DATASET_FLUSH:
            H5Iinc_ref(dst_args->args.flush.dset_id);
            break;

        case H5VL_DATASET_REFRESH:
            H5Iinc_ref(dst_args->args.refresh.dset_id);
            break;

        case H5VL_DATASET_SET_EXTENT:
            /* No items to deep copy */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
free_dataset_spec_args(H5VL_dataset_specific_args_t *args)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_DATASET_FLUSH:
            H5Idec_ref(args->args.flush.dset_id);
            break;

        case H5VL_DATASET_REFRESH:
            H5Idec_ref(args->args.refresh.dset_id);
            break;

        case H5VL_DATASET_SET_EXTENT:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
dup_native_dataset_optional_args(async_dataset_optional_args_t *dst_args,
                                 const H5VL_optional_args_t *   src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(&dst_args->args, src_args, sizeof(dst_args->args));

    /* Duplicate native operation info */
    if (src_args->op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        if (src_args->args) {
            memcpy(&dst_args->opt_args, src_args->args, sizeof(dst_args->opt_args));
            dst_args->args.args = &dst_args->opt_args;
        } /* end if */

        /* Deep copy appropriate things for each operation */
        switch (src_args->op_type) {
            case H5VL_NATIVE_DATASET_GET_VLEN_BUF_SIZE:
                dst_args->opt_args.get_vlen_buf_size.type_id = H5Tcopy(
                    ((H5VL_native_dataset_optional_args_t *)src_args->args)->get_vlen_buf_size.type_id);
                dst_args->opt_args.get_vlen_buf_size.space_id = H5Scopy(
                    ((H5VL_native_dataset_optional_args_t *)src_args->args)->get_vlen_buf_size.space_id);
                break;

            case H5VL_NATIVE_DATASET_GET_NUM_CHUNKS:
                dst_args->opt_args.get_num_chunks.space_id =
                    H5Scopy(((H5VL_native_dataset_optional_args_t *)src_args->args)->get_num_chunks.space_id);
                break;

            case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_IDX:
                dst_args->opt_args.get_chunk_info_by_idx.space_id = H5Scopy(
                    ((H5VL_native_dataset_optional_args_t *)src_args->args)->get_chunk_info_by_idx.space_id);
                break;

            case H5VL_NATIVE_DATASET_FORMAT_CONVERT:
            case H5VL_NATIVE_DATASET_GET_CHUNK_INDEX_TYPE:
            case H5VL_NATIVE_DATASET_GET_CHUNK_STORAGE_SIZE:
            case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_COORD:
            case H5VL_NATIVE_DATASET_CHUNK_READ:
            case H5VL_NATIVE_DATASET_CHUNK_WRITE:
            case H5VL_NATIVE_DATASET_GET_OFFSET:
                /* No items to deep copy */
                break;

            default:
                assert(0 && "unknown operation");
        }
    }
}

static void
free_native_dataset_optional_args(async_dataset_optional_args_t *args)
{
    assert(args);

    /* Free native operation info */
    if (args->args.op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        /* Free appropriate things for each operation */
        switch (args->args.op_type) {
            case H5VL_NATIVE_DATASET_GET_VLEN_BUF_SIZE:
                H5Tclose(args->opt_args.get_vlen_buf_size.type_id);
                H5Sclose(args->opt_args.get_vlen_buf_size.space_id);
                break;

            case H5VL_NATIVE_DATASET_GET_NUM_CHUNKS:
                H5Sclose(args->opt_args.get_num_chunks.space_id);
                break;

            case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_IDX:
                H5Sclose(args->opt_args.get_chunk_info_by_idx.space_id);
                break;

            case H5VL_NATIVE_DATASET_FORMAT_CONVERT:
            case H5VL_NATIVE_DATASET_GET_CHUNK_INDEX_TYPE:
            case H5VL_NATIVE_DATASET_GET_CHUNK_STORAGE_SIZE:
            case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_COORD:
            case H5VL_NATIVE_DATASET_CHUNK_READ:
            case H5VL_NATIVE_DATASET_CHUNK_WRITE:
            case H5VL_NATIVE_DATASET_GET_OFFSET:
                /* No items to free */
                break;

            default:
                assert(0 && "unknown operation");
        }
    }
}

static int
dup_datatype_get_args(H5VL_datatype_get_args_t *dst_args, H5VL_datatype_get_args_t *src_args,
                      async_task_t *task)
{
    hid_t *    future_id_ptr  = NULL;      /* Pointer to ID for future ID */
    H5I_type_t future_id_type = H5I_BADID; /* Type ("class") of future ID */
    hbool_t    need_future_id = false;     /* Whether an operation needs a future ID */

    assert(dst_args);
    assert(src_args);
    assert(task);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things & set up future IDs for each operation */
    switch (src_args->op_type) {
        case H5VL_DATATYPE_GET_TCPL:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_GENPROP_LST;
            future_id_ptr  = &src_args->args.get_tcpl.tcpl_id; /* Note: src_args */
            break;

        case H5VL_DATATYPE_GET_BINARY_SIZE:
        case H5VL_DATATYPE_GET_BINARY:
            /* No items to deep copy & no future IDs */
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Set up future ID for operation */
    if (need_future_id) {
        async_future_obj_t *future_obj;

        /* Sanity check */
        assert(future_id_ptr);
        assert(H5I_BADID != future_id_type);

        /* Allocate & set up future object */
        if (NULL == (future_obj = calloc(1, sizeof(async_future_obj_t)))) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s allocating future object\n", __func__);
            return -1;
        }
        future_obj->id   = H5I_INVALID_HID;
        future_obj->task = task;

        /* Set future object for task */
        task->future_obj = future_obj;

        /* Register ID for future object, to return to caller */
        *future_id_ptr =
            H5Iregister_future(future_id_type, future_obj, async_realize_future_cb, async_discard_future_cb);
        if (*future_id_ptr < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s error creating future ID\n", __func__);
            return -1;
        }
    }

    return 0;
}

static void
free_datatype_get_args(H5VL_datatype_get_args_t *args, async_task_t *task)
{
    assert(args);
    assert(task);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_DATATYPE_GET_TCPL:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_tcpl.tcpl_id;
            break;

        case H5VL_DATATYPE_GET_BINARY_SIZE:
        case H5VL_DATATYPE_GET_BINARY:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Detach future object from task (to be safe) */
    if (task->future_obj) {
        task->future_obj->task = NULL;
        task->future_obj       = NULL;
    }
}

static void
dup_datatype_spec_args(H5VL_datatype_specific_args_t *dst_args, const H5VL_datatype_specific_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_DATATYPE_FLUSH:
            H5Iinc_ref(dst_args->args.flush.type_id);
            break;

        case H5VL_DATATYPE_REFRESH:
            H5Iinc_ref(dst_args->args.refresh.type_id);
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
free_datatype_spec_args(H5VL_datatype_specific_args_t *args)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_DATATYPE_FLUSH:
            H5Idec_ref(args->args.flush.type_id);
            break;

        case H5VL_DATATYPE_REFRESH:
            H5Idec_ref(args->args.refresh.type_id);
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
dup_native_datatype_optional_args(async_datatype_optional_args_t *dst_args,
                                  const H5VL_optional_args_t *    src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(&dst_args->args, src_args, sizeof(dst_args->args));

    /* Duplicate native operation info */
    if (src_args->op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
#ifdef NOT_YET
        if (src_args->args) {
            memcpy(&dst_args->opt_args, src_args->args, sizeof(dst_args->opt_args));
            dst_args->args.args = &dst_args->opt_args;
        } /* end if */

        /* Deep copy appropriate things for each operation */
        switch (src_args->op_type) {
            default:
                assert(0 && "unknown operation");
        }
#endif /* NOT_YET */
    }
}

static void
free_native_datatype_optional_args(async_datatype_optional_args_t *args)
{
    assert(args);

    /* Free native operation info */
    if (args->args.op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
#ifdef NOT_YET
        /* Free appropriate things for each operation */
        switch (args->args.op_type) {
            default:
                assert(0 && "unknown operation");
        }
#endif /* NOT_YET */
    }
}

static int
dup_file_get_args(H5VL_file_get_args_t *dst_args, H5VL_file_get_args_t *src_args, async_task_t *task)
{
    hid_t *    future_id_ptr  = NULL;      /* Pointer to ID for future ID */
    H5I_type_t future_id_type = H5I_BADID; /* Type ("class") of future ID */
    hbool_t    need_future_id = false;     /* Whether an operation needs a future ID */

    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_FILE_GET_CONT_INFO:
            break;

        case H5VL_FILE_GET_FAPL:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_GENPROP_LST;
            future_id_ptr  = &src_args->args.get_fapl.fapl_id; /* Note: src_args */
            break;

        case H5VL_FILE_GET_FCPL:
            /* Set up for creating future ID */
            need_future_id = true;
            future_id_type = H5I_GENPROP_LST;
            future_id_ptr  = &src_args->args.get_fcpl.fcpl_id; /* Note: src_args */
            break;

        case H5VL_FILE_GET_FILENO:
            break;
        case H5VL_FILE_GET_INTENT:
            break;
        case H5VL_FILE_GET_NAME:
            break;
        case H5VL_FILE_GET_OBJ_COUNT:
            break;
        case H5VL_FILE_GET_OBJ_IDS:
            /* No items to deep copy */
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Set up future ID for operation */
    if (need_future_id) {
        async_future_obj_t *future_obj;

        /* Sanity check */
        assert(future_id_ptr);
        assert(H5I_BADID != future_id_type);

        /* Allocate & set up future object */
        if (NULL == (future_obj = calloc(1, sizeof(async_future_obj_t)))) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s allocating future object\n", __func__);
            return -1;
        }
        future_obj->id   = H5I_INVALID_HID;
        future_obj->task = task;

        /* Set future object for task */
        task->future_obj = future_obj;

        /* Register ID for future object, to return to caller */
        *future_id_ptr =
            H5Iregister_future(future_id_type, future_obj, async_realize_future_cb, async_discard_future_cb);
        if (*future_id_ptr < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s error creating future ID\n", __func__);
            return -1;
        }
    }

    return 0;
}

static void
free_file_get_args(H5VL_file_get_args_t *args, async_task_t *task)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_FILE_GET_CONT_INFO:
            break;

        case H5VL_FILE_GET_FAPL:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_fapl.fapl_id;
            break;

        case H5VL_FILE_GET_FCPL:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_fcpl.fcpl_id;
            break;

        case H5VL_FILE_GET_FILENO:
            break;

        case H5VL_FILE_GET_INTENT:
            break;

        case H5VL_FILE_GET_NAME:
            break;

        case H5VL_FILE_GET_OBJ_COUNT:
            break;

        case H5VL_FILE_GET_OBJ_IDS:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Detach future object from task (to be safe) */
    if (task->future_obj) {
        task->future_obj->task = NULL;
        task->future_obj       = NULL;
    }
}

static void
dup_file_spec_args(H5VL_file_specific_args_t *dst_args, const H5VL_file_specific_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_FILE_IS_ACCESSIBLE:
            if (src_args->args.is_accessible.filename)
                dst_args->args.is_accessible.filename = strdup(src_args->args.is_accessible.filename);
            if (src_args->args.is_accessible.fapl_id)
                dst_args->args.is_accessible.fapl_id = H5Pcopy(src_args->args.is_accessible.fapl_id);
            break;

        case H5VL_FILE_DELETE:
            if (src_args->args.del.filename)
                dst_args->args.del.filename = strdup(src_args->args.del.filename);
            if (src_args->args.del.fapl_id)
                dst_args->args.del.fapl_id = H5Pcopy(src_args->args.del.fapl_id);
            break;

        case H5VL_FILE_FLUSH:
        case H5VL_FILE_REOPEN:
        case H5VL_FILE_IS_EQUAL:
            /* No items to deep copy */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
free_file_spec_args(H5VL_file_specific_args_t *args)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_FILE_IS_ACCESSIBLE:
            if (args->args.is_accessible.filename)
                free((void *)args->args.is_accessible.filename);
            if (args->args.is_accessible.fapl_id)
                H5Pclose(args->args.is_accessible.fapl_id);
            break;

        case H5VL_FILE_DELETE:
            if (args->args.del.filename)
                free((void *)args->args.del.filename);
            if (args->args.del.fapl_id)
                H5Pclose(args->args.del.fapl_id);
            break;

        case H5VL_FILE_FLUSH:
        case H5VL_FILE_REOPEN:
        case H5VL_FILE_IS_EQUAL:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
dup_native_file_optional_args(async_file_optional_args_t *dst_args, const H5VL_optional_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(&dst_args->args, src_args, sizeof(dst_args->args));

    /* Duplicate native operation info */
    if (src_args->op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        if (src_args->args) {
            memcpy(&dst_args->opt_args, src_args->args, sizeof(dst_args->opt_args));
            dst_args->args.args = &dst_args->opt_args;
        } /* end if */

        /* Deep copy appropriate things for each operation */
        switch (src_args->op_type) {
            case H5VL_NATIVE_FILE_GET_VFD_HANDLE:
                dst_args->opt_args.get_vfd_handle.fapl_id =
                    H5Pcopy(((H5VL_native_file_optional_args_t *)src_args->args)->get_vfd_handle.fapl_id);
                break;

            case H5VL_NATIVE_FILE_CLEAR_ELINK_CACHE:
            case H5VL_NATIVE_FILE_GET_FILE_IMAGE:
            case H5VL_NATIVE_FILE_GET_FREE_SECTIONS:
            case H5VL_NATIVE_FILE_GET_FREE_SPACE:
            case H5VL_NATIVE_FILE_GET_INFO:
            case H5VL_NATIVE_FILE_GET_MDC_CONF:
            case H5VL_NATIVE_FILE_GET_MDC_HR:
            case H5VL_NATIVE_FILE_GET_MDC_SIZE:
            case H5VL_NATIVE_FILE_GET_SIZE:
            case H5VL_NATIVE_FILE_RESET_MDC_HIT_RATE:
            case H5VL_NATIVE_FILE_SET_MDC_CONFIG:
            case H5VL_NATIVE_FILE_GET_METADATA_READ_RETRY_INFO:
            case H5VL_NATIVE_FILE_START_SWMR_WRITE:
            case H5VL_NATIVE_FILE_START_MDC_LOGGING:
            case H5VL_NATIVE_FILE_STOP_MDC_LOGGING:
            case H5VL_NATIVE_FILE_GET_MDC_LOGGING_STATUS:
            case H5VL_NATIVE_FILE_FORMAT_CONVERT:
            case H5VL_NATIVE_FILE_RESET_PAGE_BUFFERING_STATS:
            case H5VL_NATIVE_FILE_GET_PAGE_BUFFERING_STATS:
            case H5VL_NATIVE_FILE_GET_MDC_IMAGE_INFO:
            case H5VL_NATIVE_FILE_GET_EOA:
            case H5VL_NATIVE_FILE_INCR_FILESIZE:
            case H5VL_NATIVE_FILE_SET_LIBVER_BOUNDS:
            case H5VL_NATIVE_FILE_GET_MIN_DSET_OHDR_FLAG:
            case H5VL_NATIVE_FILE_SET_MIN_DSET_OHDR_FLAG:
#ifdef H5_HAVE_PARALLEL
            case H5VL_NATIVE_FILE_GET_MPI_ATOMICITY:
            case H5VL_NATIVE_FILE_SET_MPI_ATOMICITY:
#endif /* H5_HAVE_PARALLEL */
            case H5VL_NATIVE_FILE_POST_OPEN:
                /* No items to deep copy */
                break;

            default:
                assert(0 && "unknown operation");
        }
    }
}

static void
free_native_file_optional_args(async_file_optional_args_t *args)
{
    assert(args);

    /* Free native operation info */
    if (args->args.op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        /* Free appropriate things for each operation */
        switch (args->args.op_type) {
            case H5VL_NATIVE_FILE_GET_VFD_HANDLE:
                H5Pclose(args->opt_args.get_vfd_handle.fapl_id);
                break;

            case H5VL_NATIVE_FILE_CLEAR_ELINK_CACHE:
            case H5VL_NATIVE_FILE_GET_FILE_IMAGE:
            case H5VL_NATIVE_FILE_GET_FREE_SECTIONS:
            case H5VL_NATIVE_FILE_GET_FREE_SPACE:
            case H5VL_NATIVE_FILE_GET_INFO:
            case H5VL_NATIVE_FILE_GET_MDC_CONF:
            case H5VL_NATIVE_FILE_GET_MDC_HR:
            case H5VL_NATIVE_FILE_GET_MDC_SIZE:
            case H5VL_NATIVE_FILE_GET_SIZE:
            case H5VL_NATIVE_FILE_RESET_MDC_HIT_RATE:
            case H5VL_NATIVE_FILE_SET_MDC_CONFIG:
            case H5VL_NATIVE_FILE_GET_METADATA_READ_RETRY_INFO:
            case H5VL_NATIVE_FILE_START_SWMR_WRITE:
            case H5VL_NATIVE_FILE_START_MDC_LOGGING:
            case H5VL_NATIVE_FILE_STOP_MDC_LOGGING:
            case H5VL_NATIVE_FILE_GET_MDC_LOGGING_STATUS:
            case H5VL_NATIVE_FILE_FORMAT_CONVERT:
            case H5VL_NATIVE_FILE_RESET_PAGE_BUFFERING_STATS:
            case H5VL_NATIVE_FILE_GET_PAGE_BUFFERING_STATS:
            case H5VL_NATIVE_FILE_GET_MDC_IMAGE_INFO:
            case H5VL_NATIVE_FILE_GET_EOA:
            case H5VL_NATIVE_FILE_INCR_FILESIZE:
            case H5VL_NATIVE_FILE_SET_LIBVER_BOUNDS:
            case H5VL_NATIVE_FILE_GET_MIN_DSET_OHDR_FLAG:
            case H5VL_NATIVE_FILE_SET_MIN_DSET_OHDR_FLAG:
#ifdef H5_HAVE_PARALLEL
            case H5VL_NATIVE_FILE_GET_MPI_ATOMICITY:
            case H5VL_NATIVE_FILE_SET_MPI_ATOMICITY:
#endif /* H5_HAVE_PARALLEL */
            case H5VL_NATIVE_FILE_POST_OPEN:
                /* No items to deep copy */
                break;

            default:
                assert(0 && "unknown operation");
        }
    }
}

static int
dup_group_get_args(H5VL_group_get_args_t *dst_args, H5VL_group_get_args_t *src_args, async_task_t *task)
{
    hid_t *    future_id_ptr  = NULL;      /* Pointer to ID for future ID */
    H5I_type_t future_id_type = H5I_BADID; /* Type ("class") of future ID */
    hbool_t    need_future_id = false;     /* Whether an operation needs a future ID */

    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_GROUP_GET_INFO:
            dup_loc_param(&dst_args->args.get_info.loc_params, &src_args->args.get_info.loc_params);
            break;

        case H5VL_GROUP_GET_GCPL:
            /* No items to deep copy */
            need_future_id = true;
            future_id_type = H5I_GENPROP_LST;
            future_id_ptr  = &src_args->args.get_gcpl.gcpl_id; /* Note: src_args */
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Set up future ID for operation */
    if (need_future_id) {
        async_future_obj_t *future_obj;

        /* Sanity check */
        assert(future_id_ptr);
        assert(H5I_BADID != future_id_type);

        /* Allocate & set up future object */
        if (NULL == (future_obj = calloc(1, sizeof(async_future_obj_t)))) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s allocating future object\n", __func__);
            return -1;
        }
        future_obj->id   = H5I_INVALID_HID;
        future_obj->task = task;

        /* Set future object for task */
        task->future_obj = future_obj;

        /* Register ID for future object, to return to caller */
        *future_id_ptr =
            H5Iregister_future(future_id_type, future_obj, async_realize_future_cb, async_discard_future_cb);
        if (*future_id_ptr < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s error creating future ID\n", __func__);
            return -1;
        }
    }

    return 0;
}

static void
free_group_get_args(H5VL_group_get_args_t *args, async_task_t *task)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_GROUP_GET_INFO:
            free_loc_param(&args->args.get_info.loc_params);
            break;

        case H5VL_GROUP_GET_GCPL:
            /* Check for future object to update */
            if (task->future_obj)
                /* Save ID to future object */
                task->future_obj->id = args->args.get_gcpl.gcpl_id;
            break;

        default:
            assert(0 && "unknown operation");
    }

    /* Detach future object from task (to be safe) */
    if (task->future_obj) {
        task->future_obj->task = NULL;
        task->future_obj       = NULL;
    }
}

static void
dup_group_spec_args(H5VL_group_specific_args_t *dst_args, const H5VL_group_specific_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_GROUP_MOUNT:
            if (src_args->args.mount.name)
                dst_args->args.mount.name = strdup(src_args->args.mount.name);
            if (src_args->args.mount.fmpl_id)
                dst_args->args.mount.fmpl_id = H5Pcopy(src_args->args.mount.fmpl_id);
            break;

        case H5VL_GROUP_UNMOUNT:
            if (src_args->args.unmount.name)
                dst_args->args.unmount.name = strdup(src_args->args.unmount.name);
            break;

        case H5VL_GROUP_FLUSH:
            H5Iinc_ref(dst_args->args.flush.grp_id);
            break;

        case H5VL_GROUP_REFRESH:
            H5Iinc_ref(dst_args->args.refresh.grp_id);
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
free_group_spec_args(H5VL_group_specific_args_t *args)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_GROUP_MOUNT:
            if (args->args.mount.name)
                free((void *)args->args.mount.name);
            if (args->args.mount.fmpl_id)
                H5Pclose(args->args.mount.fmpl_id);
            break;

        case H5VL_GROUP_UNMOUNT:
            if (args->args.unmount.name)
                free((void *)args->args.unmount.name);
            break;

        case H5VL_GROUP_FLUSH:
            H5Idec_ref(args->args.flush.grp_id);
            break;

        case H5VL_GROUP_REFRESH:
            H5Idec_ref(args->args.refresh.grp_id);
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
dup_native_group_optional_args(async_group_optional_args_t *dst_args, const H5VL_optional_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(&dst_args->args, src_args, sizeof(dst_args->args));

    /* Duplicate native operation info */
    if (src_args->op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        if (src_args->args) {
            memcpy(&dst_args->opt_args, src_args->args, sizeof(dst_args->opt_args));
            dst_args->args.args = &dst_args->opt_args;
        } /* end if */

        /* Deep copy appropriate things for each operation */
        switch (src_args->op_type) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
            case H5VL_NATIVE_GROUP_ITERATE_OLD:
                dup_loc_param(&dst_args->opt_args.iterate_old.loc_params,
                              &((H5VL_native_group_optional_args_t *)src_args->args)->iterate_old.loc_params);
                break;

            case H5VL_NATIVE_GROUP_GET_OBJINFO:
                dup_loc_param(&dst_args->opt_args.get_objinfo.loc_params,
                              &((H5VL_native_group_optional_args_t *)src_args->args)->get_objinfo.loc_params);
                break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

            default:
                assert(0 && "unknown operation");
        }
    }
}

static void
free_native_group_optional_args(async_group_optional_args_t *args)
{
    assert(args);

    /* Free native operation info */
    if (args->args.op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        /* Free appropriate things for each operation */
        switch (args->args.op_type) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
            case H5VL_NATIVE_GROUP_ITERATE_OLD:
                free_loc_param(&args->opt_args.iterate_old.loc_params);
                break;

            case H5VL_NATIVE_GROUP_GET_OBJINFO:
                free_loc_param(&args->opt_args.get_objinfo.loc_params);
                break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

            default:
                assert(0 && "unknown operation");
        }
    }
}

static void
dup_link_create_args(H5VL_link_create_args_t *dst_args, const H5VL_link_create_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each link type */
    switch (src_args->op_type) {
        case H5VL_LINK_CREATE_HARD:
            dst_args->args.hard.curr_obj = ((H5VL_async_t *)src_args->args.hard.curr_obj)->under_object;
            dup_loc_param(&dst_args->args.hard.curr_loc_params, &src_args->args.hard.curr_loc_params);
            break;

        case H5VL_LINK_CREATE_SOFT:
            dst_args->args.soft.target = strdup(src_args->args.soft.target);
            break;

        case H5VL_LINK_CREATE_UD:
            /* Note: if anything in the buffer needs to be deep copied, this
             *          will probably break.
             */
            dst_args->args.ud.buf = malloc(src_args->args.ud.buf_size);
            memcpy((void *)dst_args->args.ud.buf, src_args->args.ud.buf, src_args->args.ud.buf_size);
            break;

        default:
            assert(0 && "unknown link creation type");
    }
}

static void
free_link_create_args(H5VL_link_create_args_t *args)
{
    assert(args);

    /* Free appropriate things for each link type */
    switch (args->op_type) {
        case H5VL_LINK_CREATE_HARD:
            free_loc_param(&args->args.hard.curr_loc_params);
            break;

        case H5VL_LINK_CREATE_SOFT:
            free((void *)args->args.soft.target);
            break;

        case H5VL_LINK_CREATE_UD:
            free((void *)args->args.ud.buf);
            break;

        default:
            assert(0 && "unknown link creation type");
    }
}

static void
dup_link_get_args(H5VL_link_get_args_t *dst_args, const H5VL_link_get_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_LINK_GET_INFO:
        case H5VL_LINK_GET_NAME:
        case H5VL_LINK_GET_VAL:
            /* No items to deep copy */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
free_link_get_args(H5VL_link_get_args_t *args)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_LINK_GET_INFO:
        case H5VL_LINK_GET_NAME:
        case H5VL_LINK_GET_VAL:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
dup_link_spec_args(H5VL_link_specific_args_t *dst_args, const H5VL_link_specific_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_LINK_DELETE:
        case H5VL_LINK_EXISTS:
        case H5VL_LINK_ITER:
            /* No items to deep copy */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
free_link_spec_args(H5VL_link_specific_args_t *args)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_LINK_DELETE:
        case H5VL_LINK_EXISTS:
        case H5VL_LINK_ITER:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
dup_native_link_optional_args(async_link_optional_args_t *dst_args, const H5VL_optional_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(&dst_args->args, src_args, sizeof(dst_args->args));

    /* Duplicate native operation info */
    if (src_args->op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
#ifdef NOT_YET
        if (src_args->args) {
            memcpy(&dst_args->opt_args, src_args->args, sizeof(dst_args->opt_args));
            dst_args->args.args = &dst_args->opt_args;
        } /* end if */

        /* Deep copy appropriate things for each operation */
        switch (src_args->op_type) {
            default:
                assert(0 && "unknown operation");
        }
#endif /* NOT_YET */
    }
}

static void
free_native_link_optional_args(async_link_optional_args_t *args)
{
    assert(args);

    /* Free native operation info */
    if (args->args.op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
#ifdef NOT_YET
        /* Free appropriate things for each operation */
        switch (args->args.op_type) {
            default:
                assert(0 && "unknown operation");
        }
#endif /* NOT_YET */
    }
}

static void
dup_object_get_args(H5VL_object_get_args_t *dst_args, const H5VL_object_get_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_OBJECT_GET_FILE:
        case H5VL_OBJECT_GET_NAME:
        case H5VL_OBJECT_GET_TYPE:
        case H5VL_OBJECT_GET_INFO:
            /* No items to deep copy */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
free_object_get_args(H5VL_object_get_args_t *args)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_OBJECT_GET_FILE:
        case H5VL_OBJECT_GET_NAME:
        case H5VL_OBJECT_GET_TYPE:
        case H5VL_OBJECT_GET_INFO:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
dup_object_spec_args(H5VL_object_specific_args_t *dst_args, const H5VL_object_specific_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(dst_args, src_args, sizeof(*dst_args));

    /* Deep copy appropriate things for each operation */
    switch (src_args->op_type) {
        case H5VL_OBJECT_FLUSH:
            H5Iinc_ref(dst_args->args.flush.obj_id);
            break;

        case H5VL_OBJECT_REFRESH:
            H5Iinc_ref(dst_args->args.refresh.obj_id);
            break;

        case H5VL_OBJECT_CHANGE_REF_COUNT:
        case H5VL_OBJECT_EXISTS:
        case H5VL_OBJECT_LOOKUP:
        case H5VL_OBJECT_VISIT:
            /* No items to deep copy */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
free_object_spec_args(H5VL_object_specific_args_t *args)
{
    assert(args);

    /* Free appropriate things for each operation */
    switch (args->op_type) {
        case H5VL_OBJECT_FLUSH:
            H5Idec_ref(args->args.flush.obj_id);
            break;

        case H5VL_OBJECT_REFRESH:
            H5Idec_ref(args->args.refresh.obj_id);
            break;

        case H5VL_OBJECT_CHANGE_REF_COUNT:
        case H5VL_OBJECT_EXISTS:
        case H5VL_OBJECT_LOOKUP:
        case H5VL_OBJECT_VISIT:
            /* No items to free */
            break;

        default:
            assert(0 && "unknown operation");
    }
}

static void
dup_native_object_optional_args(async_object_optional_args_t *dst_args, const H5VL_optional_args_t *src_args)
{
    assert(dst_args);
    assert(src_args);

    /* Shallow copy everything */
    memcpy(&dst_args->args, src_args, sizeof(dst_args->args));

    /* Duplicate native operation info */
    if (src_args->op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        if (src_args->args) {
            memcpy(&dst_args->opt_args, src_args->args, sizeof(dst_args->opt_args));
            dst_args->args.args = &dst_args->opt_args;
        } /* end if */

        /* Deep copy appropriate things for each operation */
        switch (src_args->op_type) {
            case H5VL_NATIVE_OBJECT_SET_COMMENT:
                if (((H5VL_native_object_optional_args_t *)src_args->args)->set_comment.comment)
                    dst_args->opt_args.set_comment.comment =
                        strdup(((H5VL_native_object_optional_args_t *)src_args->args)->set_comment.comment);
                break;

            case H5VL_NATIVE_OBJECT_GET_COMMENT:
            case H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES:
            case H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES:
            case H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED:
            case H5VL_NATIVE_OBJECT_GET_NATIVE_INFO:
                /* No items to deep copy */
                break;

            default:
                assert(0 && "unknown operation");
        }
    }
}

static void
free_native_object_optional_args(async_object_optional_args_t *args)
{
    assert(args);

    /* Free native operation info */
    if (args->args.op_type < H5VL_RESERVED_NATIVE_OPTIONAL) {
        /* Free appropriate things for each operation */
        switch (args->args.op_type) {
            case H5VL_NATIVE_OBJECT_SET_COMMENT:
                free((void *)args->opt_args.set_comment.comment);
                break;

            case H5VL_NATIVE_OBJECT_GET_COMMENT:
            case H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES:
            case H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES:
            case H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED:
            case H5VL_NATIVE_OBJECT_GET_NATIVE_INFO:
                /* No items to free */
                break;

            default:
                assert(0 && "unknown operation");
        }
    }
}

static void
async_attr_create_fn(void *foo)
{
    void *                    obj;
    hbool_t                   acquired              = false;
    unsigned int              mutex_count           = 1;
    int                       attempt_count         = 0;
    int                       is_lock               = 0;
    hbool_t                   is_lib_state_restored = false;
    ABT_pool *                pool_ptr;
    async_task_t *            task = (async_task_t *)foo;
    async_attr_create_args_t *args = (async_attr_create_args_t *)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLattr_create(args->obj, args->loc_params, task->under_vol_id, args->name, args->type_id,
                              args->space_id, args->acpl_id, args->aapl_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free(args->name);
    args->name = NULL;
    if (args->type_id > 0)
        H5Tclose(args->type_id);
    if (args->space_id > 0)
        H5Sclose(args->space_id);
    if (args->acpl_id > 0)
        H5Pclose(args->acpl_id);
    if (args->aapl_id > 0)
        H5Pclose(args->aapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_create_fn

static H5VL_async_t *
async_attr_create(async_instance_t *aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params,
                  const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id,
                  hid_t dxpl_id, void **req)
{
    H5VL_async_t *            async_obj   = NULL;
    async_task_t *            async_task  = NULL;
    async_attr_create_args_t *args        = NULL;
    bool                      lock_parent = false;
    bool                      is_blocking = false;
    hbool_t                   acquired    = false;
    unsigned int              mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_attr_create_args_t *)calloc(1, sizeof(async_attr_create_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;

#ifdef ENABLE_WRITE_MEMCPY
    async_obj->data_size = H5Sget_select_npoints(space_id);
    async_obj->data_size *= H5Tget_size(type_id);
    if (async_obj->data_size > async_instance_g->max_attr_size)
        async_instance_g->max_attr_size = async_obj->data_size;
#endif

    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if (type_id > 0)
        args->type_id = H5Tcopy(type_id);
    if (space_id > 0)
        args->space_id = H5Scopy(space_id);
    if (acpl_id > 0)
        args->acpl_id = H5Pcopy(acpl_id);
    if (aapl_id > 0)
        args->aapl_id = H5Pcopy(aapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        /* fprintf(fout_g,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__); */
        H5VL_async_free_obj(async_obj);
        free_async_task(async_task);
        async_obj = NULL;
        goto done;
    }

    async_task->func         = async_attr_create_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = async_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0) {
            async_obj = NULL;
            goto error;
        }
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    void *                  obj;
    hbool_t                 acquired              = false;
    unsigned int            mutex_count           = 1;
    int                     attempt_count         = 0;
    int                     is_lock               = 0;
    hbool_t                 is_lib_state_restored = false;
    ABT_pool *              pool_ptr;
    async_task_t *          task = (async_task_t *)foo;
    async_attr_open_args_t *args = (async_attr_open_args_t *)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLattr_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->aapl_id,
                            args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free(args->name);
    args->name = NULL;
    if (args->aapl_id > 0)
        H5Pclose(args->aapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_open_fn

static H5VL_async_t *
async_attr_open(async_instance_t *aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params,
                const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *          async_obj   = NULL;
    async_task_t *          async_task  = NULL;
    async_attr_open_args_t *args        = NULL;
    bool                    lock_parent = false;
    bool                    is_blocking = false;
    hbool_t                 acquired    = false;
    unsigned int            mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_attr_open_args_t *)calloc(1, sizeof(async_attr_open_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if (aapl_id > 0)
        args->aapl_id = H5Pcopy(aapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_attr_open_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = async_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                 acquired              = false;
    unsigned int            mutex_count           = 1;
    int                     attempt_count         = 0;
    int                     is_lock               = 0;
    hbool_t                 is_lib_state_restored = false;
    ABT_pool *              pool_ptr;
    async_task_t *          task = (async_task_t *)foo;
    async_attr_read_args_t *args = (async_attr_read_args_t *)(task->args);
    herr_t                  status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->attr) {
        if (NULL != task->parent_obj->under_object) {
            args->attr = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status =
            H5VLattr_read(args->attr, task->under_vol_id, args->mem_type_id, args->buf, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (args->mem_type_id > 0)
        H5Tclose(args->mem_type_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_read_fn

static herr_t
async_attr_read(async_instance_t *aid, H5VL_async_t *parent_obj, hid_t mem_type_id, void *buf, hid_t dxpl_id,
                void **req)
{
    // For implicit mode (env var), make all read to be blocking
    assert(async_instance_g);
    async_task_t *          async_task  = NULL;
    async_attr_read_args_t *args        = NULL;
    bool                    lock_parent = false;
    bool                    is_blocking = false;
    hbool_t                 acquired    = false;
    unsigned int            mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_attr_read_args_t *)calloc(1, sizeof(async_attr_read_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

    args->attr = parent_obj->under_object;
    if (mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    args->buf = buf;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;
#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_attr_read_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                  acquired              = false;
    unsigned int             mutex_count           = 1;
    int                      attempt_count         = 0;
    int                      is_lock               = 0;
    hbool_t                  is_lib_state_restored = false;
    ABT_pool *               pool_ptr;
    async_task_t *           task = (async_task_t *)foo;
    async_attr_write_args_t *args = (async_attr_write_args_t *)(task->args);
    herr_t                   status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->attr) {
        if (NULL != task->parent_obj->under_object) {
            args->attr = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status =
            H5VLattr_write(args->attr, task->under_vol_id, args->mem_type_id, args->buf, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (args->mem_type_id > 0)
        H5Tclose(args->mem_type_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);

#ifdef ENABLE_WRITE_MEMCPY
    if (args->free_buf && args->buf) {
        free(args->buf);
        async_instance_g->used_mem -= args->data_size;
    }
#endif

#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_write_fn

static herr_t
async_attr_write(async_instance_t *aid, H5VL_async_t *parent_obj, hid_t mem_type_id, const void *buf,
                 hid_t dxpl_id, void **req)
{
    async_task_t *           async_task  = NULL;
    async_attr_write_args_t *args        = NULL;
    bool                     lock_parent = false;
    bool                     is_blocking = false;
    hbool_t                  acquired    = false;
    unsigned int             mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_attr_write_args_t *)calloc(1, sizeof(async_attr_write_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

    args->attr = parent_obj->under_object;
    if (mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

#ifdef ENABLE_WRITE_MEMCPY
    if (parent_obj->data_size == 0) {
        if (async_instance_g->max_attr_size > 0) {
            parent_obj->data_size = async_instance_g->max_attr_size;
        }
        else
            parent_obj->data_size = ASYNC_ATTR_MEMCPY_SIZE;
    }
    if (NULL == (args->buf = malloc(parent_obj->data_size))) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s malloc failed!\n", __func__);
        goto done;
    }
    async_instance_g->used_mem += parent_obj->data_size;
    args->data_size = parent_obj->data_size;
    args->free_buf  = true;
    /* fprintf(fout_g, "  [ASYNC VOL DBG] %s attr write size %lu\n", __func__, parent_obj->data_size); */
    memcpy(args->buf, buf, parent_obj->data_size);
#else
    args->buf = (void *)buf;
#endif

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_attr_write_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                acquired              = false;
    unsigned int           mutex_count           = 1;
    int                    attempt_count         = 0;
    int                    is_lock               = 0;
    hbool_t                is_lib_state_restored = false;
    ABT_pool *             pool_ptr;
    async_task_t *         task = (async_task_t *)foo;
    async_attr_get_args_t *args = (async_attr_get_args_t *)(task->args);
    herr_t                 status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLattr_get(args->obj, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_attr_get_args(&args->args, task);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_get_fn

static herr_t
async_attr_get(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
               H5VL_attr_get_args_t *get_args, hid_t dxpl_id, void **req)
{
    async_task_t *         async_task  = NULL;
    async_attr_get_args_t *args        = NULL;
    bool                   lock_parent = false;
    bool                   is_blocking = false;
    hbool_t                acquired    = false;
    unsigned int           mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_attr_get_args_t *)calloc(1, sizeof(async_attr_get_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj = parent_obj->under_object;
    if (dup_attr_get_args(&args->args, get_args, async_task) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with duplicating attribute get arguments\n", __func__);
        goto error;
    }
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_attr_get_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                     acquired              = false;
    unsigned int                mutex_count           = 1;
    int                         attempt_count         = 0;
    int                         is_lock               = 0;
    hbool_t                     is_lib_state_restored = false;
    ABT_pool *                  pool_ptr;
    async_task_t *              task = (async_task_t *)foo;
    async_attr_specific_args_t *args = (async_attr_specific_args_t *)(task->args);
    herr_t                      status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    if (args->args.op_type != H5VL_ATTR_ITER) {
        while (1) {
            if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
                is_lock = 1;
                break;
            }
            else {
                fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
                break;
            }
            usleep(1000);
        }
    }

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLattr_specific(args->obj, args->loc_params, task->under_vol_id, &args->args,
                                   args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free_attr_spec_args(&args->args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_specific_fn

static herr_t
async_attr_specific(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                    const H5VL_loc_params_t *loc_params, H5VL_attr_specific_args_t *spec_args, hid_t dxpl_id,
                    void **req)
{
    async_task_t *              async_task  = NULL;
    async_attr_specific_args_t *args        = NULL;
    bool                        lock_parent = false;
    bool                        is_blocking = false;
    hbool_t                     acquired    = false;
    unsigned int                mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if (qtype == BLOCKING) {
        async_instance_g->start_abt_push = true;
        is_blocking                      = true;
    }

    if ((args = (async_attr_specific_args_t *)calloc(1, sizeof(async_attr_specific_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }

    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    dup_attr_spec_args(&args->args, spec_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_attr_specific_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                     acquired              = false;
    unsigned int                mutex_count           = 1;
    int                         attempt_count         = 0;
    int                         is_lock               = 0;
    hbool_t                     is_lib_state_restored = false;
    ABT_pool *                  pool_ptr;
    async_task_t *              task = (async_task_t *)foo;
    async_attr_optional_args_t *args = (async_attr_optional_args_t *)(task->args);
    herr_t                      status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLattr_optional(args->obj, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_native_attr_optional_args(args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_optional_fn

static herr_t
async_attr_optional(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                    H5VL_optional_args_t *opt_args, hid_t dxpl_id, void **req)
{
    async_task_t *              async_task  = NULL;
    async_attr_optional_args_t *args        = NULL;
    bool                        lock_parent = false;
    bool                        is_blocking = false;
    hbool_t                     acquired    = false;
    unsigned int                mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_attr_optional_args_t *)calloc(1, sizeof(async_attr_optional_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj = parent_obj->under_object;
    dup_native_attr_optional_args(args, opt_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_attr_optional_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                  acquired              = false;
    unsigned int             mutex_count           = 1;
    int                      attempt_count         = 0;
    int                      is_lock               = 0;
    hbool_t                  is_lib_state_restored = false;
    ABT_pool *               pool_ptr;
    async_task_t *           task = (async_task_t *)foo;
    async_attr_close_args_t *args = (async_attr_close_args_t *)(task->args);
    herr_t                   status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->attr) {
        if (NULL != task->parent_obj->under_object) {
            args->attr = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLattr_close(args->attr, task->under_vol_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_attr_close_fn

static herr_t
async_attr_close(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj, hid_t dxpl_id,
                 void **req)
{
    async_task_t *           async_task  = NULL;
    async_attr_close_args_t *args        = NULL;
    bool                     lock_parent = false;
    bool                     is_blocking = false;
    hbool_t                  acquired    = false;
    unsigned int             mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if (qtype == BLOCKING) {
        async_instance_g->start_abt_push = true;
        is_blocking                      = true;
    }

    if ((args = (async_attr_close_args_t *)calloc(1, sizeof(async_attr_close_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->attr = parent_obj->under_object;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_attr_close_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == BLOCKING || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
        aid->start_abt_push = true;
    }
    /* else { */
    /*     if (get_n_running_task_in_queue(async_task, __func__) == 0) */
    /*         push_task_to_abt_pool(&aid->qhead, aid->pool); */
    /* } */

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    void *                       obj;
    hbool_t                      acquired              = false;
    unsigned int                 mutex_count           = 1;
    int                          attempt_count         = 0;
    int                          is_lock               = 0;
    hbool_t                      is_lib_state_restored = false;
    ABT_pool *                   pool_ptr;
    async_task_t *               task = (async_task_t *)foo;
    async_dataset_create_args_t *args = (async_dataset_create_args_t *)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLdataset_create(args->obj, args->loc_params, task->under_vol_id, args->name, args->lcpl_id,
                                 args->type_id, args->space_id, args->dcpl_id, args->dapl_id, args->dxpl_id,
                                 NULL);
        check_app_wait(attempt_count + 3, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free(args->name);
    args->name = NULL;
    if (args->lcpl_id > 0)
        H5Pclose(args->lcpl_id);
    if (args->type_id > 0)
        H5Tclose(args->type_id);
    if (args->space_id > 0)
        H5Sclose(args->space_id);
    if (args->dcpl_id > 0)
        H5Pclose(args->dcpl_id);
    if (args->dapl_id > 0)
        H5Pclose(args->dapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_create_fn

static H5VL_async_t *
async_dataset_create(async_instance_t *aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params,
                     const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id,
                     hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *               async_obj   = NULL;
    async_task_t *               async_task  = NULL;
    async_dataset_create_args_t *args        = NULL;
    bool                         lock_parent = false;
    bool                         is_blocking = false;
    hbool_t                      acquired    = false;
    unsigned int                 mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_dataset_create_args_t *)calloc(1, sizeof(async_dataset_create_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj = parent_obj->under_object;
    if (sizeof(*loc_params) > 0) {
        args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
        dup_loc_param(args->loc_params, loc_params);
    }
    else
        args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(H5VL_loc_params_t));
    if (NULL != name)
        args->name = strdup(name);
    if (lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if (type_id > 0)
        args->type_id = H5Tcopy(type_id);
    if (space_id > 0)
        args->space_id = H5Scopy(space_id);
    if (dcpl_id > 0)
        args->dcpl_id = H5Pcopy(dcpl_id);
    if (dapl_id > 0)
        args->dapl_id = H5Pcopy(dapl_id);
    else
        goto error;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        /* fprintf(fout_g,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__); */
        H5VL_async_free_obj(async_obj);
        free_async_task(async_task);
        async_obj = NULL;
        goto done;
    }

    async_task->func         = async_dataset_create_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = async_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

#ifdef ENABLE_WRITE_MEMCPY
    async_obj->data_size = H5Sget_select_npoints(space_id) * H5Tget_size(type_id);
#ifdef ENABLE_DBG_MSG
    if (async_obj->data_size == 0)
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with getting dataset size\n", __func__);
#endif
#endif

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    void *                     obj;
    hbool_t                    acquired              = false;
    unsigned int               mutex_count           = 1;
    int                        attempt_count         = 0;
    int                        is_lock               = 0;
    hbool_t                    is_lib_state_restored = false;
    ABT_pool *                 pool_ptr;
    async_task_t *             task = (async_task_t *)foo;
    async_dataset_open_args_t *args = (async_dataset_open_args_t *)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLdataset_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->dapl_id,
                               args->dxpl_id, NULL);
        check_app_wait(attempt_count + 3, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC ABT DBG] %s: failed!\n", __func__);
#endif
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free(args->name);
    args->name = NULL;
    if (args->dapl_id > 0)
        H5Pclose(args->dapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_open_fn

static H5VL_async_t *
async_dataset_open(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                   const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id,
                   void **req)
{
    H5VL_async_t *             async_obj   = NULL;
    async_task_t *             async_task  = NULL;
    async_dataset_open_args_t *args        = NULL;
    bool                       lock_parent = false;
    bool                       is_blocking = false;
    hbool_t                    acquired    = false;
    unsigned int               mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_dataset_open_args_t *)calloc(1, sizeof(async_dataset_open_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if (dapl_id > 0)
        args->dapl_id = H5Pcopy(dapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_dataset_open_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = async_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
            goto done;
        }
        /* Insert it into the file task list */
        DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
        if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                    acquired              = false;
    unsigned int               mutex_count           = 1;
    int                        attempt_count         = 0;
    int                        is_lock               = 0;
    hbool_t                    is_lib_state_restored = false;
    ABT_pool *                 pool_ptr;
    async_task_t *             task = (async_task_t *)foo;
    async_dataset_read_args_t *args = (async_dataset_read_args_t *)(task->args);
    herr_t                     status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dset) {
        if (NULL != task->parent_obj->under_object) {
            args->dset = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdataset_read(args->dset, task->under_vol_id, args->mem_type_id, args->mem_space_id,
                                  args->file_space_id, args->plist_id, args->buf, NULL);
        check_app_wait(attempt_count + 4, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (args->mem_type_id > 0)
        H5Tclose(args->mem_type_id);
    if (args->mem_space_id > 0)
        H5Sclose(args->mem_space_id);
    if (args->file_space_id > 0)
        H5Sclose(args->file_space_id);
    if (args->plist_id > 0)
        H5Pclose(args->plist_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_read_fn

static herr_t
async_dataset_read(async_instance_t *aid, H5VL_async_t *parent_obj, hid_t mem_type_id, hid_t mem_space_id,
                   hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    // For implicit mode (env var), make all read to be blocking
    assert(async_instance_g);
    async_task_t *             async_task  = NULL;
    async_dataset_read_args_t *args        = NULL;
    bool                       lock_parent = false;
    bool                       is_blocking = false;
    hbool_t                    acquired    = false;
    unsigned int               mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_dataset_read_args_t *)calloc(1, sizeof(async_dataset_read_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dset = parent_obj->under_object;
    if (mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    if (mem_space_id > 0)
        args->mem_space_id = H5Scopy(mem_space_id);
    if (file_space_id > 0)
        args->file_space_id = H5Scopy(file_space_id);
    if (plist_id > 0)
        args->plist_id = H5Pcopy(plist_id);
    args->buf = buf;
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_dataset_read_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                     acquired      = false;
    unsigned int                mutex_count   = 1;
    int                         attempt_count = 0;
    int                         is_lock = 0, count = 0;
    hbool_t                     is_lib_state_restored = false;
    ABT_pool *                  pool_ptr;
    async_task_t *              task = (async_task_t *)foo;
    async_dataset_write_args_t *args = (async_dataset_write_args_t *)(task->args);
    herr_t                      status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dset) {
        if (NULL != task->parent_obj->under_object) {
            args->dset = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (count > 10000) {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s cannot acquire object lock in 10s!\n", __func__);
            break;
        }

        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            is_lock = 1;
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
        count++;
    }

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdataset_write(args->dset, task->under_vol_id, args->mem_type_id, args->mem_space_id,
                                   args->file_space_id, args->plist_id, args->buf, NULL);
        check_app_wait(attempt_count + 4, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s failed\n", __func__);
#ifdef PRINT_ERROR_STACK
        H5Eprint2(task->err_stack, stderr);
#endif
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (args->mem_type_id > 0)
        H5Tclose(args->mem_type_id);
    if (args->mem_space_id > 0)
        H5Sclose(args->mem_space_id);
    if (args->file_space_id > 0)
        H5Sclose(args->file_space_id);
    if (args->plist_id > 0)
        H5Pclose(args->plist_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);

#ifdef ENABLE_WRITE_MEMCPY
    if (args->free_buf && args->buf) {
        free(args->buf);
        async_instance_g->used_mem -= args->data_size;
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC ABT DBG] %s released dset memcpy\n", __func__);
#endif
    }
#endif

#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_write_fn

#ifdef ENABLE_WRITE_MEMCPY
int
is_contig_memspace(hid_t memspace)
{
    hsize_t      nblocks;
    int          ndim;
    H5S_sel_type type;

    if (memspace == H5S_SEL_ALL || memspace == H5S_SEL_NONE) {
        return 1;
    }

    type = H5Sget_select_type(memspace);
    if (type == H5S_SEL_POINTS) {
        return 0;
    }
    else if (type == H5S_SEL_HYPERSLABS) {
        ndim = H5Sget_simple_extent_ndims(memspace);
        if (ndim != 1)
            return 0;

        nblocks = H5Sget_select_hyper_nblocks(memspace);
        if (nblocks == 1)
            return 1;
        return 0;
    }
    return 0;
}
#endif

static herr_t
async_dataset_write(async_instance_t *aid, H5VL_async_t *parent_obj, hid_t mem_type_id, hid_t mem_space_id,
                    hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    async_task_t *              async_task  = NULL;
    async_dataset_write_args_t *args        = NULL;
    bool                        lock_parent = false;
    bool                        is_blocking = false;
    hbool_t                     acquired    = false;
    unsigned int                mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_dataset_write_args_t *)calloc(1, sizeof(async_dataset_write_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dset = parent_obj->under_object;
    if (mem_type_id > 0)
        args->mem_type_id = H5Tcopy(mem_type_id);
    if (mem_space_id > 0)
        args->mem_space_id = H5Scopy(mem_space_id);
    if (file_space_id > 0)
        args->file_space_id = H5Scopy(file_space_id);
    if (plist_id > 0)
        args->plist_id = H5Pcopy(plist_id);
    args->buf = (void *)buf;
    args->req = req;

#ifdef ENABLE_WRITE_MEMCPY
    hsize_t buf_size = 0;
    if (parent_obj->data_size > 0 && args->file_space_id == H5S_ALL) {
        buf_size = parent_obj->data_size;
    }
    else {
        buf_size = H5Tget_size(mem_type_id) * H5Sget_select_npoints(mem_space_id);
#ifdef ENABLE_DBG_MSG
        if (buf_size == 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with getting dataset size\n", __func__);
#endif
    }

    /* fprintf(fout_g, "buf size = %llu\n", buf_size); */

    // Get available system memory
    hsize_t avail_mem = (hsize_t)get_avphys_pages() * sysconf(_SC_PAGESIZE);

    if (async_instance_g->used_mem + buf_size > async_instance_g->max_mem) {
        is_blocking = true;
        args->buf   = (void *)buf;
        fprintf(fout_g,
                "  [ASYNC ABT INFO] %d write size %lu larger than async memory limit %lu, switch to "
                "synchronous write\n",
                async_instance_g->mpi_rank, buf_size, async_instance_g->max_mem);
    }
    else if (buf_size > avail_mem) {
        is_blocking = true;
        args->buf   = (void *)buf;
        fprintf(fout_g,
                "  [ASYNC ABT INFO] %d write size %lu larger than available memory %lu, switch to "
                "synchronous write\n",
                async_instance_g->mpi_rank, buf_size, avail_mem);
    }
    else if (buf_size > 0) {
        if (NULL == (args->buf = malloc(buf_size))) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s malloc failed!\n", __func__);
            goto done;
        }
        async_instance_g->used_mem += buf_size;
        args->free_buf  = true;
        args->data_size = buf_size;

        // If is contiguous space, no need to go through gather process as it can be costly
        if (1 != is_contig_memspace(mem_space_id)) {
            /* fprintf(fout_g,"  [ASYNC VOL LOG] %s will gather!\n", __func__); */
            H5Dgather(mem_space_id, buf, mem_type_id, buf_size, args->buf, NULL, NULL);
            hsize_t elem_size = H5Tget_size(mem_type_id);
            if (elem_size == 0)
                elem_size = 1;
            hsize_t n_elem = (hsize_t)(buf_size / elem_size);
            if (args->mem_space_id > 0)
                H5Sclose(args->mem_space_id);
            args->mem_space_id = H5Screate_simple(1, &n_elem, NULL);
        }
        else {
            memcpy(args->buf, buf, buf_size);
        }
    }
#endif

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_dataset_write_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;

error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                   acquired              = false;
    unsigned int              mutex_count           = 1;
    int                       attempt_count         = 0;
    int                       is_lock               = 0;
    hbool_t                   is_lib_state_restored = false;
    ABT_pool *                pool_ptr;
    async_task_t *            task = (async_task_t *)foo;
    async_dataset_get_args_t *args = (async_dataset_get_args_t *)(task->args);
    herr_t                    status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dset) {
        if (NULL != task->parent_obj->under_object) {
            args->dset = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdataset_get(args->dset, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_dataset_get_args(&args->args, task);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_get_fn

static herr_t
async_dataset_get(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                  H5VL_dataset_get_args_t *get_args, hid_t dxpl_id, void **req)
{
    async_task_t *            async_task  = NULL;
    async_dataset_get_args_t *args        = NULL;
    bool                      lock_parent = false;
    bool                      is_blocking = false;
    hbool_t                   acquired    = false;
    unsigned int              mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if (qtype == BLOCKING) {
        async_instance_g->start_abt_push = true;
        is_blocking                      = true;
    }

    if ((args = (async_dataset_get_args_t *)calloc(1, sizeof(async_dataset_get_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dset = parent_obj->under_object;
    if (dup_dataset_get_args(&args->args, get_args, async_task) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with duplicating dataset get arguments\n", __func__);
        goto error;
    }
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_dataset_get_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 0;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                        acquired              = false;
    unsigned int                   mutex_count           = 1;
    int                            attempt_count         = 0;
    int                            is_lock               = 0;
    hbool_t                        is_lib_state_restored = false;
    ABT_pool *                     pool_ptr;
    async_task_t *                 task = (async_task_t *)foo;
    async_dataset_specific_args_t *args = (async_dataset_specific_args_t *)(task->args);
    herr_t                         status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdataset_specific(args->obj, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_dataset_spec_args(&args->args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_specific_fn

static herr_t
async_dataset_specific(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                       H5VL_dataset_specific_args_t *spec_args, hid_t dxpl_id, void **req)
{
    async_task_t *                 async_task  = NULL;
    async_dataset_specific_args_t *args        = NULL;
    bool                           lock_parent = false;
    bool                           is_blocking = false;
    hbool_t                        acquired    = false;
    unsigned int                   mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if (qtype == BLOCKING) {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    if ((args = (async_dataset_specific_args_t *)calloc(1, sizeof(async_dataset_specific_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj = parent_obj->under_object;
    dup_dataset_spec_args(&args->args, spec_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_dataset_specific_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                        acquired              = false;
    unsigned int                   mutex_count           = 1;
    int                            attempt_count         = 0;
    int                            is_lock               = 0;
    hbool_t                        is_lib_state_restored = false;
    ABT_pool *                     pool_ptr;
    async_task_t *                 task = (async_task_t *)foo;
    async_dataset_optional_args_t *args = (async_dataset_optional_args_t *)(task->args);
    herr_t                         status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdataset_optional(args->obj, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_native_dataset_optional_args(args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_optional_fn

static herr_t
async_dataset_optional(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                       H5VL_optional_args_t *opt_args, hid_t dxpl_id, void **req)
{
    async_task_t *                 async_task  = NULL;
    async_dataset_optional_args_t *args        = NULL;
    bool                           lock_parent = false;
    bool                           is_blocking = false;
    hbool_t                        acquired    = false;
    unsigned int                   mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_dataset_optional_args_t *)calloc(1, sizeof(async_dataset_optional_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj = parent_obj->under_object;
    dup_native_dataset_optional_args(args, opt_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_dataset_optional_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                     acquired              = false;
    unsigned int                mutex_count           = 1;
    int                         attempt_count         = 0;
    int                         is_lock               = 0;
    hbool_t                     is_lib_state_restored = false;
    ABT_pool *                  pool_ptr;
    async_task_t *              task = (async_task_t *)foo;
    async_dataset_close_args_t *args = (async_dataset_close_args_t *)(task->args);
    herr_t                      status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dset) {
        if (NULL != task->parent_obj->under_object) {
            args->dset = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdataset_close(args->dset, task->under_vol_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count + 3, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }

    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_dataset_close_fn

static herr_t
async_dataset_close(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj, hid_t dxpl_id,
                    void **req)
{
    async_task_t *              async_task  = NULL;
    async_dataset_close_args_t *args        = NULL;
    bool                        lock_parent = false;
    bool                        is_blocking = false;
    hbool_t                     acquired    = false;
    unsigned int                mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if (qtype == BLOCKING) {
        async_instance_g->start_abt_push = true;
        is_blocking                      = true;
    }

    if ((args = (async_dataset_close_args_t *)calloc(1, sizeof(async_dataset_close_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dset = parent_obj->under_object;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_dataset_close_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
            goto done;
        }
        /* Insert it into the file task list */
        DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
        if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
    }

    if (lock_parent && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
        aid->start_abt_push = true;
    }
    else {
        if (aid->ex_dclose) {
            if (get_n_running_task_in_queue(async_task, __func__) == 0)
                push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
            aid->start_abt_push = true;
        }
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;

error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                       acquired              = false;
    unsigned int                  mutex_count           = 1;
    int                           attempt_count         = 0;
    int                           is_lock               = 0;
    hbool_t                       is_lib_state_restored = false;
    ABT_pool *                    pool_ptr;
    async_task_t *                task = (async_task_t *)foo;
    async_datatype_commit_args_t *args = (async_datatype_commit_args_t *)(task->args);
    void *                        under_obj;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        under_obj =
            H5VLdatatype_commit(args->obj, args->loc_params, task->under_vol_id, args->name, args->type_id,
                                args->lcpl_id, args->tcpl_id, args->tapl_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (NULL == under_obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }
    task->async_obj->under_object = under_obj;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free(args->name);
    args->name = NULL;
    if (args->type_id > 0)
        H5Tclose(args->type_id);
    if (args->lcpl_id > 0)
        H5Pclose(args->lcpl_id);
    if (args->tcpl_id > 0)
        H5Pclose(args->tcpl_id);
    if (args->tapl_id > 0)
        H5Pclose(args->tapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_commit_fn

static H5VL_async_t *
async_datatype_commit(async_instance_t *aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params,
                      const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id,
                      hid_t dxpl_id, void **req)
{
    H5VL_async_t *                async_obj   = NULL;
    async_task_t *                async_task  = NULL;
    async_datatype_commit_args_t *args        = NULL;
    bool                          lock_parent = false;
    bool                          is_blocking = false;
    hbool_t                       acquired    = false;
    unsigned int                  mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_datatype_commit_args_t *)calloc(1, sizeof(async_datatype_commit_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if (type_id > 0)
        args->type_id = H5Tcopy(type_id);
    if (lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if (tcpl_id > 0)
        args->tcpl_id = H5Pcopy(tcpl_id);
    if (tapl_id > 0)
        args->tapl_id = H5Pcopy(tapl_id);
    else
        goto error;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_datatype_commit_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = async_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    void *                      obj;
    hbool_t                     acquired              = false;
    unsigned int                mutex_count           = 1;
    int                         attempt_count         = 0;
    int                         is_lock               = 0;
    hbool_t                     is_lib_state_restored = false;
    ABT_pool *                  pool_ptr;
    async_task_t *              task = (async_task_t *)foo;
    async_datatype_open_args_t *args = (async_datatype_open_args_t *)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLdatatype_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->tapl_id,
                                args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free(args->name);
    args->name = NULL;
    if (args->tapl_id > 0)
        H5Pclose(args->tapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_open_fn

static H5VL_async_t *
async_datatype_open(async_instance_t *aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params,
                    const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *              async_obj   = NULL;
    async_task_t *              async_task  = NULL;
    async_datatype_open_args_t *args        = NULL;
    bool                        lock_parent = false;
    bool                        is_blocking = false;
    hbool_t                     acquired    = false;
    unsigned int                mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_datatype_open_args_t *)calloc(1, sizeof(async_datatype_open_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if (tapl_id > 0)
        args->tapl_id = H5Pcopy(tapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_datatype_open_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = async_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                    acquired              = false;
    unsigned int               mutex_count           = 1;
    int                        attempt_count         = 0;
    int                        is_lock               = 0;
    hbool_t                    is_lib_state_restored = false;
    ABT_pool *                 pool_ptr;
    async_task_t *             task = (async_task_t *)foo;
    async_datatype_get_args_t *args = (async_datatype_get_args_t *)(task->args);
    herr_t                     status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dt) {
        if (NULL != task->parent_obj->under_object) {
            args->dt = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdatatype_get(args->dt, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_datatype_get_args(&args->args, task);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_get_fn

static herr_t
async_datatype_get(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                   H5VL_datatype_get_args_t *get_args, hid_t dxpl_id, void **req)
{
    async_task_t *             async_task  = NULL;
    async_datatype_get_args_t *args        = NULL;
    bool                       lock_parent = false;
    bool                       is_blocking = false;
    hbool_t                    acquired    = false;
    unsigned int               mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_datatype_get_args_t *)calloc(1, sizeof(async_datatype_get_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dt = parent_obj->under_object;
    if (dup_datatype_get_args(&args->args, get_args, async_task) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with duplicating datatype get arguments\n", __func__);
        goto error;
    }
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_datatype_get_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                         acquired              = false;
    unsigned int                    mutex_count           = 1;
    int                             attempt_count         = 0;
    int                             is_lock               = 0;
    hbool_t                         is_lib_state_restored = false;
    ABT_pool *                      pool_ptr;
    async_task_t *                  task = (async_task_t *)foo;
    async_datatype_specific_args_t *args = (async_datatype_specific_args_t *)(task->args);
    herr_t                          status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdatatype_specific(args->obj, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_datatype_spec_args(&args->args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_specific_fn

static herr_t
async_datatype_specific(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                        H5VL_datatype_specific_args_t *spec_args, hid_t dxpl_id, void **req)
{
    async_task_t *                  async_task  = NULL;
    async_datatype_specific_args_t *args        = NULL;
    bool                            lock_parent = false;
    bool                            is_blocking = false;
    hbool_t                         acquired    = false;
    unsigned int                    mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_datatype_specific_args_t *)calloc(1, sizeof(async_datatype_specific_args_t))) ==
        NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj = parent_obj->under_object;
    dup_datatype_spec_args(&args->args, spec_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_datatype_specific_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                         acquired              = false;
    unsigned int                    mutex_count           = 1;
    int                             attempt_count         = 0;
    int                             is_lock               = 0;
    hbool_t                         is_lib_state_restored = false;
    ABT_pool *                      pool_ptr;
    async_task_t *                  task = (async_task_t *)foo;
    async_datatype_optional_args_t *args = (async_datatype_optional_args_t *)(task->args);
    herr_t                          status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdatatype_optional(args->obj, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_native_datatype_optional_args(args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_optional_fn

static herr_t
async_datatype_optional(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                        H5VL_optional_args_t *opt_args, hid_t dxpl_id, void **req)
{
    async_task_t *                  async_task  = NULL;
    async_datatype_optional_args_t *args        = NULL;
    bool                            lock_parent = false;
    bool                            is_blocking = false;
    hbool_t                         acquired    = false;
    unsigned int                    mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_datatype_optional_args_t *)calloc(1, sizeof(async_datatype_optional_args_t))) ==
        NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj = parent_obj->under_object;
    dup_native_datatype_optional_args(args, opt_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_datatype_optional_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                      acquired              = false;
    unsigned int                 mutex_count           = 1;
    int                          attempt_count         = 0;
    int                          is_lock               = 0;
    hbool_t                      is_lib_state_restored = false;
    ABT_pool *                   pool_ptr;
    async_task_t *               task = (async_task_t *)foo;
    async_datatype_close_args_t *args = (async_datatype_close_args_t *)(task->args);
    herr_t                       status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->dt) {
        if (NULL != task->parent_obj->under_object) {
            args->dt = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLdatatype_close(args->dt, task->under_vol_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_datatype_close_fn

static herr_t
async_datatype_close(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj, hid_t dxpl_id,
                     void **req)
{
    async_task_t *               async_task  = NULL;
    async_datatype_close_args_t *args        = NULL;
    bool                         lock_parent = false;
    bool                         is_blocking = false;
    hbool_t                      acquired    = false;
    unsigned int                 mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_datatype_close_args_t *)calloc(1, sizeof(async_datatype_close_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->dt = parent_obj->under_object;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_datatype_close_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    else {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    aid->start_abt_push = true;
    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    void *                    obj;
    hbool_t                   acquired              = false;
    unsigned int              mutex_count           = 1;
    int                       attempt_count         = 0;
    int                       is_lock               = 0;
    hbool_t                   is_lib_state_restored = false;
    ABT_pool *                pool_ptr;
    H5VL_async_info_t *       info = NULL;
    async_task_t *            task = (async_task_t *)foo;
    async_file_create_args_t *args = (async_file_create_args_t *)(task->args);
    /* herr_t status; */
    hid_t under_vol_id;
    /* uint64_t supported;          /1* Whether 'post open' operation is supported by VOL connector *1/ */

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* async_instance_g->start_abt_push = false; */

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Get the underlying VOL ID */
    H5Pget_vol_id(args->fapl_id, &under_vol_id);
    assert(under_vol_id);

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLfile_create(args->name, args->flags, args->fcpl_id, args->fapl_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count + 6, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    /* /1* Check for 'post open' callback *1/ */
    /* supported = 0; */
    /* if(H5VLintrospect_opt_query(obj, under_vol_id, H5VL_SUBCLS_FILE, H5VL_NATIVE_FILE_POST_OPEN,
     * &supported) < 0) { */
    /*     fprintf(fout_g,"  [ASYNC ABT ERROR] %s H5VLintrospect_opt_query failed\n", __func__); */
    /*     goto done; */
    /* } */
    /* if(supported & H5VL_OPT_QUERY_SUPPORTED) { */
    /*     /1* Make the 'post open' callback *1/ */
    /*     /1* Try executing operation, without default error stack handling *1/ */
    /*     H5E_BEGIN_TRY { */
    /*         status = H5VLfile_optional_vararg(obj, under_vol_id, H5VL_NATIVE_FILE_POST_OPEN, args->dxpl_id,
     * NULL); */
    /*     } H5E_END_TRY */
    /*     if ( status < 0 ) { */
    /*         if ((task->err_stack = H5Eget_current_stack()) < 0) */
    /*             fprintf(fout_g,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__); */
    /*         goto done; */
    /*     } */
    /* } /1* end if *1/ */

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

    // Increase file open ref count
    if (ABT_mutex_lock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] with ABT_mutex_lock\n");
        goto done;
    };
    async_instance_g->nfopen++;
    if (ABT_mutex_unlock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] with ABT_mutex_ulock\n");
        goto done;
    };

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (NULL != info)
        H5VL_async_info_free(info);
    free(args->name);
    args->name = NULL;
    if (args->fcpl_id > 0)
        H5Pclose(args->fcpl_id);
    if (args->fapl_id > 0)
        H5Pclose(args->fapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_create_fn

static H5VL_async_t *
async_file_create(async_instance_t *aid, const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id,
                  hid_t dxpl_id, void **req)
{
    hid_t                     under_vol_id;
    H5VL_async_t *            async_obj   = NULL;
    async_task_t *            async_task  = NULL;
    async_file_create_args_t *args        = NULL;
    bool                      lock_self   = false;
    bool                      is_blocking = false;
    hbool_t                   acquired    = false;
    unsigned int              mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);

    H5Pget_vol_id(fapl_id, &under_vol_id);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_file_create_args_t *)calloc(1, sizeof(async_file_create_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_async_obj = async_obj;
    if (ABT_mutex_create(&(async_obj->file_task_list_mutex)) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (async_instance_g->ex_delay)
        async_instance_g->start_abt_push = false;

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (NULL != name)
        args->name = strdup(name);
    args->flags = flags;
    if (fcpl_id > 0)
        args->fcpl_id = H5Pcopy(fcpl_id);
    if (fapl_id > 0)
        args->fapl_id = H5Pcopy(fapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);

    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task        = async_task;
        new_req->file_async_obj = async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }
    // Reset sleep time for each new file
    aid->sleep_time = ASYNC_APP_CHECK_SLEEP_TIME;

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        /* fprintf(fout_g,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__); */
        H5VL_async_free_obj(async_obj);
        free_async_task(async_task);
        async_obj = NULL;
        goto done;
    }

    async_task->func         = async_file_create_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = under_vol_id;
    async_task->async_obj    = async_obj;

    /* Lock async_obj */
    while (1) {
        if (async_obj->obj_mutex && ABT_mutex_trylock(async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else
            fprintf(fout_g, "  [ASYNC VOL DBG] %s error with try_lock\n", __func__);
        usleep(1000);
    }
    lock_self = true;

    async_obj->create_task  = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(async_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    async_obj->task_cnt++;
    async_obj->pool_ptr = &aid->pool;
#ifndef ASYNC_VOL_NO_MPI
    H5Pget_coll_metadata_write(fapl_id, &async_obj->is_col_meta);
#endif
    add_task_to_queue(&aid->qhead, async_task, REGULAR);
    if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_self = false;

    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_self) {
        if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL DBG] %s with ABT_mutex_unlock\n", __func__);
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
    void *                  obj;
    hbool_t                 acquired              = false;
    unsigned int            mutex_count           = 1;
    int                     attempt_count         = 0;
    int                     is_lock               = 0;
    hbool_t                 is_lib_state_restored = false;
    ABT_pool *              pool_ptr;
    H5VL_async_info_t *     info = NULL;
    async_task_t *          task = (async_task_t *)foo;
    async_file_open_args_t *args = (async_file_open_args_t *)(task->args);
    /* herr_t status; */
    hid_t under_vol_id;
    /* uint64_t supported;          /1* Whether 'post open' operation is supported by VOL connector *1/ */

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif
    /* async_instance_g->start_abt_push = false; */

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Get the underlying VOL ID */
    H5Pget_vol_id(args->fapl_id, &under_vol_id);
    assert(under_vol_id);

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLfile_open(args->name, args->flags, args->fapl_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    /* /1* Check for 'post open' callback *1/ */
    /* supported = 0; */
    /* /1* Try executing operation, without default error stack handling *1/ */
    /* H5E_BEGIN_TRY { */
    /*     status = H5VLintrospect_opt_query(obj, under_vol_id, H5VL_SUBCLS_FILE, H5VL_NATIVE_FILE_POST_OPEN,
     * &supported); */
    /* } H5E_END_TRY */
    /* if ( status < 0 ) { */
    /*     if ((task->err_stack = H5Eget_current_stack()) < 0) */
    /*         fprintf(fout_g,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__); */
    /*     goto done; */
    /* } */
    /* if(supported & H5VL_OPT_QUERY_SUPPORTED) { */
    /*     /1* Make the 'post open' callback *1/ */
    /*     /1* Try executing operation, without default error stack handling *1/ */
    /*     H5E_BEGIN_TRY { */
    /*         status = H5VLfile_optional_vararg(obj, under_vol_id, H5VL_NATIVE_FILE_POST_OPEN, args->dxpl_id,
     * NULL); */
    /*     } H5E_END_TRY */
    /*     if ( status < 0 ) { */
    /*         if ((task->err_stack = H5Eget_current_stack()) < 0) */
    /*             fprintf(fout_g,"  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__); */
    /*         goto done; */
    /*     } */
    /* } /1* end if *1/ */

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

    // Increase file open ref count
    if (ABT_mutex_lock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] with ABT_mutex_lock\n");
        goto done;
    };
    async_instance_g->nfopen++;
    if (ABT_mutex_unlock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] with ABT_mutex_ulock\n");
        goto done;
    };

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (NULL != info)
        H5VL_async_info_free(info);
    free(args->name);
    args->name = NULL;
    if (args->fapl_id > 0)
        H5Pclose(args->fapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_open_fn

static H5VL_async_t *
async_file_open(task_list_qtype qtype, async_instance_t *aid, const char *name, unsigned flags, hid_t fapl_id,
                hid_t dxpl_id, void **req)
{
    hid_t                   under_vol_id;
    H5VL_async_t *          async_obj   = NULL;
    async_task_t *          async_task  = NULL;
    async_file_open_args_t *args        = NULL;
    bool                    lock_self   = false;
    bool                    is_blocking = false;
    hbool_t                 acquired    = false;
    unsigned int            mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);

    H5Pget_vol_id(fapl_id, &under_vol_id);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_file_open_args_t *)calloc(1, sizeof(async_file_open_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_async_obj = async_obj;
    if (ABT_mutex_create(&(async_obj->file_task_list_mutex)) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        goto error;
    }
    async_obj->pool_ptr = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    if (async_instance_g->ex_delay)
        async_instance_g->start_abt_push = false;

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (NULL != name)
        args->name = strdup(name);
    args->flags = flags;
    if (fapl_id > 0)
        args->fapl_id = H5Pcopy(fapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task        = async_task;
        new_req->file_async_obj = async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }
    // Reset sleep time for each new file
    aid->sleep_time = ASYNC_APP_CHECK_SLEEP_TIME;

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_file_open_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = under_vol_id;
    async_task->async_obj    = async_obj;

    /* Lock async_obj */
    while (1) {
        if (async_obj->obj_mutex && ABT_mutex_trylock(async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else
            fprintf(fout_g, "  [ASYNC VOL DBG] %s error with try_lock\n", __func__);
        usleep(1000);
    }
    lock_self = true;

    async_obj->create_task  = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    if (ABT_mutex_lock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(async_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(async_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_self = false;

    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;

error:
    if (lock_self) {
        if (ABT_mutex_unlock(async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL DBG] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                acquired              = false;
    unsigned int           mutex_count           = 1;
    int                    attempt_count         = 0;
    int                    is_lock               = 0;
    hbool_t                is_lib_state_restored = false;
    ABT_pool *             pool_ptr;
    async_task_t *         task = (async_task_t *)foo;
    async_file_get_args_t *args = (async_file_get_args_t *)(task->args);
    herr_t                 status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->file) {
        if (NULL != task->parent_obj->under_object) {
            args->file = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLfile_get(args->file, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_file_get_args(&args->args, task);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_get_fn

static herr_t
async_file_get(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
               H5VL_file_get_args_t *get_args, hid_t dxpl_id, void **req)
{
    async_task_t *         async_task  = NULL;
    async_file_get_args_t *args        = NULL;
    bool                   lock_parent = false;
    bool                   is_blocking = false;
    hbool_t                acquired    = false;
    unsigned int           mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_file_get_args_t *)calloc(1, sizeof(async_file_get_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->file = parent_obj->under_object;
    if (dup_file_get_args(&args->args, get_args, async_task) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with duplicating file get arguments\n", __func__);
        goto error;
    }
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_file_get_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                     acquired              = false;
    unsigned int                mutex_count           = 1;
    int                         attempt_count         = 0;
    int                         is_lock               = 0;
    hbool_t                     is_lib_state_restored = false;
    ABT_pool *                  pool_ptr;
    async_task_t *              task = (async_task_t *)foo;
    async_file_specific_args_t *args = (async_file_specific_args_t *)(task->args);
    herr_t                      status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->file) {
        if (NULL != task->parent_obj->under_object) {
            args->file = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLfile_specific(args->file, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_file_spec_args(&args->args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_specific_fn

static herr_t
async_file_specific(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                    H5VL_file_specific_args_t *spec_args, hid_t dxpl_id, void **req)
{
    async_task_t *              async_task  = NULL;
    async_file_specific_args_t *args        = NULL;
    bool                        lock_parent = false;
    bool                        is_blocking = false;
    hbool_t                     acquired    = false;
    unsigned int                mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_file_specific_args_t *)calloc(1, sizeof(async_file_specific_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->file = parent_obj->under_object;
    dup_file_spec_args(&args->args, spec_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_file_specific_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (parent_obj->file_async_obj &&
        ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (parent_obj->file_async_obj &&
        ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
            goto error;
        }
    }
    else {
        if (spec_args->op_type == H5VL_FILE_REOPEN) {
            if (parent_obj->create_task->is_done == 0)
                H5VL_async_task_wait(parent_obj->create_task);
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
            // Need blocking to make file reopen work
            is_blocking = true;
        }
        else if (NULL == req || qtype == ISOLATED)
            add_task_to_queue(&aid->qhead, async_task, ISOLATED);
        else if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
    }

    if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                     acquired              = false;
    unsigned int                mutex_count           = 1;
    int                         attempt_count         = 0;
    int                         is_lock               = 0;
    hbool_t                     is_lib_state_restored = false;
    ABT_pool *                  pool_ptr;
    async_task_t *              task = (async_task_t *)foo;
    async_file_optional_args_t *args = (async_file_optional_args_t *)(task->args);
    herr_t                      status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->file) {
        if (NULL != task->parent_obj->under_object) {
            args->file = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLfile_optional(args->file, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count + 4, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_native_file_optional_args(args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s released global lock %u\n", __func__, mutex_count);
#endif
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push) {
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);

#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC ABT DBG] %s pushed task to abt queue, mode=%d\n", __func__,
                    async_instance_g->start_abt_push);
#endif
    }
#ifdef ENABLE_DBG_MSG
    else {
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC ABT DBG] %s did not pushed task to abt queue, %p, mode=%d\n", __func__,
                    async_instance_g->qhead.queue, async_instance_g->start_abt_push);
    }
#endif

#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_optional_fn

static herr_t
async_file_optional(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                    H5VL_optional_args_t *opt_args, hid_t dxpl_id, void **req)
{
    async_task_t *              async_task  = NULL;
    async_file_optional_args_t *args        = NULL;
    bool                        lock_parent = false;
    bool                        is_blocking = false;
    hbool_t                     acquired    = false;
    unsigned int                mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_file_optional_args_t *)calloc(1, sizeof(async_file_optional_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->file = parent_obj->under_object;
    dup_native_file_optional_args(args, opt_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_file_optional_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (parent_obj->file_async_obj &&
        ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (parent_obj->file_async_obj &&
        ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;

    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (parent_obj && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                  acquired              = false;
    unsigned int             mutex_count           = 1;
    int                      attempt_count         = 0;
    int                      is_lock               = 0;
    hbool_t                  is_lib_state_restored = false;
    ABT_pool *               pool_ptr;
    async_task_t *           task = (async_task_t *)foo;
    async_file_close_args_t *args = (async_file_close_args_t *)(task->args);
    herr_t                   status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->file) {
        if (NULL != task->parent_obj->under_object) {
            args->file = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    /* assert(task->async_obj->obj_mutex); */
    /* assert(task->async_obj->magic == ASYNC_MAGIC); */
    while (task->async_obj && task->async_obj->obj_mutex) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            is_lock = 1;
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLfile_close(args->file, task->under_vol_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

    // Decrease file open ref count
    if (ABT_mutex_lock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] with ABT_mutex_lock\n");
        goto done;
    };
    if (async_instance_g->nfopen > 0 && args->is_reopen == false)
        async_instance_g->nfopen--;
    if (ABT_mutex_unlock(async_instance_mutex_g) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] with ABT_mutex_ulock\n");
        goto done;
    };

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);

    // Free all the resources allocated for this file, e.g. tasks
    if (task->task_mutex) {
        ABT_mutex_lock(task->task_mutex);
        free_file_async_resources(task->async_obj);
        ABT_mutex_unlock(task->task_mutex);
    }

    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_file_close_fn

static herr_t
async_file_close(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj, hid_t dxpl_id,
                 void **req)
{
    async_task_t *           async_task  = NULL;
    async_file_close_args_t *args        = NULL;
    bool                     lock_parent = false;
    bool                     is_blocking = false;
    hbool_t                  acquired    = false;
    unsigned int             mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    // When there is already a close task created
    if (parent_obj->close_task) {
        async_task  = parent_obj->close_task;
        is_blocking = 1;
        goto wait;
    }

    if (qtype == BLOCKING) {
        async_instance_g->start_abt_push = true;
        is_blocking                      = true;
    }

    if ((args = (async_file_close_args_t *)calloc(1, sizeof(async_file_close_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Closing a reopened file
    if (parent_obj->file_async_obj == NULL) {
        is_blocking     = true;
        args->is_reopen = true;
    }

    args->file = parent_obj->under_object;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_file_close_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

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

    if (parent_obj->file_async_obj &&
        ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (parent_obj->file_async_obj &&
        ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }
    parent_obj->task_cnt++;
    parent_obj->pool_ptr   = &aid->pool;
    parent_obj->close_task = async_task;

    /* Check if its parent has valid object */
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            // For closing a reopened file
            add_task_to_queue(&aid->qhead, async_task, REGULAR);
            /*     fprintf(fout_g,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__); */
            /*     goto error; */
        }
    }
    else {
        if (async_task->async_obj->is_col_meta == true)
            add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
        else
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
    }

    if (parent_obj->obj_mutex && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;

    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
        aid->start_abt_push = true;
    }
    else {
        if (aid->ex_fclose) {
            if (get_n_running_task_in_queue(async_task, __func__) == 0)
                push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
            aid->start_abt_push = true;
        }
    }

wait:
    aid->start_abt_push = true;
    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (parent_obj->obj_mutex && ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    void *                     obj;
    hbool_t                    acquired              = false;
    unsigned int               mutex_count           = 1;
    int                        attempt_count         = 0;
    int                        is_lock               = 0;
    hbool_t                    is_lib_state_restored = false;
    ABT_pool *                 pool_ptr;
    async_task_t *             task = (async_task_t *)foo;
    async_group_create_args_t *args = (async_group_create_args_t *)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLgroup_create(args->obj, args->loc_params, task->under_vol_id, args->name, args->lcpl_id,
                               args->gcpl_id, args->gapl_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count + 3, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free(args->name);
    args->name = NULL;
    if (args->lcpl_id > 0)
        H5Pclose(args->lcpl_id);
    if (args->gcpl_id > 0)
        H5Pclose(args->gcpl_id);
    if (args->gapl_id > 0)
        H5Pclose(args->gapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);
    free(args);
    task->args = NULL;

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_create_fn

static H5VL_async_t *
async_group_create(async_instance_t *aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params,
                   const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *             async_obj   = NULL;
    async_task_t *             async_task  = NULL;
    async_group_create_args_t *args        = NULL;
    bool                       lock_parent = false;
    bool                       is_blocking = false;
    hbool_t                    acquired    = false;
    unsigned int               mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_group_create_args_t *)calloc(1, sizeof(async_group_create_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if (lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if (gcpl_id > 0)
        args->gcpl_id = H5Pcopy(gcpl_id);
    if (gapl_id > 0)
        args->gapl_id = H5Pcopy(gapl_id);
    else
        goto error;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        /* fprintf(fout_g,"  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__); */
        H5VLfree_lib_state(async_task->h5_state);
        H5VL_async_free_obj(async_obj);
        free_async_task(async_task);
        async_task = NULL;
        async_obj  = NULL;
        goto done;
    }

    async_task->func         = async_group_create_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = async_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    if (NULL != async_task->h5_state && H5VLfree_lib_state(async_task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    async_task->h5_state = NULL;
    return NULL;
} // End async_group_create

static void
async_group_open_fn(void *foo)
{
    void *                   obj;
    hbool_t                  acquired              = false;
    unsigned int             mutex_count           = 1;
    int                      attempt_count         = 0;
    int                      is_lock               = 0;
    hbool_t                  is_lib_state_restored = false;
    ABT_pool *               pool_ptr;
    async_task_t *           task = (async_task_t *)foo;
    async_group_open_args_t *args = (async_group_open_args_t *)(task->args);

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLgroup_open(args->obj, args->loc_params, task->under_vol_id, args->name, args->gapl_id,
                             args->dxpl_id, NULL);
        check_app_wait(attempt_count + 3, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free(args->name);
    args->name = NULL;
    if (args->gapl_id > 0)
        H5Pclose(args->gapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_open_fn

static H5VL_async_t *
async_group_open(async_instance_t *aid, H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params,
                 const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *           async_obj   = NULL;
    async_task_t *           async_task  = NULL;
    async_group_open_args_t *args        = NULL;
    bool                     lock_parent = false;
    bool                     is_blocking = false;
    hbool_t                  acquired    = false;
    unsigned int             mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_group_open_args_t *)calloc(1, sizeof(async_group_open_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (NULL != name)
        args->name = strdup(name);
    if (gapl_id > 0)
        args->gapl_id = H5Pcopy(gapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_group_open_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = async_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                 acquired              = false;
    unsigned int            mutex_count           = 1;
    int                     attempt_count         = 0;
    int                     is_lock               = 0;
    hbool_t                 is_lib_state_restored = false;
    ABT_pool *              pool_ptr;
    async_task_t *          task = (async_task_t *)foo;
    async_group_get_args_t *args = (async_group_get_args_t *)(task->args);
    herr_t                  status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLgroup_get(args->obj, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_group_get_args(&args->args, task);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_get_fn

static herr_t
async_group_get(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                H5VL_group_get_args_t *get_args, hid_t dxpl_id, void **req)
{
    async_task_t *          async_task  = NULL;
    async_group_get_args_t *args        = NULL;
    bool                    lock_parent = false;
    bool                    is_blocking = false;
    hbool_t                 acquired    = false;
    unsigned int            mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_group_get_args_t *)calloc(1, sizeof(async_group_get_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj = parent_obj->under_object;
    if (dup_group_get_args(&args->args, get_args, async_task) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with duplicating group get arguments\n", __func__);
        goto error;
    }
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (qtype == BLOCKING) {
        async_instance_g->start_abt_push = true;
        is_blocking                      = true;
    }

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_group_get_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                      acquired              = false;
    unsigned int                 mutex_count           = 1;
    int                          attempt_count         = 0;
    int                          is_lock               = 0;
    hbool_t                      is_lib_state_restored = false;
    ABT_pool *                   pool_ptr;
    async_task_t *               task = (async_task_t *)foo;
    async_group_specific_args_t *args = (async_group_specific_args_t *)(task->args);
    herr_t                       status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLgroup_specific(args->obj, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_group_spec_args(&args->args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_specific_fn

static herr_t
async_group_specific(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                     H5VL_group_specific_args_t *spec_args, hid_t dxpl_id, void **req)
{
    async_task_t *               async_task  = NULL;
    async_group_specific_args_t *args        = NULL;
    bool                         lock_parent = false;
    bool                         is_blocking = false;
    hbool_t                      acquired    = false;
    unsigned int                 mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_group_specific_args_t *)calloc(1, sizeof(async_group_specific_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj = parent_obj->under_object;
    dup_group_spec_args(&args->args, spec_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_group_specific_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                      acquired              = false;
    unsigned int                 mutex_count           = 1;
    int                          attempt_count         = 0;
    int                          is_lock               = 0;
    hbool_t                      is_lib_state_restored = false;
    ABT_pool *                   pool_ptr;
    async_task_t *               task = (async_task_t *)foo;
    async_group_optional_args_t *args = (async_group_optional_args_t *)(task->args);
    herr_t                       status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLgroup_optional(args->obj, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_native_group_optional_args(args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_optional_fn

static herr_t
async_group_optional(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                     H5VL_optional_args_t *opt_args, hid_t dxpl_id, void **req)
{
    async_task_t *               async_task  = NULL;
    async_group_optional_args_t *args        = NULL;
    bool                         lock_parent = false;
    bool                         is_blocking = false;
    hbool_t                      acquired    = false;
    unsigned int                 mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_group_optional_args_t *)calloc(1, sizeof(async_group_optional_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->obj = parent_obj->under_object;
    dup_native_group_optional_args(args, opt_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_group_optional_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                   acquired              = false;
    unsigned int              mutex_count           = 1;
    int                       attempt_count         = 0;
    int                       is_lock               = 0;
    hbool_t                   is_lib_state_restored = false;
    ABT_pool *                pool_ptr;
    async_task_t *            task = (async_task_t *)foo;
    async_group_close_args_t *args = (async_group_close_args_t *)(task->args);
    herr_t                    status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->grp) {
        if (NULL != task->parent_obj->under_object) {
            args->grp = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    // There may be cases, e.g. with link iteration, that enters group close without a valid async_obj mutex
    if (task->async_obj->obj_mutex) {
        /* Aquire async obj mutex and set the obj */
        assert(task->async_obj->magic == ASYNC_MAGIC);
        while (1) {
            if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
                break;
            }
            else {
                fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
                break;
            }
            usleep(1000);
        }
        is_lock = 1;
    }

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLgroup_close(args->grp, task->under_vol_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count + 3, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (task->async_obj->obj_mutex) {
        if (is_lock == 1) {
            if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
        }
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_group_close_fn

static herr_t
async_group_close(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj, hid_t dxpl_id,
                  void **req)
{
    async_task_t *            async_task  = NULL;
    async_group_close_args_t *args        = NULL;
    bool                      lock_parent = false;
    bool                      is_blocking = false;
    hbool_t                   acquired    = false;
    unsigned int              mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_group_close_args_t *)calloc(1, sizeof(async_group_close_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    args->grp = parent_obj->under_object;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_group_close_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
            goto done;
        }
        /* Insert it into the file task list */
        DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
        if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
                goto error;
            }
        }
        else {
            if (NULL == req || qtype == ISOLATED)
                add_task_to_queue(&aid->qhead, async_task, ISOLATED);
            else if (async_task->async_obj->is_col_meta == true)
                add_task_to_queue(&aid->qhead, async_task, COLLECTIVE);
            else
                add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }

        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
            goto error;
        }
        lock_parent = false;
    }
    else {
        // link iteration may enter here with parent obj mutex to be NULL
        add_task_to_queue(&aid->qhead, async_task, REGULAR);
        is_blocking = true;
    }

    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
        aid->start_abt_push = true;
    }
    else {
        if (aid->ex_gclose) {
            if (get_n_running_task_in_queue(async_task, __func__) == 0)
                push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
            aid->start_abt_push = true;
        }
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
    }
    if (NULL != async_task->args) {
        free(args);
        async_task->args = NULL;
    }
    if (NULL != async_task->h5_state && H5VLfree_lib_state(async_task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    async_task->h5_state = NULL;
    return -1;
} // End async_group_close

static void
async_link_create_fn(void *foo)
{
    hbool_t                   acquired              = false;
    unsigned int              mutex_count           = 1;
    int                       attempt_count         = 0;
    int                       is_lock               = 0;
    hbool_t                   is_lib_state_restored = false;
    ABT_pool *                pool_ptr;
    async_task_t *            task = (async_task_t *)foo;
    async_link_create_args_t *args = (async_link_create_args_t *)(task->args);
    herr_t                    status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLlink_create(&args->args, args->obj, args->loc_params, task->under_vol_id, args->lcpl_id,
                                 args->lapl_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    /* va_end is needed as arguments is copied previously */
    va_end(args->arguments);

    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_link_create_args(&args->args);
    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    if (args->lcpl_id > 0)
        H5Pclose(args->lcpl_id);
    if (args->lapl_id > 0)
        H5Pclose(args->lapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_create_fn

herr_t
async_link_create(task_list_qtype qtype, async_instance_t *aid, H5VL_link_create_args_t *create_args,
                  H5VL_async_t *parent_obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
                  hid_t dxpl_id, void **req)
{
    H5VL_async_t *            async_obj   = NULL;
    async_task_t *            async_task  = NULL;
    async_link_create_args_t *args        = NULL;
    bool                      lock_parent = false;
    bool                      is_blocking = false;
    hbool_t                   acquired    = false;
    unsigned int              mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_link_create_args_t *)calloc(1, sizeof(async_link_create_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    dup_link_create_args(&args->args, create_args);
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    if (lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if (lapl_id > 0)
        args->lapl_id = H5Pcopy(lapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_link_create_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
    async_obj->under_vol_id = async_task->under_vol_id;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto done;
    }

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    parent_obj->task_cnt++;
    parent_obj->pool_ptr = &aid->pool;
    /* Check if its parent has valid object */
    if (NULL == parent_obj->under_object) {
        if (NULL != parent_obj->create_task) {
            add_task_to_queue(&aid->qhead, async_task, DEPENDENT);
        }
        else {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 0;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                 acquired              = false;
    unsigned int            mutex_count           = 1;
    int                     attempt_count         = 0;
    int                     is_lock               = 0;
    hbool_t                 is_lib_state_restored = false;
    ABT_pool *              pool_ptr;
    async_task_t *          task = (async_task_t *)foo;
    async_link_copy_args_t *args = (async_link_copy_args_t *)(task->args);
    herr_t                  status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

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
    /*         fprintf(fout_g,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
     */
    /* #endif */
    /*         if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
     * &task->abt_thread) != ABT_SUCCESS) { */
    /*             fprintf(fout_g,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
     * task->func); */
    /*         } */

    /*         goto done; */
    /*     } */
    /* } */

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLlink_copy(args->src_obj, args->loc_params1, args->dst_obj, args->loc_params2,
                               task->under_vol_id, args->lcpl_id, args->lapl_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params1);
    free_loc_param((H5VL_loc_params_t *)args->loc_params2);
    if (args->lcpl_id > 0)
        H5Pclose(args->lcpl_id);
    if (args->lapl_id > 0)
        H5Pclose(args->lapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_copy_fn

static herr_t
async_link_copy(async_instance_t *aid, H5VL_async_t *parent_obj1, const H5VL_loc_params_t *loc_params1,
                H5VL_async_t *parent_obj2, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                hid_t dxpl_id, void **req)
{
    async_task_t *          async_task  = NULL;
    async_link_copy_args_t *args        = NULL;
    bool                    lock_parent = false;
    bool                    is_blocking = false;
    hbool_t                 acquired    = false;
    unsigned int            mutex_count = 1;
    H5VL_async_t *          parent_obj  = parent_obj1 ? parent_obj1 : parent_obj2;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_link_copy_args_t *)calloc(1, sizeof(async_link_copy_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params1->type == H5VL_OBJECT_BY_NAME && loc_params1->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with 1 loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params1->type == H5VL_OBJECT_BY_IDX && loc_params1->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with 1 loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params2->type == H5VL_OBJECT_BY_NAME && loc_params2->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with 2 loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params2->type == H5VL_OBJECT_BY_IDX && loc_params2->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with 2 loc_by_idx.lapl_id\n", __func__);
        goto error;
    }

    if (parent_obj1)
        args->src_obj = parent_obj1->under_object;
    args->loc_params1 = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params1));
    dup_loc_param(args->loc_params1, loc_params1);
    if (parent_obj2)
        args->dst_obj = parent_obj2->under_object;
    args->loc_params2 = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params2));
    dup_loc_param(args->loc_params2, loc_params2);
    if (lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if (lapl_id > 0)
        args->lapl_id = H5Pcopy(lapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_link_copy_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                 acquired              = false;
    unsigned int            mutex_count           = 1;
    int                     attempt_count         = 0;
    int                     is_lock               = 0;
    hbool_t                 is_lib_state_restored = false;
    ABT_pool *              pool_ptr;
    async_task_t *          task = (async_task_t *)foo;
    async_link_move_args_t *args = (async_link_move_args_t *)(task->args);
    herr_t                  status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

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
    /*         fprintf(fout_g,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
     */
    /* #endif */
    /*         if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
     * &task->abt_thread) != ABT_SUCCESS) { */
    /*             fprintf(fout_g,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
     * task->func); */
    /*         } */

    /*         goto done; */
    /*     } */
    /* } */

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLlink_move(args->src_obj, args->loc_params1, args->dst_obj, args->loc_params2,
                               task->under_vol_id, args->lcpl_id, args->lapl_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params1);
    free_loc_param((H5VL_loc_params_t *)args->loc_params2);
    if (args->lcpl_id > 0)
        H5Pclose(args->lcpl_id);
    if (args->lapl_id > 0)
        H5Pclose(args->lapl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_move_fn

static herr_t
async_link_move(async_instance_t *aid, H5VL_async_t *parent_obj1, const H5VL_loc_params_t *loc_params1,
                H5VL_async_t *parent_obj2, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                hid_t dxpl_id, void **req)
{
    async_task_t *          async_task  = NULL;
    async_link_move_args_t *args        = NULL;
    bool                    lock_parent = false;
    bool                    is_blocking = false;
    hbool_t                 acquired    = false;
    unsigned int            mutex_count = 1;
    H5VL_async_t *          parent_obj  = parent_obj1 ? parent_obj1 : parent_obj2;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_link_move_args_t *)calloc(1, sizeof(async_link_move_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif

    if (loc_params1->type == H5VL_OBJECT_BY_NAME && loc_params1->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with 1 loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params1->type == H5VL_OBJECT_BY_IDX && loc_params1->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with 1 loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params2->type == H5VL_OBJECT_BY_NAME && loc_params2->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with 2 loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params2->type == H5VL_OBJECT_BY_IDX && loc_params2->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with 2 loc_by_idx.lapl_id\n", __func__);
        goto error;
    }

    if (parent_obj1)
        args->src_obj = parent_obj1->under_object;
    args->loc_params1 = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params1));
    dup_loc_param(args->loc_params1, loc_params1);
    if (parent_obj2)
        args->dst_obj = parent_obj2->under_object;
    args->loc_params2 = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params2));
    dup_loc_param(args->loc_params2, loc_params2);
    if (lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if (lapl_id > 0)
        args->lapl_id = H5Pcopy(lapl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_link_move_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                acquired              = false;
    unsigned int           mutex_count           = 1;
    int                    attempt_count         = 0;
    int                    is_lock               = 0;
    hbool_t                is_lib_state_restored = false;
    ABT_pool *             pool_ptr;
    async_task_t *         task = (async_task_t *)foo;
    async_link_get_args_t *args = (async_link_get_args_t *)(task->args);
    herr_t                 status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status =
            H5VLlink_get(args->obj, args->loc_params, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free_link_get_args(&args->args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_get_fn

static herr_t
async_link_get(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
               const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *get_args, hid_t dxpl_id, void **req)
{
    async_task_t *         async_task  = NULL;
    async_link_get_args_t *args        = NULL;
    bool                   lock_parent = false;
    bool                   is_blocking = false;
    hbool_t                acquired    = false;
    unsigned int           mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_link_get_args_t *)calloc(1, sizeof(async_link_get_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    dup_link_get_args(&args->args, get_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_link_get_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                     acquired              = false;
    unsigned int                mutex_count           = 1;
    int                         attempt_count         = 0;
    int                         is_lock               = 0;
    hbool_t                     is_lib_state_restored = false;
    ABT_pool *                  pool_ptr;
    async_task_t *              task = (async_task_t *)foo;
    async_link_specific_args_t *args = (async_link_specific_args_t *)(task->args);
    herr_t                      status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    assert(task->async_obj->magic == ASYNC_MAGIC);
    /* No need to lock the object with iteration */
    if (args->args.op_type != H5VL_LINK_ITER) {
        /* Aquire async obj mutex and set the obj */
        assert(task->async_obj->obj_mutex);
        while (1) {
            if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
                is_lock = 1;
                break;
            }
            else {
                fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
                break;
            }
            usleep(1000);
        }
    }

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLlink_specific(args->obj, args->loc_params, task->under_vol_id, &args->args,
                                   args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free_link_spec_args(&args->args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_specific_fn

static herr_t
async_link_specific(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                    const H5VL_loc_params_t *loc_params, H5VL_link_specific_args_t *spec_args, hid_t dxpl_id,
                    void **req)
{
    async_task_t *              async_task  = NULL;
    async_link_specific_args_t *args        = NULL;
    bool                        lock_parent = false;
    bool                        is_blocking = false;
    hbool_t                     acquired    = false;
    unsigned int                mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_link_specific_args_t *)calloc(1, sizeof(async_link_specific_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    dup_link_spec_args(&args->args, spec_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_link_specific_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                     acquired              = false;
    unsigned int                mutex_count           = 1;
    int                         attempt_count         = 0;
    int                         is_lock               = 0;
    hbool_t                     is_lib_state_restored = false;
    ABT_pool *                  pool_ptr;
    async_task_t *              task = (async_task_t *)foo;
    async_link_optional_args_t *args = (async_link_optional_args_t *)(task->args);
    herr_t                      status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLlink_optional(args->obj, args->loc_params, task->under_vol_id, &args->args,
                                   args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free_native_link_optional_args(args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_link_optional_fn

static herr_t
async_link_optional(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                    const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *opt_args, hid_t dxpl_id,
                    void **req)
{
    async_task_t *              async_task  = NULL;
    async_link_optional_args_t *args        = NULL;
    bool                        lock_parent = false;
    bool                        is_blocking = false;
    hbool_t                     acquired    = false;
    unsigned int                mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_link_optional_args_t *)calloc(1, sizeof(async_link_optional_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    dup_native_link_optional_args(args, opt_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_link_optional_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                   acquired              = false;
    unsigned int              mutex_count           = 1;
    int                       attempt_count         = 0;
    int                       is_lock               = 0;
    hbool_t                   is_lib_state_restored = false;
    ABT_pool *                pool_ptr;
    async_task_t *            task = (async_task_t *)foo;
    async_object_open_args_t *args = (async_object_open_args_t *)(task->args);
    void *                    obj;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        obj = H5VLobject_open(args->obj, args->loc_params, task->under_vol_id, args->opened_type,
                              args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (NULL == obj) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

    task->async_obj->under_object = obj;
    task->async_obj->is_obj_valid = 1;
    /* task->async_obj->create_task  = NULL; */

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_open_fn

static H5VL_async_t *
async_object_open(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                  const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    H5VL_async_t *            async_obj   = NULL;
    async_task_t *            async_task  = NULL;
    async_object_open_args_t *args        = NULL;
    bool                      lock_parent = false;
    bool                      is_blocking = false;
    hbool_t                   acquired    = false;
    unsigned int              mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if (qtype == BLOCKING) {
        async_instance_g->start_abt_push = true;
        is_blocking                      = true;
    }

    if ((args = (async_object_open_args_t *)calloc(1, sizeof(async_object_open_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new async object */
    if ((async_obj = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    async_obj->file_task_list_head = parent_obj->file_task_list_head;
    async_obj->file_async_obj      = parent_obj->file_async_obj;
    async_obj->is_col_meta         = parent_obj->is_col_meta;
    async_obj->pool_ptr            = &aid->pool;
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    args->opened_type = opened_type;
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_object_open_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = async_obj;
    async_task->parent_obj   = parent_obj;

    async_obj->create_task  = async_task;
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return async_obj;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                   acquired              = false;
    unsigned int              mutex_count           = 1;
    int                       attempt_count         = 0;
    int                       is_lock               = 0;
    hbool_t                   is_lib_state_restored = false;
    ABT_pool *                pool_ptr;
    async_task_t *            task = (async_task_t *)foo;
    async_object_copy_args_t *args = (async_object_copy_args_t *)(task->args);
    herr_t                    status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

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
    /*         fprintf(fout_g,"  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n", __func__);
     */
    /* #endif */
    /*         if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
     * &task->abt_thread) != ABT_SUCCESS) { */
    /*             fprintf(fout_g,"  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
     * task->func); */
    /*         } */

    /*         goto done; */
    /*     } */
    /* } */

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLobject_copy(args->src_obj, args->src_loc_params, args->src_name, args->dst_obj,
                                 args->dst_loc_params, args->dst_name, task->under_vol_id, args->ocpypl_id,
                                 args->lcpl_id, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC ABT DBG] %s: failed!\n", __func__);
#endif
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->src_loc_params);
    free(args->src_name);
    args->src_name = NULL;
    free_loc_param((H5VL_loc_params_t *)args->dst_loc_params);
    free(args->dst_name);
    args->dst_name = NULL;
    if (args->ocpypl_id > 0)
        H5Pclose(args->ocpypl_id);
    if (args->lcpl_id > 0)
        H5Pclose(args->lcpl_id);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_copy_fn

static herr_t
async_object_copy(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj1,
                  const H5VL_loc_params_t *src_loc_params, const char *src_name, H5VL_async_t *parent_obj2,
                  const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id,
                  hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    async_task_t *            async_task  = NULL;
    async_object_copy_args_t *args        = NULL;
    bool                      lock_parent = false;
    bool                      is_blocking = false;
    hbool_t                   acquired    = false;
    unsigned int              mutex_count = 1;
    H5VL_async_t *            parent_obj  = parent_obj1 ? parent_obj1 : parent_obj2;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_object_copy_args_t *)calloc(1, sizeof(async_object_copy_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (parent_obj1) {
        // Parent obj may be in queue, wait for it
        H5VL_async_task_wait(parent_obj1->create_task);
        assert(parent_obj1->under_object);
        args->src_obj = parent_obj1->under_object;
    }
    args->src_loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*src_loc_params));
    dup_loc_param(args->src_loc_params, src_loc_params);
    if (NULL != src_name)
        args->src_name = strdup(src_name);
    if (parent_obj2) {
        // Parent obj may be in queue, wait for it
        H5VL_async_task_wait(parent_obj2->create_task);
        assert(parent_obj2->under_object);
        args->dst_obj = parent_obj2->under_object;
    }
    args->dst_loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*dst_loc_params));
    dup_loc_param(args->dst_loc_params, dst_loc_params);
    if (NULL != dst_name)
        args->dst_name = strdup(dst_name);
    if (ocpypl_id > 0)
        args->ocpypl_id = H5Pcopy(ocpypl_id);
    if (lcpl_id > 0)
        args->lcpl_id = H5Pcopy(lcpl_id);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_object_copy_fn;
    async_task->args         = args;
    async_task->op           = WRITE;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            lock_parent = true;
            break;
        }
        usleep(1000);
    }

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                  acquired              = false;
    unsigned int             mutex_count           = 1;
    int                      attempt_count         = 0;
    int                      is_lock               = 0;
    hbool_t                  is_lib_state_restored = false;
    ABT_pool *               pool_ptr;
    async_task_t *           task = (async_task_t *)foo;
    async_object_get_args_t *args = (async_object_get_args_t *)(task->args);
    herr_t                   status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

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
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status =
            H5VLobject_get(args->obj, args->loc_params, task->under_vol_id, &args->args, args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free_object_get_args(&args->args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_get_fn

static herr_t
async_object_get(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                 const H5VL_loc_params_t *loc_params, H5VL_object_get_args_t *get_args, hid_t dxpl_id,
                 void **req)
{
    async_task_t *           async_task  = NULL;
    async_object_get_args_t *args        = NULL;
    bool                     lock_parent = false;
    bool                     is_blocking = false;
    hbool_t                  acquired    = false;
    unsigned int             mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_object_get_args_t *)calloc(1, sizeof(async_object_get_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    dup_object_get_args(&args->args, get_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_object_get_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock file_task_list_mutex\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    /*         fprintf(fout_g,"  [ASYNC VOL ERROR] %s parent task not created\n", __func__); */
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock parent_obj\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                       acquired              = false;
    unsigned int                  mutex_count           = 1;
    int                           attempt_count         = 0;
    int                           is_lock               = 0;
    hbool_t                       is_lib_state_restored = false;
    ABT_pool *                    pool_ptr;
    async_task_t *                task = (async_task_t *)foo;
    async_object_specific_args_t *args = (async_object_specific_args_t *)(task->args);
    herr_t                        status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    if (args->args.op_type != H5VL_OBJECT_VISIT) {
        while (1) {
            if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
                is_lock = 1;
                break;
            }
            else {
                fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
                break;
            }
            usleep(1000);
        }
    }

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLobject_specific(args->obj, args->loc_params, task->under_vol_id, &args->args,
                                     args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free_object_spec_args(&args->args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_specific_fn

static herr_t
async_object_specific(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                      const H5VL_loc_params_t *loc_params, H5VL_object_specific_args_t *spec_args,
                      hid_t dxpl_id, void **req)
{
    async_task_t *                async_task  = NULL;
    async_object_specific_args_t *args        = NULL;
    bool                          lock_parent = false;
    bool                          is_blocking = false;
    hbool_t                       acquired    = false;
    unsigned int                  mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if (qtype == BLOCKING) {
        async_instance_g->start_abt_push = true;
        is_blocking                      = true;
    }

    if ((args = (async_object_specific_args_t *)calloc(1, sizeof(async_object_specific_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }

    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    dup_object_spec_args(&args->args, spec_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_object_specific_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = true;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
    hbool_t                       acquired              = false;
    unsigned int                  mutex_count           = 1;
    int                           attempt_count         = 0;
    int                           is_lock               = 0;
    hbool_t                       is_lib_state_restored = false;
    ABT_pool *                    pool_ptr;
    async_task_t *                task = (async_task_t *)foo;
    async_object_optional_args_t *args = (async_object_optional_args_t *)(task->args);
    herr_t                        status;

#ifdef ENABLE_TIMING
    task->start_time = clock();
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif
    assert(args);
    assert(task);
    assert(task->async_obj);
    assert(task->async_obj->magic == ASYNC_MAGIC);

    pool_ptr = task->async_obj->pool_ptr;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: trying to aquire global lock\n", __func__);
#endif
    if ((attempt_count = check_app_acquire_mutex(task, &mutex_count, &acquired)) < 0)
        goto done;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s: global lock acquired %d, %u\n", __func__, acquired,
                mutex_count);
#endif

    /* Update the dependent parent object if it is NULL */
    if (NULL == args->obj) {
        if (NULL != task->parent_obj->under_object) {
            args->obj = task->parent_obj->under_object;
        }
        else {
            if (check_parent_task(task->parent_obj) != 0) {
                task->err_stack = H5Ecreate_stack();
                H5Eappend_stack(task->err_stack, task->parent_obj->create_task->err_stack, false);
                H5Epush(task->err_stack, __FILE__, __func__, __LINE__, async_error_class_g, H5E_VOL,
                        H5E_CANTCREATE, "Parent task failed");

#ifdef PRINT_ERROR_STACK
                H5Eprint2(task->err_stack, stderr);
#endif

                goto done;
            }
#ifdef ENABLE_DBG_MSG
            if (async_instance_g &&
                (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s parent object is NULL, re-insert to pool\n",
                        __func__);
#endif
            if (ABT_thread_create(*task->async_obj->pool_ptr, task->func, task, ABT_THREAD_ATTR_NULL,
                                  &task->abt_thread) != ABT_SUCCESS) {
                fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_thread_create failed for %p\n", __func__,
                        task->func);
            }

            goto done;
        }
    }

    // Restore previous library state
    assert(task->h5_state);
    if (H5VLstart_lib_state() < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLstart_lib_state failed\n", __func__);
        goto done;
    }
    if (H5VLrestore_lib_state(task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLrestore_lib_state failed\n", __func__);
        goto done;
    }
    is_lib_state_restored = true;

    /* Aquire async obj mutex and set the obj */
    assert(task->async_obj->obj_mutex);
    assert(task->async_obj->magic == ASYNC_MAGIC);
    while (1) {
        if (ABT_mutex_trylock(task->async_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        else {
            fprintf(fout_g, "  [ASYNC ABT DBG] %s error with try_lock\n", __func__);
            break;
        }
        usleep(1000);
    }
    is_lock = 1;

    /* Try executing operation, without default error stack handling */
    H5E_BEGIN_TRY
    {
        status = H5VLobject_optional(args->obj, args->loc_params, task->under_vol_id, &args->args,
                                     args->dxpl_id, NULL);
        check_app_wait(attempt_count, __func__);
    }
    H5E_END_TRY
    if (status < 0) {
        if ((task->err_stack = H5Eget_current_stack()) < 0)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5Eget_current_stack failed\n", __func__);
        goto done;
    }

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT LOG] Argobots execute %s success\n", __func__);
#endif

done:
    if (is_lib_state_restored && H5VLfinish_lib_state() < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfinish_lib_state failed\n", __func__);
    if (NULL != task->h5_state && H5VLfree_lib_state(task->h5_state) < 0)
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5VLfree_lib_state failed\n", __func__);
    task->h5_state = NULL;

    free_loc_param((H5VL_loc_params_t *)args->loc_params);
    free_native_object_optional_args(args);
    if (args->dxpl_id > 0)
        H5Pclose(args->dxpl_id);

    if (is_lock == 1) {
        if (ABT_mutex_unlock(task->async_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC ABT ERROR] %s ABT_mutex_unlock failed\n", __func__);
    }

    ABT_eventual_set(task->eventual, NULL, 0);
    task->in_abt_pool = 0;
    task->is_done     = 1;

#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC ABT DBG] %s releasing global lock\n", __func__);
#endif
    if (acquired == true && H5TSmutex_release(&mutex_count) < 0) {
        fprintf(fout_g, "  [ASYNC ABT ERROR] %s H5TSmutex_release failed\n", __func__);
    }
    if (async_instance_g && NULL != async_instance_g->qhead.queue && async_instance_g->start_abt_push)
        push_task_to_abt_pool(&async_instance_g->qhead, *pool_ptr, __func__);
#ifdef ENABLE_TIMING
    task->end_time = clock();
#endif
    return;
} // End async_object_optional_fn

static herr_t
async_object_optional(task_list_qtype qtype, async_instance_t *aid, H5VL_async_t *parent_obj,
                      const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *opt_args, hid_t dxpl_id,
                      void **req)
{
    async_task_t *                async_task  = NULL;
    async_object_optional_args_t *args        = NULL;
    bool                          lock_parent = false;
    bool                          is_blocking = false;
    hbool_t                       acquired    = false;
    unsigned int                  mutex_count = 1;

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    assert(aid);
    assert(parent_obj);
    assert(parent_obj->magic == ASYNC_MAGIC);

    async_instance_g->prev_push_state = async_instance_g->start_abt_push;

    if ((args = (async_object_optional_args_t *)calloc(1, sizeof(async_object_optional_args_t))) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }
    /* create a new task and insert into its file task list */
    if ((async_task = create_async_task()) == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with calloc\n", __func__);
        goto error;
    }

#ifdef ENABLE_TIMING
    async_task->create_time = clock();
#endif
    if (loc_params->type == H5VL_OBJECT_BY_NAME && loc_params->loc_data.loc_by_name.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_name.lapl_id\n", __func__);
        goto error;
    }
    if (loc_params->type == H5VL_OBJECT_BY_IDX && loc_params->loc_data.loc_by_idx.lapl_id < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with loc_by_idx.lapl_id\n", __func__);
        goto error;
    }
    args->obj        = parent_obj->under_object;
    args->loc_params = (H5VL_loc_params_t *)calloc(1, sizeof(*loc_params));
    dup_loc_param(args->loc_params, loc_params);
    dup_native_object_optional_args(args, opt_args);
    if (dxpl_id > 0)
        args->dxpl_id = H5Pcopy(dxpl_id);
    args->req = req;

    if (req) {
        H5VL_async_t *new_req;
        if ((new_req = H5VL_async_new_obj(NULL, parent_obj->under_vol_id)) == NULL) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object calloc\n", __func__);
            goto error;
        }
        new_req->my_task = async_task;
        /* new_req->under_object = new_req; */
        new_req->file_async_obj = parent_obj->file_async_obj;
        *req                    = (void *)new_req;
    }
    else {
        is_blocking                      = true;
        async_instance_g->start_abt_push = true;
    }

    // Retrieve current library state
    if (H5VLretrieve_lib_state(&async_task->h5_state) < 0) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5VLretrieve_lib_state failed\n", __func__);
        goto done;
    }

    async_task->func         = async_object_optional_fn;
    async_task->args         = args;
    async_task->op           = READ;
    async_task->under_vol_id = parent_obj->under_vol_id;
    async_task->async_obj    = parent_obj;
    async_task->parent_obj   = parent_obj;

    /* Lock parent_obj */
    while (1) {
        if (parent_obj->obj_mutex && ABT_mutex_trylock(parent_obj->obj_mutex) == ABT_SUCCESS) {
            break;
        }
        usleep(1000);
    }
    lock_parent = true;

    if (ABT_mutex_lock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_lock\n", __func__);
        goto done;
    }
    /* Insert it into the file task list */
    DL_APPEND2(parent_obj->file_task_list_head, async_task, file_list_prev, file_list_next);
    if (ABT_mutex_unlock(parent_obj->file_async_obj->file_task_list_mutex) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s parent task not created\n", __func__);
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
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
        goto error;
    }
    lock_parent = false;
    if (aid->ex_delay == false && !async_instance_g->pause) {
        if (get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);
    }

    /* Wait if blocking is needed */
    if (is_blocking) {
        if (async_instance_g->start_abt_push || get_n_running_task_in_queue(async_task, __func__) == 0)
            push_task_to_abt_pool(&aid->qhead, aid->pool, __func__);

        if (H5TSmutex_release(&mutex_count) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_release failed\n", __func__);
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s waiting to finish all previous tasks, SYNC MODE now!\n",
                    __func__);
#endif
        if (ABT_eventual_wait(async_task->eventual, NULL) != ABT_SUCCESS) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_eventual_wait\n", __func__);
            goto error;
        }
#ifdef ENABLE_DBG_MSG
        if (async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s finished all previous tasks, proceed\n", __func__);
#endif
        while (acquired == false && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0) {
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s H5TSmutex_acquire failed\n", __func__);
                goto done;
            }
        }

#ifdef ENABLE_DBG_MSG
        if (async_instance_g->prev_push_state == false && async_instance_g &&
            (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
            fprintf(fout_g, "  [ASYNC VOL DBG] %s restored ASYNC MODE.\n", __func__);
#endif

        /* Failed background thread execution */
        if (async_task->err_stack != 0)
            goto error;
    }

    // Restore async operation state
    async_instance_g->start_abt_push = async_instance_g->prev_push_state;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] leaving %s \n", __func__);
#endif

done:
    return 1;
error:
    if (lock_parent) {
        if (ABT_mutex_unlock(parent_obj->obj_mutex) != ABT_SUCCESS)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_unlock\n", __func__);
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

    new_obj               = (H5VL_async_t *)calloc(1, sizeof(H5VL_async_t));
    new_obj->magic        = ASYNC_MAGIC;
    new_obj->under_object = under_obj;
    new_obj->under_vol_id = under_vol_id;
    H5Iinc_ref(new_obj->under_vol_id);

    if (ABT_mutex_create(&(new_obj->obj_mutex)) != ABT_SUCCESS) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_create\n", __func__);
        return NULL;
    }

    return new_obj;
} /* end H5VL__async_new_obj() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__async_free_obj
 *
 * Purpose:     Release a async object
 *
 * Note:    Take care to preserve the current HDF5 error stack
 *      when calling HDF5 API calls.
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

    // A reopened file may not have a valid obj
    if (NULL == obj)
        return 0;

    err_id = H5Eget_current_stack();

    H5Idec_ref(obj->under_vol_id);

    H5Eset_current_stack(err_id);

    if (obj->obj_mutex && ABT_mutex_free(&(obj->obj_mutex)) != ABT_SUCCESS)
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with ABT_mutex_free\n", __func__);

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
    H5VL_async_info_t *      new_info;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INFO Copy\n");
#endif

    /* Allocate new VOL info struct for the async connector */
    new_info = (H5VL_async_info_t *)calloc(1, sizeof(H5VL_async_info_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_info->under_vol_id = info->under_vol_id;
    H5Iinc_ref(new_info->under_vol_id);
    if (info->under_vol_info)
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
    if (*cmp_value != 0)
        return 0;

    /* Compare under VOL connector info objects */
    H5VLcmp_connector_info(cmp_value, info1->under_vol_id, info1->under_vol_info, info2->under_vol_info);
    if (*cmp_value != 0)
        return 0;

    return 0;
} /* end H5VL_async_info_cmp() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_info_free
 *
 * Purpose:     Release an info object for the connector.
 *
 * Note:    Take care to preserve the current HDF5 error stack
 *      when calling HDF5 API calls.
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
    hid_t              err_id;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INFO Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and info */
    if (info->under_vol_info)
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
    const H5VL_async_info_t *info              = (const H5VL_async_info_t *)_info;
    H5VL_class_value_t       under_value       = (H5VL_class_value_t)-1;
    char *                   under_vol_string  = NULL;
    size_t                   under_vol_str_len = 0;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INFO To String\n");
#endif

    /* Get value and string for underlying VOL connector */
    H5VLget_value(info->under_vol_id, &under_value);
    H5VLconnector_info_to_str(info->under_vol_info, info->under_vol_id, &under_vol_string);

    /* Determine length of underlying VOL info string */
    if (under_vol_string)
        under_vol_str_len = strlen(under_vol_string);

    /* Allocate space for our info */
    *str = (char *)H5allocate_memory(32 + under_vol_str_len, (hbool_t)0);
    assert(*str);

    /* Encode our info
     * Normally we'd use snprintf() here for a little extra safety, but that
     * call had problems on Windows until recently. So, to be as platform-independent
     * as we can, we're using sprintf() instead.
     */
    sprintf(*str, "under_vol=%u;under_info={%s}", (unsigned)under_value,
            (under_vol_string ? under_vol_string : ""));

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
    unsigned           under_vol_value;
    const char *       under_vol_info_start, *under_vol_info_end;
    hid_t              under_vol_id;
    void *             under_vol_info = NULL;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INFO String To Info\n");
#endif

    /* Retrieve the underlying VOL connector value and info */
    sscanf(str, "under_vol=%u;", &under_vol_value);
    under_vol_id         = H5VLregister_connector_by_value((H5VL_class_value_t)under_vol_value, H5P_DEFAULT);
    under_vol_info_start = strchr(str, '{');
    under_vol_info_end   = strrchr(str, '}');
    assert(under_vol_info_end > under_vol_info_start);
    if (under_vol_info_end != (under_vol_info_start + 1)) {
        char *under_vol_info_str;

        under_vol_info_str = (char *)malloc((size_t)(under_vol_info_end - under_vol_info_start));
        memcpy(under_vol_info_str, under_vol_info_start + 1,
               (size_t)((under_vol_info_end - under_vol_info_start) - 1));
        *(under_vol_info_str + (under_vol_info_end - under_vol_info_start)) = '\0';

        H5VLconnector_str_to_info(under_vol_info_str, under_vol_id, &under_vol_info);

        free(under_vol_info_str);
    } /* end else */

    /* Allocate new async VOL connector info and set its fields */
    info                 = (H5VL_async_info_t *)calloc(1, sizeof(H5VL_async_info_t));
    info->under_vol_id   = under_vol_id;
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
    const H5VL_async_t *   o_async      = (const H5VL_async_t *)obj;
    hid_t                  under_vol_id = 0;
    void *                 under_object = NULL;
    H5VL_async_wrap_ctx_t *new_wrap_ctx;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL WRAP CTX Get\n");
#endif

    assert(o_async->magic == ASYNC_MAGIC);

    /* Allocate new VOL object wrapping context for the async connector */
    new_wrap_ctx = (H5VL_async_wrap_ctx_t *)calloc(1, sizeof(H5VL_async_wrap_ctx_t));

    if (o_async->under_vol_id > 0) {
        under_vol_id = o_async->under_vol_id;
    }
    else if (o_async->file_async_obj && o_async->file_async_obj->under_vol_id > 0) {
        under_vol_id = o_async->file_async_obj->under_vol_id;
    }
    else {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with H5VL_async_get_wrap_ctx\n");
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
    H5VL_async_t *         new_obj;
    void *                 under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL WRAP Object\n");
#endif

    /* Wrap the object with the underlying VOL */
    under = H5VLwrap_object(obj, obj_type, wrap_ctx->under_vol_id, wrap_ctx->under_wrap_ctx);
    if (under) {
        new_obj                 = H5VL_async_new_obj(under, wrap_ctx->under_vol_id);
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
 *      underlying object.
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
    void *        under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL UNWRAP Object\n");
#endif

    /* Unrap the object with the underlying VOL */
    under = H5VLunwrap_object(o->under_object, o->under_vol_id);

    if (under)
        H5VL_async_free_obj(o);

    return under;
} /* end H5VL_async_unwrap_object() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_async_free_wrap_ctx
 *
 * Purpose:     Release a "wrapper context" for an object
 *
 * Note:    Take care to preserve the current HDF5 error stack
 *      when calling HDF5 API calls.
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
    hid_t                  err_id;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL WRAP CTX Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and wrap context */
    if (wrap_ctx->under_wrap_ctx)
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
H5VL_async_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id,
                       hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *attr;
    H5VL_async_t *o = (H5VL_async_t *)obj;
    void *        under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Create\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under = H5VLattr_create(o->under_object, loc_params, o->under_vol_id, name, type_id, space_id,
                                acpl_id, aapl_id, dxpl_id, req);
        if (under) {
            attr = H5VL_async_new_obj(under, o->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        } /* end if */
        else
            attr = NULL;
    }
    else
        attr = async_attr_create(async_instance_g, o, loc_params, name, type_id, space_id, acpl_id, aapl_id,
                                 dxpl_id, req);

    return (void *)attr;
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
H5VL_async_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id,
                     hid_t dxpl_id, void **req)
{
    H5VL_async_t *attr;
    H5VL_async_t *o = (H5VL_async_t *)obj;
    void *        under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Open\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under = H5VLattr_open(o->under_object, loc_params, o->under_vol_id, name, aapl_id, dxpl_id, req);
        if (under) {
            attr = H5VL_async_new_obj(under, o->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        } /* end if */
        else
            attr = NULL;
    }
    else
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
H5VL_async_attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)attr;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Read\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLattr_read(o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_attr_read(async_instance_g, o, mem_type_id, buf, dxpl_id, req)) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_attr_read\n");
        }
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
H5VL_async_attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)attr;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Write\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLattr_write(o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_attr_write(async_instance_g, o, mem_type_id, buf, dxpl_id, req)) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_attr_write\n");
        }
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
H5VL_async_attr_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Get\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLattr_get(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_attr_get(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_attr_get\n");
    }

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
H5VL_async_attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_args_t *args,
                         hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Specific\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLattr_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if (H5VL_ATTR_EXISTS == args->op_type)
            qtype = BLOCKING;

        if ((ret_value = async_attr_specific(qtype, async_instance_g, o, loc_params, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_attr_specific\n");
    }

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
H5VL_async_attr_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Optional\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLattr_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        /* We can handle asynchronous execution of 'native' VOL connector optional
         * operations, but all others will be executed synchronously
         */
        if (args->op_type >= H5VL_RESERVED_NATIVE_OPTIONAL)
            qtype = BLOCKING;

        if ((ret_value = async_attr_optional(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_attr_optional\n");
    }

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
    H5VL_async_t *  o = (H5VL_async_t *)attr;
    herr_t          ret_value;
    hbool_t         is_term;
    task_list_qtype qtype = DEPENDENT;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL ATTRIBUTE Close\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLattr_close(o->under_object, o->under_vol_id, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);

        /* Release our wrapper, if underlying attribute was closed */
        if (ret_value >= 0)
            H5VL_async_free_obj(o);
    }
    else {
        if ((ret_value = H5is_library_terminating(&is_term)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with H5is_library_terminating\n");

        /* If the library is shutting down, execute the close synchronously */
        if (is_term)
            qtype = BLOCKING;

        if ((ret_value = async_attr_close(qtype, async_instance_g, o, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_attr_close\n");
    }

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
H5VL_async_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id,
                          hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id,
                          void **req)
{
    H5VL_async_t *dset;
    H5VL_async_t *o = (H5VL_async_t *)obj;
    void *        under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Create\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under = H5VLdataset_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, type_id,
                                   space_id, dcpl_id, dapl_id, dxpl_id, req);
        if (under) {
            dset = H5VL_async_new_obj(under, o->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        } /* end if */
        else
            dset = NULL;
    }
    else {
        dset = async_dataset_create(async_instance_g, o, loc_params, name, lcpl_id, type_id, space_id,
                                    dcpl_id, dapl_id, dxpl_id, req);
    }

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
H5VL_async_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id,
                        hid_t dxpl_id, void **req)
{
    H5VL_async_t *  dset;
    H5VL_async_t *  o     = (H5VL_async_t *)obj;
    task_list_qtype qtype = REGULAR;
    void *          under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Open\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under = H5VLdataset_open(o->under_object, loc_params, o->under_vol_id, name, dapl_id, dxpl_id, req);
        if (under) {
            dset = H5VL_async_new_obj(under, o->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        } /* end if */
        else
            dset = NULL;
    }
    else {
        dset = async_dataset_open(qtype, async_instance_g, o, loc_params, name, dapl_id, dxpl_id, req);
    }

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
H5VL_async_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                        hid_t plist_id, void *buf, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)dset;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Read\n");
#endif

    H5VL_async_dxpl_set_disable_implicit(plist_id);
    H5VL_async_dxpl_set_pause(plist_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id,
                                     file_space_id, plist_id, buf, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_dataset_read(async_instance_g, o, mem_type_id, mem_space_id, file_space_id,
                                            plist_id, buf, req)) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_dataset_read\n");
        }
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
H5VL_async_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                         hid_t plist_id, const void *buf, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)dset;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Write\n");
#endif

    H5VL_async_dxpl_set_disable_implicit(plist_id);
    H5VL_async_dxpl_set_pause(plist_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLdataset_write(o->under_object, o->under_vol_id, mem_type_id, mem_space_id,
                                      file_space_id, plist_id, buf, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_dataset_write(async_instance_g, o, mem_type_id, mem_space_id, file_space_id,
                                             plist_id, buf, req)) < 0) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_dataset_write\n");
        }
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
H5VL_async_dataset_get(void *dset, H5VL_dataset_get_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)dset;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Get\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLdataset_get(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_dataset_get(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_dataset_get\n");
    }

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
H5VL_async_dataset_specific(void *obj, H5VL_dataset_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    hid_t           under_vol_id;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL H5Dspecific\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        // Save copy of underlying VOL connector ID and prov helper, in case of
        // refresh destroying the current object
        under_vol_id = o->under_vol_id;

        ret_value = H5VLdataset_specific(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, under_vol_id);
    }
    else {
        if (H5VL_DATASET_SET_EXTENT == args->op_type)
            qtype = BLOCKING;

        if ((ret_value = async_dataset_specific(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_dataset_specific\n");
    }

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
H5VL_async_dataset_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Optional\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    // For H5Dwait
    if (H5VL_async_dataset_wait_op_g == args->op_type)
        return (H5VL_async_dataset_wait(o));
    else if (args->op_type == H5VL_async_dataset_start_op_g)
        return (H5VL_async_start());
    else if (args->op_type == H5VL_async_dataset_pause_op_g)
        return (H5VL_async_pause());
    else if (args->op_type == H5VL_async_dataset_delay_op_g) {
        H5VL_async_delay_args_t *delay_args = args->args;

        return (H5VL_async_set_delay_time(delay_args->delay_time));
    }
    else {
        if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
            ret_value = H5VLdataset_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        }
        else {
            /* We can handle asynchronous execution of 'native' VOL connector optional
             * operations, but all others will be executed synchronously
             */
            if (args->op_type >= H5VL_RESERVED_NATIVE_OPTIONAL)
                qtype = BLOCKING;

            if ((ret_value = async_dataset_optional(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
                fprintf(fout_g, "  [ASYNC VOL ERROR] with async_dataset_optional\n");
        }
    }

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
    H5VL_async_t *  o = (H5VL_async_t *)dset;
    herr_t          ret_value;
    hbool_t         is_term;
    task_list_qtype qtype = REGULAR;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATASET Close\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLdataset_close(o->under_object, o->under_vol_id, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);

        /* Release our wrapper, if underlying dataset was closed */
        if (ret_value >= 0)
            H5VL_async_free_obj(o);
    }
    else {
        if ((ret_value = H5is_library_terminating(&is_term)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with H5is_library_terminating\n");

        /* If the library is shutting down, execute the close synchronously */
        if (is_term)
            qtype = BLOCKING;

        if ((ret_value = async_dataset_close(qtype, async_instance_g, o, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_dataset_close\n");
    }

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
H5VL_async_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id,
                           hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *dt;
    H5VL_async_t *o = (H5VL_async_t *)obj;
    void *        under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Commit\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under = H5VLdatatype_commit(o->under_object, loc_params, o->under_vol_id, name, type_id, lcpl_id,
                                    tcpl_id, tapl_id, dxpl_id, req);
        if (under) {
            dt = H5VL_async_new_obj(under, o->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        } /* end if */
        else
            dt = NULL;
    }
    else {
        dt = async_datatype_commit(async_instance_g, o, loc_params, name, type_id, lcpl_id, tcpl_id, tapl_id,
                                   dxpl_id, req);
    }

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
H5VL_async_datatype_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t tapl_id,
                         hid_t dxpl_id, void **req)
{
    H5VL_async_t *dt;
    H5VL_async_t *o = (H5VL_async_t *)obj;
    void *        under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Open\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under = H5VLdatatype_open(o->under_object, loc_params, o->under_vol_id, name, tapl_id, dxpl_id, req);
        if (under) {
            dt = H5VL_async_new_obj(under, o->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        } /* end if */
        else
            dt = NULL;
    }
    else {
        dt = async_datatype_open(async_instance_g, o, loc_params, name, tapl_id, dxpl_id, req);
    }

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
H5VL_async_datatype_get(void *dt, H5VL_datatype_get_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)dt;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Get\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLdatatype_get(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_datatype_get(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_datatype_get\n");
    }

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
H5VL_async_datatype_specific(void *obj, H5VL_datatype_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;
    hid_t           under_vol_id;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Specific\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        // Save copy of underlying VOL connector ID and prov helper, in case of
        // refresh destroying the current object
        under_vol_id = o->under_vol_id;

        ret_value = H5VLdatatype_specific(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, under_vol_id);
    }
    else {
        if ((ret_value = async_datatype_specific(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_datatype_specific\n");
    }

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
H5VL_async_datatype_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Optional\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLdatatype_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        /* We can handle asynchronous execution of 'native' VOL connector optional
         * operations, but all others will be executed synchronously
         */
        if (args->op_type >= H5VL_RESERVED_NATIVE_OPTIONAL)
            qtype = BLOCKING;

        if ((ret_value = async_datatype_optional(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_datatype_optional\n");
    }

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
    H5VL_async_t *  o = (H5VL_async_t *)dt;
    herr_t          ret_value;
    hbool_t         is_term;
    task_list_qtype qtype = DEPENDENT;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL DATATYPE Close\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        assert(o->under_object);

        ret_value = H5VLdatatype_close(o->under_object, o->under_vol_id, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);

        /* Release our wrapper, if underlying datatype was closed */
        if (ret_value >= 0)
            H5VL_async_free_obj(o);
    }
    else {
        if ((ret_value = H5is_library_terminating(&is_term)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with H5is_library_terminating\n");

        /* If the library is shutting down, execute the close synchronously */
        if (is_term)
            qtype = BLOCKING;

        if ((ret_value = async_datatype_close(qtype, async_instance_g, o, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_datatype_close\n");
    }

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
H5VL_async_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id,
                       void **req)
{
    H5VL_async_info_t *info;
    H5VL_async_t *     file;
    hid_t              under_fapl_id;
    void *             under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Create\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

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
            hid_t vfd_id; /* VFD for file */

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
                    fprintf(fout_g, "[ASYNC VOL ERROR] MPI is not initialized with MPI_THREAD_MULTIPLE!\n");
                    return NULL;
                }
            } /* end if */
        }     /* end if */
    }
#endif /* MPI_VERSION */

    H5VL_async_fapl_set_disable_implicit(fapl_id);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        /* Open the file with the underlying VOL connector */
        under = H5VLfile_create(name, flags, fcpl_id, under_fapl_id, dxpl_id, req);
        if (under) {
            file = H5VL_async_new_obj(under, info->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, info->under_vol_id);
        } /* end if */
        else
            file = NULL;
    }
    else {
        file = async_file_create(async_instance_g, name, flags, fcpl_id, under_fapl_id, dxpl_id, req);
    }

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
H5VL_async_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_info_t *info;
    H5VL_async_t *     file;
    hid_t              under_fapl_id;
    task_list_qtype    qtype = REGULAR;
    void *             under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Open\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

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
            hid_t vfd_id; /* VFD for file */

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
                    fprintf(fout_g, "[ASYNC VOL ERROR] MPI is not initialized with MPI_THREAD_MULTIPLE!\n");
                    return NULL;
                }
            } /* end if */
        }     /* end if */
    }
#endif /* MPI_VERSION */

    H5VL_async_fapl_set_disable_implicit(fapl_id);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        /* Open the file with the underlying VOL connector */
        under = H5VLfile_open(name, flags, under_fapl_id, dxpl_id, req);
        if (under) {
            file = H5VL_async_new_obj(under, info->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, info->under_vol_id);
        } /* end if */
        else
            file = NULL;
    }
    else {
        /* Open the file with the underlying VOL connector */
        file = async_file_open(qtype, async_instance_g, name, flags, under_fapl_id, dxpl_id, req);
    }

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
H5VL_async_file_get(void *file, H5VL_file_get_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)file;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Get\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLfile_get(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_file_get(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_file_get\n");
    }

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
H5VL_async_file_specific(void *file, H5VL_file_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)file;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Specific\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    /* Return error if file object not open / created, unless flush */
    if (args->op_type != H5VL_FILE_FLUSH && o && !o->is_obj_valid && args->op_type != H5VL_FILE_REOPEN) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with H5VL_async_file_specific, invalid object\n");
        return (-1);
    }

    /* Check for 'is accessible' operation */
    if (args->op_type == H5VL_FILE_IS_ACCESSIBLE || args->op_type == H5VL_FILE_DELETE) {
        H5VL_file_specific_args_t my_args;
        H5VL_async_info_t *       info;
        hid_t                     new_fapl_id;

        /* Don't currently support asynchronous execution of these operations */
        /* (And it's difficult to convert them to "blocking" synchronous, even) */
        if (req)
            return (-1);

        /* Make a (shallow) copy of the arguments */
        memcpy(&my_args, args, sizeof(my_args));

        /* Set up the new FAPL for the updated arguments */

        /* Get copy of our VOL info from FAPL & copy the FAPL */
        if (args->op_type == H5VL_FILE_IS_ACCESSIBLE) {
            H5Pget_vol_info(args->args.is_accessible.fapl_id, (void **)&info);
            new_fapl_id = my_args.args.is_accessible.fapl_id = H5Pcopy(args->args.is_accessible.fapl_id);
        } /* end if */
        else {
            H5Pget_vol_info(args->args.del.fapl_id, (void **)&info);
            new_fapl_id = my_args.args.del.fapl_id = H5Pcopy(args->args.del.fapl_id);
        } /* end else */

        /* Make sure we have info about the underlying VOL to be used */
        if (!info)
            return (-1);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(new_fapl_id, info->under_vol_id, info->under_vol_info);

        /* Execute operation synchronously */
        if ((ret_value = H5VLfile_specific(NULL, info->under_vol_id, &my_args, dxpl_id, NULL)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with H5VL_async_file_specific\n");

        /* Close underlying FAPL */
        H5Pclose(new_fapl_id);

        /* Release copy of our VOL info */
        H5VL_async_info_free(info);
    } /* end if */
    else {
        hid_t under_vol_id;

        /* Keep the correct underlying VOL ID for later */
        under_vol_id = o->under_vol_id;

        if (args->op_type == H5VL_FILE_REOPEN)
            qtype = BLOCKING;

        if (args->op_type == H5VL_FILE_REOPEN || async_instance_g->disable_implicit_file ||
            async_instance_g->disable_implicit) {

            H5VL_async_file_wait((H5VL_async_t *)o);
            ret_value = H5VLfile_specific(o->under_object, o->under_vol_id, args, dxpl_id, req);
        }
        else {
            /* Execute all other operations, possibly asynchronously */
            if ((ret_value = async_file_specific(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
                fprintf(fout_g, "  [ASYNC VOL ERROR] with async_file_specific\n");
        }

        if (args->op_type == H5VL_FILE_REOPEN) {
            /* Wrap reopened file struct pointer, if we reopened one */
            if (ret_value >= 0 && *args->args.reopen.file)
                *args->args.reopen.file = H5VL_async_new_obj(*args->args.reopen.file, under_vol_id);
        } /* end else-if */
    }     /* end else */

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
H5VL_async_file_optional(void *file, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)file;
    herr_t          ret_value;
    task_list_qtype qtype = REGULAR;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL File Optional\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    // For H5Fwait
    if (args->op_type == H5VL_async_file_wait_op_g)
        return (H5VL_async_file_wait(o));
    else if (args->op_type == H5VL_async_file_start_op_g)
        return (H5VL_async_start());
    else if (args->op_type == H5VL_async_file_pause_op_g)
        return (H5VL_async_pause());
    else if (args->op_type == H5VL_async_file_delay_op_g) {
        H5VL_async_delay_args_t *delay_args = args->args;

        return (H5VL_async_set_delay_time(delay_args->delay_time));
    }
    else {
        if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
            ret_value = H5VLfile_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        }
        else {
            /* We can handle asynchronous execution of 'native' VOL connector optional
             * operations, but all others will be executed synchronously
             */
            if (args->op_type >= H5VL_RESERVED_NATIVE_OPTIONAL)
                qtype = BLOCKING;

            if ((ret_value = async_file_optional(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
                fprintf(fout_g, "  [ASYNC VOL ERROR] with async_file_optional\n");
        }
    }

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
    H5VL_async_t *  o = (H5VL_async_t *)file;
    herr_t          ret_value;
    hbool_t         is_term;
    task_list_qtype qtype = REGULAR;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL FILE Close\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLfile_close(o->under_object, o->under_vol_id, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);

        /* Release our wrapper, if underlying file was closed */
        if (ret_value >= 0)
            H5VL_async_free_obj(o);
    }
    else {
        if ((ret_value = H5is_library_terminating(&is_term)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with H5is_library_terminating\n");

        /* If the library is shutting down, execute the close synchronously */
        if (is_term)
            qtype = BLOCKING;

        if ((ret_value = async_file_close(qtype, async_instance_g, o, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_file_close\n");
    }

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
H5VL_async_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id,
                        hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *group;
    H5VL_async_t *o = (H5VL_async_t *)obj;
    void *        under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Create\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under = H5VLgroup_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, gcpl_id,
                                 gapl_id, dxpl_id, req);
        if (under) {
            group = H5VL_async_new_obj(under, o->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        } /* end if */
        else
            group = NULL;
    }
    else {
        group = async_group_create(async_instance_g, o, loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id,
                                   req);
    }

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
H5VL_async_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id,
                      hid_t dxpl_id, void **req)
{
    H5VL_async_t *group;
    H5VL_async_t *o = (H5VL_async_t *)obj;
    void *        under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Open\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under = H5VLgroup_open(o->under_object, loc_params, o->under_vol_id, name, gapl_id, dxpl_id, req);
        if (under) {
            group = H5VL_async_new_obj(under, o->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        } /* end if */
        else
            group = NULL;
    }
    else {
        group = async_group_open(async_instance_g, o, loc_params, name, gapl_id, dxpl_id, req);
    }

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
H5VL_async_group_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Get\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLgroup_get(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if (H5VL_GROUP_GET_INFO == args->op_type)
            qtype = BLOCKING;

        if ((ret_value = async_group_get(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_group_get\n");
    }

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
H5VL_async_group_specific(void *obj, H5VL_group_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *              o = (H5VL_async_t *)obj;
    H5VL_group_specific_args_t  my_args;
    H5VL_group_specific_args_t *new_args;
    herr_t                      ret_value;
    task_list_qtype             qtype = ISOLATED;
    hid_t                       under_vol_id;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Specific\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    /* Unpack arguments to get at the child file pointer when mounting a file */
    if (args->op_type == H5VL_GROUP_MOUNT) {
        /* Make a (shallow) copy of the arguments */
        memcpy(&my_args, args, sizeof(my_args));

        /* Set the object for the child file */
        my_args.args.mount.child_file = ((H5VL_async_t *)args->args.mount.child_file)->under_object;

        /* Point to modified arguments */
        new_args = &my_args;
    } /* end if */
    else
        /* Set argument pointer to current arguments */
        new_args = args;

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        // Save copy of underlying VOL connector ID and prov helper, in case of
        // refresh destroying the current object
        under_vol_id = o->under_vol_id;

        ret_value = H5VLgroup_specific(o->under_object, o->under_vol_id, new_args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, under_vol_id);
    }
    else {
        if ((ret_value = async_group_specific(qtype, async_instance_g, o, new_args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_group_specific\n");
    }

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
H5VL_async_group_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL GROUP Optional\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLgroup_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        /* We can handle asynchronous execution of 'native' VOL connector optional
         * operations, but all others will be executed synchronously
         */
        if (args->op_type >= H5VL_RESERVED_NATIVE_OPTIONAL)
            qtype = BLOCKING;

        if ((ret_value = async_group_optional(qtype, async_instance_g, o, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_group_optional\n");
    }

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
    H5VL_async_t *  o = (H5VL_async_t *)grp;
    herr_t          ret_value;
    hbool_t         is_term;
    task_list_qtype qtype = REGULAR;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL H5Gclose\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLgroup_close(o->under_object, o->under_vol_id, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);

        /* Release our wrapper, if underlying file was closed */
        if (ret_value >= 0)
            H5VL_async_free_obj(o);
    }
    else {
        if ((ret_value = H5is_library_terminating(&is_term)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with H5is_library_terminating\n");

        /* If the library is shutting down, execute the close synchronously */
        if (is_term)
            qtype = BLOCKING;

        if ((ret_value = async_group_close(qtype, async_instance_g, o, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_group_close\n");
    }

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
H5VL_async_link_create(H5VL_link_create_args_t *args, void *obj, const H5VL_loc_params_t *loc_params,
                       hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Create\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    /* Return error if object not open / created */
    if (req == NULL && loc_params && loc_params->obj_type != H5I_BADID && o && !o->is_obj_valid) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with async_link_create, invalid object\n");
        return (-1);
    }

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        H5VL_link_create_args_t  my_args;
        H5VL_link_create_args_t *new_args;
        hid_t                    under_vol_id = -1;

        /* Try to retrieve the "under" VOL id */
        if (o)
            under_vol_id = o->under_vol_id;

        /* Fix up the link target object for hard link creation */
        if (H5VL_LINK_CREATE_HARD == args->op_type) {
            /* If it's a non-NULL pointer, find the 'under object' and re-set the args */
            if (args->args.hard.curr_obj) {
                /* Make a (shallow) copy of the arguments */
                memcpy(&my_args, args, sizeof(my_args));

                /* Check if we still need the "under" VOL ID */
                if (under_vol_id < 0)
                    under_vol_id = ((H5VL_async_t *)args->args.hard.curr_obj)->under_vol_id;

                /* Set the object for the link target */
                my_args.args.hard.curr_obj = ((H5VL_async_t *)args->args.hard.curr_obj)->under_object;

                /* Set argument pointer to modified parameters */
                new_args = &my_args;
            } /* end if */
            else
                new_args = args;
        } /* end if */
        else
            new_args = args;

        /* Re-issue 'link create' call, possibly using the unwrapped pieces */
        ret_value = H5VLlink_create(new_args, (o ? o->under_object : NULL), loc_params, under_vol_id, lcpl_id,
                                    lapl_id, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, under_vol_id);
    } /* end if */
    else {
        if (o && NULL == o->under_object)
            H5VL_async_object_wait(o);

        if ((ret_value = async_link_create(qtype, async_instance_g, args, o, loc_params, lcpl_id, lapl_id,
                                           dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_link_create\n");
    } /* end else */

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
H5VL_async_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                     const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id,
                     void **req)
{
    H5VL_async_t *o_src        = (H5VL_async_t *)src_obj;
    H5VL_async_t *o_dst        = (H5VL_async_t *)dst_obj;
    hid_t         under_vol_id = -1;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Copy\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    /* Return error if objects not open / created */
    if (o_src && !o_src->is_obj_valid) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with %s, invalid o_src\n", __func__);
        return (-1);
    }
    if (o_dst && !o_dst->is_obj_valid) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with %s, invalid o_dst\n", __func__);
        return (-1);
    }

    /* Retrieve the "under" VOL id */
    if (o_src)
        under_vol_id = o_src->under_vol_id;
    else if (o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLlink_copy((o_src ? o_src->under_object : NULL), loc_params1,
                                  (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id, lcpl_id,
                                  lapl_id, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, under_vol_id);
    }
    else {
        if ((ret_value = async_link_copy(async_instance_g, o_src, loc_params1, o_dst, loc_params2, lcpl_id,
                                         lapl_id, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_link_copy\n");
    }

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
H5VL_async_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                     const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id,
                     void **req)
{
    H5VL_async_t *o_src        = (H5VL_async_t *)src_obj;
    H5VL_async_t *o_dst        = (H5VL_async_t *)dst_obj;
    hid_t         under_vol_id = -1;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Move\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    /* Return error if objects not open / created */
    if (o_src && !o_src->is_obj_valid) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with %s, invalid o_src\n", __func__);
        return (-1);
    }
    if (o_dst && !o_dst->is_obj_valid) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] with %s, invalid o_dst\n", __func__);
        return (-1);
    }

    /* Retrieve the "under" VOL id */
    if (o_src)
        under_vol_id = o_src->under_vol_id;
    else if (o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLlink_move((o_src ? o_src->under_object : NULL), loc_params1,
                                  (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id, lcpl_id,
                                  lapl_id, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, under_vol_id);
    }
    else {
        if ((ret_value = async_link_move(async_instance_g, o_src, loc_params1, o_dst, loc_params2, lcpl_id,
                                         lapl_id, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_link_move\n");
    }

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
H5VL_async_link_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *args, hid_t dxpl_id,
                    void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Get\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLlink_get(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_link_get(qtype, async_instance_g, o, loc_params, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_link_move\n");
    }

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
H5VL_async_link_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_args_t *args,
                         hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Specific\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLlink_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_link_specific(qtype, async_instance_g, o, loc_params, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_link_specific\n");
    }

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
H5VL_async_link_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args,
                         hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL LINK Optional\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLlink_optional(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        /* We can handle asynchronous execution of 'native' VOL connector optional
         * operations, but all others will be executed synchronously
         */
        if (args->op_type >= H5VL_RESERVED_NATIVE_OPTIONAL)
            qtype = BLOCKING;

        if ((ret_value = async_link_optional(qtype, async_instance_g, o, loc_params, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_link_optional\n");
    }

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
H5VL_async_object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id,
                       void **req)
{
    H5VL_async_t *  new_obj;
    H5VL_async_t *  o     = (H5VL_async_t *)obj;
    task_list_qtype qtype = BLOCKING;
    void *          under;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Open\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under = H5VLobject_open(o->under_object, loc_params, o->under_vol_id, opened_type, dxpl_id, req);
        if (under) {
            new_obj = H5VL_async_new_obj(under, o->under_vol_id);

            /* Check for async request */
            if (req && *req)
                *req = H5VL_async_new_obj(*req, o->under_vol_id);
        } /* end if */
        else
            new_obj = NULL;
    }
    else {
        if (NULL ==
            (new_obj = async_object_open(qtype, async_instance_g, o, loc_params, opened_type, dxpl_id, req)))
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_object_open\n");
    }

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
H5VL_async_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name,
                       void *dst_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name,
                       hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o_src = (H5VL_async_t *)src_obj;
    H5VL_async_t *  o_dst = (H5VL_async_t *)dst_obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Copy\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value =
            H5VLobject_copy(o_src->under_object, src_loc_params, src_name, o_dst->under_object,
                            dst_loc_params, dst_name, o_src->under_vol_id, ocpypl_id, lcpl_id, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o_src->under_vol_id);
    }
    else {
        if ((ret_value = async_object_copy(qtype, async_instance_g, o_src, src_loc_params, src_name, o_dst,
                                           dst_loc_params, dst_name, ocpypl_id, lcpl_id, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_object_copy\n");
    }

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
H5VL_async_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_args_t *args,
                      hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Get\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLobject_get(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_object_get(qtype, async_instance_g, o, loc_params, args, dxpl_id, req)) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_object_get\n");
    }

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
H5VL_async_object_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_args_t *args,
                           hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;
    hid_t           under_vol_id;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Specific\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        under_vol_id = o->under_vol_id;

        ret_value = H5VLobject_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, under_vol_id);
    }
    else {
        if (H5VL_OBJECT_REFRESH == args->op_type)
            qtype = BLOCKING;

        if ((ret_value = async_object_specific(qtype, async_instance_g, o, loc_params, args, dxpl_id, req)) <
            0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_object_specific\n");
    }

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
H5VL_async_object_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args,
                           hid_t dxpl_id, void **req)
{
    H5VL_async_t *  o = (H5VL_async_t *)obj;
    herr_t          ret_value;
    task_list_qtype qtype = ISOLATED;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL OBJECT Optional\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    if (async_instance_g->disable_implicit_file || async_instance_g->disable_implicit) {
        ret_value = H5VLobject_optional(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_async_new_obj(*req, o->under_vol_id);
    }
    else {
        if ((ret_value = async_object_optional(qtype, async_instance_g, o, loc_params, args, dxpl_id, req)) <
            0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] with async_object_optional\n");
    }

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
H5VL_async_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INTROSPECT GetConnCls\n");
#endif

    /* Check for querying this connector's class */
    if (H5VL_GET_CONN_LVL_CURR == lvl) {
        *conn_cls = &H5VL_async_g;
        ret_value = 0;
    } /* end if */
    else
        ret_value = H5VLintrospect_get_conn_cls(o->under_object, o->under_vol_id, lvl, conn_cls);

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
    herr_t                   ret_value;

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
H5VL_async_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, uint64_t *flags)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL INTROSPECT OptQuery\n");
#endif

    /* Check for 'post open' query and return immediately here, we will
     * query for the underlying VOL connector's support in the actual file
     * create or query operation.
     */
    if (H5VL_NATIVE_FILE_POST_OPEN == opt_type) {
        if (flags)
            *flags = H5VL_OPT_QUERY_SUPPORTED;
        ret_value = 0;
    } /* end if */
    else if (H5VL_REQUEST_GET_EXEC_TIME == opt_type) {
        if (flags)
            *flags = H5VL_OPT_QUERY_SUPPORTED;
        ret_value = 0;
    } /* end if */
    else
        ret_value = H5VLintrospect_opt_query(o->under_object, o->under_vol_id, cls, opt_type, flags);

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
    herr_t           ret_value = 0;
    clock_t          start_time, now_time;
    double           elapsed, trigger;
    H5VL_async_t *   request;
    async_task_t *   task;
    ABT_thread_state state;
    hbool_t          acquired    = false;
    unsigned int     mutex_count = 1;
    hbool_t          tmp         = async_instance_g->start_abt_push;

    assert(obj);
    assert(status);

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Wait\n");
#endif

#ifdef ENABLE_LOG
    if ((async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL LOG] entering %s, mode=%d\n", __func__,
                async_instance_g->start_abt_push);
#endif

    request = (H5VL_async_t *)obj;
    task    = request->my_task;
    if (task == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s task from request is invalid\n", __func__);
        *status = H5VL_REQUEST_STATUS_FAIL;
        return -1;
    }
    else if (task->magic != TASK_MAGIC) {
        // Task already completed and freed
        *status = H5VL_REQUEST_STATUS_SUCCEED;
        return ret_value;
    }

    if (timeout > 0) {
        if (H5TSmutex_release(&mutex_count) < 0)
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_release\n", __func__);
#ifdef ENABLE_DBG_MSG
        if ((async_instance_g &&
             (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK)))
            fprintf(fout_g, "  [ASYNC VOL DBG] released global lock, %d count\n", mutex_count);
#endif

        async_instance_g->start_abt_push = true;

        // There is a chance that the background task is finishing up, so check it twice
        int attempt = 2;
        while (attempt--) {
            if (task->async_obj && get_n_running_task_in_queue_obj(task->async_obj, __func__) == 0 &&
                task->async_obj->pool_ptr && async_instance_g->qhead.queue) {
                push_task_to_abt_pool(&async_instance_g->qhead, *task->async_obj->pool_ptr, __func__);

#ifdef ENABLE_DBG_MSG
                if ((async_instance_g &&
                     (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK)))
                    fprintf(fout_g, "  [ASYNC VOL DBG] %s, will push a task\n", __func__);
#endif
            }
            usleep(1000);
        }
    }

    trigger    = (double)timeout;
    start_time = clock();

    do {
        /* if (NULL == task->abt_thread) { */
        if (task->is_done == 1 || task->magic != TASK_MAGIC) {
            if (task->err_stack)
                *status = H5VL_REQUEST_STATUS_FAIL;
            else
                *status = H5VL_REQUEST_STATUS_SUCCEED;
            goto done;
        }
        /* } */

        if (timeout == H5ES_WAIT_FOREVER && task->eventual) {
            ABT_eventual_wait(task->eventual, NULL);
            *status = H5VL_REQUEST_STATUS_SUCCEED;
            if (task->err_stack != 0) {
                *status = H5VL_REQUEST_STATUS_FAIL;
                goto done;
            }
            goto done;
        }
        else if (task->abt_thread) {
            ABT_thread_get_state(task->abt_thread, &state);
            if (ABT_THREAD_STATE_TERMINATED != state) {
                *status = H5VL_REQUEST_STATUS_IN_PROGRESS;
            }
            if (task->err_stack != 0) {
                *status = H5VL_REQUEST_STATUS_FAIL;
                goto done;
            }
        }

        if (timeout > 0)
            usleep(100000);
        now_time = clock();
        elapsed  = (double)(now_time - start_time) / CLOCKS_PER_SEC;

        *status = H5VL_REQUEST_STATUS_IN_PROGRESS;
    } while (elapsed < trigger);

#ifdef ENABLE_DBG_MSG
    if (elapsed > timeout &&
        (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK)))
        fprintf(fout_g, "  [ASYNC VOL DBG] timedout during wait (elapsed=%es, timeout=%.1fs)\n", elapsed,
                trigger);
#endif

done:
    if (timeout > 0) {
        while (false == acquired && mutex_count > 0) {
            if (H5TSmutex_acquire(mutex_count, &acquired) < 0)
                fprintf(fout_g, "  [ASYNC VOL ERROR] %s with H5TSmutex_acquire\n", __func__);
        }
    }

    async_instance_g->start_abt_push = tmp;
#ifdef ENABLE_DBG_MSG
    if (async_instance_g && (async_instance_g->mpi_rank == ASYNC_DBG_MSG_RANK || -1 == ASYNC_DBG_MSG_RANK))
        fprintf(fout_g, "  [ASYNC VOL DBG] %s reacquire global lock, reset ASYNC MODE to %d\n", __func__,
                tmp);
#endif

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
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Notify\n");
#endif

    ret_value = H5VLrequest_notify(o->under_object, o->under_vol_id, cb, ctx);

    if (ret_value >= 0)
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
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Cancel\n");
#endif

    ret_value = H5VLrequest_cancel(o->under_object, o->under_vol_id, status);

    if (ret_value >= 0)
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
H5VL_async_request_specific(void *obj, H5VL_request_specific_args_t *args)
{
    H5VL_async_t *async_obj = (H5VL_async_t *)obj;
    async_task_t *task      = async_obj->my_task;
    herr_t        ret_value = -1;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Specific\n");
#endif

    if (task == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object\n", __func__);
        return -1;
    }

    if (H5VL_REQUEST_GET_ERR_STACK == args->op_type) {
        /* Increment refcount on task's error stack, if it has one */
        if (H5I_INVALID_HID != task->err_stack)
            H5Iinc_ref(task->err_stack);

        /* Return the task's error stack (including H5I_INVALID_HID) */
        args->args.get_err_stack.err_stack_id = task->err_stack;

        ret_value = 0;
    } /* end if */
    else if (H5VL_REQUEST_GET_EXEC_TIME == args->op_type) {
        /* Return the execution time info */
        *args->args.get_exec_time.exec_ts = (uint64_t)task->create_time;
        *args->args.get_exec_time.exec_time =
            (uint64_t)((task->end_time - task->start_time) / (CLOCKS_PER_SEC * 1000000000LL));

        ret_value = 0;
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
H5VL_async_request_optional(void *obj, H5VL_optional_args_t *args)
{
    H5VL_async_t *async_obj = (H5VL_async_t *)obj;
    async_task_t *task      = async_obj->my_task;
    herr_t        ret_value = -1;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL REQUEST Optional\n");
#endif

    if (task == NULL) {
        fprintf(fout_g, "  [ASYNC VOL ERROR] %s with request object\n", __func__);
        return -1;
    }

    if (args->op_type == H5VL_async_request_depend_op_g) {
        H5VL_async_req_dep_args_t *dep_args = args->args;

        if (NULL == dep_args->parent_req) {
            fprintf(fout_g, "  [ASYNC VOL ERROR] %s NULL parent request object\n", __func__);
            return -1;
        }

        ret_value = H5VL_async_set_request_dep(obj, dep_args->parent_req);
    }
    else if (args->op_type == H5VL_async_request_start_op_g)
        return (H5VL_async_start());
    else
        assert(0 && "Unknown 'optional' operation");

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
H5VL_async_blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL BLOB Put\n");
#endif

    ret_value = H5VLblob_put(o->under_object, o->under_vol_id, buf, size, blob_id, ctx);

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
H5VL_async_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL BLOB Get\n");
#endif

    ret_value = H5VLblob_get(o->under_object, o->under_vol_id, blob_id, buf, size, ctx);

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
H5VL_async_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_args_t *args)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL BLOB Specific\n");
#endif

    ret_value = H5VLblob_specific(o->under_object, o->under_vol_id, blob_id, args);

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
H5VL_async_blob_optional(void *obj, void *blob_id, H5VL_optional_args_t *args)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL BLOB Optional\n");
#endif

    ret_value = H5VLblob_optional(o->under_object, o->under_vol_id, blob_id, args);

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
H5VL_async_token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

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
H5VL_async_token_to_str(void *obj, H5I_type_t obj_type, const H5O_token_t *token, char **token_str)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

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
H5VL_async_token_from_str(void *obj, H5I_type_t obj_type, const char *token_str, H5O_token_t *token)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

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
H5VL_async_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_async_t *o = (H5VL_async_t *)obj;
    herr_t        ret_value;

#ifdef ENABLE_ASYNC_LOGGING
    printf("------- ASYNC VOL generic Optional\n");
#endif
    H5VL_async_dxpl_set_disable_implicit(dxpl_id);
    H5VL_async_dxpl_set_pause(dxpl_id);

    ret_value = H5VLoptional(o->under_object, o->under_vol_id, args, dxpl_id, req);

    return ret_value;
} /* end H5VL_async_optional() */
