#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "hdf5.h"
#include "h5_async_lib.h"

#define DIMLEN 1024

int print_dbg_msg = 1;

int
main(int argc, char *argv[])
{
    hid_t file_id = -1, grp_id = -1, grp1_id = -1, dset1_id = -1, dset0_id = -1, dspace_id = -1,
          attr_space = -1, attr0 = -1, attr1 = -1;
    const char *file_name   = "async_test_serial.h5";
    const char *grp_name    = "Group";
    int *       data0_write = NULL, *data0_read = NULL, attr_data0, attr_data1, attr_read_data0 = 0,
        attr_read_data1 = 0;
    int             i, ret = 0;
    hsize_t         ds_size[2] = {DIMLEN, DIMLEN};
    herr_t          status;
    hid_t           es_id = -1, es1_id = -1;
    hbool_t         op_failed;
    size_t          num_in_progress;
    hbool_t         es_err_status;
    size_t          es_err_count;
    size_t          es_err_cleared;
    H5ES_err_info_t err_info;
    hid_t           async_fapl = -1;

    async_fapl = H5Pcreate(H5P_FILE_ACCESS);
    if (async_fapl < 0) {
        fprintf(stderr, "Error with FAPL create\n");
        ret = -1;
        goto done;
    }
    //    H5Pset_vol_async(async_fapl);

    es_id = H5EScreate();
    if (es_id < 0) {
        fprintf(stderr, "Error with first event set create\n");
        ret = -1;
        goto done;
    }
    es1_id = H5EScreate();
    if (es1_id < 0) {
        fprintf(stderr, "Error with second event set create\n");
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        fprintf(stderr, "H5Fcreate start\n");
    fflush(stdout);

    file_id = H5Fcreate_async(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl, es_id);
    if (file_id < 0) {
        fprintf(stderr, "Error with file create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Fcreate done\n");
    fflush(stdout);

    if (print_dbg_msg)
        fprintf(stderr, "H5Gcreate start\n");
    fflush(stdout);
    grp_id = H5Gcreate_async(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
    /* fprintf(stderr, "After opening group: grp_id = %0llx\n", grp_id); */
    if (grp_id < 0) {
        fprintf(stderr, "Error with group create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Gcreate done\n");
    fflush(stdout);

    if (print_dbg_msg)
        fprintf(stderr, "H5Gcreate 2 start (should fail when executed)\n");
    fflush(stdout);
    int err_line = __LINE__ + 1;
    grp1_id      = H5Gcreate_async(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
    if (grp1_id < 0) {
        fprintf(stderr, "Error with group create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Gcreate 2 done\n");
    fflush(stdout);

    status = H5Gclose_async(grp1_id, es_id);
    if (status > 0) {
        fprintf(stderr, "Error with group close\n");
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        printf("H5ESwait start\n");
    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");
    if (!op_failed) {
        fprintf(stderr, "H5Gcreate didn't fail?!?\n");
        ret = -1;
        goto done;
    }

    es_err_status = 0;
    if (print_dbg_msg)
        fprintf(stderr, "H5ESget_err_status start\n");
    status = H5ESget_err_status(es_id, &es_err_status);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESget_err_status\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESget_err_status done\n");
    if (!es_err_status) {
        fprintf(stderr, "Event set doesn't have error status set?!?\n");
        ret = -1;
        goto done;
    }

    es_err_count = 0;
    if (print_dbg_msg)
        fprintf(stderr, "H5ESget_err_count start\n");
    status = H5ESget_err_count(es_id, &es_err_count);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESget_err_count\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESget_err_count done\n");
    if (1 != es_err_count) {
        fprintf(stderr, "Event set doesn't have 1 error?!?\n");
        ret = -1;
        goto done;
    }

    es_err_cleared = 0;
    memset(&err_info, 0, sizeof(err_info));
    if (print_dbg_msg)
        fprintf(stderr, "H5ESget_err_info start\n");
    status = H5ESget_err_info(es_id, 1, &err_info, &es_err_cleared);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESget_err_info\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESget_err_info done\n");
    if (1 != es_err_cleared) {
        fprintf(stderr, "Event set didn't clear 1 error?!?\n");
        ret = -1;
        goto done;
    }
    if (strcmp("H5Gcreate_async", err_info.api_name)) {
        fprintf(stderr, "Event set didn't return API name correctly?!?\n");
        ret = -1;
        goto done;
    }
    H5free_memory((void *)err_info.api_name);
    if (strcmp("loc_id=0x100000000000000 (file), name=\"Group\", lcpl_id=H5P_DEFAULT, gcpl_id=H5P_DEFAULT, "
               "gapl_id=H5P_DEFAULT, es_id=0x1000000000000000 (event set)",
               err_info.api_args)) {
        fprintf(stderr, "Event set didn't return API name correctly?!?\n");
        ret = -1;
        goto done;
    }
    H5free_memory((void *)err_info.api_args);

    if ((err_info.app_file_name[0] == '/' &&
         NULL != strstr("async_test_serial_event_set_error_stack.c", err_info.app_file_name)) ||
        (err_info.app_file_name[0] == 'a' &&
         strcmp("async_test_serial_event_set_error_stack.c", err_info.app_file_name))) {
        fprintf(stderr, "Event set didn't return app source file name correctly[%s]?!?\n",
                err_info.app_file_name);
        ret = -1;
        goto done;
    }
    H5free_memory((void *)err_info.app_file_name);
    if (strcmp("main", err_info.app_func_name)) {
        fprintf(stderr, "Event set didn't return app source function name correctly?!?\n");
        ret = -1;
        goto done;
    }
    H5free_memory((void *)err_info.app_func_name);
    if (err_line != err_info.app_line_num) {
        fprintf(stderr, "Event set didn't return app source line # correctly?!?, got: %u\n",
                err_info.app_line_num);
        ret = -1;
        goto done;
    }
    if (3 != err_info.op_ins_count) {
        fprintf(stderr, "Event set didn't return op counter correctly?!?\n");
        ret = -1;
        goto done;
    }
    if (0 == err_info.op_ins_ts) {
        fprintf(stderr, "Event set didn't return op timestamp correctly?!?\n");
        ret = -1;
        goto done;
    }
    if (9 != H5Eget_num(err_info.err_stack_id)) { // Somewhat fragile
        fprintf(stderr, "Event set didn't return error stack correctly?!?\n");
        ret = -1;
        goto done;
    }
    status = H5Eclose_stack(err_info.err_stack_id);
    if (status < 0) {
        fprintf(stderr, "Error stack close failed\n");
        ret = -1;
        goto done;
    }

    dspace_id = H5Screate_simple(2, ds_size, NULL);
    if (dspace_id < 0) {
        fprintf(stderr, "Dataset's dataspace create failed\n");
        ret = -1;
        goto done;
    }
    hsize_t attr_size = 1;
    attr_space        = H5Screate_simple(1, &attr_size, NULL);
    if (attr_space < 0) {
        fprintf(stderr, "Attribute's dataspace create failed\n");
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 1 start (should fail as using previous event set with failed op)\n");
    fflush(stdout);
    H5E_BEGIN_TRY
    {
        dset1_id = H5Dcreate_async(grp1_id, "dset1", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT,
                                   H5P_DEFAULT, es_id);
    }
    H5E_END_TRY
    if (dset1_id >= 0) {
        fprintf(stderr, "Should not be able to add task to an event set with failed ops\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 1 done\n");
    fflush(stdout);

    data0_write = malloc(sizeof(int) * DIMLEN * DIMLEN);
    data0_read  = malloc(sizeof(int) * DIMLEN * DIMLEN);
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        data0_write[i] = i;
    }

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    H5ESclose(es_id);

    if (print_dbg_msg)
        printf("H5Dcreate 0 start\n");

    fflush(stdout);
    dset0_id = H5Dcreate_async(grp_id, "dset0", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT,
                               H5P_DEFAULT, es1_id);
    if (dset0_id < 0) {
        fprintf(stderr, "Error with dset0 create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 0 done\n");
    fflush(stdout);

    // attribute async API have not been fully implemented, skip the test for now
    attr0 = H5Acreate_async(dset0_id, "attr_0", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT, es1_id);
    if (attr0 < 0) {
        fprintf(stderr, "First attribute create failed\n");
        ret = -1;
        goto done;
    }
    attr1 = H5Acreate_async(dset0_id, "attr_1", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT, es1_id);
    if (attr1 < 0) {
        fprintf(stderr, "Second attribute create failed\n");
        ret = -1;
        goto done;
    }

    attr_data0 = 123456;
    attr_data1 = -654321;
    status     = H5Awrite_async(attr0, H5T_NATIVE_INT, &attr_data0, es1_id);
    if (status < 0) {
        fprintf(stderr, "First attribute write failed\n");
        ret = -1;
        goto done;
    }
    status = H5Awrite_async(attr1, H5T_NATIVE_INT, &attr_data1, es1_id);
    if (status < 0) {
        fprintf(stderr, "Second attribute write failed\n");
        ret = -1;
        goto done;
    }

    status = H5Aread_async(attr0, H5T_NATIVE_INT, &attr_read_data0, es1_id);
    if (status < 0) {
        fprintf(stderr, "First attribute read failed\n");
        ret = -1;
        goto done;
    }
    status = H5Aread_async(attr1, H5T_NATIVE_INT, &attr_read_data1, es1_id);
    if (status < 0) {
        fprintf(stderr, "Second attribute read failed\n");
        ret = -1;
        goto done;
    }

    status = H5Aclose_async(attr0, es1_id);
    if (status < 0) {
        fprintf(stderr, "First attribute close failed\n");
        ret = -1;
        goto done;
    }
    status = H5Aclose_async(attr1, es1_id);
    if (status < 0) {
        fprintf(stderr, "Second attribute close failed\n");
        ret = -1;
        goto done;
    }

    status = H5Sclose(attr_space);
    if (status < 0) {
        fprintf(stderr, "Closing attribute dataspace failed\n");
        ret = -1;
        goto done;
    }

    status = H5Fwait(file_id, H5P_DEFAULT);
    if (status < 0) {
        fprintf(stderr, "H5Fwait failed\n");
        ret = -1;
        goto done;
    }

    if (attr_data0 != attr_read_data0) {
        fprintf(stderr, "Error with attr 0 read\n");
        ret = -1;
        goto done;
    }
    if (attr_data1 != attr_read_data1) {
        fprintf(stderr, "Error with attr 1 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Aread done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 0 start\n");
    fflush(stdout);
    status = H5Dwrite_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data0_write, es1_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 0 done\n");
    fflush(stdout);

    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 0 start\n");
    fflush(stdout);
    status = H5Dread_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data0_read, es1_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 0 done\n");
    fflush(stdout);

    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait start\n");
    status = H5ESwait(es1_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");

    // Verify read data
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data0_read[i] != i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data0_read[i], i);
            ret = -1;
            break;
        }
    }
    fprintf(stderr, "Finished verification\n");

done:

    /* fprintf(stderr, "Before shutting things down\n"); */
    status = H5Pclose(async_fapl);
    if (status < 0) {
        fprintf(stderr, "H5Pclose failed\n");
        ret = -1;
    }
    status = H5Sclose(dspace_id);
    if (status < 0) {
        fprintf(stderr, "Closing dataset's dataspace failed\n");
        ret = -1;
    }
    status = H5Dclose(dset0_id);
    if (status < 0) {
        fprintf(stderr, "Closing dataset failed\n");
        ret = -1;
    }
    /* fprintf(stderr, "Before close group: grp_id = %0llx\n", grp_id); */
    status = H5Gclose(grp_id);
    if (status < 0) {
        fprintf(stderr, "Closing group failed\n");
        ret = -1;
    }

    status = H5Fclose_async(file_id, es1_id);
    if (status < 0) {
        fprintf(stderr, "Closing file failed\n");
        ret = -1;
    }
    status = H5ESwait(es1_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
    }

    status = H5ESclose(es1_id);
    if (status < 0) {
        fprintf(stderr, "Can't close second event set\n");
        ret = -1;
    }

    if (data0_write != NULL)
        free(data0_write);
    if (data0_read != NULL)
        free(data0_read);

    return ret;
}
