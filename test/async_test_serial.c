#include <stdio.h>
#include <stdlib.h>
#include "hdf5.h"
#include "h5_async_lib.h"

#define DIMLEN 1024

int print_dbg_msg = 1;

static int
link_iterate_cb(hid_t group_id, const char *link_name, const H5L_info2_t *info, void *_op_data)
{
    int *nlink = (int *)_op_data;

    ++(*nlink);
    printf("nlink = %d, link_name = %s\n", *nlink, link_name);

    return H5_ITER_CONT;
}

int
main(int argc, char *argv[])
{
    hid_t       file_id, grp_id, dset1_id, dset0_id, dspace_id, async_dxpl, attr_space, attr0, attr1;
    hid_t       dspace2_id;
    const char *file_name   = "async_test_serial.h5";
    const char *grp_name    = "Group";
    int *       data0_write = NULL, *data0_read = NULL, *data1_write = NULL, *data1_read = NULL;
    int         attr_data0, attr_data1, attr_read_data0 = 0, attr_read_data1 = 0;
    int         i, ret = 0;
    hsize_t     ds_size[2]  = {DIMLEN, DIMLEN};
    hsize_t     ds2_size[2] = {0, 0};
    int         sdims;
    hsize_t     idx   = 0;
    int         nlink = 0;
    herr_t      status;
    hid_t       async_fcpl, async_fapl, async_gcpl, get_fcpl, get_gcpl;
    /* int        sleeptime = 100; */

    async_fcpl = H5Pcreate(H5P_FILE_CREATE);
    async_fapl = H5Pcreate(H5P_FILE_ACCESS);
    async_gcpl = H5Pcreate(H5P_GROUP_CREATE);
    async_dxpl = H5Pcreate(H5P_DATASET_XFER);

    if (print_dbg_msg)
        printf("H5Fcreate start\n");
    fflush(stdout);

    file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, async_fcpl, async_fapl);
    if (file_id < 0) {
        fprintf(stderr, "Error with file create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Fcreate done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5Fget_access_plist start\n");
    fflush(stdout);
    get_fcpl = H5Fget_create_plist(file_id);
    if (get_fcpl < 0) {
        fprintf(stderr, "Error with getting fcpl\n");
        ret = -1;
        goto done;
    }
    if (H5Pequal(async_fcpl, get_fcpl) <= 0) {
        fprintf(stderr, "Error with fcpl, not equal to previously used fcpl\n");
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        printf("H5Fget_access_plist done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5Gcreate start\n");
    fflush(stdout);
    grp_id = H5Gcreate(file_id, grp_name, H5P_DEFAULT, async_gcpl, H5P_DEFAULT);
    if (grp_id < 0) {
        fprintf(stderr, "Error with group create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Gcreate done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5Gget_create_plist start\n");
    fflush(stdout);
    get_gcpl = H5Gget_create_plist(grp_id);
    if (get_gcpl < 0) {
        fprintf(stderr, "Error with getting gcpl\n");
        ret = -1;
        goto done;
    }
    if (H5Pequal(async_gcpl, get_gcpl) <= 0) {
        fprintf(stderr, "Error with gcpl, not equal to previously used gcpl\n");
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        printf("H5Gget_create_plist done\n");
    fflush(stdout);

    data0_write = malloc(sizeof(int) * DIMLEN * DIMLEN);
    data1_write = malloc(sizeof(int) * DIMLEN * DIMLEN);
    data0_read  = malloc(sizeof(int) * DIMLEN * DIMLEN);
    data1_read  = malloc(sizeof(int) * DIMLEN * DIMLEN);
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        data0_write[i] = i;
        data1_write[i] = i * 2;
    }

    dspace_id         = H5Screate_simple(2, ds_size, NULL);
    hsize_t attr_size = 1;
    attr_space        = H5Screate_simple(1, &attr_size, NULL);

    if (print_dbg_msg)
        printf("H5Dcreate 0 start\n");
    fflush(stdout);
    dset0_id = H5Dcreate(grp_id, "dset0", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (dset0_id < 0) {
        fprintf(stderr, "Error with dset0 create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dcreate 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    if (print_dbg_msg)
        printf("H5Dget_space 0 start\n");
    fflush(stdout);
    dspace2_id = H5Dget_space(dset0_id);
    if (dspace2_id < 0) {
        fprintf(stderr, "Error with getting dspace2\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dget_space 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    sdims = H5Sget_simple_extent_dims(dspace2_id, ds2_size, NULL);
    if (sdims < 0 || sdims != 2) {
        fprintf(stderr, "Error with getting dspace2 dims\n");
        ret = -1;
        goto done;
    }
    if (ds2_size[0] != ds_size[0] || ds2_size[1] != ds_size[1]) {
        fprintf(stderr, "dspace2 dims wrong\n");
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        printf("H5Dcreate 1 start\n");
    fflush(stdout);
    dset1_id = H5Dcreate(grp_id, "dset1", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (dset1_id < 0) {
        fprintf(stderr, "Error with dset1 create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dcreate 1 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    attr0 = H5Acreate(dset0_id, "attr_0", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT);
    attr1 = H5Acreate(dset1_id, "attr_1", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT);
    if (print_dbg_msg)
        printf("H5Acreate 0 & 1 done\n");
    fflush(stdout);

    attr_data0 = 123456;
    attr_data1 = -654321;
    H5Awrite(attr0, H5T_NATIVE_INT, &attr_data0);
    H5Awrite(attr1, H5T_NATIVE_INT, &attr_data1);
    if (print_dbg_msg)
        printf("H5Awrite 0 & 1 done\n");
    fflush(stdout);

    H5Aread(attr0, H5T_NATIVE_INT, &attr_read_data0);
    if (print_dbg_msg)
        printf("H5Aread 0 done\n");
    fflush(stdout);
    H5Aread(attr1, H5T_NATIVE_INT, &attr_read_data1);
    if (print_dbg_msg)
        printf("H5Aread 1 done\n");
    fflush(stdout);

    H5Aclose(attr0);
    H5Aclose(attr1);
    if (print_dbg_msg)
        printf("H5Aclose 0 & 1 done\n");
    fflush(stdout);

    H5Sclose(attr_space);

    H5Fwait(file_id, H5P_DEFAULT);
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
        printf("H5Aread done\n");

    H5Pset_dxpl_disable_async_implicit(async_dxpl, true);

    // W0, R0, W1, R1, W1', W0', R0', R1'
    if (print_dbg_msg)
        printf("H5Dwrite 0 start\n");
    fflush(stdout);
    status = H5Dwrite(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_write);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dwrite 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    H5Pset_dxpl_disable_async_implicit(async_dxpl, false);

    if (print_dbg_msg)
        printf("H5Dread 0 start\n");
    fflush(stdout);
    status = H5Dread(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dread 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    if (print_dbg_msg)
        printf("Start H5Dwait\n");
    fflush(stdout);
    H5Dwait(dset0_id, H5P_DEFAULT);
    if (print_dbg_msg)
        printf("Done H5Dwait\n");
    fflush(stdout);
    // Verify read data
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data0_read[i] != i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data0_read[i], i);
            ret = -1;
            break;
        }
    }
    printf("Finished verification\n");

    if (print_dbg_msg)
        printf("H5Dwrite 1 start\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    status = H5Dwrite(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_write);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dwrite 1 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    if (print_dbg_msg)
        printf("H5Dread 1 start\n");
    status = H5Dread(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dread 1 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    if (print_dbg_msg)
        printf("Start H5Dwait\n");
    fflush(stdout);
    H5Dwait(dset1_id, H5P_DEFAULT);
    if (print_dbg_msg)
        printf("Done H5Dwait\n");
    fflush(stdout);
    // Verify read data
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data1_read[i] != 2 * i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data1_read[i], i);
            ret = -1;
            break;
        }
    }
    printf("Finished verification\n");

    // Change data 0 and 1
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        data0_write[i] *= -1;
        data1_write[i] *= -1;
    }

    if (print_dbg_msg)
        printf("H5Dwrite 1 start\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    status = H5Dwrite(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_write);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dwrite 1 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    if (print_dbg_msg)
        printf("H5Dwrite 0 start\n");
    fflush(stdout);
    status = H5Dwrite(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_write);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dwrite 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    if (print_dbg_msg)
        printf("H5Dread 0 start\n");
    fflush(stdout);
    status = H5Dread(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dread 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    if (print_dbg_msg)
        printf("Start H5Dwait\n");
    fflush(stdout);
    H5Dwait(dset0_id, H5P_DEFAULT);
    if (print_dbg_msg)
        printf("Done H5Dwait\n");
    fflush(stdout);
    // Verify read data
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data0_read[i] != -i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data0_read[i], -i);
            ret = -1;
            break;
        }
    }
    printf("Finished verification\n");

    if (print_dbg_msg)
        printf("H5Dread 1 start\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    status = H5Dread(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dread 1 done\n");
    fflush(stdout);

    /* usleep(sleeptime); */

    if (print_dbg_msg)
        printf("Start H5Dwait\n");
    fflush(stdout);
    H5Dwait(dset1_id, H5P_DEFAULT);
    if (print_dbg_msg)
        printf("Done H5Dwait\n");
    fflush(stdout);
    // Verify read data
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data1_read[i] != -2 * i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data1_read[i], i);
            ret = -1;
            goto done;
            break;
        }
    }
    printf("Finished verification\n");

    status = H5Literate2(grp_id, H5_INDEX_NAME, H5_ITER_INC, &idx, link_iterate_cb, &nlink);
    if (status < 0) {
        fprintf(stderr, "Error with H5Literate\n");
        ret = -1;
        goto done;
    }
    printf("Finished iteration\n");

    /* H5Fwait(file_id); */

    H5Pclose(async_fcpl);
    H5Pclose(async_fapl);
    H5Pclose(async_gcpl);
    H5Pclose(async_dxpl);
    H5Sclose(dspace_id);
    H5Dclose(dset0_id);
    H5Dclose(dset1_id);
    H5Gclose(grp_id);
    H5Fclose(file_id);

done:
    if (data0_write != NULL)
        free(data0_write);
    if (data0_read != NULL)
        free(data0_read);
    if (data1_write != NULL)
        free(data1_write);
    if (data1_read != NULL)
        free(data1_read);

    return ret;
}