#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "hdf5.h"
#include "h5_async_lib.h"

#define DIMLEN 1024

int print_dbg_msg = 1;

static int
link_iterate_cb(hid_t group_id, const char *link_name, const H5L_info2_t *info, void *_op_data)
{
    int *nlink = (int *)_op_data;

    ++(*nlink);
    fprintf(stderr, "nlink = %d, link_name = %s\n", *nlink, link_name);

    return H5_ITER_CONT;
}

int
main(int argc, char *argv[])
{
    hid_t       file_id, grp_id, dset1_id, dset0_id, dspace_id, async_dxpl, attr_space, attr0, attr1;
    const char *file_name = "async_test_serial.h5";
    const char *grp_name  = "Group";
    int *data0_write, *data0_read, *data1_write, *data1_read, attr_data0, attr_data1, attr_read_data0 = 0,
                                                                                      attr_read_data1 = 0;
    int     i, ret = 0;
    hsize_t ds_size[2] = {DIMLEN, DIMLEN};
    herr_t  status;
    hid_t   async_fapl;
    hsize_t idx   = 0;
    int     nlink = 0;

    async_fapl = H5Pcreate(H5P_FILE_ACCESS);
    async_dxpl = H5Pcreate(H5P_DATASET_XFER);

    //    H5Pset_vol_async(async_fapl);

    if (print_dbg_msg)
        fprintf(stderr, "H5Fcreate start\n");

    hbool_t op_failed;
    size_t  num_in_progress;
    hid_t   es_id = H5EScreate();

    file_id = H5Fcreate_async(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl, es_id);
    if (file_id < 0) {
        fprintf(stderr, "Error with file create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Fcreate done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Gcreate start\n");
    /* grp_id = H5Gcreate(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT); */
    grp_id = H5Gcreate_async(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
    if (grp_id < 0) {
        fprintf(stderr, "Error with group create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Gcreate done\n");

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
        fprintf(stderr, "H5Dcreate 0 start\n");
    /* dset0_id  = H5Dcreate(grp_id,"dset0",H5T_NATIVE_INT,dspace_id,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT); */
    dset0_id = H5Dcreate_async(grp_id, "dset0", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT,
                               H5P_DEFAULT, es_id);
    if (dset0_id < 0) {
        fprintf(stderr, "Error with dset0 create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 0 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 1 start\n");
    /* dset1_id  = H5Dcreate(grp_id,"dset1",H5T_NATIVE_INT,dspace_id,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT); */
    dset1_id = H5Dcreate_async(grp_id, "dset1", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT,
                               H5P_DEFAULT, es_id);
    if (dset1_id < 0) {
        fprintf(stderr, "Error with dset1 create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 1 done\n");

    if (H5Aexists(dset0_id, "attr_0") != 0) {
        fprintf(stderr, "Error with H5Aexist\n");
        ret = -1;
        goto done;
    }

    // attribute async test
    attr0 = H5Acreate_async(dset0_id, "attr_0", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT, es_id);
    attr1 = H5Acreate_async(dset1_id, "attr_1", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT, es_id);

    attr_data0 = 123456;
    attr_data1 = -654321;
    H5Awrite_async(attr0, H5T_NATIVE_INT, &attr_data0, es_id);
    H5Awrite_async(attr1, H5T_NATIVE_INT, &attr_data1, es_id);

    H5Aread_async(attr0, H5T_NATIVE_INT, &attr_read_data0, es_id);
    H5Aread_async(attr1, H5T_NATIVE_INT, &attr_read_data1, es_id);

    H5Aclose_async(attr0, es_id);
    H5Aclose_async(attr1, es_id);

    H5Sclose(attr_space);

    if (print_dbg_msg)
        fprintf(stderr, "H5Fwait start\n");

    H5Fwait(file_id, H5P_DEFAULT);

    if (print_dbg_msg)
        fprintf(stderr, "H5Fwait done\n");

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
        fprintf(stderr, "H5Aread data verified\n");

    if (H5Pset_dxpl_delay(async_dxpl, 100) < 0) {
        fprintf(stderr, "Error with H5Pset_dxpl_delay\n");
        ret = -1;
        goto done;
    }

    // W0, R0, W1, R1, W1', W0', R0', R1'
    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 0 start\n");
    status = H5Dwrite_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_write, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 0 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 0 start\n");
    status = H5Dread_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_read, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 0 done\n");

    /* if (print_dbg_msg) fprintf(stderr, "Start H5Dwait\n"); */
    /* H5Dwait(dset0_id); */
    /* if (print_dbg_msg) fprintf(stderr, "Done H5Dwait\n"); */

    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait start\n");

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
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

    uint64_t delay;
    if (H5Pget_dxpl_delay(async_dxpl, &delay) < 0) {
        fprintf(stderr, "Error with H5Pset_dxpl_delay\n");
        ret = -1;
        goto done;
    }
    else if (delay != 100) {
        fprintf(stderr, "Error with H5Pget_dxpl_delay %lu\n", delay);
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 1 start\n");

    status = H5Dwrite_async(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_write, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 1 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 1 start\n");
    status = H5Dread_async(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_read, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 1 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait start\n");

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");

    // Verify read data
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data1_read[i] != 2 * i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data1_read[i], i);
            ret = -1;
            break;
        }
    }
    fprintf(stderr, "Finished verification\n");

    // Change data 0 and 1
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        data0_write[i] *= -1;
        data1_write[i] *= -1;
    }

    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 1 start\n");

    status = H5Dwrite_async(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_write, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 1 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 0 start\n");

    status = H5Dwrite_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_write, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite 0 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 0 start\n");

    // Check for ops in progress
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait check start\n");
    status = H5ESwait(es_id, 0, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait check done, %lu in progress (2 expected)\n", num_in_progress);

    status = H5Dread_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_read, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 0 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait start\n");
    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");

    // Verify read data
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data0_read[i] != -i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data0_read[i], -i);
            ret = -1;
            break;
        }
    }
    fprintf(stderr, "Finished verification\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 1 start\n");

    status = H5Dread_async(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_read, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dread 1 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait start\n");
    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");

    // Verify read data
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data1_read[i] != -2 * i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data1_read[i], i);
            ret = -1;
            goto done;
            break;
        }
    }
    fprintf(stderr, "Finished verification\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Literate_async start\n");

    status = H5Literate_async(grp_id, H5_INDEX_NAME, H5_ITER_INC, &idx, link_iterate_cb, &nlink, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with H5Literate\n");
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        fprintf(stderr, "H5Literate_async done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait start\n");
    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");

    H5ESclose(es_id);

    H5Pclose(async_fapl);
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
