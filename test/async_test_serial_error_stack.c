#include <stdio.h>
#include <stdlib.h>
#include "hdf5.h"

#define DIMLEN 1024

int print_dbg_msg = 1;

int
main(int argc, char *argv[])
{
    hid_t       file_id, grp_id0, grp_id1, dset1_id, dset0_id, dspace_id, dxpl, fapl, es_id;
    const char *file_name = "async_test_serial_error_stack.h5";
    const char *grp_name  = "Group";
    int *       data0_write, *data0_read, *data1_write, *data1_read;
    int         i, ret = 0;
    hsize_t     ds_size[2] = {DIMLEN, DIMLEN};
    herr_t      status;
    hbool_t     op_failed;
    size_t      num_in_progress;

    fapl = H5Pcreate(H5P_FILE_ACCESS);
    dxpl = H5Pcreate(H5P_DATASET_XFER);

    es_id = H5EScreate();
    if (es_id < 0) {
        fprintf(stderr, "Error with first event set create\n");
        ret = -1;
        goto done;
    }

    if (print_dbg_msg)
        printf("H5Fcreate start\n");
    fflush(stdout);

    file_id = H5Fcreate_async(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, fapl, es_id);
    if (file_id < 0) {
        fprintf(stderr, "Error with file create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Fcreate done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5Gcreate start\n");
    fflush(stdout);
    grp_id0 = H5Gcreate_async(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
    if (grp_id0 < 0) {
        fprintf(stderr, "Error with group create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Gcreate done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5Gcreate 2 start (should fail when executed)\n");
    fflush(stdout);
    grp_id1 = H5Gcreate_async(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
    if (grp_id1 < 0) {
        fprintf(stderr, "Error with group create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Gcreate 2 done\n");
    fflush(stdout);

    data0_write = malloc(sizeof(int) * DIMLEN * DIMLEN);
    data1_write = malloc(sizeof(int) * DIMLEN * DIMLEN);
    data0_read  = malloc(sizeof(int) * DIMLEN * DIMLEN);
    data1_read  = malloc(sizeof(int) * DIMLEN * DIMLEN);
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        data0_write[i] = i;
        data1_write[i] = i * 2;
    }

    dspace_id = H5Screate_simple(2, ds_size, NULL);

    if (print_dbg_msg)
        printf("H5Dcreate 0 start\n");
    fflush(stdout);
    dset0_id = H5Dcreate_async(grp_id0, "dset0", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT,
                               H5P_DEFAULT, es_id);
    if (dset0_id < 0) {
        fprintf(stderr, "Error with dset0 create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dcreate 0 done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5Dcreate 1 start (should fail)\n");
    fflush(stdout);
    dset1_id = H5Dcreate_async(grp_id1, "dset1", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT,
                               H5P_DEFAULT, es_id);
    if (dset1_id < 0) {
        fprintf(stderr, "Error with dset1 create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dcreate 1 done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5Dwrite 0 start\n");
    fflush(stdout);
    status = H5Dwrite_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl, data0_write, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dwrite 0 done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5Dwrite 1 start (should fail)\n");
    fflush(stdout);
    status = H5Dwrite_async(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl, data1_write, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dwrite 1 done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5Dread 0 start\n");
    fflush(stdout);
    status = H5Dread_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl, data0_read, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5Dread 0 done\n");
    fflush(stdout);

    if (print_dbg_msg)
        printf("H5ESwait start\n");
    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5ESwait done\n");

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

    H5Pclose(fapl);
    H5Pclose(dxpl);
    H5Sclose(dspace_id);

    H5Dclose_async(dset0_id, es_id);
    H5Dclose_async(dset1_id, es_id);
    H5Gclose_async(grp_id0, es_id);
    H5Gclose_async(grp_id1, es_id);
    H5Fclose_async(file_id, es_id);

    if (print_dbg_msg)
        printf("H5ESwait start\n");
    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        printf("H5ESwait done\n");

    status = H5ESclose(es_id);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESclose\n");
        ret = -1;
        goto done;
    }

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
