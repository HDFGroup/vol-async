#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "hdf5.h"

/* #define DIMLEN 1024 */
#define DIMLEN 4

int print_dbg_msg = 1;

int
main(int argc, char *argv[])
{
    hid_t       file_id, grp_id, dset_ids[2], fspace_id[2], mspace_id[2], mem_type_ids[2];
    hsize_t     ds_size[2] = {DIMLEN, DIMLEN};
    herr_t      status;
    hbool_t     op_failed;
    size_t      num_in_progress;
    int *       write_buf[2], *read_buf[2];
    int         i, ret = 0;
    const char *file_name = "async_test_serial.h5";
    const char *grp_name  = "Group";

    if (print_dbg_msg)
        fprintf(stderr, "H5Fcreate start\n");

    hid_t es_id = H5EScreate();

    file_id = H5Fcreate_async(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT, es_id);
    if (file_id < 0) {
        fprintf(stderr, "Error with file create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Fcreate done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Gcreate start\n");
    grp_id = H5Gcreate_async(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
    if (grp_id < 0) {
        fprintf(stderr, "Error with group create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Gcreate done\n");

    write_buf[0] = malloc(sizeof(int) * DIMLEN * DIMLEN);
    write_buf[1] = malloc(sizeof(int) * DIMLEN * DIMLEN);
    read_buf[0]  = malloc(sizeof(int) * DIMLEN * DIMLEN);
    read_buf[1]  = malloc(sizeof(int) * DIMLEN * DIMLEN);
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        write_buf[0][i] = i;
        write_buf[1][i] = i * 2;
    }

    mem_type_ids[0] = H5T_NATIVE_INT;
    mem_type_ids[1] = H5T_NATIVE_INT;
    fspace_id[0]    = H5Screate_simple(2, ds_size, NULL);
    fspace_id[1]    = H5Screate_simple(2, ds_size, NULL);
    mspace_id[0]    = H5S_ALL;
    mspace_id[1]    = H5S_ALL;

    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 0 start\n");
    dset_ids[0] = H5Dcreate_async(grp_id, "dset0", mem_type_ids[0], fspace_id[0], H5P_DEFAULT, H5P_DEFAULT,
                                  H5P_DEFAULT, es_id);
    if (dset_ids[0] < 0) {
        fprintf(stderr, "Error with dset0 create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 0 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 1 start\n");
    dset_ids[1] = H5Dcreate_async(grp_id, "dset1", mem_type_ids[1], fspace_id[1], H5P_DEFAULT, H5P_DEFAULT,
                                  H5P_DEFAULT, es_id);
    if (dset_ids[1] < 0) {
        fprintf(stderr, "Error with dset1 create\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dcreate 1 done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite multi start\n");
    status = H5Dwrite_multi_async(2, dset_ids, mem_type_ids, mspace_id, fspace_id, H5P_DEFAULT,
                                  (const void **)write_buf, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dwrite multi done\n");

    if (print_dbg_msg)
        fprintf(stderr, "H5Dread multi start\n");
    status = H5Dread_multi_async(2, dset_ids, mem_type_ids, mspace_id, fspace_id, H5P_DEFAULT,
                                 (void **)read_buf, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5Dread multi done\n");
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait start\n");

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done %lu %d\n", num_in_progress, (int)op_failed);

    // Verify read data
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (read_buf[0][i] != i || read_buf[1][i] != 2 * i) {
            fprintf(stderr, "Error with dset 0 read %d/%d %d/%d\n", read_buf[0][i], i, read_buf[1][i], i * 2);
            ret = -1;
            break;
        }
    }

    if (ret != -1)
        fprintf(stderr, "Finished verification\n");

    H5ESclose(es_id);
    H5Sclose(fspace_id[0]);
    H5Sclose(fspace_id[1]);
    H5Dclose(dset_ids[0]);
    H5Dclose(dset_ids[1]);
    H5Gclose(grp_id);
    H5Fclose(file_id);

done:
    if (write_buf[0] != NULL)
        free(write_buf[0]);
    if (write_buf[1] != NULL)
        free(write_buf[1]);
    if (read_buf[0] != NULL)
        free(read_buf[0]);
    if (read_buf[1] != NULL)
        free(read_buf[1]);

    return ret;
}
