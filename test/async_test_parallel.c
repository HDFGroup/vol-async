#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "hdf5.h"
#include "mpi.h"
#include "h5_async_lib.h"

#define DIMLEN 10
/* #define DIMLEN 1024 */

int
main(int argc, char *argv[])
{
    hid_t       file_id, grp_id, dset1_id, dset0_id, fspace_id, mspace_id, dxpl_id;
    const char *file_name = "async_test_parallel.h5";
    const char *grp_name  = "Group";
    int *       data0_write, *data0_read, *data1_write, *data1_read;
    int         i, is_verified, tmp_size;
    hsize_t     ds_size[2] = {DIMLEN, DIMLEN};
    hsize_t     my_size[2] = {DIMLEN, DIMLEN};
    hsize_t     offset[2]  = {0, 0};
    herr_t      status;
    hid_t       async_fapl;
    int         proc_num, my_rank, ret = 0;
    int         mpi_thread_lvl_provided = -1;

    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_thread_lvl_provided);
    assert(MPI_THREAD_MULTIPLE == mpi_thread_lvl_provided);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    my_size[0] = DIMLEN / proc_num;
    tmp_size   = DIMLEN / proc_num;
    if (my_rank == proc_num - 1) {
        if (DIMLEN % proc_num != 0)
            my_size[0] += DIMLEN % proc_num;
    }

    async_fapl = H5Pcreate(H5P_FILE_ACCESS);

    /* status = H5Pset_vol_async(async_fapl); */
    /* assert(status >= 0); */
    status = H5Pset_fapl_mpio(async_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    assert(status >= 0);

    file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl);
    if (file_id < 0) {
        fprintf(stderr, "Error with file create\n");
        ret = -1;
        goto done;
    }

    grp_id = H5Gcreate(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (grp_id < 0) {
        fprintf(stderr, "Error with group create\n");
        ret = -1;
        goto done;
    }

    data0_write = calloc(sizeof(int), DIMLEN * DIMLEN);
    data1_write = calloc(sizeof(int), DIMLEN * DIMLEN);
    data0_read  = calloc(sizeof(int), DIMLEN * DIMLEN);
    data1_read  = calloc(sizeof(int), DIMLEN * DIMLEN);
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        data0_write[i] = my_rank * tmp_size * DIMLEN + i;
        data1_write[i] = (my_rank * tmp_size * DIMLEN + i) * 2;
    }

    // Set collective operation
    dxpl_id = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(dxpl_id, H5FD_MPIO_COLLECTIVE);

    mspace_id = H5Screate_simple(2, my_size, NULL);
    fspace_id = H5Screate_simple(2, ds_size, NULL);

    dset0_id = H5Dcreate(grp_id, "dset0", H5T_NATIVE_INT, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (dset0_id < 0) {
        fprintf(stderr, "Error with dset0 create\n");
        ret = -1;
        goto done;
    }

    dset1_id = H5Dcreate(grp_id, "dset1", H5T_NATIVE_INT, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (dset1_id < 0) {
        fprintf(stderr, "Error with dset1 create\n");
        goto done;
    }

    offset[0] = my_rank * (DIMLEN / proc_num);
    offset[1] = 0;
    H5Sselect_hyperslab(fspace_id, H5S_SELECT_SET, offset, NULL, my_size, NULL);

    /* printf("Rank %d: write offset (%d, %d), size (%d, %d)\n", my_rank, offset[0], offset[1], my_size[0],
     * my_size[1]); */

    // W0, R0, W1, R1, W1', W0'
    status = H5Dwrite(dset0_id, H5T_NATIVE_INT, mspace_id, fspace_id, dxpl_id, data0_write);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        ret = -1;
        goto done;
    }
    else
        fprintf(stderr, "Succeed with dset 0 write\n");

    status = H5Dread(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl_id, data0_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }

    // Verify read data
    H5Dwait(dset0_id, H5P_DEFAULT);
    is_verified = 1;
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data0_read[i] != i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data0_read[i], i);
            is_verified = -1;
            ret         = -1;
            break;
        }
    }
    if (is_verified == 1)
        fprintf(stderr, "Succeed with dset 0 read: %d \n", data0_read[0]);

    status = H5Dwrite(dset1_id, H5T_NATIVE_INT, mspace_id, fspace_id, dxpl_id, data1_write);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 write\n");
        ret = -1;
        goto done;
    }
    else
        fprintf(stderr, "Succeed with dset 1 write\n");

    status = H5Dread(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl_id, data1_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        ret = -1;
        goto done;
    }

    // Verify read data
    H5Dwait(dset1_id, H5P_DEFAULT);
    is_verified = 1;
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data1_read[i] != 2 * i) {
            fprintf(stderr, "Error with dset 1 read %d/%d\n", data1_read[i], i * 2);
            is_verified = -1;
            ret         = -1;
            break;
        }
    }
    if (is_verified == 1)
        fprintf(stderr, "Succeed with dset 1 read: %d\n", data1_read[0]);

    // Change data 0 and 1
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        data0_write[i] *= -1;
        data1_write[i] *= -1;
    }

    status = H5Dwrite(dset1_id, H5T_NATIVE_INT, mspace_id, fspace_id, dxpl_id, data1_write);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 write\n");
        ret = -1;
        goto done;
    }
    else
        fprintf(stderr, "Succeed with dset 1 write\n");

    status = H5Dwrite(dset0_id, H5T_NATIVE_INT, mspace_id, fspace_id, H5P_DEFAULT, data0_write);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        ret = -1;
        goto done;
    }
    else
        fprintf(stderr, "Succeed with dset 0 write\n");

    status = H5Dread(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl_id, data0_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }

    // Verify read data
    H5Dwait(dset0_id, H5P_DEFAULT);
    is_verified = 1;
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data0_read[i] != -i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data0_read[i], -i);
            is_verified = -1;
            ret         = -1;
            break;
        }
    }
    if (is_verified == 1)
        fprintf(stderr, "Succeed with dset 0 read: %d\n", data0_read[0]);

    status = H5Dread(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl_id, data1_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        ret = -1;
        goto done;
    }
    H5Dwait(dset1_id, H5P_DEFAULT);
    // Verify read data
    is_verified = 1;
    for (i = 0; i < DIMLEN * DIMLEN; ++i) {
        if (data1_read[i] != -2 * i) {
            fprintf(stderr, "Error with dset 1 read %d/%d\n", data1_read[i], -2 * i);
            is_verified = -1;
            ret         = -1;
            break;
        }
    }
    if (is_verified == 1)
        fprintf(stderr, "Succeed with dset 1 read: %d\n", data1_read[0]);

    H5Pclose(async_fapl);
    H5Sclose(mspace_id);
    H5Dclose(dset0_id);
    H5Dclose(dset1_id);
    H5Gclose(grp_id);

done:
    /* H5Fwait(file_id); */
    H5Fclose(file_id);

    if (data0_write != NULL)
        free(data0_write);
    if (data0_read != NULL)
        free(data0_read);
    if (data1_write != NULL)
        free(data1_write);
    if (data1_read != NULL)
        free(data1_read);

    MPI_Finalize();
    return ret;
}
