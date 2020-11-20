#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "hdf5.h"
#include "mpi.h"
#include "h5_async_lib.h"

#define DIMLEN 10
/* #define DIMLEN 1024 */

int verify(int *data, int size, int multiplier)
{
    int i;
    assert(data);

    for(i = 0; i < size; ++i) 
        if (data[i] != i * multiplier) 
            return -1;

    return 1;
}

int main(int argc, char *argv[])
{
    hid_t file_id, grp_id, dset1_id, dset0_id, fspace_id, mspace_id, dxpl_id, async_fapl;
    const char *file_name = "async_test_parallel2.h5";
    const char *grp_name  = "Group";
    int        *data0_write, *data0_read, *data1_write, *data1_read;
    int        i, tmp_size, ret=0;
    hsize_t    ds_size[2] = {DIMLEN, DIMLEN};
    hsize_t    my_size[2] = {DIMLEN, DIMLEN};
    hsize_t    offset[2] = {0, 0};
    herr_t     status;
    int proc_num, my_rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    tmp_size = DIMLEN / proc_num;
    my_size[0] = DIMLEN / proc_num;
    if (my_rank == proc_num - 1) {
        if (DIMLEN % proc_num != 0) 
            my_size[0] += DIMLEN % proc_num;
    }
   
    async_fapl = H5Pcreate (H5P_FILE_ACCESS);
    
    H5Pset_vol_async(async_fapl);
    H5Pset_fapl_mpio(async_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);

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

    data0_write = calloc (sizeof(int), DIMLEN*DIMLEN);
    data1_write = calloc (sizeof(int), DIMLEN*DIMLEN);
    data0_read  = calloc (sizeof(int), DIMLEN*DIMLEN);
    data1_read  = calloc (sizeof(int), DIMLEN*DIMLEN);
    for(i = 0; i < DIMLEN*DIMLEN; ++i) {
        data0_write[i] = my_rank*tmp_size*DIMLEN + i;
        data1_write[i] = (my_rank*tmp_size*DIMLEN + i)*2;
    }

    // Set collective operation
    dxpl_id = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(dxpl_id, H5FD_MPIO_COLLECTIVE);

    mspace_id = H5Screate_simple(2, my_size, NULL);
    fspace_id = H5Screate_simple(2, ds_size, NULL);

    dset0_id  = H5Dcreate(grp_id,"dset0",H5T_NATIVE_INT,fspace_id,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT);
    if (dset0_id < 0) {
        fprintf(stderr, "Error with dset0 create\n");
        ret = -1;
        goto done;
    }

    dset1_id  = H5Dcreate(grp_id,"dset1",H5T_NATIVE_INT,fspace_id,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT);
    if (dset1_id < 0) {
        fprintf(stderr, "Error with dset1 create\n");
        ret = -1;
        goto done;
    }

    offset[0] = my_rank * (DIMLEN / proc_num);
    offset[1] = 0;
    H5Sselect_hyperslab(fspace_id, H5S_SELECT_SET, offset, NULL, my_size, NULL);

    // W0, W1,   R0, W0', R1, W1', R1', R0'
    status = H5Dwrite(dset0_id, H5T_NATIVE_INT, mspace_id, fspace_id, dxpl_id, data0_write);
    if (status < 0) {
        fprintf(stderr, "Error with W0\n");
        ret = -1;
        goto done;
    }
    else
        printf("Succeed with W0\n");

    status = H5Dwrite(dset1_id, H5T_NATIVE_INT, mspace_id, fspace_id, dxpl_id, data1_write);
    if (status < 0) {
        fprintf(stderr, "Error with W1\n");
        ret = -1;
        goto done;
    }
    else
        printf("Succeed with W1\n");

    // Change data 0 and 1, should not affect the previous writes as we make a copy in the async vol
    for(i = 0; i < DIMLEN*DIMLEN; ++i) {
        data0_write[i] *= -1;
        data1_write[i] *= -1;
    }

    // R0
    status = H5Dread(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl_id, data0_read);
    if (status < 0) {
        fprintf(stderr, "Error with R0 read\n");
        ret = -1;
        goto done;
    }
    status = H5Dwait(dset0_id, H5P_DEFAULT);
    if (status < 0) {
        fprintf(stderr, "Error with H5Dwait\n");
        ret = -1;
        goto done;
    }
    // Verify read data
    if (verify(data0_read, DIMLEN*DIMLEN, 1) != 1) {
        fprintf(stderr, "Error with R0 verify %d/%d\n", data0_read[i], i);
        ret = -1;
        goto done;
    }
    else
        printf("Succeed with R0\n");

    // W0'
    status = H5Dwrite(dset0_id, H5T_NATIVE_INT, mspace_id, fspace_id, dxpl_id, data0_write);
    if (status < 0) {
        fprintf(stderr, "Error with W0\n");
        ret = -1;
        goto done;
    }
    else
        printf("Succeed with W0'\n");

    // R1
    status = H5Dread(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl_id, data1_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        ret = -1;
        goto done;
    }
    status = H5Dwait(dset1_id, H5P_DEFAULT);
    if (status < 0) {
        fprintf(stderr, "Error with H5Dwait\n");
        ret = -1;
        goto done;
    }
    // Verify read data
    if (verify(data1_read, DIMLEN*DIMLEN, 2) != 1) {
        fprintf(stderr, "Error with dset 1 read %d/%d\n", data1_read[i], i);
        ret = -1;
        goto done;
    }
    else
        printf("Succeed with R1\n");

    // W1'
    status = H5Dwrite(dset1_id, H5T_NATIVE_INT, mspace_id, fspace_id, dxpl_id, data1_write);
    if (status < 0) {
        fprintf(stderr, "Error with W1\n");
        ret = -1;
        goto done;
    }
    else
        printf("Succeed with W1'\n");

    // R1'
    status = H5Dread(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl_id, data1_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        ret = -1;
        goto done;
    }
    status = H5Dwait(dset1_id, H5P_DEFAULT);
    if (status < 0) {
        fprintf(stderr, "Error with H5Dwait\n");
        ret = -1;
        goto done;
    }
    // Verify read data
    if (verify(data1_read, DIMLEN*DIMLEN, -2) != 1) {
        fprintf(stderr, "Error with dset 1 read %d/%d\n", data1_read[i], i);
        ret = -1;
        goto done;
    }
    else
        printf("Succeed with R1'\n");

    // R0'
    status = H5Dread(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, dxpl_id, data0_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        ret = -1;
        goto done;
    }
    status = H5Dwait(dset0_id, H5P_DEFAULT);
    if (status < 0) {
        fprintf(stderr, "Error with H5Dwait\n");
        ret = -1;
        goto done;
    }
    // Verify read data
    if (verify(data0_read, DIMLEN*DIMLEN, -1) != 1) {
        fprintf(stderr, "Error with dset 0 read %d/%d\n", data0_read[i], i);
        ret = -1;
        goto done;
    }
    else
        printf("Succeed with R0'\n");

    H5Pclose(async_fapl);
    H5Pclose(dxpl_id);
    H5Sclose(mspace_id);
    H5Sclose(fspace_id);
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

    MPI_Finalize();
    return ret;
}
