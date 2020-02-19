#include <stdio.h>
#include <stdlib.h>
#include "hdf5.h"
#include "h5_vol_external_async_native.h"

#define DIMLEN 8192

int print_dbg_msg = 1;

int main(int argc, char *argv[])
{
    hid_t file_id, grp_id, dset1_id, dset0_id, dspace_id, mspace_id, async_dxpl;
    const char *file_name = "async_test_serial.h5";
    const char *grp_name  = "Group";
    int        *data0_write, *data0_read, *data1_write, *data1_read;
    int        i;
    hsize_t    ds_size[2] = {DIMLEN, DIMLEN};
    herr_t     status;
    hid_t      async_fapl;
    int        sleeptime = 100, token_status;
    H5RQ_token_t token1, token2, token3, token4, token5, token6, token7, token8;

    printf("This test is for experimantal purposes, and may not be working now!\n");
    
    async_fapl = H5Pcreate (H5P_FILE_ACCESS);
    async_dxpl = H5Pcreate (H5P_DATASET_XFER);
    
    H5Pset_vol_async(async_fapl);
    H5Pset_dxpl_async(async_dxpl, true);

    if (print_dbg_msg) printf("H5Fcreate start\n");
    fflush(stdout);

    file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl);
    if (file_id < 0) {
        fprintf(stderr, "Error with file create\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Fcreate done\n");
    fflush(stdout);


    if (print_dbg_msg) printf("H5Gcreate start\n");
    fflush(stdout);
    grp_id = H5Gcreate(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (grp_id < 0) {
        fprintf(stderr, "Error with group create\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Gcreate done\n");
    fflush(stdout);

    data0_write = malloc (sizeof(int)*DIMLEN*DIMLEN);
    data1_write = malloc (sizeof(int)*DIMLEN*DIMLEN);
    data0_read  = malloc (sizeof(int)*DIMLEN*DIMLEN);
    data1_read  = malloc (sizeof(int)*DIMLEN*DIMLEN);
    for(i = 0; i < DIMLEN*DIMLEN; ++i) {
        data0_write[i] = i;
        data1_write[i] = i*2;
    }

    dspace_id = H5Screate_simple(2, ds_size, NULL); 

    if (print_dbg_msg) printf("H5Dcreate 0 start\n");
    fflush(stdout);
    dset0_id  = H5Dcreate(grp_id,"dset0",H5T_NATIVE_INT,dspace_id,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT);
    if (dset0_id < 0) {
        fprintf(stderr, "Error with dset0 create\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dcreate 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    if (print_dbg_msg) printf("H5Dcreate 1 start\n");
    fflush(stdout);
    dset1_id  = H5Dcreate(grp_id,"dset1",H5T_NATIVE_INT,dspace_id,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT);
    if (dset1_id < 0) {
        fprintf(stderr, "Error with dset1 create\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dcreate 1 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */

    /* H5Fwait(file_id); */

    // W0, R0, W1, R1, W1', W0', R0', R1'
    if (print_dbg_msg) printf("H5Dwrite 0 start\n");
    fflush(stdout);
    status = H5Dwrite_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_write, &token1);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dwrite 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    H5RQ_token_check(token1, &token_status);
    printf("H5Dwrite 0 token status: %d\n", token_status);
    H5RQ_token_wait(token1);
    printf("H5Dwrite 0 token wait\n");
    H5RQ_token_check(token1, &token_status);
    printf("H5Dwrite 0 token status: %d\n", token_status);
    H5RQ_token_free(token1);


    if (print_dbg_msg) printf("H5Dread 0 start\n");
    fflush(stdout);
    status = H5Dread_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_read, &token2);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dread 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    H5RQ_token_check(token2, &token_status);
    printf("H5Dread 0 token status: %d\n", token_status);
    H5RQ_token_wait(token2);
    printf("H5Dread 0 wait\n");
    H5RQ_token_check(token2, &token_status);
    printf("H5Dread 0 token status: %d\n", token_status);
    H5RQ_token_free(token2);

    /* if (print_dbg_msg) printf("Start H5Dwait\n"); */
    /* fflush(stdout); */
    /* H5Dwait(dset0_id); */
    /* if (print_dbg_msg) printf("Done H5Dwait\n"); */
    /* fflush(stdout); */
    // Verify read data
    for(i = 0; i < DIMLEN*DIMLEN; ++i) {
        if (data0_read[i] != i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data0_read[i], i);
            break;
        }
    }
    printf("Finished verification\n");

    if (print_dbg_msg) printf("H5Dwrite 1 start\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    status = H5Dwrite_async(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_write, &token3);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 write\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dwrite 1 done\n");
    fflush(stdout);

    H5RQ_token_check(token3, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_wait(token3);
    printf("token wait\n");
    H5RQ_token_check(token3, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_free(token3);

    /* usleep(sleeptime); */

    if (print_dbg_msg) printf("H5Dread 1 start\n");
    status = H5Dread_async(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_read, &token4);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dread 1 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    H5RQ_token_check(token4, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_wait(token4);
    printf("token wait\n");
    H5RQ_token_check(token4, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_free(token4);


    /* if (print_dbg_msg) printf("Start H5Dwait\n"); */
    /* fflush(stdout); */
    /* H5Dwait(dset1_id); */
    /* if (print_dbg_msg) printf("Done H5Dwait\n"); */
    /* fflush(stdout); */
    // Verify read data
    for(i = 0; i < DIMLEN*DIMLEN; ++i) {
        if (data1_read[i] != 2*i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data1_read[i], i);
            break;
        }
    }
    printf("Finished verification\n");

    // Change data 0 and 1
    for(i = 0; i < DIMLEN*DIMLEN; ++i) {
        data0_write[i] *= -1;
        data1_write[i] *= -1;
    }

    if (print_dbg_msg) printf("H5Dwrite 1 start\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    status = H5Dwrite_async(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_write, &token5);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 write\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dwrite 1 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    H5RQ_token_check(token5, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_wait(token5);
    printf("token wait\n");
    H5RQ_token_check(token5, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_free(token5);


    /* H5Pset_dxpl_async_cp_limit(async_dxpl, 1); */

    if (print_dbg_msg) printf("H5Dwrite 0 start\n");
    fflush(stdout);
    status = H5Dwrite_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_write, &token6);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 write\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dwrite 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    H5RQ_token_check(token6, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_wait(token6);
    printf("token wait\n");
    H5RQ_token_check(token6, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_free(token6);

    if (print_dbg_msg) printf("H5Dread 0 start\n");
    fflush(stdout);
    status = H5Dread_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data0_read, &token7);
    if (status < 0) {
        fprintf(stderr, "Error with dset 0 read\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dread 0 done\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    H5RQ_token_check(token7, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_wait(token7);
    printf("token wait\n");
    H5RQ_token_check(token7, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_free(token7);

    if (print_dbg_msg) printf("Start H5Dwait\n");
    fflush(stdout);
    H5Dwait(dset0_id);
    if (print_dbg_msg) printf("Done H5Dwait\n");
    fflush(stdout);
    // Verify read data
    for(i = 0; i < DIMLEN*DIMLEN; ++i) {
        if (data0_read[i] != -i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data0_read[i], i);
            break;
        }
    }
    printf("Finished verification\n");

    if (print_dbg_msg) printf("H5Dread 1 start\n");
    fflush(stdout);
    /* usleep(sleeptime); */
    status = H5Dread(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data1_read);
    if (status < 0) {
        fprintf(stderr, "Error with dset 1 read\n");
        goto done;
    }
    if (print_dbg_msg) printf("H5Dread 1 done\n");
    fflush(stdout);

    /* usleep(sleeptime); */

    if (print_dbg_msg) printf("Start H5Dwait\n");
    fflush(stdout);
    H5Dwait(dset1_id);
    if (print_dbg_msg) printf("Done H5Dwait\n");
    fflush(stdout);
    // Verify read data
    for(i = 0; i < DIMLEN*DIMLEN; ++i) {
        if (data1_read[i] != -2*i) {
            fprintf(stderr, "Error with dset 0 read %d/%d\n", data1_read[i], i);
            break;
        }
    }
    printf("Finished verification\n");

    /* H5Fwait(file_id); */

    H5Pclose(async_fapl);
    H5Pclose(async_dxpl);
    H5Sclose(dspace_id);
    H5Dclose(dset0_id);
    H5Dclose(dset1_id);
    H5Gclose(grp_id);

    printf("H5Fclose\n");
    H5Fclose_async(file_id, &token8);
    H5RQ_token_check(token8, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_wait(token8);
    printf("token wait\n");
    H5RQ_token_check(token8, &token_status);
    printf("token status: %d\n", token_status);
    H5RQ_token_free(token8);


done:
    if (data0_write != NULL) 
        free(data0_write);
    if (data0_read != NULL) 
        free(data0_read);
    if (data1_write != NULL) 
        free(data1_write);
    if (data1_read != NULL) 
        free(data1_read);

    return 0;
}
