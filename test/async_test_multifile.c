#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include "hdf5.h"
#include "h5_async_lib.h"

/* #define DIMLEN 10 */
#define DIMLEN 1024

int print_dbg_msg = 1;

int
main(int argc, char *argv[])
{
    hid_t          file_id, grp_id, dset1_id, dset0_id, dspace_id, async_dxpl, attr_space, attr0, attr1;
    struct timeval t0;
    struct timeval t1;
    struct timeval t2;
    struct timeval t3;
    struct timeval ts;
    struct timeval te;
    double         e1, e2;

    char    file_name[128];
    char *  grp_name = "Group", *fpath;
    int *   write_data, attr_data0, attr_data1;
    int     i, ret = 0;
    herr_t  status;
    hid_t   async_fapl;
    hbool_t op_failed;
    size_t  num_in_progress;
    hid_t   es_id = H5EScreate();

    int ifile, nfile = 3, sleeptime = 1;

    fpath = ".";
    if (argc >= 2)
        fpath = argv[1];
    if (argc >= 3)
        sleeptime = atoi(argv[2]);

    write_data = malloc(sizeof(int) * DIMLEN * DIMLEN);

    hsize_t ds_size[2] = {DIMLEN, DIMLEN};
    hsize_t attr_size  = 1;

    dspace_id  = H5Screate_simple(2, ds_size, NULL);
    attr_space = H5Screate_simple(1, &attr_size, NULL);

    async_fapl = H5Pcreate(H5P_FILE_ACCESS);
    async_dxpl = H5Pcreate(H5P_DATASET_XFER);
    /* H5Pset_vol_async(async_fapl); */

    gettimeofday(&ts, 0);
    for (ifile = 0; ifile < nfile; ifile++) {

        if (ifile > 0) {
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
        }

        printf("Compute/sleep for %d seconds...\n", sleeptime);
        fflush(stdout);
        sleep(sleeptime);

        gettimeofday(&t0, 0);

        sprintf(file_name, "%s/test_%d.h5", fpath, ifile);
        file_id = H5Fcreate_async(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl, es_id);
        if (file_id < 0) {
            fprintf(stderr, "Error with file create\n");
            ret = -1;
            goto done;
        }
        if (print_dbg_msg) {
            printf("Create file [%s]\n", file_name);
            fflush(stdout);
        }

        /* H5Fset_delay_time(file_id, H5P_DEFAULT, 200); */

        grp_id = H5Gcreate_async(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
        if (grp_id < 0) {
            fprintf(stderr, "Error with group create\n");
            ret = -1;
            goto done;
        }

        dset0_id = H5Dcreate_async(grp_id, "dset0", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT,
                                   H5P_DEFAULT, es_id);
        if (dset0_id < 0) {
            fprintf(stderr, "Error with dset0 create\n");
            ret = -1;
            goto done;
        }

        dset1_id = H5Dcreate_async(grp_id, "dset1", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT,
                                   H5P_DEFAULT, es_id);
        if (dset1_id < 0) {
            fprintf(stderr, "Error with dset1 create\n");
            ret = -1;
            goto done;
        }

        /* H5Dset_delay_time(dset1_id, H5P_DEFAULT, 500); */

        gettimeofday(&t2, 0);

        attr0 =
            H5Acreate_async(dset0_id, "attr_0", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT, es_id);
        attr1 =
            H5Acreate_async(dset1_id, "attr_1", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT, es_id);

        attr_data0 = 123456;
        attr_data1 = -654321;
        H5Awrite_async(attr0, H5T_NATIVE_INT, &attr_data0, es_id);
        H5Awrite_async(attr1, H5T_NATIVE_INT, &attr_data1, es_id);
        H5Aclose_async(attr0, es_id);
        H5Aclose_async(attr1, es_id);

        for (i = 0; i < DIMLEN * DIMLEN; ++i)
            write_data[i] = ifile * DIMLEN * DIMLEN + i;

        gettimeofday(&t3, 0);

        status = H5Dwrite_async(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, write_data, es_id);
        if (status < 0) {
            fprintf(stderr, "Error with dset 0 write\n");
            ret = -1;
            goto done;
        }
        if (print_dbg_msg) {
            printf("Write dset 0\n");
            fflush(stdout);
        }

        /* for(i = 0; i < DIMLEN*DIMLEN; ++i) */
        /*     write_data[i] *= -1; */

        status = H5Dwrite_async(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, write_data, es_id);
        if (status < 0) {
            fprintf(stderr, "Error with dset 0 write\n");
            ret = -1;
            goto done;
        }
        if (print_dbg_msg) {
            printf("Write dset 1\n");
            fflush(stdout);
        }

        gettimeofday(&t1, 0);

        H5Dclose_async(dset0_id, es_id);
        H5Dclose_async(dset1_id, es_id);
        H5Gclose_async(grp_id, es_id);

        H5Fclose_async(file_id, es_id);

        e1 = ((t1.tv_sec - t3.tv_sec) * 1000000 + t1.tv_usec - t3.tv_usec) / 1000000.0;
        printf("  Observed write dset time: %f\n", e1);

        e2 = ((t3.tv_sec - t2.tv_sec) * 1000000 + t3.tv_usec - t2.tv_usec) / 1000000.0;
        printf("  Observed write attr time: %f\n", e2);

        e1 = ((t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec) / 1000000.0;
        printf("  Observed total write time: %f\n", e1);
    }

    H5Sclose(dspace_id);
    H5Sclose(attr_space);
    H5Pclose(async_fapl);
    H5Pclose(async_dxpl);

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
    H5ESclose(es_id);

    gettimeofday(&t1, 0);

    gettimeofday(&te, 0);
    e1 = ((te.tv_sec - ts.tv_sec) * 1000000 + te.tv_usec - ts.tv_usec) / 1000000.0;
    e2 = ((te.tv_sec - t1.tv_sec) * 1000000 + te.tv_usec - t1.tv_usec) / 1000000.0;
    printf("Total execution time: %f\n", e1);
    printf("Finalize time: %f\n", e2);
done:
    if (write_data != NULL)
        free(write_data);

    return ret;
}
