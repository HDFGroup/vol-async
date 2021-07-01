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

    char   file_name[128];
    char * grp_name = "Group", *fpath;
    int *  write_data, attr_data0, attr_data1;
    int    i;
    herr_t status;
    hid_t  async_fapl;
    int    ifile, nfile = 3, sleeptime = 2;

    fpath = ".";
    if (argc >= 2)
        fpath = argv[1];
    if (argc >= 3)
        sleeptime = atoi(argv[2]);

    write_data = malloc(sizeof(int) * DIMLEN * DIMLEN);

    hsize_t ds_size[2] = {DIMLEN, DIMLEN};
    hsize_t attr_size  = 1;

    async_fapl = H5Pcreate(H5P_FILE_ACCESS);
    async_dxpl = H5Pcreate(H5P_DATASET_XFER);

    dspace_id  = H5Screate_simple(2, ds_size, NULL);
    attr_space = H5Screate_simple(1, &attr_size, NULL);

    gettimeofday(&ts, 0);
    for (ifile = 0; ifile < nfile; ifile++) {

        gettimeofday(&t0, 0);

        printf("Compute/sleep for %d seconds...\n", sleeptime);
        fflush(stdout);
        sleep(sleeptime);

        sprintf(file_name, "%s/test_%d.h5", fpath, ifile);
        file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl);
        if (file_id < 0) {
            fprintf(stderr, "Error with file create\n");
            goto done;
        }
        if (print_dbg_msg) {
            printf("Create file [%s]\n", file_name);
            fflush(stdout);
        }

        grp_id = H5Gcreate(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (grp_id < 0) {
            fprintf(stderr, "Error with group create\n");
            goto done;
        }

        dset0_id =
            H5Dcreate(grp_id, "dset0", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (dset0_id < 0) {
            fprintf(stderr, "Error with dset0 create\n");
            goto done;
        }

        dset1_id =
            H5Dcreate(grp_id, "dset1", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (dset1_id < 0) {
            fprintf(stderr, "Error with dset1 create\n");
            goto done;
        }

        gettimeofday(&t2, 0);

        attr0 = H5Acreate(dset0_id, "attr_0", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT);
        attr1 = H5Acreate(dset1_id, "attr_1", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT);

        attr_data0 = 123456;
        attr_data1 = -654321;
        H5Awrite(attr0, H5T_NATIVE_INT, &attr_data0);
        H5Awrite(attr1, H5T_NATIVE_INT, &attr_data1);
        H5Aclose(attr0);
        H5Aclose(attr1);

        for (i = 0; i < DIMLEN * DIMLEN; ++i)
            write_data[i] = ifile * DIMLEN * DIMLEN + i;

        gettimeofday(&t3, 0);

        status = H5Dwrite(dset0_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, write_data);
        if (status < 0) {
            fprintf(stderr, "Error with dset 0 write\n");
            goto done;
        }
        if (print_dbg_msg) {
            printf("Write dset 0\n");
            fflush(stdout);
        }

        /* for(i = 0; i < DIMLEN*DIMLEN; ++i) */
        /*     write_data[i] *= -1; */

        status = H5Dwrite(dset1_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, write_data);
        if (status < 0) {
            fprintf(stderr, "Error with dset 0 write\n");
            goto done;
        }
        if (print_dbg_msg) {
            printf("Write dset 1\n");
            fflush(stdout);
        }

        gettimeofday(&t1, 0);

        H5Dclose(dset0_id);
        H5Dclose(dset1_id);
        H5Gclose(grp_id);
        H5Fclose(file_id);

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

    gettimeofday(&t1, 0);
    H5close();

    gettimeofday(&te, 0);
    e1 = ((te.tv_sec - ts.tv_sec) * 1000000 + te.tv_usec - ts.tv_usec) / 1000000.0;
    e2 = ((te.tv_sec - t1.tv_sec) * 1000000 + te.tv_usec - t1.tv_usec) / 1000000.0;
    printf("Total execution time: %f\n", e1);
    printf("Finalize time: %f\n", e2);

done:
    if (write_data != NULL)
        free(write_data);

    return 0;
}
