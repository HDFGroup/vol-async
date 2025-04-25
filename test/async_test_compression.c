#include <hdf5.h>
#include <stdio.h>

#define DIM0 10
#define DIM1 10

int
main()
{
    hid_t   fid, dspace, dset, dcpl, es_id;
    hsize_t dims[2];
    int     ret = 0, data[DIM0][DIM1];
    herr_t  status;
    hbool_t op_failed;
    size_t  num_in_progress;

    for (int i = 0; i < DIM0; i++)
        for (int j = 0; j < DIM1; j++)
            data[i][j] = i * j;

    es_id = H5EScreate();

    fid = H5Fcreate_async("async_compressed_dset.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT, es_id);
    if (fid < 0) {
        fprintf(stderr, "Error with file create\n");
        ret = -1;
        goto done;
    }

    dims[0] = DIM0;
    dims[1] = DIM1;
    dspace  = H5Screate_simple(2, dims, NULL);

    dcpl = H5Pcreate(H5P_DATASET_CREATE);
    H5Pset_chunk(dcpl, 2, dims);
    H5Pset_deflate(dcpl, 9);

    dset =
        H5Dcreate_async(fid, "compress_dset", H5T_NATIVE_INT, dspace, H5P_DEFAULT, dcpl, H5P_DEFAULT, es_id);
    if (dset < 0) {
        fprintf(stderr, "Error with dset create\n");
        ret = -1;
        goto done;
    }

    status = H5Dwrite_async(dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with dset write\n");
        ret = -1;
        goto done;
    }

    H5Pclose(dcpl);
    H5Sclose(dspace);
    H5Dclose_async(dset, es_id);
    H5Fclose_async(fid, es_id);

    printf("Wait for async\n");
    fflush(stdout);

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        ret = -1;
        goto done;
    }

    printf("Wait done\n");
    fflush(stdout);

    H5ESclose(es_id);

done:
    return ret;
}
