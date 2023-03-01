#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "mpi.h"
#include "hdf5.h"

#define MAXDSETS 1024
#define NSEG 4
#define DATALEN 1024
/* #define NSEG 128 */
/* #define DATALEN 33554432 */

int main(int argc, char *argv[])
{
    int proc_num, my_rank;
    int i, j, alt;
    int write_data[DATALEN], read_data[DATALEN];
    int mpi_thread_lvl_provided = -1;
    char *fname = "test.h5";
    char dname[128];

    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_thread_lvl_provided);
    assert(MPI_THREAD_MULTIPLE == mpi_thread_lvl_provided);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    if (argc > 1)
        fname = argv[1];

    if (proc_num > MAXDSETS) {
        if (my_rank == 0)
            fprintf(stderr, "More ranks than expected %d/%d\n", proc_num, MAXDSETS);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    if (my_rank == 0)
        fprintf(stderr, "Writing to [%s]\n", fname);

    for (i = 0; i < DATALEN; i++) {
        /* write_data[i] = my_rank*DATALEN + i; */
        write_data[i] = i;
        read_data[i] = -1;
    }


    hid_t fid, fapl, dcpl, dxpl, fspace;
    /* hid_t dset_ids[NSEG]; */
    hid_t mdset_ids[MAXDSETS];
    hid_t mem_type_ids[NSEG];
    hid_t mem_space_ids[NSEG];
    hid_t file_space_ids[NSEG];
    hsize_t dset_dims[1];
    hsize_t mem_start[1], mem_count[1], file_start[1], file_count[1];
    herr_t status;
    hbool_t op_failed;
    size_t  num_in_progress;
    hid_t   es_id = H5EScreate();
    
    fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    H5Pset_alignment(fapl, 1024, 16777216);
    H5Pset_coll_metadata_write(fapl, 1);

    dcpl = H5Pcreate(H5P_DATASET_CREATE);

    dxpl = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(dxpl, H5FD_MPIO_COLLECTIVE);

    fid = H5Fcreate_async(fname, H5F_ACC_TRUNC, H5P_DEFAULT, fapl, es_id);
    if (fid < 0) {
        fprintf(stderr, "Error with file create\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    dset_dims[0] = DATALEN;
    fspace = H5Screate_simple(1, dset_dims, NULL);

    for (i = 0; i < proc_num; i++) {
        sprintf(dname, "dset%d", i);
        mdset_ids[i] = H5Dcreate_async(fid, dname, H5T_NATIVE_INT, fspace, H5P_DEFAULT, dcpl, H5P_DEFAULT, es_id);
        if (mdset_ids[i] < 0) {
            fprintf(stderr, "Error with dset %d create\n", i);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
    }

    for (i = 0; i < NSEG; i++) {
        alt = i;
        /* alt = (i+2) % NSEG; */
        /* if (i < NSEG/2) */
        /*     alt = i*2; */
        /* else */
        /*     alt = (i%(NSEG/2))*2+1; */
        /* fprintf(stderr, "Rank %d: i %d, alt %d\n", my_rank, i, alt); */

        mem_type_ids[i] = H5T_NATIVE_INT;

        mem_space_ids[i] = H5Screate_simple(1, dset_dims, NULL);
        mem_start[0] = DATALEN / NSEG / proc_num * i + DATALEN / proc_num * my_rank;
        mem_count[0] = DATALEN / NSEG / proc_num;
        H5Sselect_hyperslab(mem_space_ids[i], H5S_SELECT_SET, mem_start, NULL, mem_count, NULL);

        file_space_ids[i] = H5Screate_simple(1, dset_dims, NULL);
        file_start[0] = DATALEN / NSEG / proc_num * alt + DATALEN / proc_num * my_rank;
        file_count[0] = DATALEN / NSEG / proc_num;
        H5Sselect_hyperslab(file_space_ids[i], H5S_SELECT_SET, file_start, NULL, file_count, NULL);
        if (proc_num <= 2)
            fprintf(stderr, "Rank %d: mem %lu %lu, file %lu %lu\n", my_rank, mem_start[0], mem_count[0], file_start[0], file_count[0]);
    }

    for (i = 0; i < proc_num; i++) {
        for (j = 0; j < NSEG; j++) {
            status = H5Dwrite_async(mdset_ids[i], mem_type_ids[j], mem_space_ids[j], file_space_ids[j], dxpl, write_data, es_id);
            if (status < 0) {
                fprintf(stderr, "Error with dset %d seg %d write\n", i, j);
                MPI_Abort(MPI_COMM_WORLD, -1);
            }
        }
    }

    /* int *data_mdset[NSEG]; */
    /* for (i = 0; i < NSEG; i++) */
    /*     data_mdset[i] = write_data; */
    /* for (i = 0; i < proc_num; i++) { */
    /*     for (j = 0; j < NSEG; j++) { */
    /*         /1* dset_ids[i] = did; *1/ */
    /*         dset_ids[j] = mdset_ids[i]; */
    /*     } */
    /*     H5Dwrite_multi(NSEG, dset_ids, mem_type_ids, mem_space_ids, file_space_ids, dxpl, (void*)data_mdset); */
    /* } */

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (op_failed != 0) {
        fprintf(stderr, "Had failed async operations\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    for (i = 0; i < NSEG; i++) {
        alt = i;
        /* alt = (i+2) % NSEG; */
        /* if (i < NSEG/2) */
        /*     alt = i*2; */
        /* else */
        /*     alt = (i%(NSEG/2))*2+1; */
        /* fprintf(stderr, "Rank %d: i %d, alt %d\n", my_rank, i, alt); */

        mem_type_ids[i] = H5T_NATIVE_INT;

        mem_space_ids[i] = H5Screate_simple(1, dset_dims, NULL);
        mem_start[0] = DATALEN / NSEG * i;
        mem_count[0] = DATALEN / NSEG;
        H5Sselect_hyperslab(mem_space_ids[i], H5S_SELECT_SET, mem_start, NULL, mem_count, NULL);

        file_space_ids[i] = H5Screate_simple(1, dset_dims, NULL);
        file_start[0] = DATALEN / NSEG * alt;
        file_count[0] = DATALEN / NSEG;
        H5Sselect_hyperslab(file_space_ids[i], H5S_SELECT_SET, file_start, NULL, file_count, NULL);
        if (proc_num <= 2)
            fprintf(stderr, "Read Rank %d: mem %lu %lu, file %lu %lu\n", my_rank, mem_start[0], mem_count[0], file_start[0], file_count[0]);
    }


    for (j = 0; j < NSEG; j++) {
        status = H5Dread_async(mdset_ids[my_rank], mem_type_ids[j], mem_space_ids[j], file_space_ids[j], dxpl, read_data, es_id);
        if (status < 0) {
            fprintf(stderr, "Error with dset %d seg %d write\n", i, j);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
    }

    for (i = 0; i < NSEG; i++) {
        H5Sclose(mem_space_ids[i]);
        H5Sclose(file_space_ids[i]);
    }

    H5Pclose(fapl);
    H5Pclose(dcpl);

    for (i = 0; i < proc_num; i++) {
        status = H5Dclose_async(mdset_ids[i], es_id);
        if (status < 0) {
            fprintf(stderr, "Error with dset close\n");
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
    }

    status = H5Fclose_async(fid, es_id);
    if (status < 0) {
        fprintf(stderr, "Error with file close\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (op_failed != 0) {
        fprintf(stderr, "Had failed async operations\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    /* if (my_rank == 0) { */
    /*     fprintf(stderr, "\n"); */
    /*     for (i = 0; i < DATALEN; i++) { */
    /*         fprintf(stderr, " %d", read_data[i]); */
    /*     } */
    /*     fprintf(stderr, "\n"); */
    /* } */

    MPI_Barrier(MPI_COMM_WORLD);

    for (i = 0; i < DATALEN; i++) {
        if (read_data[i] != write_data[i]) {
            fprintf(stderr, "Rank %d, Error with read data %d: %d/%d\n", my_rank, i, read_data[i], write_data[i]);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
    }

    H5ESclose(es_id);

    MPI_Finalize();
    return 0;
}
