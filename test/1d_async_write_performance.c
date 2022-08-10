/************************************************************

  This  example shows how to write and read a hyperslab.  It
  is derived from the h5_read.c and h5_write.c examples in
  the "Introduction to HDF5". It works on the 3 dimensional data.

 ************************************************************/

#include "hdf5.h"
#include <string.h>
#include <stdlib.h> // for strtol()
#include <time.h>   // for time()
#include <unistd.h> // for sleep()

#define FILE        "sds.h5"
#define DATASETNAME "IntArray"

#define RANK     1 // since it will work on  2 dimensional data
#define RANK_OUT 1

//#define X 262144  // 4 byte*256*1024=1 MB

//#define X 256

void
drawBorder()
{
    fprintf(stderr, "\n");
    for (int i = 0; i < 60; i++) {
        fprintf(stderr, "#");
    }
    fprintf(stderr, "\n");
}

int
main(int argc, char **argv)
{
    hsize_t dimsf[1]; /* dataset dimensions */
    int     size, counter;
    int     X = 256;
    long    arg1, arg2;
    fprintf(stderr, "argc=%d", argc);
    // return 1;

    if (argc == 3) {
        char *p;

        arg1 = strtol(argv[1], &p, 10);
        arg2 = strtol(argv[2], &p, 10);
        size = arg1;

        counter = arg2;
    }
    else {

        fprintf(stderr, "Need two values for size and counter values\n");
        return 0;
    }
    size = size * X;
    fprintf(stderr, "start=%d\n", size); // 4 byte*256*1024=1 MB
    int data[size * counter];            /* data to write */
                                         // 0-262143 262144- 2*

    // 2 parameters
    // size=1KB count=50

    // size=2KB count=50

    // size=1024KB count=50

    /*
     * Data  and output buffer initialization.
     */
    hid_t   file, dataset; /* handles */
    hid_t   dataspace;
    hid_t   memspace;
    hsize_t dimsm[1];    /* memory space dimensions 1D*/
    hsize_t dims_out[1]; /* dataset dimensions 1D */
    herr_t  status;
    hid_t   property_list_id_MPIO; /* property list identifier */
    hid_t   data_transfer_propertylist;

    int data_out[counter * size]; // data out 1d is 60

    hsize_t count[1];     /* size of the hyperslab in the file */
    hsize_t offset[1];    /* hyperslab offset in the file */
    hsize_t count_out[1]; /* size of the hyperslab in memory */
    hsize_t offset_out[1];

    int     i, j, k, status_n, rank;
    int     print_dbg_msg = 1;
    hbool_t op_failed;
    size_t  num_in_progress;
    hid_t   es_id = H5EScreate();
    int     provided;
    /*
     * MPI variables
     */
    int      mpi_size, mpi_rank;
    MPI_Comm comm = MPI_COMM_WORLD;
    MPI_Info info = MPI_INFO_NULL;
    double   time1, time2, duration, global;

    /*
     * Initialize MPI
     */
    // MPI_Init(&argc, &argv);
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);

    property_list_id_MPIO = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(property_list_id_MPIO, comm, info);

    data_transfer_propertylist = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(data_transfer_propertylist, H5FD_MPIO_COLLECTIVE);

    // 1d data
    int l = 0;

    for (i = 0; i < size * counter; i++) {
        data[i] = l;
        l++;
    }

    /*
    if(mpi_rank==0){

         for (i = 0; i < size*counter; i++)
            fprintf(stderr,"%5d", data[i]);
        fprintf(stderr,"\n");

        }   */

    // 1d operations

    file = H5Fcreate_async(FILE, H5F_ACC_TRUNC, H5P_DEFAULT, property_list_id_MPIO, es_id);

    dimsf[0]  = size * counter;
    dataspace = H5Screate_simple(RANK, dimsf, NULL);
    dimsm[0]  = size * counter;
    memspace  = H5Screate_simple(RANK_OUT, dimsm, NULL); // RANK_OUT=3
    dataset   = H5Dcreate_async(file, DATASETNAME, H5T_STD_I32BE, dataspace, H5P_DEFAULT, H5P_DEFAULT,
                              H5P_DEFAULT, es_id);

    int p = 0;

    int array[counter][size];
    for (int i = 0; i < counter; i++) {
        array[0 + i][0] = 0 + i * size;
        array[0 + i][1] = size;
    }

    /* for(int i=0;i<counter;i++){
        fprintf(stderr,"start=%d count=%d\n",array[0+i][0],array[0+i][1]);
    } */
    /* for (i = 0; i < 8; i++){
             for (j = 0; j < 2; j++)
                 fprintf(stderr," %4d ", array[0][i][j]);
         fprintf(stderr,"\n");
         } */

    /* int array[][4][2]={
               {{0,5},
                 {5,3},
                 {8,2},   //testcase 1
                 {10,10}
               }
   }; */

    if (mpi_rank == 0) {
        time1 = MPI_Wtime();

        for (int i = 0; i < counter; i++) {

            offset[0] = array[i][0];
            count[0]  = array[i][1];

            status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);

            // printf("MPI rank=%d Hyperslab operation on dataspace using offset  %llu and count %llu
            // \n",mpi_rank,offset[0],count[0]);
            offset_out[0] = array[i][0];
            count_out[0]  = array[i][1];
            // fprintf(stderr,"%lld=%lld %lld=%lld\n",offset[0],offset_out[0],count[0],count_out[0]);
            status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);

            // fprintf(stderr,"-----------------  on memory space using offset  %llu and count %llu
            // \n",offset_out[0],count_out[0]);

            status = H5Dwrite_async(dataset, H5T_NATIVE_INT, memspace, dataspace, data_transfer_propertylist,
                                    data, es_id);
        }

        status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
        if (status < 0) {
            fprintf(stderr, "Error with H5ESwait\n");
        }

        time2    = MPI_Wtime();
        duration = time2 - time1;

        printf("Runtime is  %f \n", duration);
    }

    if (print_dbg_msg)
        fprintf(stderr, "before H5ESwait done\n");

    /* status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");

    } */
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");

    MPI_Barrier(comm);

    if (mpi_rank == 0) {

        status = H5Dread_async(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_out, es_id);

        fprintf(stderr, "\nData out from the file\n");

        status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
        if (status < 0) {
            fprintf(stderr, "Error with H5ESwait\n");
        }
        for (i = 0; i < size * counter; i++)
            if (data_out[i] != i) {
                fprintf(stderr, "  Wrong value=%d,%4d ", i, data_out[i]);
                fprintf(stderr, "\n");
            }
    }
    /*
     * Close/release resources.
     */
    H5Sclose(dataspace);
    // fprintf(stderr,"dataspace closed\n");

    H5Sclose(memspace);
    // fprintf(stderr,"memspace closed\n");

    status = H5Dclose_async(dataset, es_id);
    if (status < 0) {
        fprintf(stderr, "Closing dataset failed\n");
        // ret = -1;
    }

    status = H5Fclose_async(file, es_id);
    if (status < 0) {
        fprintf(stderr, "Closing file failed\n");
        // ret = -1;
    }
    // H5Fclose(file);
    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        // ret = -1;
    }

    status = H5ESclose(es_id);
    if (status < 0) {
        fprintf(stderr, "Can't close second event set\n");
        // ret = -1;
    }
    H5Pclose(property_list_id_MPIO);
    H5Pclose(data_transfer_propertylist);
    MPI_Barrier(comm);
    MPI_Finalize();

    return 0;
}
