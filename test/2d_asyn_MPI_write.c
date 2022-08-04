/************************************************************

  This example shows how to write and read a hyperslab.  It
  is derived from the h5_read.c and h5_write.c examples in
  the "Introduction to HDF5". It works on the 3 dimensional data.
 ************************************************************/

#include "hdf5.h"

#define FILE        "sds.h5"
#define DATASETNAME "IntArray"

#define RANK     2 // since it will work on  2 dimensional data
#define RANK_OUT 2

#define X 4
#define Y 6

#define X1 4
#define Y1 6

#define X2 4
#define Y2 6

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
    hsize_t dimsf[2];   /* dataset dimensions */
    int     data[X][Y]; /* data to write */

    /*
     * Data  and output buffer initialization.
     */
    hid_t   file, dataset; /* handles */
    hid_t   dataspace;
    hid_t   memspace;
    hsize_t dimsm[2];    /* memory space dimensions 1D*/
    hsize_t dims_out[2]; /* dataset dimensions 1D */
    herr_t  status;
    hid_t   property_list_id_MPIO; /* property list identifier */
    hid_t   data_transfer_propertylist;

    int data_out[X][Y];    // data out 3d is 6x6x3
    int data_out1[X1][Y1]; // data out1  is 2x6x3
    int data_out2[X2][Y2]; // data out1  is 3x6x3

    hsize_t count[2];     /* size of the hyperslab in the file */
    hsize_t offset[2];    /* hyperslab offset in the file */
    hsize_t count_out[2]; /* size of the hyperslab in memory */
    hsize_t offset_out[2];

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

    // 3d data
    int l = 0;

    for (i = 0; i < X; i++)
        for (j = 0; j < Y; j++) {
            data[i][j] = l; // 4x6
            l++;
        }

    if (mpi_rank == 0) {

        for (i = 0; i < X; i++) {
            for (j = 0; j < Y; j++)
                fprintf(stderr, "%5d", data[i][j]);
            fprintf(stderr, "\n");
        }
    }

    // 2d operations

    file = H5Fcreate_async(FILE, H5F_ACC_TRUNC, H5P_DEFAULT, property_list_id_MPIO, es_id);

    dimsf[0] = X;
    dimsf[1] = Y;

    dataspace = H5Screate_simple(RANK, dimsf, NULL);

    dataset = H5Dcreate_async(file, DATASETNAME, H5T_STD_I32BE, dataspace, H5P_DEFAULT, H5P_DEFAULT,
                              H5P_DEFAULT, es_id); // H5T_STD_I32BE = 32-bit big-endian signed integers

    dimsm[0] = X1;
    dimsm[1] = Y1;

    memspace = H5Screate_simple(RANK_OUT, dimsm, NULL); // RANK_OUT=3

    /* int array[][4][4]={
       {
       {0,0,2,2},
       {0,2,2,1},
       {0,3,2,2},  //Testcase# 1
       {0,5,2,1}
      },
      {
      {0,0,2,2},
      {2,0,1,2},
      {0,3,2,2}, //Testcase# 2
      {0,5,2,1}
      },
      {
      {0,0,2,1},
      {0,1,2,2},
      {0,3,2,1},  //Testcase# 3
      {2,1,2,2}
      },
      {
      {0,1,2,3},
      {0,4,2,2},
      {2,0,2,1},  //Testcase# 4
      {2,1,2,3}
      },
      {
      {1,1,2,3},
      {1,4,2,1},
      {3,1,1,3},  //Testcase# 5
      {3,4,1,1}
      },
      {
      {1,1,2,3},
      {3,1,1,3},
      {3,4,1,1},//Testcase# 6
      {1,4,2,1},
      },
      {
      {3,1,1,3},
      {1,1,2,3},
      {3,4,1,1},//Testcase# 7
      {1,4,2,1},
      },
      {
      {3,1,1,3},
      {1,1,2,3},
      {1,5,2,1},//Testcase# 8
      {1,4,2,1},
      }
   }; */
    int array[][4][4] = {
        {{1, 1, 2, 3},
         {1, 4, 2, 1},
         {3, 1, 1, 3}, // Testcase# 5
         {3, 4, 1, 1}},
        {{1, 1, 2, 3},
         {1, 4, 2, 1},
         {3, 1, 1, 3}, // Testcase# 5
         {3, 4, 1, 1}},

    };
    int   testcases = (int)sizeof(array) / sizeof(array[0]);
    hid_t dataset_array[testcases];
    char  datasetname[testcases][12];
    // int arr[4][2];
    // fprintf(stderr, "%ld",sizeof(array)/sizeof(array[0]));
    for (int j = 0; j < testcases; j++) {

        snprintf(datasetname[j], 12, "Testcase# %d", j + 1);
        // fprintf(stderr,"dataset name= %s \n",datasetname[j]);
        dataset_array[j] = H5Dcreate_async(file, datasetname[j], H5T_STD_I32BE, dataspace, H5P_DEFAULT,
                                           H5P_DEFAULT, H5P_DEFAULT, es_id);
    }

    if (mpi_rank == 0) {

        for (int j = 0; j < testcases; j++) {
            drawBorder();
            fprintf(stderr, "dataset name= %s \n", datasetname[j]);
            drawBorder();
            for (int i = 0; i < 4; i++) {

                offset[0] = array[j][i][0];
                offset[1] = array[j][i][1];
                count[0]  = array[j][i][2];
                count[1]  = array[j][i][3];
                status    = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);

                // printf("MPI rank=%d Hyperslab operation on dataspace using offset  %llux%llu and count
                // %llux%llu \n",mpi_rank,offset[0],offset[1],count[0],count[1]);
                offset_out[0] = array[j][i][0]; // offset=0x0
                offset_out[1] = array[j][i][1];
                count_out[0]  = array[j][i][2]; // count_out=2 X 2
                count_out[1]  = array[j][i][3];

                status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);

                // printf("-----------------  on memory space using offset  %llux%llu and count %llux%llu
                // \n",offset_out[0],offset_out[1],count_out[0],count_out[1]);

                status = H5Dwrite_async(dataset_array[j], H5T_NATIVE_INT, memspace, dataspace,
                                        data_transfer_propertylist, data, es_id);
            }
            /* status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
             if (status < 0) {
                 fprintf(stderr, "Error with H5ESwait\n");

            } */
        }

        /* offset[0] = 0;
        offset[1]=0;
        count[0]  = 2;
        count[1]  = 2;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);

        printf("MPI rank=%d Hyperslab operation on dataspace using offset  %llux%llu and count %llux%llu
        \n",mpi_rank,offset[0],offset[1],count[0],count[1]); offset_out[0] = 0;  //offset=0x0 offset_out[1] =
        0; count_out[0]  = 2; //count_out=2 X 2 count_out[1]  = 2;

        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);

        printf("-----------------  on memory space using offset  %llux%llu and count %llux%llu
        \n",offset_out[0],offset_out[1],count_out[0],count_out[1]);

        status = H5Dwrite_async (dataset, H5T_NATIVE_INT, memspace, dataspace, data_transfer_propertylist,
        data,es_id);


        offset[0] = 0;
        offset[1]=2;
        count[0]  = 2;
        count[1]  = 1;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);

        printf("MPI rank=%d Hyperslab operation on dataspace using offset  %llux%llu and count %llux%llu
        \n",mpi_rank,offset[0],offset[1],count[0],count[1]); offset_out[0] = 0;  //offset=0x0 offset_out[1] =
        2; count_out[0]  = 2; //count_out=2 X 2 count_out[1]  = 1;

        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);

        printf("-----------------  on memory space using offset  %llux%llu and count %llux%llu
        \n",offset_out[0],offset_out[1],count_out[0],count_out[1]);

        status = H5Dwrite_async (dataset, H5T_NATIVE_INT, memspace, dataspace, data_transfer_propertylist,
        data,es_id);


        offset[0] = 0;
        offset[1]=3;
        count[0]  = 2;
        count[1]  = 2;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);

        printf("MPI rank=%d Hyperslab operation on dataspace using offset  %llux%llu and count %llux%llu
        \n",mpi_rank,offset[0],offset[1],count[0],count[1]); offset_out[0] = 0;  //offset=0x0 offset_out[1] =
        3; count_out[0]  = 2; //count_out=2 X 2 count_out[1]  = 2;

        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);

        printf("-----------------  on memory space using offset  %llux%llu and count %llux%llu
        \n",offset_out[0],offset_out[1],count_out[0],count_out[1]);

        status = H5Dwrite_async (dataset, H5T_NATIVE_INT, memspace, dataspace, data_transfer_propertylist,
        data,es_id);


        offset[0] = 0;
        offset[1]=5;
        count[0]  = 2;
        count[1]  = 1;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);

        printf("MPI rank=%d Hyperslab operation on dataspace using offset  %llux%llu and count %llux%llu
        \n",mpi_rank,offset[0],offset[1],count[0],count[1]); offset_out[0] = 0;  //offset=0x0 offset_out[1] =
        5; count_out[0]  = 2; //count_out=2 X 2 count_out[1]  = 1;

        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);

        printf("-----------------  on memory space using offset  %llux%llu and count %llux%llu
        \n",offset_out[0],offset_out[1],count_out[0],count_out[1]);

        status = H5Dwrite_async (dataset, H5T_NATIVE_INT, memspace, dataspace, data_transfer_propertylist,
        data,es_id);
         */
    }
    if (mpi_rank == 1) {

        offset[0] = 2;
        offset[1] = 0;

        count[0] = 2;
        count[1] = 2; // count=4x2
        status   = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);
        printf("MPI rank=%d Hyperslab operation on dataspace using offset  %llux%llu and count %llux%llu \n",
               mpi_rank, offset[0], offset[1], count[0], count[1]);

        offset_out[0] = 2;
        offset_out[1] = 0; // offset_out=2x0

        count_out[0] = 2;
        count_out[1] = 2; // count_out=4x2
        status       = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);
        printf("-----------------  on memory space using offset  %llux%llu and count %llux%llu  \n",
               offset_out[0], offset_out[1], count_out[0], count_out[1]);

        // status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace, H5P_DEFAULT,
        // data_out2,es_id);
        status = H5Dwrite_async(dataset, H5T_NATIVE_INT, memspace, dataspace, data_transfer_propertylist,
                                data, es_id);

        offset[0] = 2;
        offset[1] = 2;

        count[0] = 2;
        count[1] = 2; // count=4x2
        status   = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);
        printf("MPI rank=%d Hyperslab operation on dataspace using offset  %llux%llu and count %llux%llu \n",
               mpi_rank, offset[0], offset[1], count[0], count[1]);

        offset_out[0] = 2;
        offset_out[1] = 2; // offset_out=2x2

        count_out[0] = 2;
        count_out[1] = 2; // count_out=4x2
        status       = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);
        printf("-----------------  on memory space using offset  %llux%llu and count %llux%llu  \n",
               offset_out[0], offset_out[1], count_out[0], count_out[1]);

        // status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace, H5P_DEFAULT,
        // data_out2,es_id);
        status = H5Dwrite_async(dataset, H5T_NATIVE_INT, memspace, dataspace, data_transfer_propertylist,
                                data, es_id);

        offset[0] = 2;
        offset[1] = 4;

        count[0] = 2;
        count[1] = 2; // count=2x2
        status   = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);
        printf("MPI rank=%d Hyperslab operation on dataspace using offset  %llux%llu and count %llux%llu \n",
               mpi_rank, offset[0], offset[1], count[0], count[1]);

        offset_out[0] = 2;
        offset_out[1] = 4; // offset_out=2x4

        count_out[0] = 2;
        count_out[1] = 2; // count_out=2x2
        status       = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);
        printf("-----------------  on memory space using offset  %llux%llu and count %llux%llu  \n",
               offset_out[0], offset_out[1], count_out[0], count_out[1]);

        // status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace, H5P_DEFAULT,
        // data_out2,es_id);
        status = H5Dwrite_async(dataset, H5T_NATIVE_INT, memspace, dataspace, data_transfer_propertylist,
                                data, es_id);
    }

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");

    MPI_Barrier(comm);

    if (mpi_rank == 0) {
        // fprintf(stderr,"%d\n",testcases);
        for (int p = 0; p < testcases; p++) {
            drawBorder();
            fprintf(stderr, "Testcase= %d\n", p + 1);
            drawBorder();
            status = H5Dread_async(dataset_array[p], H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_out,
                                   es_id);

            fprintf(stderr, "\nData out from the file\n");

            status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
            if (status < 0) {
                fprintf(stderr, "Error with H5ESwait\n");
            }

            for (i = 0; i < X; i++) {
                for (j = 0; j < Y; j++)
                    fprintf(stderr, " %4d ", data_out[i][j]);
                fprintf(stderr, "\n");
            }
        }
    }

    /*
     * Close/release resources.
     */
    H5Sclose(dataspace);

    H5Sclose(memspace);

    for (int j = 0; j < testcases; j++) {

        status = H5Dclose_async(dataset_array[j], es_id);
        if (status < 0) {
            fprintf(stderr, "Closing dataset failed\n");
            // ret = -1;
        }
    }

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