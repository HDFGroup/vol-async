/************************************************************
  
  This example shows how to write and read a hyperslab.  It 
  is derived from the h5_read.c and h5_write.c examples in 
  the "Introduction to HDF5". It works on the 1 dimensional data.

 ************************************************************/
 
#include "hdf5.h"


#define FILE        "sds1d.h5"
#define FILE1D      "sds1d.h5"
#define DATASETNAME "IntArray" 

#define RANK1D 1
#define RANK1D_OUT 1

#define N 40
#define N1 25
#define N2 15



int
main (int argc, char **argv)
{
    //hsize_t     dimsf[2];              /* dataset dimensions */
    hsize_t     dimsf1d[1]; 
    //int         data[X][Y];            /* data to write */

    int data1d[N];

    /* 
     * Data  and output buffer initialization. 
     */
    hid_t       file, dataset,file1d;         /* handles */
    hid_t       dataspace;   
    hid_t       memspace; 
    hsize_t     dimsm1d[1];              /* memory space dimensions 1D*/
    hsize_t     dims_out1d[1];           /* dataset dimensions 1D */      
    herr_t      status;      
    hid_t	mpio_plist_id;                 /* property list identifier */                       

    
    int data_out1d[N];   //data out 1d is 10
    int data_out_1d[N];  //data out _1d is 10  
     
    hsize_t     count1d[1];              /* size of the hyperslab in the file */
    hsize_t    offset1d[1];             /* hyperslab offset in the file */
    hsize_t     count_out1d[1];          /* size of the hyperslab in memory */
    hsize_t    offset_out1d[1];

    int         i, j, k, status_n, rank;
    int print_dbg_msg=1;
    hbool_t op_failed;
    size_t  num_in_progress;
    hid_t   es_id = H5EScreate();
    int provided;
     /*
     * MPI variables
     */
    int mpi_size, mpi_rank;
    MPI_Comm comm  = MPI_COMM_WORLD;
    MPI_Info info  = MPI_INFO_NULL;

    /*
     * Initialize MPI
     */
    //MPI_Init(&argc, &argv);
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);  


    mpio_plist_id = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(mpio_plist_id, comm, info);


   if(mpi_rank==0){
   
        printf("MPI rank=%d",mpi_rank);
   
            
        //1d data
        
        for (i = 0; i < N; i++)
            data1d[i] = i;   //data1d=[0,1,....,9]
            
    
        
        //1d operations
        
        file1d = H5Fcreate_async (FILE1D, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT,es_id);  
        /*
        * Describe the size of the array and create the data space for fixed
        * size dataset. 
        */
        

        dimsf1d[0]=N;
        dataspace = H5Screate_simple (RANK1D, dimsf1d, NULL); 
        
        
        dataset = H5Dcreate_async (file1d, DATASETNAME, H5T_STD_I32BE, dataspace,H5P_DEFAULT,H5P_DEFAULT, H5P_DEFAULT,es_id);   //H5T_STD_I32BE = 32-bit big-endian signed integers

        
        status = H5Dwrite_async (dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                        H5P_DEFAULT, data1d,es_id);  //H5T_NATIVE_INT = C-style int
    
        
    
        status = H5Dread_async (dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                    H5P_DEFAULT, data_out1d,es_id);

        
        status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
        if (status < 0) {
            fprintf(stderr, "Error with H5ESwait\n");
            
        }
        if (print_dbg_msg)
            fprintf(stderr, "H5ESwait done\n");

        

        printf ("First 1D Data:\n ");
        
        
        for (i = 0; i < N; i++) printf("%d ", data_out1d[i]);
        printf("\n ");
        

        H5Sclose (dataspace);
        status = H5Dclose_async(dataset, es_id);
        if (status < 0) {
            fprintf(stderr, "Closing dataset failed\n");
            //ret = -1;
        }
        
        status = H5Fclose_async(file1d, es_id);
        if (status < 0) {
            fprintf(stderr, "Closing file failed\n");
            //ret = -1;
        } 
        //H5Fclose(file);
        status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
        if (status < 0) {
            fprintf(stderr, "Error with H5ESwait\n");
            //ret = -1;
        } 
        
    }
  /*************************************************************  

  This reads the hyperslab from the sds1d.h5 file just 
  created, into a 1-dimensional plane of the 1-dimensional 
  array.

 ************************************************************/  
    
  //for 1d data

    
     
    for (i = 0; i < N; i++) {
	    
		data_out_1d[i] = 0;
	}
    /*
     * Open the file and the dataset.
     */
    file1d = H5Fopen_async (FILE1D, H5F_ACC_RDONLY, mpio_plist_id,es_id); //H5F_ACC_RDONLY= An existing file is opened with read-only access. 
                                                        //If the file does not exist, H5Fopen fails. (Default)
    dataset = H5Dopen_async(file1d, DATASETNAME,H5P_DEFAULT,es_id);  //#define DATASETNAME "IntArray" 

    dataspace = H5Dget_space(dataset);    /* dataspace handle */
    rank      = H5Sget_simple_extent_ndims (dataspace);
    status_n  = H5Sget_simple_extent_dims (dataspace, dims_out1d, NULL);

     /*
     * 0 1 2 3 4 5 6 7 8 9.....
     */
    if(mpi_rank==0){
        
        dimsm1d[0] = N1;
    
   
        memspace = H5Screate_simple (RANK1D_OUT, dimsm1d, NULL);   //RANK1D_OUT=1
        offset1d[0] = 0;   // select from 0 and add 3 elements
        count1d[0]  = 3;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset1d, NULL, count1d, NULL);  
        
        
        offset_out1d[0] = 0;  // select from 0 and add 3 elements
        count_out1d[0]  = 3; //count_out=3  
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out1d, NULL, 
                                    count_out1d, NULL);
        
        
        
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace,H5P_DEFAULT, data_out_1d,es_id);

        
        //  left middle 
        
        offset1d[0] = 3; // select from 3 and add 2 elements
        count1d[0]  = 2;
        
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset1d, NULL, count1d, NULL);  
        
        offset_out1d[0] = 3;   // select from 3 and add 2 elements
        count_out1d[0]  = 2;  
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out1d, NULL, count_out1d, NULL);
        
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace,H5P_DEFAULT, data_out_1d,es_id);


        
        
        
        offset1d[0] = 5;   // select from 5 and add 5 elements
        count1d[0]  = 5;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset1d, NULL, count1d, NULL);  
        
        
        offset_out1d[0] = 5;    // select from 5 and add 5 elements
        count_out1d[0]  = 5;  
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out1d, NULL, count_out1d, NULL);
        
        
        
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace, H5P_DEFAULT, data_out_1d,es_id);
        
        
        offset1d[0] = 10;   // select from 10 and add 5 elements
        count1d[0]  = 5;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset1d, NULL, count1d, NULL);  
                
        offset_out1d[0] = 10;  // select from 10 and add 5 elements
        count_out1d[0]  = 5;  
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out1d, NULL, 
                                    count_out1d, NULL);
        
        
        
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace,
                        H5P_DEFAULT, data_out_1d,es_id);
        
        
        
        offset1d[0] = 15;   // select from 15 and add 5 elements
        count1d[0]  = 5;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset1d, NULL, count1d, NULL);  
       
        
        offset_out1d[0] = 15;
        count_out1d[0]  = 5; // select from 15 and add 5 elements
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out1d, NULL, count_out1d, NULL);
        
        
        
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace,H5P_DEFAULT, data_out_1d,es_id);

        
        
        offset1d[0] = 20;   // select from 20 and add 5 elements
        count1d[0]  = 5;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset1d, NULL, count1d, NULL);  
       
        
        offset_out1d[0] = 20;
        count_out1d[0]  = 5; // select from 20 and add 5 elements
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out1d, NULL, count_out1d, NULL);
        
        
        
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace, H5P_DEFAULT, data_out_1d,es_id);
    }
    
    
   if(mpi_rank==1){
    //right middle

        dimsm1d[0] = N2;
    
   
        memspace = H5Screate_simple (RANK1D_OUT, dimsm1d, NULL);   //RANK1D_OUT=1
 
        offset1d[0] = 25;  // select from 25 and add 3 elements
        count1d[0]  = 3;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset1d, NULL, count1d, NULL);  
        
        offset_out1d[0] = 0; // select from 25 and add 3 elements
        count_out1d[0]  = 3;
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out1d, NULL, 
                                    count_out1d, NULL);
        
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace,H5P_DEFAULT, data_out_1d,es_id);
        
            
        
       
        
        offset1d[0] = 28;   // select from 28 and add 2 elements
        count1d[0]  = 2;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset1d, NULL, count1d, NULL);  
        
        offset_out1d[0] = 3;  // select from 3 and add 2 elements
        count_out1d[0]  = 2;
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out1d, NULL, 
                                    count_out1d, NULL);
       
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace,H5P_DEFAULT, data_out_1d,es_id);

        
        
        offset1d[0] = 30;   // select from 30 and add 10 elements
        count1d[0]  = 10;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset1d, NULL, count1d, NULL);  
        
        offset_out1d[0] = 5;  // select from 5 and add 10 elements
        count_out1d[0]  = 10;
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out1d, NULL, 
                                    count_out1d, NULL);
      
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace,H5P_DEFAULT, data_out_1d,es_id);
        


        
   }

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");

    
    if(mpi_rank==1){
    printf ("MPI rank=%d Data from rank 1:\n ",mpi_rank);
    printf("select from 0 and add 3 elements\n ");
    printf("select from 3 and add 2 elements\n ");
    printf("select from 5 and add 10 elements\n ");

    for (i = 0; i < N2; i++) printf("%d ", data_out_1d[i]);
	printf("\n ");
    }
    //0 0 0 0 0 5 6 7 8 9
    MPI_Barrier(comm);
    if(mpi_rank==0){
        printf ("MPI rank=%d Data from rank 0:\n ",mpi_rank);
        printf("select from 0 and add 3 elements\n ");
        printf("select from 3 and add 2 elements\n ");
        printf("select from 5 and add 5 elements\n ");
        printf("select from 10 and add 5 elements\n ");
        printf("select from 15 and add 5 elements\n ");
        printf("select from 20 and add 5 elements\n ");
       
        for (i = 0; i < N1; i++) printf("%d ", data_out_1d[i]);
        printf("\n");
    }
    /*
     * 0 1 2 3 4 0 0 0 0 0
     */
    
    
    
    status = H5Dclose_async(dataset, es_id);
    if (status < 0) {
        fprintf(stderr, "Closing dataset failed\n");
        
    }
   
    H5Sclose (dataspace);
    H5Sclose (memspace);

    status = H5Fclose_async(file1d, es_id);
    if (status < 0) {
        fprintf(stderr, "Closing file failed\n");
       
    }
    
    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        
    }

    //1d close
   
    status = H5ESclose(es_id);
    if (status < 0) {
        fprintf(stderr, "Can't close second event set\n");
        //ret = -1;
    }
    H5Pclose(mpio_plist_id);
    MPI_Barrier(comm);
    MPI_Finalize();

    return 0;

}     

