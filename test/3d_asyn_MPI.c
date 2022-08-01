/************************************************************
  
  This example shows how to write and read a hyperslab.  It 
  is derived from the h5_read.c and h5_write.c examples in 
  the "Introduction to HDF5". It works on the 3 dimensional data.

 ************************************************************/
 
#include "hdf5.h"


#define FILE        "sds.h5"
#define DATASETNAME "IntArray" 

#define RANK 2   //since it will work on  3 dimensional data
#define RANK_OUT 2

#define X 6
#define Y 6

#define X1 3
#define Y1 6

#define X2 3
#define Y2 6


int
main (int argc, char **argv)
{
    hsize_t     dimsf[2];              /* dataset dimensions */
    int         data[X][Y];            /* data to write */

    
    /* 
     * Data  and output buffer initialization. 
     */
    hid_t       file, dataset;         /* handles */
    hid_t       dataspace;   
    hid_t       memspace; 
    hsize_t     dimsm[2];              /* memory space dimensions 1D*/
    hsize_t     dims_out[2];           /* dataset dimensions 1D */      
    herr_t      status;      
    hid_t	mpio_plist_id;                 /* property list identifier */                       

    
    int data_out[X][Y];   //data out 3d is 6x6x3
    int data_out1[X1][Y1];  //data out1  is 6x6x3  
    int data_out2[X2][Y2];  //data out2  is 6x6x3  
    

    hsize_t     count[2];              /* size of the hyperslab in the file */
    hsize_t    offset[2];             /* hyperslab offset in the file */
    hsize_t     count_out[2];          /* size of the hyperslab in memory */
    hsize_t    offset_out[2];

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
   
            /* 
            * Set up file access property list with parallel I/O access
            */
           //3d data
           int l=0;
           
            for (i = 0; i < X; i++)
                for (j = 0; j < X; j++)
                        {data[i][j] = l;   //6x6x6
                        l++;}
                   
              
       
           
           //3d operations
           
            file = H5Fcreate_async (FILE, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT,es_id);  
            /*
            * Describe the size of the array and create the data space for fixed
            * size dataset. 
            */
            

            dimsf[0]=X;
            dimsf[1]=Y;
           
            dataspace = H5Screate_simple (RANK, dimsf, NULL); 
            
            /*
            * Create a new dataset within the file using defined dataspace and
            * default dataset creation properties.
            */
            dataset = H5Dcreate_async (file, DATASETNAME, H5T_STD_I32BE, dataspace,
                                H5P_DEFAULT,H5P_DEFAULT, H5P_DEFAULT,es_id);   //H5T_STD_I32BE = 32-bit big-endian signed integers

            /*
            * Write the data to the dataset using default transfer properties.
            */
            status = H5Dwrite_async (dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                            H5P_DEFAULT, data,es_id);  //H5T_NATIVE_INT = C-style int
        
            
        
            status = H5Dread_async (dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                        H5P_DEFAULT, data_out,es_id);

            
            status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
            if (status < 0) {
                fprintf(stderr, "Error with H5ESwait\n");
                
            }
            if (print_dbg_msg)
                fprintf(stderr, "H5ESwait done\n");

            
            
                printf ("\nFirst 3D Data :\n ");
                
                
                for (i = 0; i < X; i++){ 
                    for (j = 0; j < Y; j++) 
                        printf("%5d", data_out[i][j]);
                printf("\n ");
                }
            
                /*
            * Close/release resources.
            */
            H5Sclose (dataspace);
            status = H5Dclose_async(dataset, es_id);
            if (status < 0) {
                fprintf(stderr, "Closing dataset failed\n");
                //ret = -1;
            }
        
        status = H5Fclose_async(file, es_id);
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

  This reads the hyperslab from the sds.h5 file just 
  created, into a 3-dimensional plane of the 3-dimensional 
  array.

 ************************************************************/  
    
  //for 3d data

    
     
   
	  for (i = 0; i < X1; i++) {
	   for (j = 0; j < Y1; j++) {
           
		data_out1[i][j] = 0;
	    }
         }
            

    
   
	  for (i = 0; i < X2; i++) {
	   for (j = 0; j < Y2; j++) {
           
		data_out2[i][j] = 0;
	    }
         }
            
    /*
     * Open the file and the dataset.
     */
    file = H5Fopen_async (FILE, H5F_ACC_RDONLY, mpio_plist_id,es_id); //H5F_ACC_RDONLY= An existing file is opened with read-only access. 
                                                        //If the file does not exist, H5Fopen fails. (Default)
    dataset = H5Dopen_async(file, DATASETNAME,H5P_DEFAULT,es_id);  //#define DATASETNAME "IntArray" 

    dataspace = H5Dget_space_async (dataset,es_id);    /* dataspace handle */
    rank      = H5Sget_simple_extent_ndims (dataspace);
    status_n  = H5Sget_simple_extent_dims (dataspace, dims_out, NULL);
   // printf("\n3D data Rank: %d\nDimensions: %lu x %lu x %lu\n", rank,
	  // (unsigned long)(dims_out[0]),(unsigned long)(dims_out[1]),(unsigned long)(dims_out[2]));



    
    /* 
     * Define hyperslab in the dataset. 
     */

     /*
     * First 3D Data :
     * 0    1    2    3    4    5
     * 6    7    8    9   10   11
     *12   13   14   15   16   17
     *18   19   20   21   22   23
     *24   25   26   27   28   29
     *30   31   32   33   34   35
     */
    if(mpi_rank==0){
        
        dimsm[0] = X1;
        dimsm[1] = Y1;
        
        memspace = H5Screate_simple (RANK_OUT, dimsm, NULL);   //RANK_OUT=2
       

        // offset from 0x0 and count 3x3
        offset[0] = 0;
        offset[1]=0;  // select=0x0
        count[0]  = 3;
        count[1]  = 3;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);  
       
        
        offset_out[0] = 0;  //offset=0x0
        offset_out[1] = 0;                 
        count_out[0]  = 3;   
        count_out[1]  = 3;   //count_out= 3 x 3  
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out, NULL, 
                                    count_out, NULL);
        
        
        
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace,
                        H5P_DEFAULT, data_out1,es_id);
    
    }
    
    
   if(mpi_rank==1){

        
        dimsm[0] = X2;
        dimsm[1] = Y2;
        
    
        memspace = H5Screate_simple (RANK_OUT, dimsm, NULL);   //RANK_OUT=3
    //offset from 3x0  to 0x0x0 and count 3x2
        offset[0] = 3;
        offset[1] = 0;    //offset=3x0
        count[0]  = 3;  //count=3x2
        count[1]  = 2;
        status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);  
        
        
        offset_out[0] = 0; //offset_out=0x0
        offset_out[1] = 0;
        count_out[0]  = 3;  //count_out=3x2
        count_out[1]  = 2;
        status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out, NULL, 
                                    count_out, NULL);
        
        status = H5Dread_async (dataset, H5T_NATIVE_INT, memspace, dataspace,
                        H5P_DEFAULT, data_out2,es_id);

        
   }

    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        
    }
    if (print_dbg_msg)
        fprintf(stderr, "H5ESwait done\n");

    if(mpi_rank==1){
    printf ("MPI rank=%d Data from rank 1:\n ",mpi_rank);
    printf("offset from 3x0  to 0x0x0 and count 3x2 \n ");
   
    
                printf ("\n 2D Data :\n ");
                
                
                for (i = 0; i < X2; i++){ 
                    for (j = 0; j < Y2; j++) 
                        printf("%5d", data_out2[i][j]);
                printf("\n ");
                }
            } 
    
    
    MPI_Barrier(comm);
    if(mpi_rank==0){
        printf ("MPI rank=%d Data from rank 0:\n ",mpi_rank);
        printf("offset from 0x0 and count 3x3\n ");
        
        
                printf ("\n 2D Data :\n ");
                
                
                for (i = 0; i < X1; i++){ 
                    for (j = 0; j < Y1; j++) 
                        printf("%5d", data_out1[i][j]);
                printf("\n ");
                }
            
    }
    
    
    
    
    status = H5Dclose_async(dataset, es_id);
    if (status < 0) {
        fprintf(stderr, "Closing dataset failed\n");
        //ret = -1;
    }
   
    H5Sclose (dataspace);
    H5Sclose (memspace);

    status = H5Fclose_async(file, es_id);
    if (status < 0) {
        fprintf(stderr, "Closing file failed\n");
        //ret = -1;
    }
    
    status = H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed);
    if (status < 0) {
        fprintf(stderr, "Error with H5ESwait\n");
        //ret = -1;
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

