/************************************************************
  
  This example shows how to write and read a hyperslab.  It 
  is derived from the h5_read.c and h5_write.c examples in 
  the "Introduction to HDF5".

 ************************************************************/
 
#include "hdf5.h"

#define FILE        "sds.h5"
#define DATASETNAME "IntArray" 
#define NX_SUB  3                      /* hyperslab dimensions */ 
#define NY_SUB  4 
#define NX 7                           /* output buffer dimensions */ 
#define NY 7 
#define NZ  3 
#define RANK         2
#define RANK_OUT     3

#define X     5                        /* dataset dimensions */
#define Y     6

int
main (void)
{
    hsize_t     dimsf[2];              /* dataset dimensions */
    int         data[X][Y];            /* data to write */

    /* 
     * Data  and output buffer initialization. 
     */
    hid_t       file, dataset;         /* handles */
    hid_t       dataspace;   
    hid_t       memspace; 
    hsize_t     dimsm[3];              /* memory space dimensions */
    hsize_t     dims_out[2];           /* dataset dimensions */      
    herr_t      status;                             

    int         data_out[NX][NY][NZ ]; /* output buffer */
    int data_out1[X][Y];
   
    hsize_t     count[2];              /* size of the hyperslab in the file */
    hsize_t    offset[2];             /* hyperslab offset in the file */
    hsize_t     count_out[3];          /* size of the hyperslab in memory */
    hsize_t    offset_out[3];         /* hyperslab offset in memory */
    int         i, j, k, status_n, rank;



/*********************************************************  
   This writes data to the HDF5 file.  
 *********************************************************/  
 
    /* 
     * Data  and output buffer initialization. 
     */
    for (j = 0; j < X; j++) {
	for (i = 0; i < Y; i++)
	    data[j][i] = i + j;
    }     
    /*
     * 0 1 2 3 4 5 
     * 1 2 3 4 5 6
     * 2 3 4 5 6 7
     * 3 4 5 6 7 8
     * 4 5 6 7 8 9
     */

    /*
     * Create a new file using H5F_ACC_TRUNC access,
     * the default file creation properties, and the default file
     * access properties.
     */
    file = H5Fcreate (FILE, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);  
     /* H5F_ACC_TRUNC -- If the file already exists, the file is opened with read-write access, 
        and new data will overwrite any existing data. 
        If the file does not exist, it is created and opened with read-write access.*/

    /*
     * Describe the size of the array and create the data space for fixed
     * size dataset. 
     */
    dimsf[0] = X;
    dimsf[1] = Y;
    dataspace = H5Screate_simple (RANK, dimsf, NULL); 

    /*
     * Create a new dataset within the file using defined dataspace and
     * default dataset creation properties.
     */
    dataset = H5Dcreate (file, DATASETNAME, H5T_STD_I32BE, dataspace,
                         H5P_DEFAULT,H5P_DEFAULT, H5P_DEFAULT);   //H5T_STD_I32BE = 32-bit big-endian signed integers

    /*
     * Write the data to the dataset using default transfer properties.
     */
    status = H5Dwrite (dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                      H5P_DEFAULT, data);  //H5T_NATIVE_INT = C-style int
   
    
   
    status = H5Dread (dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                   H5P_DEFAULT, data_out1);

     printf ("First Data:\n ");
    
    for (j = 0; j < X; j++) {
	for (i = 0; i < Y; i++) printf("%d ", data_out1[j][i]);
	printf("\n ");
    }
	printf("\n");
    /*
     * Close/release resources.
     */
    H5Sclose (dataspace);
    H5Dclose (dataset);
    H5Fclose (file);
 

/*************************************************************  

  This reads the hyperslab from the sds.h5 file just 
  created, into a 2-dimensional plane of the 3-dimensional 
  array.

 ************************************************************/  

    for (j = 0; j < NX; j++) {
	for (i = 0; i < NY; i++) {
	    for (k = 0; k < NZ ; k++)
		data_out[j][i][k] = 0;
	}
    } 
 
    /*
     * Open the file and the dataset.
     */
    file = H5Fopen (FILE, H5F_ACC_RDONLY, H5P_DEFAULT); //H5F_ACC_RDONLY= An existing file is opened with read-only access. 
                                                        //If the file does not exist, H5Fopen fails. (Default)
    dataset = H5Dopen(file, DATASETNAME,H5P_DEFAULT);  //#define DATASETNAME "IntArray" 

    dataspace = H5Dget_space (dataset);    /* dataspace handle */
    rank      = H5Sget_simple_extent_ndims (dataspace);
    status_n  = H5Sget_simple_extent_dims (dataspace, dims_out, NULL);
    printf("\nRank: %d\nDimensions: %lu x %lu \n", rank,
	   (unsigned long)(dims_out[0]), (unsigned long)(dims_out[1]));

    /* 
     * Define hyperslab in the dataset. 
     */
    /*
    #define FILE        "sds.h5"
    define DATASETNAME "IntArray" 
    #define NX_SUB  3                      // hyperslab dimensions 
    #define NY_SUB  4 
    #define NX 7                           // output buffer dimensions  
    #define NY 7 
    #define NZ  3 
    #define RANK         2
    #define RANK_OUT     3

    #define X     5                        // dataset dimensions 
    #define Y     6
    */
    offset[0] = 1;
    offset[1] = 2; // offset=1x2
    count[0]  = NX_SUB;
    count[1]  = NY_SUB; //count=3x4
    status = H5Sselect_hyperslab (dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);  
    //H5S_SELECT_SET=Replace the existing selection with the parameters from this call. Overlapping blocks are not supported with this operator.
    //H5Sselect_hyperslab selects a hyperslab region to 
    //add to the current selected region for a specified dataspace.
    //offset value from row 1, col 2


    /*
     * Define the memory dataspace.
     */
    dimsm[0] = NX;
    dimsm[1] = NY;
    dimsm[2] = NZ;  //dimsm =7x7x3
    memspace = H5Screate_simple (RANK_OUT, dimsm, NULL);   //RANK_OUT=3, RANK=2
    // 7x7x3 array with all zeros

    /* 
     * Define memory hyperslab. 
     */
    offset_out[0] = 3;
    //offset_out[0] = 0;
    offset_out[1] = 0;  // offset_out = 3 X 0 X 0
    offset_out[2] = 0;
    count_out[0]  = NX_SUB; //count_out=3 X 4 X 1 
    count_out[1]  = NY_SUB;
    count_out[2]  = 1;
    //count_out[2]  = 0;
    status = H5Sselect_hyperslab (memspace, H5S_SELECT_SET, offset_out, NULL, 
                                  count_out, NULL);
                                  //H5Sselect_hyperslab selects a hyperslab region to add to the current selected region 
                                  //for the dataspace specified by space_id.

    /*
     * Read data from hyperslab in the file into the hyperslab in 
     * memory and display.
     */
    status = H5Dread (dataset, H5T_NATIVE_INT, memspace, dataspace,
                      H5P_DEFAULT, data_out);

    /*
    mem_space_id specifies the memory dataspace and the selection within it. 
    file_space_id specifies the selection within the file dataset's dataspace.
    herr_t H5Dread( hid_t dataset_id, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t xfer_plist_id, void * buf )
Purpose:
Reads raw data from a dataset into a buffer.
Description:
H5Dread reads a (partial) dataset, specified by its identifier dataset_id, from the file into an application memory buffer buf. 
Data transfer properties are defined by the argument xfer_plist_id. The memory datatype of the (partial) dataset is identified 
by the identifier mem_type_id. The part of the dataset to read is defined by mem_space_id and file_space_id.
file_space_id is used to specify only the selection within the file dataset's dataspace. Any dataspace specified in 
file_space_id is ignored by the library and the dataset's dataspace is always used. file_space_id can be the constant H5S_ALL. 
which indicates that the entire file dataspace, as defined by the current dimensions of the dataset, is to be selected.

mem_space_id is used to specify both the memory dataspace and the selection within that dataspace. 
mem_space_id can be the constant H5S_ALL, in which case the file dataspace is used for the memory dataspace and 
the selection defined with file_space_id is used for the selection within that dataspace.

If raw data storage space has not been allocated for the dataset and a fill value has been defined, 
the returned buffer buf is filled with the fill value.
    */
    printf ("Data:\n ");
    for (j = 0; j < NX; j++) {
	for (i = 0; i < NY; i++) printf("%d ", data_out[j][i][0]);
	printf("\n ");
    }
	printf("\n");
    /*
     * 0 0 0 0 0 0 0
     * 0 0 0 0 0 0 0
     * 0 0 0 0 0 0 0
     * 3 4 5 6 0 0 0  
     * 4 5 6 7 0 0 0
     * 5 6 7 8 0 0 0
     * 0 0 0 0 0 0 0
     */
    printf ("Data:\n ");
    for (j = 0; j < NX; j++) {
	for (i = 0; i < NY; i++) printf("%d ", data_out[j][i][1]);
	printf("\n ");
    }
	printf("\n");
    
    
    
    /*
     * Close and release resources.
     */
    H5Dclose (dataset);
    H5Sclose (dataspace);
    H5Sclose (memspace);
    H5Fclose (file);

}     
