/* 
 *  This program shows how the H5Sselect_hyperslab and H5Sselect_elements
 *  functions are used to write selected data from memory to the file.
 *  Program takes 48 elements from the linear buffer and writes them into
 *  the matrix using 3x2 blocks, (4,3) stride and (2,4) count. 
 *  Then four elements  of the matrix are overwritten with the new values and 
 *  file is closed. Program reopens the file and reads and displays the result.
 */ 
 
#include "hdf5.h" 

#define FILE "Select.h5"

#define MSPACE1_RANK     1          /* Rank of the first dataset in memory */
#define MSPACE1_DIM      50         /* Dataset size in memory */ 

#define MSPACE2_RANK     1          /* Rank of the second dataset in memory */ 
#define MSPACE2_DIM      4          /* Dataset size in memory */ 

#define FSPACE_RANK      2          /* Dataset rank as it is stored in the file */
#define FSPACE_DIM1      8          /* Dimension sizes of the dataset as it is
                                       stored in the file */
#define FSPACE_DIM2      12 

                                    /* We will read dataset back from the file
                                       to the dataset in memory with these
                                       dataspace parameters. */  
#define MSPACE_RANK      2
#define MSPACE_DIM1      8 
#define MSPACE_DIM2      12 

#define NPOINTS          4          /* Number of points that will be selected 
                                       and overwritten */ 
int main (void)
{

   hid_t   file, dataset;           /* File and dataset identifiers */
   hid_t   mid1, mid2, fid;         /* Dataspace identifiers */
   hsize_t dim1[] = {MSPACE1_DIM};  /* Dimension size of the first dataset 
                                       (in memory) */ 
   hsize_t dim2[] = {MSPACE2_DIM};  /* Dimension size of the second dataset
                                       (in memory */ 
   hsize_t fdim[] = {FSPACE_DIM1, FSPACE_DIM2};   //8x12
                                    /* Dimension sizes of the dataset (on disk) */

   hsize_t start[2];  /* Start of hyperslab */
   hsize_t stride[2]; /* Stride of hyperslab */
   hsize_t count[2];  /* Block count */
   hsize_t block[2];  /* Block sizes */

   hsize_t coord[NPOINTS][FSPACE_RANK]; /* Array to store selected points 
                                            from the file dataspace */ 
   herr_t  ret;
   uint    i,j;
   int     matrix[MSPACE_DIM1][MSPACE_DIM2];  //8x12
   int     vector[MSPACE1_DIM];
   int     values[] = {53, 59, 61, 67};  /* New values to be written */

   /*
    * Buffers' initialization.
    */
   vector[0] = vector[MSPACE1_DIM - 1] = -1;
   for (i = 1; i < MSPACE1_DIM - 1; i++) vector[i] = i;

   for (i = 0; i < MSPACE_DIM1; i++) {
       for (j = 0; j < MSPACE_DIM2; j++)  //8x12
       matrix[i][j] = 0;
    }

    /*
     * Create a file.
     */
    file = H5Fcreate(FILE, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);  //H5F_ACC_TRUNC	If the file already exists, the file is 
    //opened with read-write access, and new data will overwrite any existing data. If the file does not exist, it is created 
    //and opened with read-write access.

    /* 
     * Create dataspace for the dataset in the file.
     */
    fid = H5Screate_simple(FSPACE_RANK, fdim, NULL);  //FSPACE_RANK=2  fdim=8x12

    /*
     * Create dataset and write it into the file.
     */
    dataset = H5Dcreate(file, "Matrix in file", H5T_NATIVE_INT, fid, H5P_DEFAULT,H5P_DEFAULT, H5P_DEFAULT);
    ret = H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, matrix);

    /*
     * Select hyperslab for the dataset in the file, using 3x2 blocks, 
     * (4,3) stride and (2,4) count starting at the position (0,1).
     */
    start[0]  = 0; start[1]  = 1;
    stride[0] = 4; stride[1] = 3;
    count[0]  = 2; count[1]  = 4;    
    block[0]  = 3; block[1]  = 2;
    ret = H5Sselect_hyperslab(fid, H5S_SELECT_SET, start, stride, count, block);

    /*
     * Create dataspace for the first dataset.
     */
    mid1 = H5Screate_simple(MSPACE1_RANK, dim1, NULL);  //MSPACE1_RANK=1  dim1=50 H5Screate_simple=Creates a new simple dataspace and opens it for access.

    /*
     * Select hyperslab. 
     * We will use 48 elements of the vector buffer starting at the second element.
     * Selected elements are 1 2 3 . . . 48
     */
    start[0]  = 1;
    stride[0] = 1;
    count[0]  = 48;
    block[0]  = 1;
    ret = H5Sselect_hyperslab(mid1, H5S_SELECT_SET, start, stride, count, block);  //H5S_SELECT_SET	Replaces the existing selection with the parameters from this call. Overlapping blocks are not supported with this operator.
 
    /*
     * Write selection from the vector buffer to the dataset in the file.
     *
     * File dataset should look like this:       
     *                    0  1  2  0  3  4  0  5  6  0  7  8 
     *                    0  9 10  0 11 12  0 13 14  0 15 16
     *                    0 17 18  0 19 20  0 21 22  0 23 24
     *                    0  0  0  0  0  0  0  0  0  0  0  0
     *                    0 25 26  0 27 28  0 29 30  0 31 32
     *                    0 33 34  0 35 36  0 37 38  0 39 40
     *                    0 41 42  0 43 44  0 45 46  0 47 48
     *                    0  0  0  0  0  0  0  0  0  0  0  0
     */
     ret = H5Dwrite(dataset, H5T_NATIVE_INT, mid1, fid, H5P_DEFAULT, vector);  //H5Dwrite=Writes raw data from a buffer to a dataset.
     /*
     * Read data back to the buffer matrix.
     */
    ret = H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, matrix);  //H5Dread=Reads raw data from a dataset into a buffer.
   
   /*
     * Display the result.
     */
    printf("\n----matrix dataset at the beginning------\n");
    for (i=0; i < MSPACE_DIM1; i++) {   //8x12
        for(j=0; j < MSPACE_DIM2; j++) printf("%3d  ", matrix[i][j]);
        printf("\n");
    }

    /*
     * Reset the selection for the file dataspace fid.
     */
    ret = H5Sselect_none(fid);  //H5Sselect_none=Resets the selection region to include no elements.

    /*
     * Create dataspace for the second dataset.
     */
    mid2 = H5Screate_simple(MSPACE2_RANK, dim2, NULL);  //MSPACE2_RANK=1  ,dim2= 4  H5Screate_simple=Creates a new simple dataspace and opens it for access.

    /*
     * Select sequence of NPOINTS points in the file dataspace.
     */
    coord[0][0] = 0; coord[0][1] = 0;
    coord[1][0] = 3; coord[1][1] = 3;
    coord[2][0] = 3; coord[2][1] = 5;
    coord[3][0] = 5; coord[3][1] = 6;

    ret = H5Sselect_elements(fid, H5S_SELECT_SET, NPOINTS, 
                             coord);  //Selects array elements to be included in the selection for a dataspace.
                             //NPOINTS=4

    /*
     * Write new selection of points to the dataset.
     */
    ret = H5Dwrite(dataset, H5T_NATIVE_INT, mid2, fid, H5P_DEFAULT, values);   
    

    /*
     * File dataset should look like this:     
     *                   53  1  2  0  3  4  0  5  6  0  7  8 
     *                    0  9 10  0 11 12  0 13 14  0 15 16
     *                    0 17 18  0 19 20  0 21 22  0 23 24
     *                    0  0  0 59  0 61  0  0  0  0  0  0
     *                    0 25 26  0 27 28  0 29 30  0 31 32
     *                    0 33 34  0 35 36 67 37 38  0 39 40
     *                    0 41 42  0 43 44  0 45 46  0 47 48
     *                    0  0  0  0  0  0  0  0  0  0  0  0
     *                                        
     */
     
    /*
     * Close memory file and memory dataspaces.
     */
    ret = H5Sclose(mid1); 
    ret = H5Sclose(mid2); 
    ret = H5Sclose(fid); 
 
    /*
     * Close dataset.
     */
    ret = H5Dclose(dataset);

    /*
     * Close the file.
     */
    ret = H5Fclose(file);

    /*
     * Open the file.
     */
    file = H5Fopen(FILE, H5F_ACC_RDONLY, H5P_DEFAULT);

    /*
     * Open the dataset.
     */
    dataset = H5Dopen(file,"Matrix in file",H5P_DEFAULT);

    /*
     * Read data back to the buffer matrix.
     */
    ret = H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, matrix);  //H5Dread=Reads raw data from a dataset into a buffer.

    printf("\n----matrix dataset after opening the dataset------\n");
    /*
     * Display the result.
     */
    for (i=0; i < MSPACE_DIM1; i++) {   //8x12
        for(j=0; j < MSPACE_DIM2; j++) printf("%3d  ", matrix[i][j]);
        printf("\n");
    }

    return 0;
}
