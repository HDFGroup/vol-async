/* 
 *   Writing and reading an existing dataset.
 */

#include "hdf5.h"
#define FILE "dset.h5"
#define DATASETNAME "IntArray"

main() {

   hid_t       file_id, dataset_id;  /* identifiers */
   herr_t      status;
   int         i, j, dset_data[4][6];

   /* Initialize the dataset. */
   for (i = 0; i < 4; i++)
      for (j = 0; j < 6; j++)
         dset_data[i][j] = i * 6 + j + 1;

   /* Open an existing file. */
   /*
   file_id = H5Fopen(FILE, H5F_ACC_RDWR, H5P_DEFAULT);


   dataset_id = H5Dopen(file_id, DATASETNAME,H5P_DEFAULT);


   status = H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, 
                     dset_data);

   status = H5Dread(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, 
                    dset_data);

 
   status = H5Dclose(dataset_id);
  

   status = H5Fclose(file_id);*/
}

