/*********************************************************************
 *
 *  Copyright (C) 2019, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 *
 *********************************************************************/
/* $Id$ */

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * This example shows how to opee file using PnetCDF VOL.
 *
 *    To compile:
 *        mpicc -O2 create_open.c -o create_open -lpnetcdf
 *
 * Example commands for MPI run and outputs from running ncmpidump on the
 * netCDF file produced by this example program:
 *
 *    % mpiexec -n 4 ./create_open /pvfs2/wkliao/testfile.nc
 *
 *    % ncmpidump /pvfs2/wkliao/testfile.nc
 *    netcdf testfile {
 *    // file format: CDF-1
 *    }
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include <stdio.h>
#include <stdlib.h>

#include <hdf5.h>
#include "ncmpi_vol.h"

#define N 10

int main(int argc, char **argv) {  
    int rank, np;
    int i;
    const char *file_name;  
    hid_t fapl_id, pnc_vol_id;
    hid_t file_id, dspace_id, dset_id, mspace_id, dxpl_id;
    hsize_t dims[3], start[2], count[2];
    int buf[N];
    H5VL_ncmpi_info_t pnc_vol_info; // PnetCDF VOL callback

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &np);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (argc > 1){
        file_name = argv[1];
    }
    else{
        file_name = "test.nc";
    }
    if(rank == 0) printf("Writing file_name = %s at rank 0 \n", file_name);

    // PnetCDF VOL require MPI-IO and parallel access
    // Set MPI-IO and parallel access proterty.
    fapl_id = H5Pcreate(H5P_FILE_ACCESS); 
    H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL);
    H5Pset_all_coll_metadata_ops(fapl_id, 1);
    H5Pset_coll_metadata_write(fapl_id, 1);

    // Resgiter PnetCDF VOL
    pnc_vol_id = H5VLregister_connector(&H5VL_ncmpi_g, H5P_DEFAULT); 
    pnc_vol_info.comm = MPI_COMM_WORLD;
    H5Pset_vol(fapl_id, pnc_vol_id, &pnc_vol_info);

    // Collective I/O
    dxpl_id = H5Pcreate (H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(dxpl_id, H5FD_MPIO_COLLECTIVE);

    // Create file
    file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);     
    
    // Define dataset
    dims [0]    = np;
    dims [1]    = N;
    dspace_id = H5Screate_simple(2, dims, NULL);   // Dataset space
    dset_id  = H5Dcreate(file_id, "M", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
    mspace_id = H5Screate_simple(1, dims + 1, NULL);    // Memory space for I/O

    // Select region
    start[0] = rank;
    start[0] = N - 1;
    H5Sselect_elements(dspace_id, H5S_SELECT_SET, 1, start);
    for(i = 0; i < N; i++){
        start[1] = N - i - 1;
        H5Sselect_elements(dspace_id, H5S_SELECT_OR, 1, start);
    }
    
    for(i = 0; i < N; i++){
        buf[i] = rank + 1 + i;
    }
    H5Dwrite(dset_id, H5T_NATIVE_INT, mspace_id, dspace_id, dxpl_id, buf);  
    H5Fflush(file_id, H5F_SCOPE_GLOBAL);

    // Close file
    H5Dclose(dset_id);
    H5Fclose(file_id);

    // Open file
    file_id = H5Fopen(file_name, H5F_ACC_RDONLY, fapl_id);     
    
    // Open dataset
    dset_id  = H5Dopen2(file_id, "M", H5P_DEFAULT);  
    
    for(i = 0; i < N; i++){
        buf[i] = 0;
    }
    H5Dread(dset_id, H5T_NATIVE_INT, mspace_id, dspace_id, dxpl_id, buf);  
    H5Fflush(file_id, H5F_SCOPE_GLOBAL);

    for(i = 0; i < N; i++){
        if (buf[i] != rank + 1 + i){
            printf("Rank %d: Error. Expect buf[%d] = %d, but got %d\n", rank, i, rank + 1 + i, buf[i]);
        }
    }

    // Close file
    H5Sclose(dspace_id);
    H5Sclose(mspace_id);
    H5Dclose(dset_id);
    H5Fclose(file_id);

    H5VLclose(pnc_vol_id);
    H5Pclose(fapl_id);
    H5Pclose(dxpl_id);
    
    MPI_Finalize();

    return 0;  
}  


