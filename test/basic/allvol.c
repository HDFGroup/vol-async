#include <stdio.h>
#include <stdlib.h>

#include <hdf5.h>
#include "ncmpi_vol.h"

#define N 10

int main(int argc, char **argv) {  
    int i, j;  
    int rank, np;
    const char *file_name;  
    char dataset_name[]="data";  
    hid_t file_id, group_id, datasetId, dataspaceId, file_space, memspace_id, attid_f, attid_d, attid_g;
    hid_t pnc_fapl, pnc_vol_id, dxplid;  
    hsize_t dims[2], start[2], count[2];
    int buf[N];
    H5VL_ncmpi_info_t pnc_vol_info;

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

    //Register DataElevator plugin 
    pnc_fapl = H5Pcreate(H5P_FILE_ACCESS); 
    H5Pset_fapl_mpio(pnc_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    H5Pset_all_coll_metadata_ops(pnc_fapl, 1);
    H5Pset_coll_metadata_write(pnc_fapl, 1);
    pnc_vol_id = H5VLregister_connector(&H5VL_ncmpi_g, H5P_DEFAULT); 
    pnc_vol_info.comm = MPI_COMM_WORLD;
    H5Pset_vol(pnc_fapl, pnc_vol_id, &pnc_vol_info);

    // Create file
    file_id  = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, pnc_fapl);     
    
    // Create group
    group_id = H5Gcreate1(file_id, "test", 0); 

    dims [0]    = np;
    dims [1]    = N;
    dataspaceId = H5Screate_simple(2, dims, NULL);   
    datasetId   = H5Dcreate(group_id,dataset_name,H5T_NATIVE_INT,dataspaceId,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT);  
    memspace_id = H5Screate_simple(1, dims + 1, NULL);
    file_space = H5Dget_space(datasetId);
    attid_d = H5Acreate(datasetId, "Dataset_Att", H5T_NATIVE_INT, memspace_id, H5P_DEFAULT, H5P_DEFAULT);
    attid_f = H5Acreate(file_id, "File_Att", H5T_NATIVE_INT, memspace_id, H5P_DEFAULT, H5P_DEFAULT);
    attid_g = H5Acreate(group_id, "Group_Att", H5T_NATIVE_INT, memspace_id, H5P_DEFAULT, H5P_DEFAULT);

    start[0] = rank;
    start[1] = 0;
    count[0] = 1;
    count[1] = N;
    H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start, NULL, count, NULL);

    dxplid = H5Pcreate (H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(dxplid, H5FD_MPIO_COLLECTIVE);

    for(i = 0; i < N; i++){
        buf[i] = rank + 1 + i;
    }
    H5Dwrite(datasetId, H5T_NATIVE_INT, memspace_id, file_space, dxplid, buf);  
    H5Fflush(file_id, H5F_SCOPE_GLOBAL);

    for(i = 0; i < N; i++){
        buf[i] = np + 1 + i;
    }
    H5Awrite (attid_d, H5T_NATIVE_INT, buf);
    H5Awrite (attid_g, H5T_NATIVE_INT, buf);
    H5Awrite (attid_f, H5T_NATIVE_INT, buf);

    for(i = 0; i < N; i++){
        buf[i] = 0;
    }
    H5Dread(datasetId, H5T_NATIVE_INT, memspace_id, file_space, dxplid, buf);  
    H5Fflush(file_id, H5F_SCOPE_GLOBAL);
    for(i = 0; i < N; i++){
        if (buf[i] != rank + 1 + i){
            printf("Rank %d: Error. Expect buf[%d] = %d, but got %d\n", rank, i, rank + 1 + i, buf[i]);
        }
    }

    for(i = 0; i < N; i++){
        buf[i] = 0;
    }
    H5Aread(attid_d, H5T_NATIVE_INT, buf);
    for(i = 0; i < N; i++){
        if (buf[i] != np + 1 + i){
            printf("Rank %d: Error. Expect buf[%d] = %d, but got %d\n", rank, i, np + 1 + i, buf[i]);
        }
    }

    for(i = 0; i < N; i++){
        buf[i] = 0;
    }
    H5Aread(attid_g, H5T_NATIVE_INT, buf);
    for(i = 0; i < N; i++){
        if (buf[i] != np + 1 + i){
            printf("Rank %d: Error. Expect buf[%d] = %d, but got %d\n", rank, i, np + 1 + i, buf[i]);
        }
    }

    for(i = 0; i < N; i++){
        buf[i] = 0;
    }
    H5Aread(attid_f, H5T_NATIVE_INT, buf);
    for(i = 0; i < N; i++){
        if (buf[i] != np + 1 + i){
            printf("Rank %d: Error. Expect buf[%d] = %d, but got %d\n", rank, i, np + 1 + i, buf[i]);
        }
    }
    
    H5Pset_dxpl_mpio(dxplid, H5FD_MPIO_INDEPENDENT);

    if (rank & 1){
        for(i = 0; i < N; i++){
            buf[i] = rank + 1 + i;
        }
        H5Dwrite(datasetId, H5T_NATIVE_INT, memspace_id, file_space, dxplid, buf); 

        for(i = 0; i < N; i++){
            buf[i] = 0;
        }
        H5Dread(datasetId, H5T_NATIVE_INT, memspace_id, file_space, dxplid, buf);  
        for(i = 0; i < N; i++){
            if (buf[i] != rank + 1 + i){
                printf("Rank %d: Error. Expect buf[%d] = %d, but got %d\n", rank, i, rank + 1 + i, buf[i]);
            }
        }
    }

    H5Sclose(file_space);
    H5Sclose(memspace_id);
    H5Sclose(dataspaceId);  
    H5Aclose(attid_d);
    //H5Aclose(attid_g);
    //H5Aclose(attid_f);
    H5Pclose(dxplid);  
    H5Dclose(datasetId);  
    H5Gclose(group_id);
    H5Fclose(file_id);
    
    MPI_Finalize();

    return 0;  
}  


