/****** Copyright Notice ***
 *
 * PIOK - Parallel I/O Kernels - VPIC-IO, VORPAL-IO, and GCRM-IO, Copyright
 * (c) 2015, The Regents of the University of California, through Lawrence
 * Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy).  All rights reserved.
 *
 * If you have questions about your rights to use or distribute this
 * software, please contact Berkeley Lab's Innovation & Partnerships Office
 * at  IPO@lbl.gov.
 *
 * NOTICE.  This Software was developed under funding from the U.S.
 * Department of Energy and the U.S. Government consequently retains
 * certain rights. As such, the U.S. Government has been granted for itself
 * and others acting on its behalf a paid-up, nonexclusive, irrevocable,
 * worldwide license in the Software to reproduce, distribute copies to the
 * public, prepare derivative works, and perform publicly and display
 * publicly, and to permit other to do so.
 *
 ****************************/

/**
 *
 * Email questions to SByna@lbl.gov
 * Scientific Data Management Research Group
 * Lawrence Berkeley National Laboratory
 *
*/

// Description: This is a simple benchmark based on VPIC's I/O interface
//		Each process writes a specified number of particles into
//		a hdf5 output file using only HDF5 calls
// Author:	Suren Byna <SByna@lbl.gov>
//		Lawrence Berkeley National Laboratory, Berkeley, CA
// Created:	in 2011
// Modified:	01/06/2014 --> Removed all H5Part calls and using HDF5 calls
//          	02/19/2019 --> Add option to write multiple timesteps of data - Tang
//


#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>
#include "h5_vol_external_async_native.h"

#define ENABLE_MPI 1

// A simple timer based on gettimeofday

#define DTYPE float

struct timeval start_time[5];
float elapse[5];
#define timer_on(id) gettimeofday (&start_time[id], NULL)
#define timer_off(id) 	\
		{	\
		     struct timeval result, now; \
		     gettimeofday (&now, NULL);  \
		     timeval_subtract(&result, &now, &start_time[id]);	\
		     elapse[id] += result.tv_sec+ (DTYPE) (result.tv_usec)/1000000.;	\
		}

#define timer_msg(id, msg) \
	printf("%f seconds elapsed in %s\n", (DTYPE)(elapse[id]), msg);  fflush(stdout); \

#define timer_reset(id) elapse[id] = 0

/* Subtract the `struct timeval' values X and Y,
   storing the result in RESULT.
   Return 1 if the difference is negative, otherwise 0.  */

int
timeval_subtract (struct timeval *result, struct timeval *x, struct timeval *y)
{
  /* Perform the carry for the later subtraction by updating y. */
  if (x->tv_usec < y->tv_usec) {
    int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
    y->tv_usec -= 1000000 * nsec;
    y->tv_sec += nsec;
  }
  if (x->tv_usec - y->tv_usec > 1000000) {
    int nsec = (y->tv_usec - x->tv_usec) / 1000000;
    y->tv_usec += 1000000 * nsec;
    y->tv_sec -= nsec;
  }

  /* Compute the time remaining to wait.
     tv_usec is certainly positive. */
  result->tv_sec = x->tv_sec - y->tv_sec;
  result->tv_usec = x->tv_usec - y->tv_usec;

  /* Return 1 if result is negative. */
  return x->tv_sec < y->tv_sec;
}



// HDF5 specific declerations
herr_t ierr;

// Variables and dimensions
long numparticles = 8388608;	// 8  meg particles per process
long long total_particles = 8388608, offset = 0;

float *x, *y, *z;
float *px, *py, *pz;
int *id1, *id2;
int x_dim = 64;
int y_dim = 64;
int z_dim = 64;

// Uniform random number
double uniform_random_number()
{
    return (((double)rand())/((double)(RAND_MAX)));
}

// Initialize particle data
void init_particles ()
{
    int i;
    for (i=0; i<numparticles; i++)
    {
        id1[i] = i;
        id2[i] = i*2;
        x[i] = uniform_random_number()*x_dim;
        y[i] = uniform_random_number()*y_dim;
        z[i] = ((double)id1[i]/numparticles)*z_dim;
        px[i] = uniform_random_number()*x_dim;
        py[i] = uniform_random_number()*y_dim;
        pz[i] = ((double)id2[i]/numparticles)*z_dim;
    }
}

// Create HDF5 file and write data
void create_and_write_synthetic_h5_data(int rank, hid_t loc, hid_t *dset_ids, hid_t filespace, hid_t memspace, hid_t plist_id)
{
    /* sleep(1); */
    int i;
    // Note: printf statements are inserted basically
    // to check the progress. Other than that they can be removed
    dset_ids[0] = H5Dcreate(loc, "x", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_ids[1] = H5Dcreate(loc, "y", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_ids[2] = H5Dcreate(loc, "z", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_ids[3] = H5Dcreate(loc, "id1", H5T_NATIVE_INT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_ids[4] = H5Dcreate(loc, "id2", H5T_NATIVE_INT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_ids[5] = H5Dcreate(loc, "px", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_ids[6] = H5Dcreate(loc, "py", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_ids[7] = H5Dcreate(loc, "pz", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    ierr = H5Dwrite(dset_ids[0], H5T_NATIVE_FLOAT, memspace, filespace, plist_id, x);
    /* if (rank == 0) printf ("Written variable 1 \n"); */

    ierr = H5Dwrite(dset_ids[1], H5T_NATIVE_FLOAT, memspace, filespace, plist_id, y);
    /* if (rank == 0) printf ("Written variable 2 \n"); */

    ierr = H5Dwrite(dset_ids[2], H5T_NATIVE_FLOAT, memspace, filespace, plist_id, z);
    /* if (rank == 0) printf ("Written variable 3 \n"); */

    ierr = H5Dwrite(dset_ids[3], H5T_NATIVE_INT, memspace, filespace, plist_id, id1);
    /* if (rank == 0) printf ("Written variable 4 \n"); */

    ierr = H5Dwrite(dset_ids[4], H5T_NATIVE_INT, memspace, filespace, plist_id, id2);
    /* if (rank == 0) printf ("Written variable 5 \n"); */

    ierr = H5Dwrite(dset_ids[5], H5T_NATIVE_FLOAT, memspace, filespace, plist_id, px);
    /* if (rank == 0) printf ("Written variable 6 \n"); */

    ierr = H5Dwrite(dset_ids[6], H5T_NATIVE_FLOAT, memspace, filespace, plist_id, py);
    /* if (rank == 0) printf ("Written variable 7 \n"); */

    ierr = H5Dwrite(dset_ids[7], H5T_NATIVE_FLOAT, memspace, filespace, plist_id, pz);
    /* if (rank == 0) printf ("Written variable 8 \n"); */
    if (rank == 0) printf ("  Finished written 8 variables \n");

}

void print_usage(char *name)
{
    printf("Usage: %s /path/to/file #timestep sleep_sec [# mega particles]\n", name);
}

int main (int argc, char* argv[])
{
    char *file_name = argv[1];

    int my_rank = 0, num_procs = 1, nts, i, j, sleep_time = 0;

    #ifdef ENABLE_MPI
    MPI_Init(&argc,&argv);
    MPI_Comm_rank (MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size (MPI_COMM_WORLD, &num_procs);

    MPI_Comm comm  = MPI_COMM_WORLD;
    MPI_Info info  = MPI_INFO_NULL;
    #endif

    hid_t file_id, filespace, memspace, *grp_ids, async_fapl, async_dxpl, **dset_ids;
    char grp_name[128];

    if (argc < 4) {
        print_usage(argv[0]);
        return 0;
    }

    nts = atoi(argv[2]);
    if (nts <= 0) {
        print_usage(argv[0]);
        return 0;
    }

    sleep_time = atoi(argv[3]);
    if (sleep_time < 0) {
        print_usage(argv[0]);
        return 0;
    }

    if (argc == 5) {
        numparticles = (atoi (argv[4]))*1024*1024;
    }
    else {
        numparticles = 8*1024*1024;
    }

    if (my_rank == 0) {
        printf ("Number of paritcles: %ld \n", numparticles);
    }
    total_particles = numparticles;

    x=(float*)malloc(numparticles*sizeof(double));
    y=(float*)malloc(numparticles*sizeof(double));
    z=(float*)malloc(numparticles*sizeof(double));

    px=(float*)malloc(numparticles*sizeof(double));
    py=(float*)malloc(numparticles*sizeof(double));
    pz=(float*)malloc(numparticles*sizeof(double));

    id1=(int*)malloc(numparticles*sizeof(int));
    id2=(int*)malloc(numparticles*sizeof(int));

    init_particles ();

    if (my_rank == 0)
        printf ("Finished initializeing particles \n");


    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif
    timer_on (0);

    #ifdef ENABLE_MPI
    MPI_Allreduce(&numparticles, &total_particles, 1, MPI_LONG_LONG, MPI_SUM, comm);
    MPI_Scan(&numparticles, &offset, 1, MPI_LONG_LONG, MPI_SUM, comm);
    offset -= numparticles;
    #endif

    async_fapl = H5Pcreate (H5P_FILE_ACCESS);
    async_dxpl = H5Pcreate (H5P_DATASET_XFER);
    H5Pset_vol_async(async_fapl);
    H5Pset_alignment(async_fapl, 1048576, 16777216);
    H5Pset_dxpl_async_cp_limit(async_dxpl, 0);
    H5Pset_dxpl_async(async_dxpl, true);
    H5Pset_coll_metadata_write(async_fapl, true);
    #ifdef ENABLE_MPI
    H5Pset_fapl_mpio(async_fapl, comm, info);
    #endif

    file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl);

    if (my_rank == 0)
        printf ("Opened HDF5 file... \n");

    filespace = H5Screate_simple(1, (hsize_t *) &total_particles, NULL);
    memspace =  H5Screate_simple(1, (hsize_t *) &numparticles, NULL);

    H5Sselect_hyperslab(filespace, H5S_SELECT_SET, (hsize_t *) &offset, NULL, (hsize_t *) &numparticles, NULL);

    dset_ids = (hid_t**)calloc(nts, sizeof(hid_t*));
    for (i = 0; i < nts; i++) 
        dset_ids[i] = (hid_t*)calloc(8, sizeof(hid_t));
    grp_ids  = (hid_t*)calloc(nts, sizeof(hid_t));

    /* #ifdef ENABLE_MPI */
    /* MPI_Barrier(MPI_COMM_WORLD); */
    /* #endif */
    timer_on (1);

    for (i = 0; i < nts; i++) {
        sprintf(grp_name, "Timestep_%d", i);
        grp_ids[i] = H5Gcreate(file_id, grp_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        if (my_rank == 0)
            printf ("Writing %s ... \n", grp_name);

        timer_reset(2);
        timer_on(2);
        timer_on(3);
        create_and_write_synthetic_h5_data(my_rank, grp_ids[i], dset_ids[i], filespace, memspace, async_dxpl);
        timer_off(2);
        if (my_rank == 0)
            timer_msg (2, "create and write");
        
        fflush(stdout);

        for (j = 0; j < 8; j++) 
            H5Dclose(dset_ids[i][j]);
        H5Gclose(grp_ids[i]);

        H5VL_async_start();

        if (i != nts - 1) {
            if (my_rank == 0) { printf ("  sleep for %ds start\n", sleep_time); fflush(stdout);}
            if (sleep_time > 0) sleep(sleep_time);
            if (my_rank == 0) { printf ("  sleep for %ds end\n", sleep_time); fflush(stdout);}
        }

        /* H5Fwait(file_id); */

        #ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        #endif
        timer_off(3);
        if (my_rank == 0)
            timer_msg (3, "write and wait");
        timer_reset(3);
    }

    timer_off (1);

    if (my_rank == 0) {
        printf ("\nTiming results\n");
        timer_msg (1, "just writing data");
    }


    H5Sclose(memspace);
    H5Sclose(filespace);
    H5Pclose(async_dxpl);
    H5Pclose(async_fapl);
    /* if (my_rank == 0) printf ("Before closing HDF5 file \n"); */
    /* H5Fwait(file_id); */
    H5Fclose(file_id);
    if (my_rank == 0) {printf ("Closed HDF5 file \n"); fflush(stdout);}

    H5VLasync_finalize();

    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif
    timer_off (0);

    if (my_rank == 0) {
        timer_msg (0, "opening, writing, closing file");
        printf ("\n");
    }

    free(x);
    free(y);
    free(z);
    free(px);
    free(py);
    free(pz);
    free(id1);
    free(id2);

    for (i = 0; i < nts; i++) 
        free(dset_ids[i]);
    free(dset_ids);
    free(grp_ids);


    #ifdef ENABLE_MPI
    MPI_Finalize();
    #endif

    return 0;
}
