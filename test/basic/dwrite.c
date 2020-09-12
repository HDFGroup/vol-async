#include <hdf5.h>
#include <stdio.h>
#include <stdlib.h>

#include "h5vl_async_public.h"
#include "testutils.h"

#define N 10

int main (int argc, char **argv) {
	herr_t err = 0;
	int nerrs  = 0;
	int i;
	int rank, np;
	const char *file_name;
	int ndim;
	hid_t fid, did, sid, msid;
	hid_t faplid, dxplid;
	hid_t log_vlid;
	hsize_t dims[2] = {0, N};
	hsize_t start[2], count[2];
	int buf[N];

	MPI_Init (&argc, &argv);
	MPI_Comm_size (MPI_COMM_WORLD, &np);
	MPI_Comm_rank (MPI_COMM_WORLD, &rank);

	if (argc > 2) {
		if (!rank) printf ("Usage: %s [filename]\n", argv[0]);
		MPI_Finalize ();
		return 1;
	} else if (argc > 1) {
		file_name = argv[1];
	} else {
		file_name = "test.h5";
	}
	SHOW_TEST_INFO ("Blocking write on datasets")

	// Use Async VOL plugin
	faplid = H5Pcreate (H5P_FILE_ACCESS);
	H5Pset_vol_async (faplid);
	// Create file
	fid = H5Fcreate (file_name, H5F_ACC_TRUNC, H5P_DEFAULT, faplid);
	CHECK_ERR (fid)

	// Create datasets
	dims[0] = np;
	sid		= H5Screate_simple (2, dims, dims);
	CHECK_ERR (sid);
	did = H5Dcreate2 (fid, "D", H5T_STD_I32LE, sid, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	CHECK_ERR (did)

	// Write to dataset
	for (i = 0; i < N; i++) { buf[i] = rank + i; }
	start[0] = rank;
	start[1] = 0;
	count[0] = 1;
	count[1] = N;
	err		 = H5Sselect_hyperslab (sid, H5S_SELECT_SET, start, NULL, count, NULL);
	CHECK_ERR (err)
	msid = H5Screate_simple (1, dims + 1, dims + 1);
	CHECK_ERR (msid);
	err = H5Dwrite (did, H5T_NATIVE_INT, msid, sid, H5P_DEFAULT, buf);
	CHECK_ERR (err)

	err = H5Sclose (sid);
	CHECK_ERR (err)
	err = H5Dclose (did);
	CHECK_ERR (err)
	err = H5Fclose (fid);
	CHECK_ERR (err)

	err = H5Pclose (faplid);
	CHECK_ERR (err)

err_out:;
	SHOW_TEST_RESULT

	MPI_Finalize ();

	return (nerrs > 0);
}
