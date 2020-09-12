#include <hdf5.h>
#include <stdio.h>
#include <stdlib.h>

#include "h5vl_async_public.h"
#include "testutils.h"

#define N 10

int main (int argc, char **argv) {
	int err, nerrs = 0;
	int rank, np;
	int buf = 1;
	const char *file_name;
	hid_t fid, gid, faid, gaid, sid;
	hid_t faplid, dxplid;
	hid_t log_vlid;

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
	SHOW_TEST_INFO ("Creating attributes")

	faplid = H5Pcreate (H5P_FILE_ACCESS);
	H5Pset_vol_async (faplid);

	// Create file
	fid = H5Fcreate (file_name, H5F_ACC_TRUNC, H5P_DEFAULT, faplid);
	CHECK_ERR (fid)
	// Create group
	gid = H5Gcreate2 (fid, "test", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	CHECK_ERR (gid)

	// Create attr
	sid	 = H5Screate (H5S_SCALAR);
	faid = H5Acreate2 (fid, "test_attr", H5T_STD_I32LE, sid, H5P_DEFAULT, H5P_DEFAULT);
	CHECK_ERR (faid)
	gaid = H5Acreate2 (gid, "test_attr", H5T_STD_I32LE, sid, H5P_DEFAULT, H5P_DEFAULT);
	CHECK_ERR (gaid)

	// Write attr
	err = H5Awrite (faid, H5T_NATIVE_INT32, &buf);
	err = H5Awrite (gaid, H5T_NATIVE_INT32, &buf);

	err = H5Aclose (faid);
	CHECK_ERR (err)
	err = H5Aclose (gaid);
	CHECK_ERR (err)
	err = H5Gclose (gid);
	CHECK_ERR (err)
	err = H5Fclose (fid);
	CHECK_ERR (err)
	err = H5Pclose (faplid);
	CHECK_ERR (err)

err_out:
	SHOW_TEST_RESULT

	MPI_Finalize ();

	return (nerrs > 0);
}
