#include <hdf5.h>
#include <stdio.h>
#include <stdlib.h>

#include "h5vl_async_public.h"
#include "testutils.h"

#define N 10

int main (int argc, char **argv) {
	int err, nerrs = 0;
	int rank, np;
	const char *file_name;
	hid_t fid, gid, sgid;
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
	SHOW_TEST_INFO ("Creating groups")

	// Use Async VOL plugin
	faplid = H5Pcreate (H5P_FILE_ACCESS);
	H5Pset_vol_async (faplid);

	// Create file
	fid = H5Fcreate (file_name, H5F_ACC_TRUNC, H5P_DEFAULT, faplid);
	CHECK_ERR (fid)

	// Create group
	gid = H5Gcreate2 (fid, "test", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	CHECK_ERR (gid)
	// Create sub-group
	sgid = H5Gcreate2 (gid, "test2", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	CHECK_ERR (sgid)
	err = H5Gclose (sgid);
	CHECK_ERR (err)
	err = H5Gclose (gid);
	CHECK_ERR (err)

	// Open group
	gid = H5Gopen2 (fid, "test", H5P_DEFAULT);
	CHECK_ERR (gid)
	// Open sub-group
	sgid = H5Gopen2 (gid, "test2", H5P_DEFAULT);
	CHECK_ERR (sgid)
	err = H5Gclose (sgid);
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
