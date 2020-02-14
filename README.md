# HDF5 Asynchronous I/O VOL connector

## Background
Asynchronous I/O is becoming increasingly popular with the large amount of data access required by scientific applications. They can take advantage of an asynchronous interface by scheduling I/O as early as possible and overlap computation or communication with I/O operations, which hides the cost associated with I/O and improves the overall performance


Some configuration parameters used in the instructions:

        VOL_DIR               : directory of unpacked Asynchronous I/O VOL connector source code
        ABT_DIR               : directory of unpacked Argobots source code
        H5_DIR                : directory of HDF5 source code

1, Preparation

    1.1 Download the Asynchronous I/O VOL connector code 

       > git clone git@bitbucket.org:berkeleylab/exahdf5.git VOL_DIR

    1.2 Download the Argobots code

       > git clone git@github.com:pmodels/argobots.git ABT_DIR

    1.3 Download the HDF5 and switch to "async" branch 

       > git clone https://bitbucket.hdfgroup.org/scm/hdffv/hdf5.git  H5_DIR
       > cd hdf5 && git checkout async

    1.4 automake/autoconf may be needed on NERSC machines, if there are any "configuration errors".

       > module load automake
       > module load autoconf

2, Installation

    2.1 Compile HDF5 

        >  cd H5_DIR
        >  ./autogen.sh  (skip this step if ./configure exists)
        >  CC=cc ./configure --prefix=H5_DIR/build --enable-parallel --enable-threadsafe --enable-unsupported
            Note: (add --disable-shared on NERSC machines) 
        >  make install

    2.2 Compile Argobots

        >  cd ABT_DIR
        >  ./autogen.sh  (skip this step if ./configure exists)
        >  CC=cc ./configure --prefix=ABT_DIR/build 
        >  make install
        
    2.3 Compile Asynchronous VOL connector
        > cd VOL_DIR
        > make

3, Test

    > cd VOL_DIR/test
        Note: edit "Makefile" by updating below two variables with right locations
        -- HDF5_DIR = HDF5_DIR/build
        -- ABT_DIR = ABT_DIR/build

    > make

    Run the serial test

        > ./async_test_serial.exe

    Run the parallel test

        > srun -n 2 ./async_test_parallel.exe

4, Using the Asynchronous I/O VOL connector with application code

    Please refer to the Makefile and source code under VOL_DIR/benchmarks/vpicio_async. Only the following code needs to be added:

    4.1 Include header file

        > #include "h5_vol_external_async_native.h" 

    4.2 Initialize Asynchronous I/O VOL connector, and set file access property with the number of threads to be used for asynchronous task execution. 

        > H5VLasync_init(H5P_DEFAULT);
        > async_fapl = H5Pcreate (H5P_FILE_ACCESS);
        > int nthread = 1;
        > H5Pset_vol_async(async_fapl, nthread);

    4.3 Explicit wait for asynchronous task to be completed (Optional, only needed when the user wants to reuse the data buffer)

        > H5Dwait(dset_id);
        > H5Fwait(file_id);

    4.4 Finalize asynchronous I/O VOL, needs to be called before exiting the application to ensure all asynchronous I/O tasks have been completed.

        > H5VLasync_finalize();
