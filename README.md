# HDF5 Asynchronous I/O VOL Connector

## Background
Asynchronous I/O is becoming increasingly popular with the large amount of data access required by scientific applications. They can take advantage of an asynchronous interface by scheduling I/O as early as possible and overlap computation or communication with I/O operations, which hides the cost associated with I/O and improves the overall performance.


Some configuration parameters used in the instructions:

        VOL_DIR               : directory of unpacked Asynchronous I/O VOL connector source code
        ABT_DIR               : directory of unpacked Argobots source code
        H5_DIR                : directory of HDF5 source code

1, Preparation

    1.1 Download the Asynchronous I/O VOL connector code (this repository)

       > git clone https://bitbucket.hdfgroup.org/scm/hdf5vol/async.git 

    1.2 Download the Argobots code

       > wget https://github.com/pmodels/argobots/releases/download/v1.0/argobots-1.0.tar.gz
       > tar xf argobots-1.0.tar.gz

    1.3 Download the HDF5 code 

       > git clone https://bitbucket.hdfgroup.org/scm/hdffv/hdf5.git

    1.4 (optional) automake/autoconf may be needed on NERSC machines, if there are any "configuration errors", do the following:

       > module load automake
       > module load autoconf

2, Installation

    2.1 Compile HDF5 ( need to switch to the "async" branch )

        > cd H5_DIR && git checkout async
        > ./autogen.sh  (may skip this step if ./configure exists)
        > CC=cc ./configure --prefix=H5_DIR/build --enable-parallel --enable-threadsafe --enable-unsupported
        > make install

    2.2 Compile Argobots

        >  cd ABT_DIR
        >  ./autogen.sh  (may skip this step if ./configure exists)
        >  CC=cc ./configure --prefix=ABT_DIR/build 
        >  make install
        
    2.3 Compile Asynchronous VOL connector
        > cd VOL_DIR
        > Edit "Makefile" by updating H5_DIR and ABT_DIR to the previously installed locations
        > make

3, Test

    > cd VOL_DIR/test
    > Edit "Makefile" by updating H5_DIR and ABT_DIR to the previously installed locations
    > make

    Run the serial test

        > ./async_test_serial.exe

    Run the parallel test

        > srun -n 2 ./async_test_parallel.exe

4, Using the Asynchronous I/O VOL connector with application code (Implicit mode with environmental variable)

    The implicit mode allows an application to enable asynchronous I/O VOL connector through setting the following environemental variables and without any application code modification. By default, the dataset writes creates a copy of the data buffer, which is automatically freed after the asynchronous write, and all read operations are blocking to ensure the read data is correct.

        > export HDF5_VOL_CONNECTOR="async under_vol=0;under_info={}" 
        > export HDF5_PLUGIN_PATH="VOL_DIR"
        > Run your application

5, Using the Asynchronous I/O VOL connector with application code (Explicit mode)

    Please refer to the Makefile and source code under VOL_DIR/test/ for an example 

    5.1 Include header file

        > #include "h5_vol_external_async_native.h" 

    5.2 Create and set the file access property to be used for asynchronous task execution, currently uses 1 Argobots thread in the background. 

        > hid_t async_fapl = H5Pcreate (H5P_FILE_ACCESS);
        > H5Pset_vol_async(async_fapl);
        > H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl);

    5.3 Create and set the data transfer property for dataset reads and writes. 

        > hid_t async_dxpl = H5Pcreate (H5P_DATASET_XFER);
        > H5Pset_dxpl_async(async_dxpl, true);
        > H5Pget_dxpl_async_cp_limit(async_dxpl, 1048576); // Optional, for a dataset read or write, if the actual data size is larger the set limit (in bytes), then it becomes blocking, to prevent the data buffer being modified by the application before the I/O operations complete.
        > H5Dwrite(dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, async_dxpl, data);

    5.4 Finalize asynchronous I/O VOL, must be called before exiting the application to block and allow all asynchronous I/O tasks to be completed in the background thread.

        > H5VLasync_finalize();

    5.5 Explicitly wait for asynchronous task to be completed. (Optional)

        > H5Dwait(dset_id); // Wait for all I/O tasks of the dataset
        > H5Fwait(file_id); // Wait for all I/O tasks of the file

    5.6 May need to set the following environmental variable before running your application.

        > export LD_LIBRARY_PATH=VOL_DIR/src:H5_DIR/build/lib:$LD_LIBRARY_PATH
