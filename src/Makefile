HDF5_DIR=/global/homes/h/houhun/hdf5_tmp/hdf5/async_threadsafe/hdf5
ABT_DIR=/global/homes/h/houhun/cori/argobots/install
ASYNC_DIR=.
DEBUG=-g -O0

COMMON=-shared -fPIC -I$(ABT_DIR)/include -I$(ASYNC_DIR) -I$(HDF5_DIR)/include -L$(HDF5_DIR)/lib -L$(ABT_DIR)/lib -labt -lhdf5 $(DEBUG)  $(ASYNC_DIR)/h5_vol_external_async_native.c 
#-Wall

CC=cc

all:
	$(CC) $(COMMON) -o libh5async.so 

clean:
	rm -rf ./*.so 
