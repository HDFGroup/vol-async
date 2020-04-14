#HDF5_DIR = /global/homes/h/houhun/hdf5_tmp/hdf5/async_threadsafe/hdf5
#ABT_DIR = /global/homes/h/houhun/cori/argobots/install
HDF5_DIR = /Users/koziol/HDF5/misc_bugs4/review/vol/build_async_parallel/hdf5
ABT_DIR = /Users/koziol/HDF5/misc_bugs4/review/vol/argobots/build_argobots

#CC = cc
#CC = gcc-9
CC = mpicc

DEBUG = -g -O0
CFLAGS = $(DEBUG) -fPIC -I$(ABT_DIR)/include -I$(HDF5_DIR)/include -Wall
LDFLAGS = $(DEBUG) -shared -fPIC -L$(HDF5_DIR)/lib -L$(ABT_DIR)/lib -labt -lhdf5 -lz

SRC = h5_vol_external_async_native.c
OBJ = $(SRC:.c=.o)
LIB = libh5async.so

$(LIB): $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

.PHONY: clean
clean:
	rm -rf $(LIB) $(OBJ)
