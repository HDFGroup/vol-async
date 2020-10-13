#HDF5_DIR = /global/homes/h/houhun/hdf5_tmp/hdf5/async_threadsafe/hdf5
#ABT_DIR = /global/homes/h/houhun/cori/argobots/install
HDF5_DIR = /global/homes/h/houhun/hdf5/async_gnu/hdf5
ABT_DIR = /global/homes/h/houhun/cori/argobots/install

CC = cc
#CC = gcc-9
# CC = mpicc

DEBUG = -g -O0
CFLAGS = $(DEBUG) -fPIC -I$(ABT_DIR)/include -I$(HDF5_DIR)/include -Wall
LDFLAGS = $(DEBUG) -shared -fPIC -L$(HDF5_DIR)/lib -L$(ABT_DIR)/lib -labt -lhdf5 -lz
# LDFLAGS = $(DEBUG) -dynamiclib -current_version 1.0  -fPIC -L$(HDF5_DIR)/lib -L$(ABT_DIR)/lib -labt -lhdf5 -lz

SRC = h5_vol_external_async_native.c
OBJ = $(SRC:.c=.o)
LIB = libh5async.so
# LIB = libh5async.dylib

$(LIB): $(OBJ)
	$(CC) $(LDFLAGS) $^ -o $@

.PHONY: clean
clean:
	rm -rf $(LIB) $(OBJ)
