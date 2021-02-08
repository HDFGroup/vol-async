#HDF5_DIR  = ../../../build_parallel_debug/hdf5
#ABT_DIR = /Users/koziol/HDF5/github/hdfgroup/async/argobots/build_argobots
HDF5_DIR  = /Users/koziol/HDF5/github/hpc-io/async_vol_register_optional/build_parallel_debug/hdf5
ABT_DIR = /Users/koziol/NERSC/install
HDF5_DIR=$(HDF5_ROOT)
ABT_DIR=$(HOME)/opt/argobots/
#CC = cc
#CC = gcc-9
CC = mpicc

# These lines may need to be adjusted for your compiler environment:
#DEBUG = -g -O0 -DENABLE_ASYNC_LOGGING
DEBUG = -g -O3
CFLAGS = $(DEBUG) -fPIC -I$(ABT_DIR)/include -I$(HDF5_DIR)/include -Wall
LIBS = -L$(HDF5_DIR)/lib -L$(ABT_DIR)/lib  -lhdf5 -lz $(ABT_DIR)/lib/libabt.a
ARFLAGS = rs

# Uncomment this line Linux builds:
DYNLDFLAGS = $(DEBUG) -shared -fPIC $(LIBS)
# Uncomment this line MacOS builds:
#DYNLDFLAGS = $(DEBUG) -dynamiclib -current_version 1.0  -fPIC $(LIBS)

DYNSRC = h5_vol_external_async_native.c
DYNHDR = h5_vol_external_async_native.h
DYNOBJ = $(DYNSRC:.c=.o)
DYNPCH = $(DYNHDR:.h=.h.gch)
# Uncomment this line Linux builds:
# DYNLIB = libh5async.so
# Uncomment this line MacOS builds:
DYNLIB = libh5async.so

STATSRC = h5_async_lib.c
STATHDR = h5_async_lib.h
STATOBJ = $(STATSRC:.c=.o)
STATPCH = $(STATHDR:.h=.h.gch)
STATLIB = libasynchdf5.a

all: $(DYNLIB) $(STATLIB)

$(DYNOBJ): $(DYNSRC) $(DYNHDR)

$(DYNLIB): $(DYNOBJ)
	$(CC) $(CFLAGS) $(DYNLDFLAGS) $^ -o $@

$(STATOBJ): $(STATSRC) $(STATHDR)
	$(CC) -c $(CFLAGS) $^

$(STATLIB): $(STATOBJ)
	$(AR) $(ARFLAGS) $@ $^

.PHONY: clean
clean:
	rm -rf $(DYNLIB) $(DYNOBJ) $(DYNPCH) $(STATLIB) $(STATOBJ) $(STATPCH)
