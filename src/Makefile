ifndef HDF5_DIR
    HDF5_DIR = /global/u1/h/houhun/hdf5/develop_gnu/hdf5
endif

ifndef ABT_DIR
    ABT_DIR = /global/u1/h/houhun/cori/argobots/install
endif

CC = cc
#CC = gcc-9
# CC = mpicc

HDF5_LIBDIR = $(shell if [ -d $$HDF5_DIR/lib ]; then echo "lib"; else echo "lib64"; fi)
ABT_LIBDIR =  $(shell if [ -d $$ABT_DIR/lib ]; then echo "lib"; else echo "lib64"; fi)

# These lines may need to be adjusted for your compiler environment:
DEBUG = -g -O0 
CFLAGS = $(DEBUG) -fPIC -I$(ABT_DIR)/include -I$(HDF5_DIR)/include -Wall
LIBS = -L$(HDF5_DIR)/$(HDF5_LIBDIR) -L$(ABT_DIR)/$(ABT_LIBDIR) -labt -lhdf5 -lz
ARFLAGS = rs

# Uncomment this line Linux builds:
DYNLDFLAGS = $(DEBUG) -shared -fPIC $(LIBS)
# Uncomment this line MacOS builds:
# DYNLDFLAGS = $(DEBUG) -dynamiclib -current_version 1.0  -fPIC $(LIBS)

DYNSRC = h5_async_vol.c
DYNHDR = h5_async_vol.h
DYNOBJ = $(DYNSRC:.c=.o)
DYNPCH = $(DYNHDR:.h=.h.gch)
# Uncomment this line Linux builds:
DYNLIB = libh5async.so
# Uncomment this line MacOS builds:
# DYNLIB = libh5async.dylib

STATSRC = h5_async_lib.c
STATHDR = h5_async_lib.h
STATOBJ = $(STATSRC:.c=.o)
STATPCH = $(STATHDR:.h=.h.gch)
STATLIB = libasynchdf5.a

all: $(DYNLIB) $(STATLIB)

$(DYNOBJ): $(DYNSRC) $(DYNHDR)

$(DYNLIB): $(DYNOBJ)
	$(CC) $(CFLAGS) $(DYNLDFLAGS) $^ -o $@

$(STATOBJ): $(STATSRC)
	$(CC) -c $(CFLAGS) $^

$(STATLIB): $(STATOBJ)
	$(AR) $(ARFLAGS) $@ $^

.PHONY: clean
clean:
	rm -rf $(DYNLIB) $(DYNOBJ) $(DYNPCH) $(STATLIB) $(STATOBJ) $(STATPCH)

format:
	clang-format -i -style=file ./*.c
	clang-format -i -style=file ./*.h
	clang-format -i -style=file ../test/*.c
