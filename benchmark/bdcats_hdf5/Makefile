# HDF5_DIR = /global/homes/h/houhun/hdf5_tmp/hdf5/async_threadsafe/hdf5

HDF5_DIR  = /gpfs/alpine/csc300/world-shared/gnu_build/hdf5-1.10.6/build/hdf5

# CC = cc
# CC = gcc-9
CC = mpicc

# DEBUG = -g -O0
CFLAGS = $(DEBUG) -I$(HDF5_DIR)/include
#-Wall
LDFLAGS = $(DEBUG) -L$(HDF5_DIR)/lib -lhdf5 -Wl,-rpath,$(HDF5_DIR)/lib

SRC = bdcatsio.c
OBJ = $(SRC:.c=.o)
EXE = $(SRC:.c=.exe)
DBG = $(SRC:.c=.exe.dSYM)
LIB = libh5async.so

# Add '.exe' as a targetable suffix
.SUFFIXES: .exe

.c.exe:
	$(CC) $(CFLAGS) $^ -o $*.exe $(LDFLAGS)

all: $(EXE)

.PHONY: clean
clean:
	rm -rf $(OBJ) $(EXE) $(DBG) ./*.h5
