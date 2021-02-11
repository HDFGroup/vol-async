# HDF5_DIR = /global/homes/h/houhun/hdf5_tmp/hdf5/async_threadsafe/hdf5

HDF5_DIR = /global/u1/h/houhun/hpc-io/hdf5/async_optional_gnu/hdf5

CC = cc
# CC = gcc-9
# CC = mpicc

# DEBUG = -g -O0
CFLAGS = $(DEBUG) -I$(HDF5_DIR)/include
#-Wall
LDFLAGS = $(DEBUG) -L$(HDF5_DIR)/lib -lhdf5 -Wl,-rpath,$(HDF5_DIR)/lib

SRC = vpicio_uni_h5.c
OBJ = $(SRC:.c=.o)
EXE = $(SRC:.c=.exe)
DBG = $(SRC:.c=.exe.dSYM)

# Add '.exe' as a targetable suffix
.SUFFIXES: .exe

.c.exe:
	$(CC) $(CFLAGS) $^ -o $*.exe $(LDFLAGS)

all: $(EXE)

.PHONY: clean
clean:
	rm -rf $(OBJ) $(EXE) $(DBG) ./*.h5
