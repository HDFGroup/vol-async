# HDF5_DIR = /global/homes/h/houhun/hdf5_tmp/hdf5/async_threadsafe/hdf5
# ABT_DIR = /global/homes/h/houhun/cori/argobots/install
# ASYNC_DIR = /global/u1/h/houhun/hdf5vol/async/src

HDF5_DIR  = /autofs/nccs-svm1_home1/houjun/summit/hdf5/async_threadsafe/hdf5
ABT_DIR = /autofs/nccs-svm1_home1/houjun/summit/argobots/install
ASYNC_DIR = /autofs/nccs-svm1_home1/houjun/summit/async/src

# CC = cc
# CC = gcc-9
CC = mpicc

DEBUG = -g -O0
CFLAGS = $(DEBUG) -I$(ABT_DIR)/include -I$(ASYNC_DIR) -I$(HDF5_DIR)/include
#-Wall
LDFLAGS = $(DEBUG) -L$(ASYNC_DIR) -L$(ABT_DIR)/lib -L$(HDF5_DIR)/lib -labt -lhdf5 -lh5async -Wl,-rpath=$(ABT_DIR)/lib -Wl,-rpath=$(HDF5_DIR)/lib -Wl,-rpath=$(ASYNC_DIR)

SRC = vpicio_uni_h5.c
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
