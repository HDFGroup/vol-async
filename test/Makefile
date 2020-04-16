#HDF5_DIR = /global/homes/h/houhun/hdf5_tmp/hdf5/async_threadsafe/hdf5
#ABT_DIR = /global/homes/h/houhun/cori/argobots/install
HDF5_DIR = /Users/koziol/HDF5/misc_bugs4/review/vol/build_async_parallel_production/hdf5
ABT_DIR = /Users/koziol/HDF5/misc_bugs4/review/vol/argobots/build_argobots
ASYNC_DIR = ../src

#CC = cc
#CC = gcc-9
CC = mpicc

DEBUG = -g -O0
CFLAGS = $(DEBUG) -I$(ABT_DIR)/include -I$(ASYNC_DIR) -I$(HDF5_DIR)/include
#-Wall
LDFLAGS = $(DEBUG) -L$(ASYNC_DIR) -L$(ABT_DIR)/lib -L$(HDF5_DIR)/lib -labt -lhdf5 -lh5async

SRC = async_test_serial.c async_test_serial2.c \
    async_test_multifile.c async_test_multifile_env.c \
    async_test_parallel.c async_test_parallel2.c async_test_parallel3.c \
    async_test_parallel4.c async_test_parallel5.c
    # async_test_token.c
OBJ = $(SRC:.c=.o)
EXE = $(SRC:.c=.exe)
DBG = $(SRC:.c=.exe.dSYM)
#LIB = libh5async.so
LIB = libh5async.dylib

# Add '.exe' as a targetable suffix
.SUFFIXES: .exe

.c.exe:
	$(CC) $(CFLAGS) $^ -o $*.exe $(LDFLAGS)

all: $(EXE)

.PHONY: clean
clean:
	rm -rf $(OBJ) $(EXE) $(DBG) ./*.h5
