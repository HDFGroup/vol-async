ASYNC_DIR = ../src
# HDF5_DIR  = /ccs/home/houjun/summit/hdf5/async_threadsafe/hdf5
# ABT_DIR = /ccs/home/houjun/summit/argobots/install/
HDF5_DIR = /global/u1/h/houhun/hdf5/async_gnu/hdf5
ABT_DIR = /global/homes/h/houhun/cori/argobots/install

CC = cc
#CC = gcc-9
# CC = mpicc

DEBUG = -g -O0
CFLAGS = $(DEBUG) -I$(ABT_DIR)/include -I$(ASYNC_DIR) -I$(HDF5_DIR)/include
#-Wall
LDFLAGS = $(DEBUG) -L$(ASYNC_DIR) -L$(ABT_DIR)/lib -L$(HDF5_DIR)/lib -Wl,-rpath=$(ASYNC_DIR) -Wl,-rpath=$(ABT_DIR)/lib -Wl,-rpath=$(HDF5_DIR)/lib -labt -lhdf5 -lh5async

SRC = async_test_serial.c async_test_serial2.c \
    async_test_multifile.c async_test_multifile_env.c \
    async_test_parallel.c async_test_parallel2.c async_test_parallel3.c \
    async_test_parallel4.c async_test_parallel5.c \
    async_test_serial_event_set.c async_test_serial_error_stack.c
#async_test_no_fclose.c
OBJ = $(SRC:.c=.o)
EXE = $(SRC:.c=.exe)
DBG = $(SRC:.c=.exe.dSYM)
LIB = libh5async.so
# LIB = libh5async.dylib

# Add '.exe' as a targetable suffix
.SUFFIXES: .exe

.c.exe:
	$(CC) $(CFLAGS) $^ -o $*.exe $(LDFLAGS)

all: $(EXE)

check: 
	./pytest.py -p

check_serial: 
	./pytest.py

.PHONY: clean
clean:
	rm -rf $(OBJ) $(EXE) $(DBG) ./*.h5
