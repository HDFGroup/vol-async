ifndef HDF5_DIR
    HDF5_DIR = /global/u1/h/houhun/hdf5/develop_gnu/hdf5
endif

ifndef ABT_DIR
    ABT_DIR = /global/u1/h/houhun/cori/argobots/install
endif

ifndef ASYNC_DIR
    ASYNC_DIR = /global/homes/h/houhun/hdf5vol/vol-async/src
endif

STATLIB = libasynchdf5.a
# Uncomment this line Linux builds:
DYNLIB = libh5async.so
# Uncomment this line MacOS builds:
# DYNLIB = libh5async.dylib

CC = cc
# CC = gcc-9
# CC = mpicc

HDF5_LIBDIR = $(shell if [ -d $$HDF5_DIR/lib ]; then echo "lib"; else echo "lib64"; fi)
ABT_LIBDIR =  $(shell if [ -d $$ABT_DIR/lib ]; then echo "lib"; else echo "lib64"; fi)

# These lines may need to be adjusted for your compiler environment:
DEBUG = -g -O0 -Wall
CFLAGS = $(DEBUG) -I$(ABT_DIR)/include -I$(ASYNC_DIR) -I$(HDF5_DIR)/include

# Uncomment this line Linux builds:
LDFLAGS = $(DEBUG) -L$(ASYNC_DIR) -L$(ABT_DIR)/$(ABT_LIBDIR) -L$(HDF5_DIR)/$(HDF5_LIBDIR) -Wl,-rpath=$(ASYNC_DIR) -Wl,-rpath=$(ABT_DIR)/$(ABT_LIBDIR) -Wl,-rpath=$(HDF5_DIR)/$(HDF5_LIBDIR) -labt -lhdf5 -lh5async -lasynchdf5
# Uncomment this line MacOS builds:
# LDFLAGS = $(DEBUG) -L$(ASYNC_DIR) -L$(ABT_DIR)/lib -L$(HDF5_DIR)/lib -labt -lhdf5 -lasynchdf5 -lh5async 

SRC = async_test_serial.c async_test_serial2.c \
    async_test_multifile.c async_test_multifile_env.c \
    async_test_parallel.c async_test_parallel2.c async_test_parallel3.c \
    async_test_parallel4.c async_test_parallel5.c \
    async_test_serial_event_set.c async_test_serial_error_stack.c \
    async_test_serial_event_set_error_stack.c

#async_test_no_fclose.c
OBJ = $(SRC:.c=.o)
EXE = $(SRC:.c=.exe)
DBG = $(SRC:.c=.exe.dSYM)

# Add '.exe' as a targetable suffix
.SUFFIXES: .exe

.c.exe: $(ASYNC_DIR)/$(DYNLIB) $(ASYNC_DIR)/$(STATLIB)
	$(CC) $(CFLAGS) $^ -o $*.exe $(LDFLAGS)

all: $(EXE)

check: 
	python3 ./pytest.py -p

check_serial: 
	python3 ./pytest.py

.PHONY: clean
clean:
	rm -rf $(OBJ) $(EXE) $(DBG) ./*.h5
