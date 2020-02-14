HDF5_DIR=/global/homes/h/houhun/hdf5_tmp/hdf5/async_threadsafe/hdf5
ABT_DIR=/global/homes/h/houhun/cori/argobots/install
cc=cc
ASYNC_DIR=../src

COMMON=-I$(ABT_DIR)/include -I$(ASYNC_DIR) -I$(HDF5_DIR)/include -L$(ASYNC_DIR) -L$(ABT_DIR)/lib -L$(HDF5_DIR)/lib -labt -lhdf5 -g -lh5async 

all:
	$(cc) async_test_serial.c    $(COMMON) -o async_test_serial.exe  	   
	$(cc) async_test_multifile.c    $(COMMON) -o async_test_multifile.exe  	   
	# $(cc) async_test_token.c     $(COMMON) -o async_test_token.exe  	   
	$(cc) async_test_parallel.c  $(COMMON) -o async_test_parallel.exe         
	$(cc) async_test_parallel2.c $(COMMON) -o async_test_parallel2.exe        
	$(cc) async_test_parallel3.c $(COMMON) -o async_test_parallel3.exe        
	$(cc) async_test_parallel4.c $(COMMON) -o async_test_parallel4.exe        
	$(cc) async_test_parallel5.c $(COMMON) -o async_test_parallel5.exe        

clean:
	rm -rf ./*.o ./*.exe ./*.h5
