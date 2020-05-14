/*
 * test_node_local_util.c
 *
 *  Created on: Apr 27, 2020
 *      Author: tonglin
 */


#include <sys/mman.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include "node_local_util.h"

int main(int argc, char* argv[]) {
    char* str = "This is not a quite long string, with a return at the end.\n";
    size_t map_size = 0;
    int pid = getpid();
    int test_cnt = atoi(argv[1]);
    mmap_file** test_maps = (mmap_file**)calloc(test_cnt, 1*sizeof(mmap_file*));
    unsigned long t1 = get_time_usec();
    for(int i = 0; i < test_cnt; i++){
        char file_path[100] = {'\0'};  // = argv[1];
        sprintf(file_path, "%d-%d.mmp", pid, i);
        int fd = open(file_path, O_RDWR|O_CREAT, 0666);
        mmap_file* mmpf = mmap_new_fd(strlen(str)+1, fd);
        mmpf->file_path = strdup(file_path);
        assert(mmpf);
        test_maps[i] = mmpf;
    }
    unsigned long t2 = get_time_usec();
    //mmap_file* mmpf = mmap_setup_newfile(strlen(str) + 1, file_path, O_RDWR|O_CREAT);



    //mmap_cpy(mmpf, (void*)str, 0, strlen(str) + 1);
    //size_t new_size = mmap_extend_quick(mmpf, 9000);

//    printf("extended mmap size = %zu bytes\n", mmpf->current_size);
//
//    mmap_cpy(mmpf, (void*)str, strlen(str), strlen(str) + 1);

    unsigned long t3 = get_time_usec();
    for(int i = 0; i < test_cnt; i++){
        char file_path[100] = {'\0'};  // = argv[1];
        sprintf(file_path, "%d-%d.mmp", pid, i);
        mmap_free(test_maps[i], 1, 1);
    }
    unsigned long t4 = get_time_usec();

    free(test_maps);
    //munmap(mmpf, map_size);
    //close(fd);
    printf("Created and freed %d new mmaps, create avg = %zu usec, freed avg = %zu usec\n", test_cnt, (t2-t1)/test_cnt, (t4-t3)/test_cnt);

  return 0;
}
