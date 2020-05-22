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

unsigned long mmap_create_overhead(int test_cnt){
    mmap_file** test_maps = (mmap_file**)calloc(test_cnt, 1*sizeof(mmap_file*));
    int pid = getpid();
    unsigned long t1 = get_time_usec();
    for(int i = 0; i < test_cnt; i++){
        char file_path[100] = {'\0'};  // = argv[1];
        sprintf(file_path, "%d-%d.mmp", pid, i);
        int fd = open(file_path, O_RDWR|O_CREAT, 0666);
        mmap_file* mmpf = mmap_new_fd(1000, fd);
        mmpf->file_path = strdup(file_path);
        assert(mmpf);
        test_maps[i] = mmpf;
    }
    unsigned long t2 = get_time_usec();

    printf("mmap_create_overhead ran %d times, avg time = %lu us\n", test_cnt, (t2-t1)/test_cnt);
    for(int i = 0; i < test_cnt; i++){
        mmap_free(test_maps[i], 1, 1);
    }
    free(test_maps);
    return t2 - t1;
}

unsigned long mmap_cpy_overwrite_overhead(int test_cnt, size_t write_size, mmap_sync_mode mode){
    int pid = getpid();
    char file_path[100] = {'\0'};  // = argv[1];
    sprintf(file_path, "%d.mmp", pid);
    int fd = open(file_path, O_RDWR|O_CREAT, 0666);
    mmap_file* mmpf = mmap_new_fd(1000, fd);
    mmpf->file_path = strdup(file_path);
    assert(mmpf);

    char* write_buf_src = calloc(1, write_size * sizeof(char));
    memset(write_buf_src, 'w', write_size);

    unsigned long t1 = get_time_usec();
    for(int i = 0; i < test_cnt; i++){
        mmap_cpy(mmpf, write_buf_src, 0, write_size, mode);
    }
    unsigned long t2 = get_time_usec();
    mmap_free(mmpf, 1, 1);
    unsigned long t3 = get_time_usec();
    printf("mmap_cpy ran %d times, cost total = %lu us, avg time = %lu, free/flushing costs %lu us\n",
            test_cnt, t2 - t1, (t2 - t1)/test_cnt, t3 - t2);
    return t2 - t1;
}

unsigned long write_overhead(int test_cnt, size_t write_size, char* prefix){
    int pid = getpid();
    char file_path[100] = {'\0'};  // = argv[1];
    sprintf(file_path, "%s/%d.mmp", prefix, pid);
    int fd_mmap = open(file_path, O_RDWR|O_CREAT, 0666);
    mmap_file* mmpf = mmap_new_fd(write_size, fd_mmap);
    mmpf->file_path = strdup(file_path);
    assert(mmpf);

    char* write_buf_src = calloc(1, write_size * sizeof(char));
    memset(write_buf_src, 'w', write_size);

    unsigned long t1 = get_time_usec();
    for(int i = 0; i < test_cnt; i++){
        mmap_cpy(mmpf, write_buf_src, 0, write_size, MMAP_SYNC);
    }
    unsigned long t2 = get_time_usec();
    printf("mmap_cpy ran %d times, wrote %lu bytes each time, avg time = %lu us\n",
            test_cnt, write_size, (t2-t1)/test_cnt);

    sleep(2);
    sprintf(file_path, "%d.psx", pid);
    int fd_posix = open(file_path, O_RDWR|O_CREAT, 0666);
    unsigned long t3 = get_time_usec();
    for(int i = 0; i < test_cnt; i++){
        write(fd_posix, write_buf_src, write_size);
        //syncfs(fd_posix);
        sync();
    }
    unsigned long t4 = get_time_usec();

    printf("posix write ran %d times, wrote %lu bytes each time, avg time = %lu us\n",
            test_cnt, write_size, (t4-t3)/test_cnt);
    return 0;
}

int main(int argc, char* argv[]) {
    int test_cnt = atoi(argv[1]);

    int write_size = atoi(argv[2]);
    //mmap_cpy_overwrite_overhead(test_cnt, 1000, MMAP_SYNC);
    char* prefix = argv[3];
    write_overhead(test_cnt, write_size, prefix);

  return 0;
}
