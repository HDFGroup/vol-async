/*
 * node_local_util.c
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
#include <sys/time.h>

#include "node_local_util.h"
//assume to attach to a empty file, and need to extend for mmapping.
mmap_file* _mmap_setup(mmap_file* mmf, size_t init_size, int fd){
    int page_size = getpagesize();
    assert(init_size > 0);
    size_t map_size = 0;

    if(init_size % page_size != 0)
        map_size = page_size * (init_size / page_size + 1);
    else
        map_size = init_size;

    //printf("fd = %d\n", fd);
        int err = ftruncate(fd, map_size);//allocate space for file extension.
        if(err != 0){
            perror("ftruncate failed");
            return NULL;
        }
        void* map = mmap(
                NULL, //no preference
                map_size,
                PROT_READ|PROT_WRITE|PROT_EXEC, //access mode
                MAP_SHARED, //change write to file and visible immediately
                fd,
                0           //offset
            );

        if(map == MAP_FAILED){
            perror("mmap failed");
            return NULL;
        }

        (*mmf).fd = fd;
        (*mmf).current_size = map_size;
        (*mmf).map = map;
        return mmf;
}

mmap_file* mmap_new_fd(size_t init_size, int fd){
    mmap_file* mmf = (mmap_file*)calloc(1, sizeof(mmap_file));
    mmf->file_path = NULL;
    return _mmap_setup(mmf, init_size, fd);
}


//open_flags = O_RDWR|O_CREAT by default
mmap_file* mmap_new_file(size_t init_size, char* file_path, int open_flags){
    mmap_file* mmf = (mmap_file*)calloc(1, sizeof(mmap_file));
    mmf->file_path = strdup(file_path);
    //int open_flags = O_RDWR|O_CREAT;
    int fd = open(file_path, open_flags, 0666);//O_CREAT|O_RDWR|O_APPEND
    if(fd < 0){
        perror("open failed");
        return NULL;
    }
    return _mmap_setup(mmf, init_size, fd);
}

int mmap_free(mmap_file* mmf, int rm_file, int close_fd){
    assert(mmf && mmf->map);
    munmap(mmf->map, mmf->current_size);

    if(rm_file == 1)
        unlink(mmf->file_path);

    if(mmf->file_path)
        free(mmf->file_path);

    if(close_fd == 1)
        close(mmf->fd);

    free(mmf);
    return 0;
}

int mmap_cpy(mmap_file* mmf,  void* src, size_t offset, size_t len){
    assert(mmf && mmf->map);
    if(offset + len > mmf->current_size){
        printf("Attempting offset + len > map size. It must be less than %zu bytes. Please call mmap_extend() first.\n", mmf->current_size);
        return -1;
    }
    memcpy(mmf->map + offset, (char*)src, len);
    return 0;
}

size_t mmap_extend_quick(mmap_file* mmf, size_t new_size){
    assert(mmf->map );
    int page_size = getpagesize();
    if(new_size <= mmf->current_size){
        return mmf->current_size;
    }
    size_t new_map_size = 2* (new_size/page_size + 1) * page_size;

    int err = ftruncate(mmf->fd, new_map_size);
    if(err!=0){//fail to double
        printf("ftruncate failed to double size, attempting exact size (%zu bytes) ...\n", new_map_size/2);
        err = ftruncate(mmf->fd, new_map_size);
        if(err != 0){
            printf("ftruncate failed to extend to exact size (%zu bytes), no enough space.\n", new_map_size/2);
            return -1;
        } else {//success
            new_map_size = new_map_size/2;
        }
    }
    mmf->current_size = new_map_size;
    return new_map_size;
}

size_t mmap_extend_increament(mmap_file* mmf, size_t addition_size){
    assert(mmf && mmf->map);
    int page_size = getpagesize();
    assert(mmf->current_size > 0 && mmf->current_size % page_size == 0 && addition_size > 0);
    size_t new_map_size = 0;
    if(addition_size % page_size == 0)
        new_map_size = mmf->current_size + addition_size;
    else
        new_map_size = mmf->current_size + (addition_size/page_size + 1) * page_size;
    int err = ftruncate(mmf->fd, new_map_size);

    if(err != 0){
        perror("ftruncate failed");
        return -1;
    } else {
        mmf->current_size = new_map_size;
        return new_map_size;
    }
}


unsigned long get_time_usec(){
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return 1000000 * tv.tv_sec + tv.tv_usec;
}
