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

#include "node_local_util.h"
// assume to create a empty file, and need to extend for writing.
mmap_file *
mmap_attach_fd(size_t init_size, int fd)
{
    return NULL;
}

mmap_file *
mmap_setup_newfile(size_t init_size, char *file_path, int open_flags)
{

    int page_size = getpagesize();
    assert(init_size > 0);
    size_t map_size = 0;

    if (init_size % page_size != 0)
        map_size = page_size * (init_size / page_size + 1);
    else
        map_size = init_size;

    // int open_flags = O_RDWR|O_CREAT;
    int fd = open(file_path, open_flags, 0666); // O_CREAT|O_RDWR|O_APPEND
    if (fd < 0) {
        perror("open failed");
        return NULL;
    }
    printf("fd = %d\n", fd);
    int err = ftruncate(fd, map_size); // allocate space for file extension.
    if (err != 0) {
        perror("ftruncate failed");
        return NULL;
    }
    void *map = mmap(NULL, // no preference
                     map_size,
                     PROT_READ | PROT_WRITE | PROT_EXEC, // access mode
                     MAP_SHARED,                         // change write to file and visible immediately
                     fd,
                     0 // offset
    );

    if (map == MAP_FAILED) {
        perror("mmap failed");
        return NULL;
    }

    mmap_file *mmf    = (mmap_file *)calloc(1, sizeof(mmap_file));
    mmf->current_size = map_size;
    mmf->file_path    = strdup(file_path);
    mmf->fd           = fd;
    mmf->open_flags   = open_flags;
    mmf->map          = map;
    return mmf;
}

int
mmap_free(mmap_file *mmf)
{
    assert(mmf && mmf->map);
    munmap(mmf->map, mmf->current_size);
    if (mmf->file_path)
        free(mmf->file_path);
    free(mmf);
    return 0;
}

int
mmap_cpy(mmap_file *mmf, void *src, size_t offset, size_t len)
{
    assert(mmf && mmf->map);
    if (offset + len > mmf->current_size) {
        printf("Attempting offset + len > map size. It must be less than %zu bytes. Please call "
               "mmap_extend() first.\n",
               mmf->current_size);
        return -1;
    }
    memcpy(mmf->map + offset, (char *)src, len);
    return 0;
}

size_t
mmap_extend(mmap_file *mmf, size_t addition_size)
{
    assert(mmf && mmf->map);
    int page_size = getpagesize();
    assert(mmf->current_size > 0 && mmf->current_size % page_size == 0 && addition_size > 0);
    size_t new_map_size = 0;
    if (addition_size % page_size == 0)
        new_map_size = mmf->current_size + addition_size;
    else
        new_map_size = mmf->current_size + (addition_size / page_size + 1) * page_size;
    int err           = ftruncate(mmf->fd, new_map_size);
    mmf->current_size = new_map_size;

    if (err != 0) {
        perror("ftruncate failed");
        return err;
    }
    else {
        return new_map_size;
    }
}
