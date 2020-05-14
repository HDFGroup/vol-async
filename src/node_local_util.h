/*
 * node_local_util.h
 *
 *  Created on: Apr 27, 2020
 *      Author: tonglin
 */
#include <fcntl.h>

#ifndef SRC_NODE_LOCAL_UTIL_H_
#define SRC_NODE_LOCAL_UTIL_H_

#define DEBUG printf("%s:%d\n", __func__, __LINE__);
#define OPEN_FLAGS_DEFAULT O_RDWR|O_CREAT

typedef struct Mmaped_file{
    char* file_path;
    int fd;
    int open_flags;
    size_t current_size;
    void* map;
}mmap_file;

//open_flags = O_RDWR|O_CREAT for default.
mmap_file* mmap_new_file(size_t init_size, char* file_path, int open_flags);
mmap_file* mmap_new_fd(size_t init_size, int fd);
int mmap_free(mmap_file* mmf, int rm_file, int close_fd);
int mmap_cpy(mmap_file* mmf, void* src, size_t offset, size_t len);
size_t mmap_extend_quick(mmap_file* mmf, size_t new_size);
size_t mmap_extend_increament(mmap_file* mmf, size_t addition_size);
unsigned long get_time_usec();
#endif /* SRC_NODE_LOCAL_UTIL_H_ */
