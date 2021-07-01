/*
 * node_local_util.h
 *
 *  Created on: Apr 27, 2020
 *      Author: tonglin
 */

#ifndef SRC_NODE_LOCAL_UTIL_H_
#define SRC_NODE_LOCAL_UTIL_H_

#define DEBUG printf("%s:%d\n", __func__, __LINE__);

typedef struct Mmaped_file {
    char * file_path;
    int    fd;
    int    open_flags;
    size_t current_size;
    void * map;
} mmap_file;

// open_flags = O_RDWR|O_CREAT for default.
mmap_file *mmap_setup_newfile(size_t init_size, char *file_path, int open_flags);
int        mmap_free(mmap_file *mmf);
int        mmap_cpy(mmap_file *mmf, void *src, size_t offset, size_t len);
size_t     mmap_extend(mmap_file *mmf, size_t addition_size);

#endif /* SRC_NODE_LOCAL_UTIL_H_ */
