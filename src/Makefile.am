# Copyright (C) 2020, Lawrence Berkeley National Laboratory.                
# All rights reserved.                                                      
#                                                                           
# This file is part of AsyncVOL. The full AsyncVOL copyright notice,    
# including terms governing use, modification, and redistribution, is       
# contained in the file COPYING at the root of the source code distribution 
# tree.     

SUFFIXES = .c .hpp .o

AM_CPPFLAGS = -I.

M4FLAGS = -I${top_srcdir}/m4

lib_LTLIBRARIES = libh5vl_async.la

M4_SRCS = 

H_SRCS =    

CXX_SRCS = 

C_SRCS =    h5vl_async.c \
            h5vl_async_attr.c \
            h5vl_async_blob.c \
            h5vl_async_dset.c \
            h5vl_async_dtype.c \
            h5vl_async_file.c \
            h5vl_async_group.c \
            h5vl_async_info.c \
            h5vl_async_link.c \
            h5vl_async_obj.c \
            h5vl_async_public.c \
            h5vl_async_req.c \
            h5vl_async_token.c \
            h5vl_async_wrap.c \
            h5vl_asynci_vector.c \
            h5vl_asynci_mutex.c 
            
# if ASYNCVOL_DEBUG
# C_SRCS += h5vl_async_debug.c
# endif

include_HEADERS = h5vl_async_public.h

$(M4_SRCS:.m4=.c): Makefile

.m4.c:
	$(M4) $(AM_M4FLAGS) $(M4FLAGS) $< >$@

libh5vl_async_la_SOURCES = $(C_SRCS) $(CXX_SRCS) $(H_SRCS)
nodist_libh5vl_async_la_SOURCES = $(M4_SRCS:.m4=.c) $(M4H_SRCS:.m4h=.h)

dist-hook:
	$(SED_I) -e "s|RELEASE_DATE|@ASYNCVOL_RELEASE_DATE@|g" $(distdir)/main.c
	$(SED_I) -e "1,10s|_ASYNCVOL_RELEASE_DATE_|@ASYNCVOL_RELEASE_DATE@|" $(distdir)/RELEASE_NOTES.md
	$(SED_I) -e "1,10s|_ASYNCVOL_VERSION_|$(ASYNCVOL_VERSION)|" $(distdir)/RELEASE_NOTES.md

.PHONY:

# Allow running make tests without make
tests-local: all