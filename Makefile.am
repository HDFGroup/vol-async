# Copyright (C) 2020, Lawrence Berkeley National Laboratory.                
# All rights reserved.                                                      
#                                                                           
# This file is part of AsyncVOL. The full AsyncVOL copyright notice,    
# including terms governing use, modification, and redistribution, is       
# contained in the file COPYING at the root of the source code distribution 
# tree.     

# This is not a standard GNU package.
AUTOMAKE_OPTIONS = foreign

ACLOCAL_AMFLAGS = -I m4

SUBDIRS = src test

# Files which need to go in the distribution tarball.
DIST_SUBDIRS = ${SUBDIRS}
EXTRA_DIST = COPYING README.md

# Build test programs without check
tests-local:
	@echo '|'
	@echo '|  All test programs have been successfully built.'
	@echo '|'