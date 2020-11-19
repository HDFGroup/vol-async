#
# Copyright (C) 2020, Northwestern University and Argonne National Laboratory
# See COPYRIGHT notice in top-level directory.
#
# $Id$
#
# @configure_input@

SUBDIRS = basic
DIST_SUBDIRS = $(SUBDIRS) 


SUFFIXES = .o .c

AM_DEFAULT_SOURCE_EXT = .c

AM_CPPFLAGS = -I$(top_srcdir)/src \
			  -I$(top_srcdir)/test 

LDADD = $(top_builddir)/src/.libs/libh5vl_async.la ${LIBS}

check_PROGRAMS =	async_test_serial \
					async_test_serial2 \
					async_test_serial_event_set \
					async_test_parallel \
					async_test_parallel2 \
					async_test_parallel3 \
					async_test_parallel4 \
					async_test_parallel5 \
					async_test_multifile \
					async_test_multifile_env

EXTRA_DIST = # wrap_runs.sh parallel_run.sh

TESTPROGRAMS = $(check_PROGRAMS)

# autimake 1.11.3 has not yet implemented AM_TESTS_ENVIRONMENT
# For newer versions, we can use AM_TESTS_ENVIRONMENT instead
# AM_TESTS_ENVIRONMENT  = export TESTPROGRAMS="$(TESTPROGRAMS)";
# AM_TESTS_ENVIRONMENT += export TESTSEQRUN="$(TESTSEQRUN)";
# AM_TESTS_ENVIRONMENT += export TESTOUTDIR="$(TESTOUTDIR)";
TESTS_ENVIRONMENT  = export SED="$(SED)";
TESTS_ENVIRONMENT += export srcdir="$(srcdir)";
TESTS_ENVIRONMENT += export TESTOUTDIR="$(TESTOUTDIR)";
TESTS_ENVIRONMENT += export TESTSEQRUN="$(TESTSEQRUN)";
TESTS_ENVIRONMENT += export TESTMPIRUN="$(TESTMPIRUN)";
TESTS_ENVIRONMENT += export TESTPROGRAMS="$(TESTPROGRAMS)";
TESTS_ENVIRONMENT += export check_PROGRAMS="$(check_PROGRAMS)";

H5_FILES = $(check_PROGRAMS:%=$(TESTOUTDIR)/%.h5)

CLEANFILES = $(H5_FILES) core core.* *.gcda *.gcno *.gcov gmon.out

TEST_EXTENSIONS = .sh
LOG_COMPILER = $(srcdir)/log_comp.sh
SH_LOG_COMPILER =

TESTS = $(TESTPROGRAMS)

# build check targets but not invoke
tests-local: all $(check_PROGRAMS)



