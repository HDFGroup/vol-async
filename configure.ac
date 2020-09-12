# Copyright (C) 2020, Lawrence Berkeley National Laboratory.                
# All rights reserved.                                                      
#                                                                           
# This file is part of Taskworks. The full Taskworks copyright notice,      
# including terms governing use, modification, and redistribution, is       
# contained in the file COPYING at the root of the source code distribution 
# tree.      

AC_PREREQ(2.59)
AC_INIT([asyncvol], [0.1], [])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/h5vl_async_public.h])
AC_CONFIG_HEADER([src/config.h])

# Init automake
AM_INIT_AUTOMAKE
AM_SILENT_RULES

AC_DEFUN([UD_MSG_DEBUG],
				 [  if test "x${async_ac_debug}" = xyes ; then
						 AC_MSG_NOTICE([DEBUG: $1])
					fi
				 ]
)

# Set the prefix to an async_vol directory in the source root.
# AC_PREFIX_DEFAULT([`pwd`/async_vol])

AH_TEMPLATE([HAVE_MPI], [Enable MPI support.])
AH_TEMPLATE([ASYNCVOL_DEBUG], [Debug build.])

# Make tests
AM_EXTRA_RECURSIVE_TARGETS([tests])

## ----------------------------------------------------------------------
## Source any special files that we need.  These files normally aren't
## present but can be used by the maintainers to fine tune things like
## turning on debug or profiling flags for the compiler.  The search order
## is:
##
##    CPU-VENDOR-OS
##    VENDOR-OS
##    CPU-OS
##    CPU-VENDOR
##    OS
##    VENDOR
##    CPU
##
## If the `OS' ends with a version number then remove it. For instance,
## `freebsd3.1' would become `freebsd'
AC_CANONICAL_HOST

case $host_os in
  aix*)
    host_os_novers=aix
    ;;
  freebsd*)
    host_os_novers=freebsd
    ;;
  netbsd*)
    host_os_novers=netbsd
    ;;
  solaris*)
    host_os_novers=solaris
    ;;
  *)
    host_os_novers=$host_os
    ;;
esac

host_config="none"
for f in $host_cpu-$host_vendor-$host_os \
         $host_cpu-$host_vendor-$host_os_novers \
         $host_vendor-$host_os \
         $host_vendor-$host_os_novers \
         $host_cpu-$host_os \
         $host_cpu-$host_os_novers \
         $host_cpu-$host_vendor \
         $host_os \
         $host_os_novers \
         $host_vendor \
         $host_cpu ; do
  AC_MSG_CHECKING([for config $f])
  if test -f "$srcdir/hdf5_config_scripts/$f"; then
    host_config=$srcdir/hdf5_config_scripts/$f
    AC_MSG_RESULT([found])
    break
  fi
  AC_MSG_RESULT([no])
done
if test "X$host_config" != "Xnone"; then
  CC_BASENAME="`echo $CC | cut -f1 -d' ' | xargs basename 2>/dev/null`"
  . $host_config
fi

# ----------------------------------------------------------------------
## Source any special site-specific file
# ----------------------------------------------------------------------
hname="`hostname`"
while test -n "$hname"; do
  file=$srcdir/hdf5_config_scripts/site-specific/host-$hname
  AC_MSG_CHECKING([for config $file])
  if test -f "$file"; then
    . $file
    AC_MSG_RESULT([found])
    break
  fi
  AC_MSG_RESULT([no])
  hname_tmp=$hname
  hname="`echo $hname | cut -d. -f2-99`"
  test "$hname_tmp" = "$hname" && break
done
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# Search for mpi compiler if parallel is enabled
# ----------------------------------------------------------------------
AC_ARG_ENABLE([parallel], [AS_HELP_STRING([--enable-parallel], [Search for MPI-IO and MPI support files])],[enable_parallel=${enableval}], [enable_parallel=no])
if test "x$enable_parallel" = "xyes"; then
	AC_ARG_VAR(MPICC,  [MPI C compiler, @<:@default: CC@:>@])
	AC_ARG_VAR(MPICXX, [MPI C++ compiler, @<:@default: CXX@:>@])

	ac_user_MPICC=$MPICC
	ac_user_MPICXX=$MPICXX
	if test "x$MPICC"  = x && test "x$CC"  != x ; then ac_user_MPICC=$CC   ; fi
	if test "x$MPICXX" = x && test "x$CXX" != x ; then ac_user_MPICXX=$CXX ; fi

	CANDIDATE_MPICC="${MPICC} mpicc mpicc_r"
	CANDIDATE_MPICXX="${MPICXX} mpicxx mpic++ mpiCC mpcxx mpc++ mpicxx_r mpiCC_r mpcxx_r mpic++_r mpc++_r"
	dnl add GNU MPI compilers
	CANDIDATE_MPICC="$CANDIDATE_MPICC mpigcc mpgcc mpigcc_r mpgcc_r"
	CANDIDATE_MPICXX="$CANDIDATE_MPICXX mpig++ mpg++ mpig++_r mpg++_r"
	dnl add IBM MPI compilers
	CANDIDATE_MPICC="$CANDIDATE_MPICC mpcc_r mpcc mpixlc_r mpixlc"
	CANDIDATE_MPICXX="$CANDIDATE_MPICXX mpCC_r mpCC mpixlcxx_r mpixlcxx mpixlC_r mpixlC"
	dnl add IBM BGL MPI compilers
	CANDIDATE_MPICC="$CANDIDATE_MPICC blrts_xlc mpxlc_r mpxlc"
	CANDIDATE_MPICXX="$CANDIDATE_MPICXX blrts_xlC mpxlC_r mpxlC mpixlc++ mpxlcxx mpxlc++ mpxlCC mpixlc++_r mpxlcxx_r mpxlc++_r mpxlCC_r"
	dnl add Fujitsu MPI compilers
	CANDIDATE_MPICC="$CANDIDATE_MPICC mpifccpx"
	CANDIDATE_MPICXX="$CANDIDATE_MPICXX mpiFCCpx"
	dnl add Cray MPI compiler wrappers
	CANDIDATE_MPICC="$CANDIDATE_MPICC cc"
	CANDIDATE_MPICXX="$CANDIDATE_MPICXX CC"
	dnl add Intel MPI compiler wrappers
	CANDIDATE_MPICC="$CANDIDATE_MPICC mpiicc icc"
	CANDIDATE_MPICXX="$CANDIDATE_MPICXX mpiicpc mpiicxx mpiic++ mpiiCC icpc"
	dnl add PGI MPI compiler wrappers
	CANDIDATE_MPICC="$CANDIDATE_MPICC mpipgcc mppgcc"
	CANDIDATE_MPICXX="$CANDIDATE_MPICXX mpipgCC mppgCC"

	dnl find the full path of MPICC from CANDIDATE_MPICC and MPI_INSTALL
	if test "x${ac_user_MPICC}" = x ; then
		dnl if MPICC or CC has not been set by users, then search from
		dnl CANDIDATE_MPICC, and find the full path of MPICC
		UD_MPI_PATH_PROGS([MPICC], [$CANDIDATE_MPICC])
	else
		dnl check whether user specified MPICC is valid
		UD_MPI_PATH_PROG([MPICC], [$ac_user_MPICC])
	fi

	if test "x${MPICC}" = x ; then
		if test "x$ac_user_MPICC" = x ; then
			ERR_MSG="No MPI C compiler can be found"
		else
			ERR_MSG="Specified MPI C compiler \"$ac_user_MPICC\" cannot be found"
		fi
		if test "x$MPI_INSTALL" != x ; then
			ERR_MSG="$ERR_MSG under $MPI_INSTALL"
		fi
		AC_MSG_ERROR([
		-----------------------------------------------------------------------
			$ERR_MSG
			--enable-parallel is set, but no working MPI C compiler.
			Abort.
		-----------------------------------------------------------------------])
	fi
	CC=${MPICC}
fi
# ----------------------------------------------------------------------

# Checks for programs.
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_MAKE_SET

# Default to building static library, because opa by default builds itself
# statically without -fPIC, preventing it from working with a shared library.
AC_DISABLE_SHARED

# Initialize libtool.
LT_INIT

# ----------------------------------------------------------------------
# Check if we can compile MPI program
# ----------------------------------------------------------------------
HAVE_MPI=no
MPI_DEF="#undef"
if test "x$enable_parallel" = "xyes"; then
	echo "enable_parallel=$enable_parallel"
	AC_MSG_CHECKING([whether a simple MPI-IO C program can be linked])
	AC_LINK_IFELSE([AC_LANG_PROGRAM([[#include <mpi.h>]],
									 [[ MPI_Init(0, (void *)0);
						MPI_File_open(0, (void *)0, 0, 0, (void *)0);]])],
				[AC_MSG_RESULT([yes])],
						[AC_MSG_RESULT([no])
						AC_MSG_ERROR([unable to link a simple MPI-IO C program])])


	# Require MPI 3.x
	AC_LINK_IFELSE(
			[AC_LANG_PROGRAM(
					[[
							#include <mpi.h>
					]],
					[[
							MPI_Message message;
							MPI_Init(0, (void *) 0);
							MPI_Mprobe(0, 0, 0, &message, (void *) 0);
							MPI_Imrecv((void *) 0, 0, 0, (void *) 0, (void *) 0);
					]]
			)],
			[AC_MSG_RESULT([yes])
			 PARALLEL_FILTERED_WRITES=yes],
			[AC_MSG_RESULT([no])
			 AC_MSG_WARN([A simple MPI program using the MPI_Mprobe and MPI_Imrecv functions could not be compiled and linked.
										Parallel writes of filtered data will be disabled.])])

	# We are building a parallel library
	HAVE_MPI=yes
	AC_DEFINE(HAVE_MPI, 1)
	MPI_DEF="#define"
fi
AM_CONDITIONAL(HAVE_MPI, [test x$enable_parallel = xyes])
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
# Check for taskworks
# ----------------------------------------------------------------------
AC_ARG_WITH([tw],
						[AC_HELP_STRING([--with-tw=DIR],
														[Use taskwork library [default=yes]])],,
						[withval=yes])

tw_ext_lib=""

case "$withval" in
	yes)
		HAVE_TW="yes"
		AC_CHECK_HEADERS([taskworks.h],, [unset HAVE_TW])
		AC_CHECK_LIB([tw], [TW_Init],, [unset HAVE_TW])

		if test -z "$HAVE_TW"; then
			AC_MSG_ERROR([could not find taskworks library])
		fi
		;;
	no)
		AC_MSG_ERROR([taskwork library required to build async VOL])
		;;
	*)
		HAVE_TW="yes"
		case "$withval" in
			*,*)
				tw_inc="`echo $withval | cut -f1 -d,`"
				tw_lib="`echo $withval | cut -f2 -d, -s`"
				tw_ext_lib="`grep 'LDFLAGS':' $tw_inc/taskworks.settings  | sed 's/^.*: //'`"
				tw_ext_link="`grep 'Extra libraries:' $tw_inc/taskworks.settings  | sed 's/^.*: //'`"
				;;
			*)
				if test -n "$withval"; then
					tw_inc="$withval/include"
					tw_lib="$withval/lib"
					tw_ext_lib="`grep 'LDFLAGS:' $withval/include/taskworks.settings  | sed 's/^.*: //'`"
					tw_ext_link="`grep 'Extra libraries:' $withval/include/taskworks.settings  | sed 's/^.*: //'`"
				fi
				;;
		esac

		## Trying to include -I/usr/include and -L/usr/lib is redundant and
		## can mess some compilers up.
		if test "X$tw_inc" = "X/usr/include"; then
			tw_inc=""
		fi
		if test "X$tw_lib" = "X/usr/lib"; then
			tw_lib=""
		fi

		if test -n "$tw_inc"; then
			CPPFLAGS="$CPPFLAGS -I$tw_inc"
		fi

		AC_CHECK_HEADERS([taskworks.h],, [unset HAVE_TW])

		if test -n "$tw_lib"; then
			LDFLAGS="$LDFLAGS -L$tw_lib"
		fi
		
		if test -n "$tw_ext_lib"; then
			LDFLAGS="$LDFLAGS $tw_ext_lib"
		fi

		LIBS="$LIBS -ltw"
		if test -n "$tw_ext_link"; then
			LIBS="$LIBS $tw_ext_link"
		fi

		AC_CHECK_LIB([tw], [TW_Init], [${tw_ext_lib}], [unset HAVE_TW])

		if test -z "$HAVE_TW"; then
			AC_MSG_ERROR([could not find taskworks library])
		fi
		;;
esac
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
# Check for HDF5
# ----------------------------------------------------------------------
AC_ARG_WITH([hdf5],
						[AC_HELP_STRING([--with-hdf5=DIR],
														[Use HDF5 library [default=yes]])],,
						[withval=yes])

case "$withval" in
	yes)
		HAVE_HDF5="yes"
		AC_CHECK_HEADERS([hdf5.h],, [unset HAVE_HDF5])
		AC_CHECK_LIB([hdf5], [H5Fcreate],, [unset HAVE_HDF5])

		if test -z "$HAVE_HDF5"; then
			AC_MSG_ERROR([could not find HDF5 library])
		fi
		;;
	no)
		AC_MSG_ERROR([HDF5 library required to build async VOL])
		;;
	*)
		HAVE_HDF5="yes"
		case "$withval" in
			*,*)
				hdf5_inc="`echo $withval | cut -f1 -d,`"
				hdf5_lib="`echo $withval | cut -f2 -d, -s`"
				;;
			*)
				if test -n "$withval"; then
					hdf5_inc="$withval/include"
					hdf5_lib="$withval/lib"
				fi
				;;
		esac

		## Trying to include -I/usr/include and -L/usr/lib is redundant and
		## can mess some compilers up.
		if test "X$hdf5_inc" = "X/usr/include"; then
			hdf5_inc=""
		fi
		if test "X$hdf5_lib" = "X/usr/lib"; then
			hdf5_lib=""
		fi

		if test -n "$hdf5_inc"; then
			CPPFLAGS="$CPPFLAGS -I$hdf5_inc"
		fi

		AC_CHECK_HEADERS([hdf5.h],, [unset HAVE_HDF5])

		if test -n "$hdf5_lib"; then
			LDFLAGS="$LDFLAGS -L$hdf5_lib"
		fi

		AC_CHECK_LIB([hdf5], [H5Fcreate],, [unset HAVE_HDF5])

		if test -z "$HAVE_HDF5"; then
			AC_MSG_ERROR([could not find HDF5 library])
		fi
		;;
esac

AC_CHECK_LIB([hdf5], [H5VLfile_create],, [unset HAVE_HDF5])

if test -z "$HAVE_HDF5"; then
    AC_MSG_ERROR([Provided version of HDF5 library does not support VOL])
fi
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
# Whether to enable debug build
# ----------------------------------------------------------------------
AC_ARG_ENABLE([debug], [AS_HELP_STRING([--enable-debug], [Build with debug information. Turn off all optimizations])],[enable_debug=${enableval}], [enable_debug=no])
if test "x$enable_debug" = "xyes"; then
   CFLAGS=`echo $CFLAGS | ${SED} 's/-O. *//g' | ${SED} 's/-fast *//g'`
   CFLAGS+=" -O0 -g"
   AC_DEFINE(ASYNCVOL_DEBUG, 1)
fi
AM_CONDITIONAL([ASYNCVOL_DEBUG], [test "x$enable_debug" = xyes])
# ----------------------------------------------------------------------

AC_SUBST([prefix])

AC_SUBST([CC])
AC_SUBST([CXX])

AC_SUBST([LDFLAGS])
AC_SUBST([AM_LDFLAGS])
AC_SUBST([LIBS])

AC_SUBST([CPPFLAGS])
AC_SUBST([AM_CPPFLAGS])
AC_SUBST([CFLAGS])
AC_SUBST([CPPFLAGS])
AC_SUBST([AM_CFLAGS])
AC_SUBST([enable_shared])
AC_SUBST([enable_static])

AC_SUBST([HAVE_MPI])
AC_SUBST([HAVE_ABT])
AC_SUBST([HAVE_LIBEVENT])

AC_SUBST([MPI_DEF])
AC_SUBST([ABT_DEF])
AC_SUBST([LIBEVENT_DEF])

AC_CONFIG_FILES([
		Makefile 
		src/Makefile 
		src/h5vl_async.settings
		test/Makefile
		test/basic/Makefile ])

AC_OUTPUT

cat src/h5vl_async.settings
