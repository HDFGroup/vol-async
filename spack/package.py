# Copyright 2013-2021 Lawrence Livermore National Security, LLC and other
# Spack Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (Apache-2.0 OR MIT)

from spack import *

class Hdf5volasync(CMakePackage):
    """An asynchronous I/O framework that support all HDF5 I/O operations 
       and manages data dependencies transparently and automatically."""

    homepage = "https://hdf5-vol-async.readthedocs.io"
    git      = "https://github.com/hpc-io/vol-async"

    maintainers = ['houjun', 'sbyna']

    version('develop', branch='develop')

    depends_on('argobots@main')
    depends_on('hdf5@develop-1.13+mpi+threadsafe')

    def cmake_args(self):
        """Populate cmake arguments for HDF5 VOL."""
        spec = self.spec

        args = [
            '-DBUILD_SHARED_LIBS:BOOL=ON',
            '-DBUILD_TESTING:BOOL=ON',
        ]
        return args
