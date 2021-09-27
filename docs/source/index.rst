.. HDF5 Asynchronous I/O VOL Connector documentation master file, created by
   sphinx-quickstart on Mon Jul  5 23:54:03 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

HDF5 Asynchronous I/O VOL Connector
===============================================================

Moving toward exascale computing, the size of data stored and accessed by applications is ever increasing. However, traditional disk-based storage has not seen improvements that keep up with the explosion of data volume or the speed of processors. Multiple levels of non-volatile storage devices are being added to handle bursty I/O, however, moving data across the storage hierarchy can take longer than the data generation or analysis. Asynchronous I/O can reduce the impact of I/O latency as it allows applications to schedule I/O early and to check their status later. I/O is thus overlapped with application communication or computation or both, effectively hiding some or all of the I/O latency. POSIX-IO and MPI-IO provide asynchronous read and write operations, but lack the support for non-data operations such as file open and close. Users also have to manually manage data dependencies and use low-level byte offsets, which requires significant effort and expertise to adopt. 


We have recently developed an asynchronous I/O framework that supports all types of I/O operations, manages data dependencies transparently and automatically, provides implicit and explicit modes to perform asynchronous I/O, and gives an interface for error information retrieval. Our implementation of asynchronous I/O uses background threads, as the asynchronous interface offered by existing operating systems and low-level I/O frameworks (POSIX AIO and MPI-IO) does not support all file operations. We have implemented this approach for the  HDF5, a popular parallel I/O library. HDF5's Virtual Object Layer (VOL) allows interception of all operations on a file and VOL connectors can perform those operations using new infrastructure, such as background threads. 


The HDF5 Asynchronous I/O VOL connector maintains a queue of asynchronous tasks and tracks their dependencies as a directed acyclic graph, where a task can only be executed when all its parent tasks have been completed successfully. Collective operations are executed in the same order as in the application, in an ordered but asynchronous manner. To reduce overhead and avoid contention for shared resources between an application's main thread and the background thread that performs the asynchronous I/O operations, we use a status detection mechanism to check when the main thread is performing non-I/O tasks. We also provide an EventSet interface in HDF5 to monitor asynchronous operation status and to check errors for a set of operations instead of individual ones.


To cite the HDF5 Asynchronous I/O VOL connector, there are two publications. 

- Houjun Tang, Quincey Koziol, Suren Byna, and John Ravi, "`Transparent Asynchronous Parallel I/O using Background Threads <https://ieeexplore.ieee.org/document/9459479>`_ ", in IEEE Transactions on Parallel & Distributed Systems, vol. , no. 01, pp. 1-1, 5555. doi: 10.1109/TPDS.2021.3090322
- Houjun Tang, Quincey Koziol, Suren Byna, John Mainzer, and Tonglin Li, "`Enabling Transparent Asynchronous I/O using Background Threads <https://ieeexplore.ieee.org/abstract/document/8955215>`_ ", 2019 IEEE/ACM Fourth International Parallel Data Systems Workshop (PDSW), 2019, pp. 11-19, doi: 10.1109/PDSW49588.2019.00006.



.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   gettingstarted

.. toctree::
   :maxdepth: 2
   :caption: Async I/O API

   hdf5api
   asyncapi

.. toctree::
   :maxdepth: 1
   :caption: Hello Async I/O

   example

.. toctree::
   :maxdepth: 1
   :caption: Best Practices

   bestpractice
   debug

.. toctree::
   :maxdepth: 1
   :caption: Known Issues

   issue

.. toctree::
   :maxdepth: 2
   :caption: Legal

   copyright
   license




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
