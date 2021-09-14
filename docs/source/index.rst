.. HDF5 Asynchronous I/O VOL Connector documentation master file, created by
   sphinx-quickstart on Mon Jul  5 23:54:03 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

HDF5 Asynchronous I/O VOL Connector
===============================================================

With scientific applications moving toward exascale levels, an increasing amount of data is being produced and ana- lyzed. Providing efficient data access is crucial to the productivity of the scientific discovery process. Compared to improvements in CPU and network speeds, I/O performance lags far behind, such that moving data across the storage hierarchy can take longer than data generation or analysis. To alleviate this I/O bottleneck, asynchronous read and write operations have been provided by the POSIX and MPI-I/O interfaces and can overlap I/O operations with computation, and thus hide I/O latency. However, these standards lack support for non-data operations such as file open, stat, and close, and their read and write operations require users to both manually manage data dependencies and use low- level byte offsets. This requires significant effort and expertise for applications to utilize.

To overcome these issues, we present an asynchronous I/O framework that provides support for all I/O operations and manages data dependencies transparently and automatically. Our prototype asynchronous I/O implementation as an HDF5 VOL connector demonstrates the effectiveness of hiding the I/O cost from the application with low overhead and easy-to-use programming interface.


.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   gettingstarted

.. toctree::
   :maxdepth: 2
   :caption: API

   hdf5api
   asyncapi

.. toctree::
   :maxdepth: 1
   :caption: Hello Async

   example

.. toctree::
   :maxdepth: 1
   :caption: Best Practice

   bestpractice
   debug

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
