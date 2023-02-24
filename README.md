[![linux](https://github.com/hpc-io/vol-async/actions/workflows/linux.yml/badge.svg?branch=develop)](https://github.com/hpc-io/vol-async/actions/workflows/linux.yml)
# HDF5 Asynchronous I/O VOL Connector

Asynchronous I/O is becoming increasingly popular with the large amount of data access required by scientific applications. They can take advantage of an asynchronous interface by scheduling I/O as early as possible and overlap computation or communication with I/O operations, which hides the cost associated with I/O and improves the overall performance. This work is part of the [ECP-ExaIO](https://www.exascaleproject.org/research-project/exaio) project.

[<img src="https://lh3.googleusercontent.com/pw/AM-JKLX033FP6RFe5CqYx7vQY_YF834O4SOfFr53xzUdB-TOGIVnG-jNn0fp-8aHbgqZtogRlgSNHJxQqI8gAG0sZo3HNOhmf3k8GZpFyvz2sCBEl2lekbOh8ne3TJyAjbP0XbVZ79JczoDe3pqSIjbfJa-M=w3090-h613-no?authuser=0">](overview)

## Documentation
[**Async VOL documentation website**](https://hdf5-vol-async.readthedocs.io) has detailed build instructions and examples.



## Citation
To cite Async VOL, please use the following:

```
@ARTICLE{9459479,
  author={Tang, Houjun and Koziol, Quincey and Ravi, John and Byna, Suren},
  journal={IEEE Transactions on Parallel and Distributed Systems}, 
  title={Transparent Asynchronous Parallel I/O Using Background Threads}, 
  year={2022},
  volume={33},
  number={4},
  pages={891-902},
  doi={10.1109/TPDS.2021.3090322}}
  
@INPROCEEDINGS{8955215,
  author={Tang, Houjun and Koziol, Quincey and Byna, Suren and Mainzer, John and Li, Tonglin},
  booktitle={2019 IEEE/ACM Fourth International Parallel Data Systems Workshop (PDSW)}, 
  title={Enabling Transparent Asynchronous I/O using Background Threads}, 
  year={2019},
  volume={},
  number={},
  pages={11-19},
  doi={10.1109/PDSW49588.2019.00006}}

```
