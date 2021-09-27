Best Practices
==============

Overlap time to hide I/O latency
--------------------------------
To take full advantage of the async I/O VOL connector, applications should have enough non-I/O time for the asynchronous operations to overlap with. This can often be achieved to separate the I/O phases of an application from the compute/communication phase. The compute/communication phases have to be long enough to hide the latency in an I/O phase. When there is not enough compute/communication time to overlap, async I/O may only hide partial I/O latency. 


