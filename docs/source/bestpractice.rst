Best Practice
=============

Enough Overlap Time
-------------------
To take full advantage of the async VOL connector, applications should have enough non-I/O time for the asynchronous operations to overlap with. This can often be achieved to separate the I/O phase of the application from the compute/communication phase.


