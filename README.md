# Chronicle

**Chronicle is a scalable, distributed, fault-tolerant permanode.**

An IOTA node keeps a record of valid transactions.  There are two types of nodes:  [IRI node](https://docs.iota.org/docs/iri/0.1/introduction/overview) and Chronicle permanode.  The difference is that an IRI node stores current transactions and keeps a snapshot of important data from older transactions.  A Chronicle permanode keeps all the transactions.   This means Chronicle must store lots of data in a trustworthy and reliable way.

A single computer could not hold such big data.  Even if it could, it would create a bottleneck when using it.  Data must be  distributed.  Creating data clusters is one way to distribute data.  It is scalable because you can add more clusters.  It is fault-tolerant because there is no single point of failure.  

Chronicle apps use a distributed database that scales well and has contingency plans to guard against failures.

