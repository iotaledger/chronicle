# Running Chronicle

#### Location
Chronicle and ScyllaDB should be located in the same datacenter but run on different nodes.  There are future plans to support running across multiple datacenters.

#### Data transmission
A Jumbo MTU with a maximum transmission unit of 9000 bytes is recommended between ScyllaDB and Chronicle.  

#### Default setup
The default node setup for Chronicle is a single node setup.  A multiple node setup is allowed.  

#### Power outage
Failure occurs with a full power outage.  Computers running Chronicle and ScyllaDB should have a backup power supply and internet connections.  A power outage for a number of nodes will not affect data consistency if you have at least one active node writing the same queries.  Node swarm is still being developed.  
