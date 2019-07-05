# Data security

**IOTA transactions provide a trustworthy record of data and value so securing this data is important**

Access is authorized.  Users are authenticated.  Data inside the database are secured.  Data in transit to and from the database are secured.  Security audits provide a way to check that the security setup is working.

#### ScyllaDB database security guide

[Scylla documentation has instructions](https://docs.scylladb.com/operating-scylla/security/) for setting up data security including authorization, authentication, encryption, in-transit encryption, and security audits.

#### User access

A Chronicle node may have many users.  Users have roles.  Roles have permissions.  For example, you may be authorized to query a specific table.  Another user may be authorized to store data in this table.  The administrator may be authorized to create a new table.

In order to use data, a user must be authorized.  This is called "authorization".  Then, a user must prove who they are.  This is called "authentication".

#### [AUTHORIZATION](https://docs.scylladb.com/operating-scylla/security/authorization/)

Authorization is the process where users are granted permissions which entitle them to access or to change data on specific keyspaces, tables or an entire datacenter. Authorization for Scylla is done internally within Scylla.

#### [AUTHENTICATION](https://docs.scylladb.com/operating-scylla/security/authentication/)

Authentication is the process where login accounts and their passwords are verified and the user is allowed access.  Authentication is done internally within Scylla. 
