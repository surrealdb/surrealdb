/**
[test]
run = false
reason = "Setup for network_order: a table whose fields declare inet and cidr, so a request that holds no DDL lowers against their declared types"
*/

DEFINE TABLE host SCHEMAFULL;
DEFINE FIELD id ON host TYPE int;
DEFINE FIELD addr ON host TYPE option<string> SQL TYPE POSTGRESQL 'inet';
DEFINE FIELD net ON host TYPE option<string> SQL TYPE POSTGRESQL 'cidr';
