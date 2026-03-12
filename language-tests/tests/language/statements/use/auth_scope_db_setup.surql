/**
[test]
run = false

[env]
namespace = "ns1"
database = "db1"
*/

DEFINE TABLE data;
CREATE data:1 SET value = "db1 secret";

USE DB db2;
DEFINE TABLE data;
CREATE data:1 SET value = "db2 secret";
