/**
[test]
run = false

[env]
namespace = "ns1"
database = "db1"
*/

DEFINE TABLE data;
CREATE data:1 SET value = "ns1 secret";

USE NS ns2 DB db1;
DEFINE TABLE data;
CREATE data:1 SET value = "ns2 secret";
