/**
[env]
namespace = true
database = true

[test]
reason = "CREATE EXTENSION of anything but vector is refused as PostgreSQL refuses an extension it does not have"

[test.results]
request-error = "PostgreSQL DDL lowering error: extension \"postgis\" is not available"
*/

CREATE EXTENSION postgis;
