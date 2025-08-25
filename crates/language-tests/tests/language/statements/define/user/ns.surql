/**
[env]
namespace = false
clean = true
timeout = 10000

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The user 'test' does not exist in the database 'test'"

[[test.results]]
match = """
$result = /DEFINE USER test ON NAMESPACE PASSHASH '\\$argon2id\\$.*' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/
"""
error = false


[[test.results]]
match = """
$result = /DEFINE USER test ON NAMESPACE PASSHASH '\\$argon2id\\$.*' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/
"""
error = false

[[test.results]]
error = "The root user 'test' does not exist"

*/
USE NS ns;
DEFINE USER test ON NS PASSWORD 'test';

INFO FOR USER test;
INFO FOR USER test ON NS;
INFO FOR USER test ON NAMESPACE;
INFO FOR USER test ON ROOT;
