/**
[env]
database = false
namespace = false
clean = true
timeout = 10000

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''$result = /DEFINE USER test ON DATABASE PASSHASH '\$argon2id\$.*' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/'''
error = false

[[test.results]]
match = '''$result = /DEFINE USER test ON DATABASE PASSHASH '\$argon2id\$.*' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/'''
error = false

[[test.results]]
match = '''$result = /DEFINE USER test ON DATABASE PASSHASH '\$argon2id\$.*' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/'''
error = false

[[test.results]]
error = "The user 'test' does not exist in the namespace 'ns'"
*/


USE NS ns;
USE DB db;
DEFINE USER test ON DB PASSWORD 'test';

INFO FOR USER test;
INFO FOR USER test ON DB;
INFO FOR USER test ON DATABASE;
INFO FOR USER test ON NS;
