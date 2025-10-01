/**
[env]
timeout = 400000

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The root user 'example' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The root user 'example' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE USER example ON ROOT PASSWORD "example" ROLES OWNER DURATION FOR TOKEN 15m, FOR SESSION 6h;
DEFINE USER IF NOT EXISTS example ON ROOT PASSWORD "example" ROLES OWNER DURATION FOR TOKEN 15m, FOR SESSION 6h;
DEFINE USER OVERWRITE example ON ROOT PASSWORD "example" ROLES OWNER DURATION FOR TOKEN 15m, FOR SESSION 6h;
DEFINE USER example ON ROOT PASSWORD "example" ROLES OWNER DURATION FOR TOKEN 15m, FOR SESSION 6h;
REMOVE USER IF EXISTS example ON ROOT;
REMOVE USER example ON ROOT;
REMOVE USER IF EXISTS example ON ROOT;
