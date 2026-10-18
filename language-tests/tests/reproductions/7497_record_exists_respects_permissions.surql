/**
[env]
imports = ["reproductions/7497_record_exists_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:u" }
timeout = 5000

[test]
reason = "record::exists answers under the caller's table permission and derives no computed field"
issue = 7497

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

RETURN record::exists(shown:1);
RETURN record::exists(shown:2);
RETURN record::exists(hidden:1);
RETURN record::exists(mine:1);
RETURN record::exists(mine:2);
RETURN mine:1.exists();
RETURN mine:2.exists();
