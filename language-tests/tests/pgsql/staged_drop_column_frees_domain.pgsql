/**
[env]
namespace = true
database = true
imports = ["pgsql/_domain_column.surql"]

[test]
reason = "A column dropped earlier in a request no longer names its domain, so a DROP DOMAIN after it removes the domain, and the statements after that read the domain as removed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
ALTER TABLE held DROP COLUMN x;
DROP DOMAIN pos;
SELECT typname FROM pg_type WHERE typname = 'pos';
