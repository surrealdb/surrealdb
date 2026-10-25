/**
[env]
namespace = true
database = true
auth = { level = "viewer" }

[test]
reason = "A viewer is refused a PostgreSQL statement that defines several things, as it is refused a single definition"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"
*/

CREATE TABLE ddl_perm_person (id INT PRIMARY KEY, name TEXT, age INT);

CREATE TABLE ddl_perm_one (x INT);

ALTER TABLE ddl_perm_one ADD COLUMN y INT, ADD COLUMN z INT;

DROP TABLE ddl_perm_one, ddl_perm_two;
