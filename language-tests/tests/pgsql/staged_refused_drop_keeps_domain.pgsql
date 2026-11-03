/**
[env]
namespace = true
database = true
imports = ["pgsql/_domain_column.surql"]

[test]
reason = "A DROP DOMAIN of a domain that a column of an existing table names is refused, and the statements after it in the same request read the domain as it stands"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'pos': the field 'x' on the table 'held' has a type that names it"

[[test.results]]
value = "[{ typname: 'pos' }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `y` of `u:1`: Expected `pos (constraint pos_check failed)` but found `-5i32`"

*/
DROP DOMAIN pos;
SELECT typname FROM pg_type WHERE typname = 'pos';
CREATE TABLE u (id int PRIMARY KEY, y pos);
INSERT INTO u (id, y) VALUES (1, -5);
